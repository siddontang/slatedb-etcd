//! Count gRPC messages and terminal status at the client listener.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use anyhow::Result;
use axum::body::{Body, Bytes};
use axum::extract::{Request, State};
use axum::http::HeaderMap;
use axum::middleware::Next;
use axum::response::Response;
use http_body::{Body as HttpBody, Frame, SizeHint};
use prometheus::{IntCounter, IntCounterVec, Opts, Registry};
use prost::Message;

const CODES: [&str; 17] = [
    "OK",
    "Canceled",
    "Unknown",
    "InvalidArgument",
    "DeadlineExceeded",
    "NotFound",
    "AlreadyExists",
    "PermissionDenied",
    "ResourceExhausted",
    "FailedPrecondition",
    "Aborted",
    "OutOfRange",
    "Unimplemented",
    "Internal",
    "Unavailable",
    "DataLoss",
    "Unauthenticated",
];

pub(super) struct Observer {
    methods: HashMap<String, Arc<Method>>,
    requests: IntCounterVec,
}

struct Traffic {
    messages: IntCounter,
    bytes: IntCounter,
}

struct Method {
    started: IntCounter,
    received: Traffic,
    sent: Traffic,
    handled: Vec<IntCounter>,
    duration: Option<prometheus::Histogram>,
    shape: &'static str,
    current_client: IntCounter,
    unknown_client: IntCounter,
}

impl Observer {
    pub fn new(registry: &Registry, level: crate::server::MetricsLevel) -> Result<Self> {
        let vector = |name, help, labels: &[&str]| -> Result<IntCounterVec> {
            let value = IntCounterVec::new(Opts::new(name, help), labels)?;
            registry.register(Box::new(value.clone()))?;
            Ok(value)
        };
        let labels = ["grpc_type", "grpc_service", "grpc_method"];
        let started = vector("grpc_server_started_total", "Started RPCs.", &labels)?;
        let received = vector(
            "grpc_server_msg_received_total",
            "Received RPC messages.",
            &labels,
        )?;
        let sent = vector("grpc_server_msg_sent_total", "Sent RPC messages.", &labels)?;
        let handled = vector(
            "grpc_server_handled_total",
            "Finished RPCs by status.",
            &["grpc_type", "grpc_service", "grpc_method", "grpc_code"],
        )?;
        let requests = vector(
            "etcd_server_client_requests_total",
            "Requests by client API version.",
            &["type", "client_api_version"],
        )?;
        let duration = match level {
            crate::server::MetricsLevel::Basic => None,
            crate::server::MetricsLevel::Extensive => Some(super::state::register(
                registry,
                prometheus::HistogramVec::new(
                    prometheus::HistogramOpts::new(
                        "grpc_server_handling_seconds",
                        "Duration of complete gRPC requests in seconds.",
                    ),
                    &labels,
                )?,
            )?),
        };
        let received_bytes = super::state::register(
            registry,
            IntCounter::new(
                "etcd_network_client_grpc_received_bytes_total",
                "Received protobuf payload bytes.",
            )?,
        )?;
        let sent_bytes = super::state::register(
            registry,
            IntCounter::new(
                "etcd_network_client_grpc_sent_bytes_total",
                "Sent protobuf payload bytes.",
            )?,
        )?;
        let mut methods = HashMap::new();
        let mut add = |service: &str, method: &str, shape| {
            let labels = [shape, service, method];
            methods.insert(
                format!("/{service}/{method}"),
                Arc::new(Method {
                    current_client: requests.with_label_values(&[
                        if shape == "unary" { "unary" } else { "stream" },
                        "3.6",
                    ]),
                    unknown_client: requests.with_label_values(&[
                        if shape == "unary" { "unary" } else { "stream" },
                        "unknown",
                    ]),
                    started: started.with_label_values(&labels),
                    duration: duration.as_ref().map(|vec| vec.with_label_values(&labels)),
                    received: Traffic {
                        messages: received.with_label_values(&labels),
                        bytes: received_bytes.clone(),
                    },
                    sent: Traffic {
                        messages: sent.with_label_values(&labels),
                        bytes: sent_bytes.clone(),
                    },
                    handled: CODES
                        .iter()
                        .map(|code| handled.with_label_values(&[shape, service, method, code]))
                        .collect(),
                    shape,
                }),
            );
        };
        let descriptors = prost_types::FileDescriptorSet::decode(
            include_bytes!(concat!(env!("OUT_DIR"), "/services.pb")).as_slice(),
        )?;
        for file in descriptors.file {
            if file.package() == "etcdserverpb" {
                for service in file.service {
                    let name = format!("etcdserverpb.{}", service.name());
                    for method in service.method {
                        let shape = match (method.client_streaming(), method.server_streaming()) {
                            (false, false) => "unary",
                            (false, true) => "server_stream",
                            (true, false) => "client_stream",
                            (true, true) => "bidi_stream",
                        };
                        add(&name, method.name(), shape);
                    }
                }
            }
        }
        add("grpc.health.v1.Health", "Check", "unary");
        add("grpc.health.v1.Health", "Watch", "server_stream");
        Ok(Self { methods, requests })
    }
}

fn status(headers: &HeaderMap) -> Option<usize> {
    headers.get("grpc-status").map(|value| {
        value
            .to_str()
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .filter(|&code| code < CODES.len())
            .unwrap_or(2)
    })
}

pub(super) async fn observe(
    State(observer): State<Arc<Observer>>,
    req: Request,
    next: Next,
) -> Response {
    if req.method() != axum::http::Method::POST
        || !req
            .headers()
            .get(axum::http::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| {
                v.split(';')
                    .next()
                    .is_some_and(|t| t == "application/grpc" || t.starts_with("application/grpc+"))
            })
    {
        return next.run(req).await;
    }
    let Some(method) = observer.methods.get(req.uri().path()).cloned() else {
        return next.run(req).await;
    };
    method.started.inc();
    let start = method.duration.as_ref().map(|_| std::time::Instant::now());
    let version = req
        .headers()
        .get("client-api-version")
        .and_then(|v| v.to_str().ok());
    // Limit client-controlled label values to canonical etcd v3 minor versions.
    let version = match version {
        None => "unknown",
        Some(v)
            if v.strip_prefix("3.").is_some_and(|n| {
                !n.is_empty() && n.len() <= 2 && n.bytes().all(|c| c.is_ascii_digit())
            }) =>
        {
            v
        }
        Some(_) => "other",
    };
    let kind = if method.shape == "unary" {
        "unary"
    } else {
        "stream"
    };
    match version {
        "3.6" => method.current_client.inc(),
        "unknown" => method.unknown_client.inc(),
        _ => observer.requests.with_label_values(&[kind, version]).inc(),
    }
    let incoming = Observation::Request(method.clone());
    let mut outgoing = Observation::Response {
        method,
        start,
        finished: false,
    };
    let req = req.map(|body| Body::new(ObservedBody::new(body, incoming)));
    let response = next.run(req).await;
    if let Some(code) = status(response.headers()) {
        outgoing.finish(code);
    }
    response.map(|body| Body::new(ObservedBody::new(body, outgoing)))
}

#[derive(Default)]
struct MessageFrames {
    header: [u8; 5],
    filled: usize,
    remaining: usize,
}

impl MessageFrames {
    fn consume(&mut self, mut data: &[u8]) -> (u64, u64) {
        let (mut messages, mut bytes) = (0, 0);
        while !data.is_empty() {
            if self.filled < 5 {
                let n = (5 - self.filled).min(data.len());
                self.header[self.filled..self.filled + n].copy_from_slice(&data[..n]);
                data = &data[n..];
                self.filled += n;
                if self.filled < 5 {
                    break;
                }
                self.remaining = u32::from_be_bytes(self.header[1..].try_into().unwrap()) as usize;
            }
            let n = self.remaining.min(data.len());
            data = &data[n..];
            self.remaining -= n;
            if self.remaining == 0 {
                messages += 1;
                bytes += u64::from(u32::from_be_bytes(self.header[1..].try_into().unwrap()));
                self.filled = 0;
            }
        }
        (messages, bytes)
    }
}

struct ObservedBody {
    inner: Body,
    frames: MessageFrames,
    observation: Observation,
}

impl ObservedBody {
    fn new(inner: Body, observation: Observation) -> Self {
        Self {
            inner,
            frames: MessageFrames::default(),
            observation,
        }
    }
}

enum Observation {
    Request(Arc<Method>),
    Response {
        method: Arc<Method>,
        start: Option<std::time::Instant>,
        finished: bool,
    },
}

impl Observation {
    fn traffic(&self) -> &Traffic {
        match self {
            Self::Request(method) => &method.received,
            Self::Response { method, .. } => &method.sent,
        }
    }

    fn finish(&mut self, code: usize) {
        if let Self::Response {
            method,
            start,
            finished,
        } = self
        {
            if !std::mem::replace(finished, true) {
                method.handled[code].inc();
                if let (Some(duration), Some(start)) = (&method.duration, start) {
                    duration.observe(start.elapsed().as_secs_f64());
                }
            }
        }
    }
}

impl Drop for Observation {
    fn drop(&mut self) {
        self.finish(1);
    }
}

impl HttpBody for ObservedBody {
    type Data = Bytes;
    type Error = axum::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, axum::Error>>> {
        let result = std::task::ready!(Pin::new(&mut self.inner).poll_frame(cx));
        match &result {
            Some(Ok(frame)) => {
                if let Some(data) = frame.data_ref() {
                    let (n, bytes) = self.frames.consume(data);
                    if n != 0 {
                        let traffic = self.observation.traffic();
                        traffic.messages.inc_by(n);
                        traffic.bytes.inc_by(bytes);
                    }
                } else if let Some(code) = frame.trailers_ref().and_then(status) {
                    self.observation.finish(code);
                }
            }
            Some(Err(_)) | None => self.observation.finish(2),
        }
        Poll::Ready(result)
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn count_complete_messages_across_data_frames() {
        let stream = [
            0, 0, 0, 0, 2, 1, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 3, 4, 5, 0, 0,
        ];
        for split in 0..=stream.len() {
            let mut frames = MessageFrames::default();
            let a = frames.consume(&stream[..split]);
            let b = frames.consume(&stream[split..]);
            assert_eq!((a.0 + b.0, a.1 + b.1), (3, 5));
        }
    }
}
