use std::collections::BTreeMap;
use std::time::Duration;

use prost::Message;
use tokio::sync::{mpsc, oneshot};
use tonic::{Request, Response, Status};

mod auth;
mod cluster;
mod forwarding;
mod health;
mod lease;
mod maintenance;
mod watch;

use crate::mvcc::{self, Outcome};
use crate::node::Event;
use crate::proto::etcdserverpb::kv_client::KvClient;
use crate::proto::etcdserverpb::kv_server::{Kv, KvServer};
use crate::proto::etcdserverpb::request_op::Request as Op;
use crate::proto::etcdserverpb::response_op::Response as Resp;
use crate::proto::etcdserverpb::*;

#[derive(Clone)]
pub struct Service {
    events: mpsc::Sender<Event>,
    forwarding: std::sync::Arc<forwarding::Forwarding>,
    member: u64,
    tokens: crate::auth::TokenIssuer,
}

pub fn service(
    member: u64,
    events: mpsc::Sender<Event>,
    peers: &BTreeMap<u64, String>,
    tokens: crate::auth::TokenIssuer,
    transport: crate::tls::Transport,
) -> anyhow::Result<axum::Router> {
    let service = Service::new(member, events, peers, tokens, transport)?;
    Ok(axum::Router::new()
        .route_service(
            "/etcdserverpb.Cluster/{*method}",
            cluster_server::ClusterServer::new(service.clone()),
        )
        .route_service(
            "/grpc.health.v1.Health/{*method}",
            tonic_health::pb::health_server::HealthServer::new(service.clone()),
        )
        .route_service(
            "/etcdserverpb.Maintenance/{*method}",
            maintenance_server::MaintenanceServer::new(service.clone())
                .max_encoding_message_size(64 * 1024 * 1024),
        )
        .route_service(
            "/etcdserverpb.Auth/{*method}",
            auth_server::AuthServer::new(service.clone()),
        )
        .route_service(
            "/etcdserverpb.KV/{*method}",
            KvServer::new(service.clone())
                .max_decoding_message_size(1536 * 1024)
                .max_encoding_message_size(64 * 1024 * 1024),
        )
        .route_service(
            "/etcdserverpb.Lease/{*method}",
            lease_server::LeaseServer::new(service.clone()),
        )
        .route_service(
            "/etcdserverpb.Watch/{*method}",
            watch_server::WatchServer::new(service).max_encoding_message_size(64 * 1024 * 1024),
        ))
}

impl Service {
    pub(crate) fn new(
        member: u64,
        events: mpsc::Sender<Event>,
        peers: &BTreeMap<u64, String>,
        tokens: crate::auth::TokenIssuer,
        transport: crate::tls::Transport,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            events,
            forwarding: std::sync::Arc::new(forwarding::Forwarding::new(peers, transport)?),
            member,
            tokens,
        })
    }

    // A forwarded result can supply a revision for the next local STM read.
    async fn local_barrier(&self) -> Result<(), Status> {
        let (tx, rx) = oneshot::channel();
        self.events
            .try_send(Event::LinearRead(
                RequestOp {
                    request: Some(Op::RequestRange(RangeRequest {
                        key: vec![0],
                        limit: 1,
                        ..Default::default()
                    })),
                },
                tx,
            ))
            .map_err(|_| Status::unavailable("etcdserver: too many requests"))?;
        let value = tokio::time::timeout(Duration::from_secs(10), rx)
            .await
            .map_err(|_| {
                Status::deadline_exceeded("etcdserver: local apply timed out; outcome unknown")
            })?
            .map_err(|_| Status::unavailable("etcdserver: node unavailable"))?
            .map_err(|e| Status::unavailable(e.error))?;
        let outcome: Outcome =
            serde_json::from_value(value).map_err(|e| Status::internal(e.to_string()))?;
        outcome.result.map_err(|e| e.status())?;
        Ok(())
    }

    async fn execute(
        &self,
        op: Op,
        forwarded: bool,
        credentials: crate::auth::Credentials,
    ) -> Result<ResponseOp, Status> {
        let request = RequestOp {
            request: Some(op.clone()),
        };
        if request.encoded_len() > 1536 * 1024 {
            return Err(Status::resource_exhausted(
                "etcdserver: request is too large",
            ));
        }
        mvcc::validate(&request).map_err(|e| e.status())?;
        let (tx, rx) = oneshot::channel();
        let event = Event::Kv(request, tx);
        self.events
            .try_send(Event::Authenticated(credentials.clone(), Box::new(event)))
            .map_err(|_| Status::unavailable("etcdserver: too many requests"))?;
        let reply = tokio::time::timeout(Duration::from_secs(10), rx)
            .await
            .map_err(|_| {
                Status::deadline_exceeded("etcdserver: request timed out; outcome unknown")
            })?
            .map_err(|_| Status::unavailable("etcdserver: node unavailable"))?;
        match reply {
            Ok(outcome) => outcome.result.map_err(|e| e.status()),
            Err(error) if error.error == "not leader" && !forwarded => {
                let channel = self.peer_channel(error.leader_id).await?;
                let mut client = KvClient::new(channel).max_decoding_message_size(64 * 1024 * 1024);
                let response = match op {
                    Op::RequestRange(r) => Resp::ResponseRange(
                        client
                            .range(forward_auth(r, &credentials))
                            .await?
                            .into_inner(),
                    ),
                    Op::RequestPut(r) => Resp::ResponsePut(
                        client
                            .put(forward_auth(r, &credentials))
                            .await?
                            .into_inner(),
                    ),
                    Op::RequestDeleteRange(r) => Resp::ResponseDeleteRange(
                        client
                            .delete_range(forward_auth(r, &credentials))
                            .await?
                            .into_inner(),
                    ),
                    Op::RequestTxn(r) => Resp::ResponseTxn(
                        client
                            .txn(forward_auth(r, &credentials))
                            .await?
                            .into_inner(),
                    ),
                };
                let mut response = ResponseOp {
                    response: Some(response),
                };
                let revision = mvcc::response_header(&mut response)
                    .ok_or_else(|| Status::internal("forwarded KV response has no header"))?
                    .revision;
                // The leader already confirmed this result. Only local MVCC progress is missing.
                self.wait_revision(revision).await?;
                if let Some(header) = mvcc::response_header(&mut response) {
                    header.member_id = self.member;
                }
                Ok(response)
            }
            Err(error) => Err(Status::unavailable(format!("etcdserver: {}", error.error))),
        }
    }
}

fn forward<T>(body: T) -> Request<T> {
    let mut request = Request::new(body);
    request
        .metadata_mut()
        .insert("x-slatedb-forwarded", "true".parse().unwrap());
    request
}

fn credentials<T>(request: &Request<T>) -> crate::auth::Credentials {
    let user = if request
        .extensions()
        .get::<crate::tls::PeerServer>()
        .is_some()
    {
        request
            .metadata()
            .get("x-slatedb-user")
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned)
    } else {
        request
            .extensions()
            .get::<crate::tls::VerifiedUser>()
            .map(|u| u.0.clone())
    };
    crate::auth::Credentials {
        user,
        token: request
            .metadata()
            .get("token")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_owned(),
        time: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs(),
    }
}

fn forward_auth<T>(body: T, auth: &crate::auth::Credentials) -> Request<T> {
    let mut request = forward(body);
    if let Some(user) = &auth.user {
        if let Ok(user) = user.parse() {
            request.metadata_mut().insert("x-slatedb-user", user);
        }
    }
    if !auth.token.is_empty() {
        if let Ok(token) = auth.token.parse() {
            request.metadata_mut().insert("token", token);
        }
    }
    request
}

fn is_forwarded<T>(request: &Request<T>) -> bool {
    request
        .extensions()
        .get::<crate::tls::ClientServer>()
        .is_none()
        && request.metadata().contains_key("x-slatedb-forwarded")
}

#[tonic::async_trait]
impl Kv for Service {
    async fn range(
        &self,
        request: Request<RangeRequest>,
    ) -> Result<Response<RangeResponse>, Status> {
        let credentials = credentials(&request);
        let forwarded = is_forwarded(&request);
        match self
            .execute(
                Op::RequestRange(request.into_inner()),
                forwarded,
                credentials,
            )
            .await?
            .response
        {
            Some(Resp::ResponseRange(r)) => Ok(Response::new(r)),
            _ => Err(Status::internal("unexpected response")),
        }
    }

    async fn put(&self, request: Request<PutRequest>) -> Result<Response<PutResponse>, Status> {
        let credentials = credentials(&request);
        let forwarded = is_forwarded(&request);
        match self
            .execute(Op::RequestPut(request.into_inner()), forwarded, credentials)
            .await?
            .response
        {
            Some(Resp::ResponsePut(r)) => Ok(Response::new(r)),
            _ => Err(Status::internal("unexpected response")),
        }
    }

    async fn delete_range(
        &self,
        request: Request<DeleteRangeRequest>,
    ) -> Result<Response<DeleteRangeResponse>, Status> {
        let credentials = credentials(&request);
        let forwarded = is_forwarded(&request);
        match self
            .execute(
                Op::RequestDeleteRange(request.into_inner()),
                forwarded,
                credentials,
            )
            .await?
            .response
        {
            Some(Resp::ResponseDeleteRange(r)) => Ok(Response::new(r)),
            _ => Err(Status::internal("unexpected response")),
        }
    }

    async fn txn(&self, request: Request<TxnRequest>) -> Result<Response<TxnResponse>, Status> {
        let credentials = credentials(&request);
        let forwarded = is_forwarded(&request);
        match self
            .execute(Op::RequestTxn(request.into_inner()), forwarded, credentials)
            .await?
            .response
        {
            Some(Resp::ResponseTxn(r)) => Ok(Response::new(r)),
            _ => Err(Status::internal("unexpected response")),
        }
    }

    async fn compact(
        &self,
        request: Request<CompactionRequest>,
    ) -> Result<Response<CompactionResponse>, Status> {
        let credentials = credentials(&request);
        let forwarded = is_forwarded(&request);
        let request = request.into_inner();
        let (tx, rx) = oneshot::channel();
        self.events
            .try_send(Event::Authenticated(
                credentials.clone(),
                Box::new(Event::EtcdCompact(request, tx)),
            ))
            .map_err(|_| Status::unavailable("etcdserver: too many requests"))?;
        let reply = tokio::time::timeout(Duration::from_secs(10), rx)
            .await
            .map_err(|_| {
                Status::deadline_exceeded("etcdserver: request timed out; outcome unknown")
            })?
            .map_err(|_| Status::unavailable("etcdserver: node unavailable"))?;
        let mut response = match reply {
            Ok(value) => {
                let outcome: Outcome<CompactionResponse> =
                    serde_json::from_value(value).map_err(|e| Status::internal(e.to_string()))?;
                outcome.result.map_err(|e| e.status())?
            }
            Err(error) if error.error == "not leader" && !forwarded => {
                let channel = self.peer_channel(error.leader_id).await?;
                let result = KvClient::new(channel)
                    .compact(forward_auth(request, &credentials))
                    .await;
                let compacted = result.as_ref().is_err_and(|s| {
                    s.code() == tonic::Code::OutOfRange
                        && s.message().strip_prefix("etcdserver: ") == Some(crate::mvcc::COMPACTED)
                });
                if result.is_ok() || (request.physical && compacted) {
                    self.local_barrier().await?;
                    if request.physical {
                        self.wait_compaction(request.revision).await?;
                    }
                }
                result?.into_inner()
            }
            Err(error) => return Err(Status::unavailable(format!("etcdserver: {}", error.error))),
        };
        if let Some(header) = &mut response.header {
            header.member_id = self.member;
        }
        Ok(Response::new(response))
    }
}
