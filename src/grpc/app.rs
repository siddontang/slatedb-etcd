//! One listener's HTTP service: gRPC paths go straight to the tonic servers, others to axum.
//!
//! An axum `Router` with layers boxes the service, the response future and both bodies once per
//! layer, and clones each boxed service per request. Every RPC pays for that. This service
//! matches the same gRPC paths with a `match`, inserts the same request extensions, records
//! the same metrics as the former `from_fn` middleware, and boxes nothing of its own.

use std::convert::Infallible;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use axum::body::Bytes;
use axum::http::{Request, Response};
use http_body::{Body as HttpBody, Frame, SizeHint};
use tonic::codegen::BoxFuture;

use super::Servers;
use crate::metrics::{ObservedBody, RpcObserver, WatchMeters};
use crate::tls::{ConnectionService, VerifiedUser};

type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Which listener a request arrived on; gRPC handlers read it from the request extensions.
#[derive(Clone, Copy)]
pub(crate) enum Listener {
    Client,
    Peer,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Route {
    Cluster,
    Health,
    Maintenance,
    Auth,
    Kv,
    Lease,
    Watch,
}

/// Match `/<service>/<method>` like the axum route `/<service>/{*method}`: the method is
/// non-empty and may contain further slashes.
fn route(path: &str) -> Option<Route> {
    let (service, method) = path.strip_prefix('/')?.split_once('/')?;
    if method.is_empty() {
        return None;
    }
    Some(match service {
        "etcdserverpb.KV" => Route::Kv,
        "etcdserverpb.Lease" => Route::Lease,
        "etcdserverpb.Watch" => Route::Watch,
        "etcdserverpb.Cluster" => Route::Cluster,
        "etcdserverpb.Maintenance" => Route::Maintenance,
        "etcdserverpb.Auth" => Route::Auth,
        "grpc.health.v1.Health" => Route::Health,
        _ => return None,
    })
}

struct Inner {
    servers: Servers,
    listener: Option<Listener>,
    watch: Option<Arc<WatchMeters>>,
    observer: Option<Arc<RpcObserver>>,
    fallback: axum::Router,
}

/// Cheap to clone: hyper clones the service once per request.
#[derive(Clone)]
pub(crate) struct App {
    inner: Arc<Inner>,
    user: Option<VerifiedUser>,
}

impl App {
    pub(crate) fn new(
        servers: Servers,
        listener: Option<Listener>,
        watch: Option<Arc<WatchMeters>>,
        observer: Option<Arc<RpcObserver>>,
        fallback: axum::Router,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                servers,
                listener,
                watch,
                observer,
                fallback,
            }),
            user: None,
        }
    }
}

impl ConnectionService for App {
    type Body = AppBody;

    /// The service for one TLS connection whose client certificate names `user`.
    fn with_user(mut self, user: VerifiedUser) -> Self {
        self.user = Some(user);
        self
    }
}

impl<B> tower::Service<Request<B>> for App
where
    B: HttpBody<Data = Bytes> + Send + Unpin + 'static,
    B::Error: Into<BoxError> + Send + 'static,
{
    type Response = Response<AppBody>;
    type Error = Infallible;
    type Future = AppFuture;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, mut req: Request<B>) -> AppFuture {
        let inner = &*self.inner;
        if let Some(user) = &self.user {
            req.extensions_mut().insert(user.clone());
        }
        let Some(route) = route(req.uri().path()) else {
            return AppFuture::Http(Box::new(inner.fallback.clone().call(req)));
        };
        let extensions = req.extensions_mut();
        match inner.listener {
            Some(Listener::Client) => {
                extensions.insert(crate::tls::ClientServer);
            }
            Some(Listener::Peer) => {
                extensions.insert(crate::tls::PeerServer);
            }
            None => {}
        }
        if route == Route::Watch {
            if let Some(watch) = &inner.watch {
                extensions.insert(watch.clone());
            }
        }
        let (req, outgoing) = match &inner.observer {
            Some(observer) => observer.request(req),
            None => (req.map(ObservedBody::plain), None),
        };
        let servers = &inner.servers;
        let fut = match route {
            Route::Kv => servers.kv.clone().call(req),
            Route::Lease => servers.lease.clone().call(req),
            Route::Watch => servers.watch.clone().call(req),
            Route::Cluster => servers.cluster.clone().call(req),
            Route::Maintenance => servers.maintenance.clone().call(req),
            Route::Auth => servers.auth.clone().call(req),
            Route::Health => servers.health.clone().call(req),
        };
        AppFuture::Grpc { fut, outgoing }
    }
}

pub(crate) enum AppFuture {
    Grpc {
        fut: BoxFuture<Response<tonic::body::Body>, Infallible>,
        outgoing: Option<crate::metrics::RpcObservation>,
    },
    // Boxed: diagnostics and scrapes are rare, and the future is large.
    Http(Box<axum::routing::future::RouteFuture<Infallible>>),
}

impl Future for AppFuture {
    type Output = Result<Response<AppBody>, Infallible>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.get_mut() {
            Self::Grpc { fut, outgoing } => {
                let Ok(response) = std::task::ready!(fut.as_mut().poll(cx));
                let response = RpcObserver::response(outgoing.take(), response);
                Poll::Ready(Ok(response.map(AppBody::Grpc)))
            }
            Self::Http(fut) => Pin::new(&mut **fut)
                .poll(cx)
                .map(|result| result.map(|response| response.map(AppBody::Http))),
        }
    }
}

pub(crate) enum AppBody {
    Grpc(ObservedBody<tonic::body::Body>),
    Http(axum::body::Body),
}

impl HttpBody for AppBody {
    type Data = Bytes;
    type Error = BoxError;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, BoxError>>> {
        match self.get_mut() {
            Self::Grpc(body) => Pin::new(body)
                .poll_frame(cx)
                .map(|frame| frame.map(|result| result.map_err(Into::into))),
            Self::Http(body) => Pin::new(body)
                .poll_frame(cx)
                .map(|frame| frame.map(|result| result.map_err(Into::into))),
        }
    }

    fn is_end_stream(&self) -> bool {
        match self {
            Self::Grpc(body) => body.is_end_stream(),
            Self::Http(body) => body.is_end_stream(),
        }
    }

    fn size_hint(&self) -> SizeHint {
        match self {
            Self::Grpc(body) => body.size_hint(),
            Self::Http(body) => body.size_hint(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tower::ServiceExt;

    /// Every path the former axum routes accepted, and nothing else, reaches a tonic server.
    #[tokio::test]
    async fn routes_match_axum_paths() {
        let services = [
            ("etcdserverpb.KV", Route::Kv),
            ("etcdserverpb.Lease", Route::Lease),
            ("etcdserverpb.Watch", Route::Watch),
            ("etcdserverpb.Cluster", Route::Cluster),
            ("etcdserverpb.Maintenance", Route::Maintenance),
            ("etcdserverpb.Auth", Route::Auth),
            ("grpc.health.v1.Health", Route::Health),
        ];
        let mut router = axum::Router::new();
        for (service, _) in services {
            router = router.route_service(
                &format!("/{service}/{{*method}}"),
                tower::service_fn(|_: Request<axum::body::Body>| async {
                    Ok::<_, Infallible>(Response::new(axum::body::Body::empty()))
                }),
            );
        }
        let mut paths = vec![
            "/".to_owned(),
            "".to_owned(),
            "/kv".to_owned(),
            "/metrics".to_owned(),
            "/etcdserverpb.Unknown/Range".to_owned(),
            "/etcdserverpb.kv/Range".to_owned(),
            "/slatedb.peer.Transport/Exchange".to_owned(),
            "//etcdserverpb.KV/Range".to_owned(),
        ];
        for (service, _) in services {
            for suffix in ["", "/", "/Range", "/a/b", "/Range/", "/%41"] {
                paths.push(format!("/{service}{suffix}"));
            }
        }
        for path in paths {
            let request = Request::builder()
                .method("POST")
                .uri(format!("http://localhost{path}"))
                .body(axum::body::Body::empty());
            let Ok(request) = request else { continue };
            let expected = router.clone().oneshot(request).await.unwrap().status() == 200;
            let actual = route(&path);
            assert_eq!(actual.is_some(), expected, "{path}");
            if let Some(actual) = actual {
                let service = path[1..].split('/').next().unwrap();
                assert!(services.contains(&(service, actual)), "{path}");
            }
        }
    }
}
