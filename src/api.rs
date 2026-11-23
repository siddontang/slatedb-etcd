use std::collections::BTreeSet;
use std::time::Duration;

use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::Value;
use tokio::sync::{mpsc, oneshot};

use crate::node::Event;
use crate::{ApiError, Request};

#[derive(Clone)]
pub struct ApiState {
    pub events: mpsc::Sender<Event>,
    pub cluster_id: String,
    /// The object namespace that serves and receives snapshot objects.
    pub(crate) location: Option<std::sync::Arc<crate::state::transfer::Location>>,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = if self.error == "not leader" {
            StatusCode::CONFLICT
        } else {
            StatusCode::SERVICE_UNAVAILABLE
        };
        (status, Json(self)).into_response()
    }
}

pub fn router(state: ApiState) -> Router {
    Router::new()
        .route_service(
            "/slatedb.peer.Transport/Exchange",
            crate::peer::service(state.clone()),
        )
        .route("/kv", post(propose))
        .route("/raft", post(raft_message))
        .route(crate::state::transfer::FILE_ROUTE, get(snapshot_file))
        .route("/status", get(status))
        .route("/debug/log-stats", get(log_stats))
        .route("/checkpoint", post(checkpoint))
        .route("/debug/value", get(local_value))
        .route("/debug/partition", post(partition))
        .route("/debug/snapshot-delay", post(snapshot_delay))
        .route("/debug/apply-delay", post(apply_delay))
        .route("/debug/log-fault", post(log_fault))
        .route("/debug/log-read-fault", post(log_read_fault))
        .layer(DefaultBodyLimit::max(64 * 1024 * 1024))
        .with_state(state)
}

async fn dispatch(
    state: ApiState,
    event: impl FnOnce(crate::node::Response) -> Event,
) -> Result<Json<Value>, ApiError> {
    let unavailable = || ApiError {
        error: "node unavailable or request outcome unknown".to_owned(),
        leader_id: 0,
    };
    let (tx, rx) = oneshot::channel();
    state
        .events
        .try_send(event(tx))
        .map_err(|_| unavailable())?;
    let result = tokio::time::timeout(Duration::from_secs(10), rx)
        .await
        .map_err(|_| unavailable())?
        .map_err(|_| unavailable())??;
    Ok(Json(result))
}

async fn propose(State(state): State<ApiState>, Json(request): Json<Request>) -> Response {
    if let Err(error) = request.validate() {
        return (StatusCode::BAD_REQUEST, error.to_string()).into_response();
    }
    match serde_json::to_vec(&request) {
        Ok(bytes) if bytes.len() <= 2 * 1024 * 1024 => {}
        _ => return (StatusCode::PAYLOAD_TOO_LARGE, "request exceeds 2 MiB").into_response(),
    }
    dispatch(state, |tx| Event::Propose(request, tx))
        .await
        .into_response()
}

async fn raft_message(State(state): State<ApiState>, headers: HeaderMap, body: Bytes) -> Response {
    if headers
        .get("x-cluster-id")
        .and_then(|value| value.to_str().ok())
        != Some(&state.cluster_id)
    {
        return StatusCode::FORBIDDEN.into_response();
    }
    let size = body.len();
    let Ok(message) = crate::peer::decode_message(body) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    if message.get_msg_type() == raft::eraftpb::MessageType::MsgSnapshot {
        // Copy the checkpoint objects before Raft accepts the snapshot. A failed copy fails the
        // request, so the leader reports the snapshot as failed and retries later.
        let descriptor =
            match crate::state::transfer::Descriptor::decode(message.get_snapshot().get_data()) {
                Ok(descriptor) => descriptor,
                Err(_) => return StatusCode::BAD_REQUEST.into_response(),
            };
        if let (Some(descriptor), Some(location)) = (descriptor, &state.location) {
            if let Err(error) = location.prepare(&descriptor).await {
                tracing::warn!(%error, "cannot copy snapshot objects");
                return (StatusCode::SERVICE_UNAVAILABLE, error.to_string()).into_response();
            }
        }
    }
    dispatch(state, |tx| Event::PeerFrame(message, size, tx))
        .await
        .into_response()
}

#[derive(Deserialize)]
struct FileQuery {
    root: String,
    path: String,
}

/// Serve one object of the current database to a peer that copies a snapshot.
async fn snapshot_file(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Query(query): Query<FileQuery>,
) -> Response {
    if headers
        .get("x-cluster-id")
        .and_then(|value| value.to_str().ok())
        != Some(&state.cluster_id)
    {
        return StatusCode::FORBIDDEN.into_response();
    }
    let Some(location) = &state.location else {
        return StatusCode::NOT_FOUND.into_response();
    };
    match location.serve(&query.root, &query.path).await {
        Ok(Some(result)) => axum::body::Body::from_stream(result.into_stream()).into_response(),
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(error) => (StatusCode::SERVICE_UNAVAILABLE, error.to_string()).into_response(),
    }
}

async fn status(State(state): State<ApiState>) -> Result<Json<Value>, ApiError> {
    dispatch(state, Event::Status).await
}

async fn log_stats(State(state): State<ApiState>) -> Result<Json<Value>, ApiError> {
    dispatch(state, Event::LogStats).await
}

async fn checkpoint(State(state): State<ApiState>) -> Result<Json<Value>, ApiError> {
    dispatch(state, Event::Checkpoint).await
}

#[derive(Deserialize)]
struct KeyQuery {
    key: String,
}

async fn local_value(
    State(state): State<ApiState>,
    Query(query): Query<KeyQuery>,
) -> Result<Json<Value>, ApiError> {
    dispatch(state, |tx| Event::LocalValue(query.key, tx)).await
}

#[derive(Deserialize)]
struct Partition {
    blocked: BTreeSet<u64>,
}

async fn partition(
    State(state): State<ApiState>,
    Json(partition): Json<Partition>,
) -> Result<Json<Value>, ApiError> {
    dispatch(state, |tx| Event::BlockPeers(partition.blocked, tx)).await
}

#[derive(Deserialize)]
struct SnapshotDelay {
    delay_ms: u64,
}

async fn snapshot_delay(
    State(state): State<ApiState>,
    Json(delay): Json<SnapshotDelay>,
) -> Result<Json<Value>, ApiError> {
    dispatch(state, |tx| Event::SnapshotDelay(delay.delay_ms, tx)).await
}

async fn apply_delay(
    State(state): State<ApiState>,
    Json(delay): Json<SnapshotDelay>,
) -> Result<Json<Value>, ApiError> {
    dispatch(state, |tx| Event::ApplyDelay(delay.delay_ms, tx)).await
}

#[derive(Deserialize)]
struct LogFault {
    #[serde(default)]
    delay_ms: u64,
    #[serde(default)]
    fail: bool,
}

async fn log_fault(
    State(state): State<ApiState>,
    Json(fault): Json<LogFault>,
) -> Result<Json<Value>, ApiError> {
    dispatch(state, |tx| Event::LogFault(fault.delay_ms, fault.fail, tx)).await
}

async fn log_read_fault(
    State(state): State<ApiState>,
    Json(fault): Json<LogFault>,
) -> impl IntoResponse {
    dispatch(state, |tx| {
        Event::LogReadFault(fault.delay_ms, fault.fail, tx)
    })
    .await
}
