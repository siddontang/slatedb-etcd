use std::collections::BTreeSet;
use std::time::Duration;

use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use protobuf::Message as ProtobufMessage;
use raft::eraftpb::Message;
use serde::Deserialize;
use serde_json::Value;
use tokio::sync::{mpsc, oneshot};

use crate::node::Event;
use crate::{ApiError, Request};

#[derive(Clone)]
pub struct ApiState {
    pub events: mpsc::Sender<Event>,
    pub cluster_id: String,
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
        .route("/kv", post(propose))
        .route("/raft", post(raft_message))
        .route("/status", get(status))
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
    match Message::parse_from_bytes(&body) {
        Ok(message) => dispatch(state, |tx| Event::Step(message, tx))
            .await
            .into_response(),
        Err(_) => StatusCode::BAD_REQUEST.into_response(),
    }
}

async fn status(State(state): State<ApiState>) -> Result<Json<Value>, ApiError> {
    dispatch(state, Event::Status).await
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
