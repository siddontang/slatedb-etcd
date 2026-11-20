use super::*;

use crate::mvcc::Outcome;
use std::sync::Arc;

/// Share a pinned scan until each client needs its own protobuf response.
#[derive(Debug)]
pub enum KvReply {
    Owned(Outcome),
    Shared(Arc<Outcome>),
}

impl KvReply {
    pub fn into_outcome(self) -> Outcome {
        match self {
            Self::Owned(outcome) => outcome,
            Self::Shared(outcome) => Arc::unwrap_or_clone(outcome),
        }
    }
}

pub(super) enum ClientResponse {
    Json(Response),
    Kv(KvResponse),
}

impl From<Response> for ClientResponse {
    fn from(response: Response) -> Self {
        Self::Json(response)
    }
}

impl ClientResponse {
    pub fn is_closed(&self) -> bool {
        match self {
            Self::Json(response) => response.is_closed(),
            Self::Kv(response) => response.is_closed(),
        }
    }

    pub fn error(self, error: ApiError) {
        match self {
            Self::Json(response) => {
                let _ = response.send(Err(error));
            }
            Self::Kv(response) => {
                let _ = response.send(Err(error));
            }
        }
    }

    pub fn json(self, value: Value) -> Result<()> {
        match self {
            Self::Json(response) => {
                let _ = response.send(Ok(value));
            }
            Self::Kv(response) => {
                // Authorization errors can arrive through the common command response path.
                let _ = response.send(Ok(KvReply::Owned(serde_json::from_value(value)?)));
            }
        }
        Ok(())
    }

    pub fn shared_kv(self, outcome: Arc<Outcome>) -> Result<()> {
        match self {
            Self::Json(response) => {
                let _ = response.send(Ok(serde_json::to_value(outcome.as_ref())?));
            }
            Self::Kv(response) => {
                let _ = response.send(Ok(KvReply::Shared(outcome)));
            }
        }
        Ok(())
    }

    pub fn kv(self, outcome: crate::mvcc::Outcome) -> Result<()> {
        match self {
            Self::Json(response) => {
                let _ = response.send(Ok(serde_json::to_value(outcome)?));
            }
            Self::Kv(response) => {
                let _ = response.send(Ok(KvReply::Owned(outcome)));
            }
        }
        Ok(())
    }
}
