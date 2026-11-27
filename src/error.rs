//! The error of a request: an etcd status code and message, and the class of a storage error.
//!
//! This module uses only `storage_fault`, so `auth`, `cluster`, `mvcc`, `state`, and `node` all
//! sit above it.

use serde::{Deserialize, Serialize};
use tonic::{Code, Status};

use crate::storage_fault;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KvError {
    pub code: i32,
    pub message: String,
    /// The class of a storage error. It does not cross the wire, so a reply keeps only its code.
    #[serde(skip)]
    fault: Option<storage_fault::StorageFault>,
}

impl KvError {
    pub(crate) fn new(code: Code, message: &str) -> Self {
        Self {
            code: code as i32,
            message: format!("etcdserver: {message}"),
            fault: None,
        }
    }

    pub(crate) fn invalid(message: &str) -> Self {
        Self::new(Code::InvalidArgument, message)
    }

    /// Classify a storage error of a read. A transient or missing object fails the request with
    /// a retryable status. Stored data that does not decode fails it with `DataLoss`, and the
    /// read counts it. The caller states that `error` comes from a storage call or from stored
    /// data, so an error of no known class is corrupt data.
    pub(crate) fn storage(error: impl Into<anyhow::Error>) -> Self {
        use storage_fault::{StorageFault, CORRUPT_READS};
        let error = error.into();
        let fault = storage_fault::classify(&error).unwrap_or(StorageFault::Corrupt);
        let (code, message) = match fault {
            StorageFault::Transient => {
                tracing::warn!(error = format!("{error:#}"), "a storage read failed");
                (Code::Unavailable, "storage unavailable; retry".to_owned())
            }
            StorageFault::Missing => (
                Code::Unavailable,
                "a checkpoint object is missing; retry".to_owned(),
            ),
            StorageFault::Corrupt => {
                CORRUPT_READS.inc();
                tracing::error!(
                    error = format!("{error:#}"),
                    "a read met corrupt stored data"
                );
                (Code::DataLoss, format!("corrupt stored data: {error:#}"))
            }
        };
        Self {
            fault: Some(fault),
            ..Self::new(code, &message)
        }
    }

    /// The error of a read outside the KV path, such as a diagnostic. A classified storage
    /// error keeps its class. Another error fails the request with `Unavailable`.
    pub(crate) fn read_failure(error: anyhow::Error) -> Self {
        match storage_fault::classify(&error) {
            Some(_) => Self::storage(error),
            None => Self::new(Code::Unavailable, &format!("{error:#}")),
        }
    }

    /// A stored row that does not decode or contradicts another one.
    pub(crate) fn corrupt(message: &str) -> Self {
        Self::storage(storage_fault::stored(message))
    }

    /// An error of this member's own invariants, such as a write batch that cannot count its
    /// bytes. It stops the member.
    pub(crate) fn invariant(error: impl std::fmt::Display) -> Self {
        Self::new(Code::Internal, &format!("invariant error: {error}"))
    }

    /// Return the error of this member's own invariants, which stops the member. A storage
    /// error fails only its request, so it returns `None`.
    pub(crate) fn stops_member(&self) -> Option<anyhow::Error> {
        match self.fault {
            Some(_) => None,
            None => self.unapplied(),
        }
    }

    /// Return the error that stops apply instead of becoming the result of an entry: a storage
    /// error, whose class survives, or an invariant error. Each member must reach the same
    /// result, so a read that failed on this member cannot be one.
    pub(crate) fn unapplied(&self) -> Option<anyhow::Error> {
        if let Some(fault) = self.fault {
            return Some(anyhow::Error::new(storage_fault::Classified {
                fault,
                message: self.message.clone(),
            }));
        }
        (self.code == Code::Internal as i32).then(|| anyhow::anyhow!("{}", self.message))
    }

    pub fn status(self) -> Status {
        Status::new(Code::from_i32(self.code), self.message)
    }
}
