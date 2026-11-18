mod batch;
pub use batch::WriteBatch;

pub mod api;
pub mod auth;
pub mod cluster;
mod failpoint;
pub mod grpc;
pub mod lease;
pub mod maintenance;
pub mod mvcc;
pub mod node;
pub mod object;
mod peer;
pub mod proto;
pub mod server;
pub mod state;
pub mod storage;
pub mod tls;
pub mod watch;

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Operation {
    Put { key: String, value: String },
    Delete { key: String },
    Get { key: String },
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Request {
    pub operations: Vec<Operation>,
}

impl Request {
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(!self.operations.is_empty(), "empty request");
        anyhow::ensure!(self.operations.len() <= 128, "too many operations");
        for op in &self.operations {
            let key = match op {
                Operation::Put { key, value } => {
                    anyhow::ensure!(value.len() <= 1024 * 1024, "value exceeds 1 MiB");
                    key
                }
                Operation::Delete { key } | Operation::Get { key } => key,
            };
            anyhow::ensure!(!key.is_empty() && key.len() <= 8192, "invalid key length");
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Command {
    pub id: String,
    pub request: Request,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(untagged)]
pub enum RaftCommand {
    Legacy(Command),
    Cluster {
        id: String,
        request: cluster::Command,
        credentials: auth::Credentials,
    },
    Maintenance {
        id: String,
        request: maintenance::Command,
        credentials: auth::Credentials,
    },
    Auth {
        id: String,
        auth: auth::Command,
        credentials: auth::Credentials,
    },
    Secured {
        id: String,
        credentials: auth::Credentials,
        command: Box<RaftCommand>,
    },
    Etcd {
        id: String,
        #[serde(default)]
        origin: u64,
        etcd: proto::etcdserverpb::RequestOp,
    },
    Lease {
        id: String,
        #[serde(default)]
        origin: u64,
        lease: lease::LeaseCommand,
    },
    Compact {
        id: String,
        compact: proto::etcdserverpb::CompactionRequest,
    },
}

impl RaftCommand {
    pub fn secure(self, credentials: Option<auth::Credentials>) -> Self {
        match credentials {
            Some(credentials) => Self::Secured {
                id: self.id().to_owned(),
                credentials,
                command: Box::new(self),
            },
            None => self,
        }
    }

    pub fn id(&self) -> &str {
        match self {
            Self::Legacy(c) => &c.id,
            Self::Cluster { id, .. }
            | Self::Maintenance { id, .. }
            | Self::Auth { id, .. }
            | Self::Secured { id, .. }
            | Self::Etcd { id, .. }
            | Self::Compact { id, .. }
            | Self::Lease { id, .. } => id,
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Reply {
    pub index: u64,
    pub values: Vec<Option<String>>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ApiError {
    pub error: String,
    pub leader_id: u64,
}
