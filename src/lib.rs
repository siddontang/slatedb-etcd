pub mod api;
pub mod grpc;
pub mod mvcc;
pub mod node;
pub mod proto;
pub mod state;
pub mod storage;

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
    Etcd {
        id: String,
        etcd: proto::etcdserverpb::RequestOp,
    },
}

impl RaftCommand {
    pub fn id(&self) -> &str {
        match self {
            Self::Legacy(c) => &c.id,
            Self::Etcd { id, .. } => id,
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
