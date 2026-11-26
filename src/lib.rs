// Tests read SlateDB directly; `clippy.toml` restricts only the server code.
#![cfg_attr(test, allow(clippy::disallowed_methods))]

mod batch;
mod codec;
mod command;
pub mod convert;
mod crc;
pub use batch::WriteBatch;

pub mod api;
pub mod auth;
pub mod cluster;
pub mod compactor;
mod failpoint;
pub mod grpc;
pub mod lease;
pub mod maintenance;
mod metrics;
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
#[cfg(feature = "write-profile")]
pub mod write_profile;

use serde::{Deserialize, Serialize};

/// The etcd API version this service advertises to clients.
pub const ETCD_API_VERSION: &str = "3.6.5";

pub(crate) const STORAGE_FORMAT: u32 = 3;

/// The format of native snapshots and backups that this binary writes.
///
/// Format 4 rows can hold hash digests and Watch journals without Put values. Binaries that read
/// only format 3 reject format 4 and do not misread those rows.
pub(crate) const SNAPSHOT_FORMAT: u32 = 4;

/// Return true when this binary can install snapshot rows of `version`.
/// Format 3 rows hold full hash values and inline journal values, which this binary still reads.
pub(crate) fn readable_snapshot(version: u32) -> bool {
    version == STORAGE_FORMAT || version == SNAPSHOT_FORMAT
}

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
    Diagnostic(Command),
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
    /// The shared-mode leader announces a durable checkpoint of the shared state. Members open
    /// it as their next base. It changes no etcd state.
    ///
    /// The enum is untagged, so this variant must precede `CompactStep`, which matches any `id`.
    ViewCheckpoint {
        id: String,
        checkpoint: String,
        /// The index through which the checkpoint holds the whole state: no entry after its row
        /// changed state. An entry of an earlier binary has no value, and zero means the row.
        #[serde(default)]
        covers: u64,
    },
    /// A replicated-mode leader sets the logical quota of the cluster. A non-positive `bytes`
    /// disables the check. A `seed` entry applies only when the state has no cluster quota.
    /// Another entry applies only when its `(requested_at, member, bytes)` is newer than the
    /// stored request. It changes no etcd state except the `etcd/quota` row. Shared mode ignores
    /// it.
    ///
    /// The enum is untagged, so this variant must precede `CompactStep`, which matches any `id`.
    /// The JSON names the id `quota_id`. An older binary then fails to decode the entry and
    /// stops, instead of applying it as a `CompactStep`.
    SetQuota {
        #[serde(rename = "quota_id")]
        id: String,
        bytes: i64,
        seed: bool,
        /// The wall clock of the operator request on `member`, in milliseconds. Zero for a seed.
        requested_at: u64,
        member: u64,
    },
    /// A leader proposes this entry at the start of each term, in both state modes. A state
    /// without the `etcd/members` row stores `members`, so every member stores the same row. A
    /// state with the row keeps it, and compares it with `digest`. See `state::cluster`.
    ///
    /// Like `SetQuota`, this variant precedes `CompactStep`, and the JSON names its id
    /// `members_id`. An older binary then stops on the entry.
    SeedMembers(cluster::Seed),
    /// The leader proposes bounded physical compaction work after a Compact entry.
    CompactStep {
        id: String,
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
            Self::Diagnostic(c) => &c.id,
            Self::Cluster { id, .. }
            | Self::Maintenance { id, .. }
            | Self::Auth { id, .. }
            | Self::Secured { id, .. }
            | Self::Etcd { id, .. }
            | Self::Compact { id, .. }
            | Self::CompactStep { id }
            | Self::ViewCheckpoint { id, .. }
            | Self::SetQuota { id, .. }
            | Self::Lease { id, .. } => id,
            Self::SeedMembers(seed) => &seed.id,
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
