use std::collections::BTreeMap;

use crate::WriteBatch;
use serde::{Deserialize, Serialize};
use slatedb::Db;
use tonic::Code;

use crate::mvcc::{self, KvError};
use crate::proto::etcdserverpb::*;

const PREFIX: &[u8] = b"etcd/leases/";

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LeaseRecord {
    pub ttl: i64,
    pub generation: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum LeaseCommand {
    Grant { id: i64, ttl: i64 },
    Revoke { id: i64 },
    KeepAlive { id: i64 },
    TimeToLive { id: i64, keys: bool },
    List,
    Expire { id: i64, generation: u64 },
}

#[derive(Debug, Serialize, Deserialize)]
pub enum LeaseReply {
    Grant(LeaseGrantResponse),
    Revoke(LeaseRevokeResponse),
    KeepAlive(LeaseKeepAliveResponse),
    TimeToLive(LeaseTimeToLiveResponse),
    List(LeaseLeasesResponse),
}

impl LeaseReply {
    pub fn header_mut(&mut self) -> Option<&mut ResponseHeader> {
        match self {
            Self::Grant(r) => r.header.as_mut(),
            Self::Revoke(r) => r.header.as_mut(),
            Self::KeepAlive(r) => r.header.as_mut(),
            Self::TimeToLive(r) => r.header.as_mut(),
            Self::List(r) => r.header.as_mut(),
        }
    }
}

fn key(id: i64) -> Vec<u8> {
    [PREFIX, &id.to_be_bytes()].concat()
}

pub async fn load(db: &Db) -> anyhow::Result<BTreeMap<i64, LeaseRecord>> {
    let mut leases = BTreeMap::new();
    let mut scan = db.scan_prefix(PREFIX, ..).await?;
    while let Some(row) = scan.next().await? {
        let id = i64::from_be_bytes(row.key[PREFIX.len()..].try_into()?);
        leases.insert(id, bincode::deserialize(&row.value)?);
    }
    Ok(leases)
}

pub type LeaseChange = Option<(i64, Option<LeaseRecord>)>;
pub type PreparedLease = (LeaseReply, WriteBatch, i64, LeaseChange);

pub async fn prepare(
    db: &Db,
    revision: i64,
    compact_revision: i64,
    index: u64,
    leases: &BTreeMap<i64, LeaseRecord>,
    command: &LeaseCommand,
) -> Result<PreparedLease, KvError> {
    let (prepared, _) =
        prepare_with_counts(db, revision, compact_revision, index, leases, command).await?;
    Ok(prepared)
}

pub(crate) async fn prepare_with_counts(
    db: &Db,
    revision: i64,
    compact_revision: i64,
    index: u64,
    leases: &BTreeMap<i64, LeaseRecord>,
    command: &LeaseCommand,
) -> Result<(PreparedLease, u64), KvError> {
    let mut deletes = 0;
    let mut batch = WriteBatch::new();
    let mut next = revision;
    let mut change = None;
    let header = mvcc::header(revision);
    let reply = match *command {
        LeaseCommand::Grant { id, ttl } => {
            if id == 0 {
                return Err(KvError::new(Code::NotFound, "requested lease not found"));
            }
            if ttl > 9_000_000_000 {
                return Err(KvError::new(Code::OutOfRange, "too large lease TTL"));
            }
            if leases.contains_key(&id) {
                return Err(KvError::new(
                    Code::FailedPrecondition,
                    "lease already exists",
                ));
            }
            let ttl = ttl.max(2);
            change = Some((
                id,
                Some(LeaseRecord {
                    ttl,
                    generation: index,
                }),
            ));
            LeaseReply::Grant(LeaseGrantResponse {
                header,
                id,
                ttl,
                error: String::new(),
            })
        }
        LeaseCommand::KeepAlive { id } => {
            let ttl = leases.get(&id).map_or(0, |lease| lease.ttl);
            if ttl > 0 {
                change = Some((
                    id,
                    Some(LeaseRecord {
                        ttl,
                        generation: index,
                    }),
                ));
            }
            LeaseReply::KeepAlive(LeaseKeepAliveResponse { header, id, ttl })
        }
        LeaseCommand::Revoke { id } | LeaseCommand::Expire { id, .. } => {
            let valid = leases.get(&id).is_some_and(|lease| match *command {
                LeaseCommand::Expire { generation, .. } => lease.generation == generation,
                _ => true,
            });
            if !valid && matches!(command, LeaseCommand::Revoke { .. }) {
                return Err(KvError::new(Code::NotFound, "requested lease not found"));
            }
            if valid {
                (batch, next, deletes) =
                    mvcc::revoke_keys(db, revision, compact_revision, id, leases).await?;
                change = Some((id, None));
            }
            LeaseReply::Revoke(LeaseRevokeResponse {
                header: mvcc::header(next),
            })
        }
        LeaseCommand::TimeToLive { id, keys } => {
            let record = leases.get(&id);
            LeaseReply::TimeToLive(LeaseTimeToLiveResponse {
                header,
                id,
                ttl: record.map_or(-1, |l| l.ttl),
                granted_ttl: record.map_or(0, |l| l.ttl),
                keys: if keys && record.is_some() {
                    mvcc::lease_keys(db, id).await?
                } else {
                    vec![]
                },
            })
        }
        LeaseCommand::List => LeaseReply::List(LeaseLeasesResponse {
            header,
            leases: leases.keys().map(|&id| LeaseStatus { id }).collect(),
        }),
    };
    if let Some((id, record)) = &change {
        if let Some(record) = record {
            batch.put(
                key(*id),
                bincode::serialize(record).map_err(KvError::storage)?,
            );
        } else {
            batch.delete(key(*id));
        }
    }
    Ok(((reply, batch, next, change), deletes))
}
