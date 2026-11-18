use super::*;
use crate::maintenance::{Command, ALARMS, HASH};
use crate::mvcc::KvError;
use crate::proto::etcdserverpb::*;
use prost::Message;
use serde_json::{json, Value};
use slatedb::compactor::{CompactionSpec, CompactionStatus, SourceId};
use tonic::Code;

impl StateMachine {
    pub fn logical_size(&self) -> u64 {
        self.logical_bytes
    }

    pub async fn maintenance(
        &mut self,
        command: &Command,
        credentials: &crate::auth::Credentials,
        mut status: StatusResponse,
    ) -> Result<Outcome<Value>> {
        if let Err(error) = self.auth.admin(credentials) {
            return Ok(Outcome { result: Err(error) });
        }
        status.header = mvcc::header(self.revision);
        let result = match command {
            Command::Status => {
                let size = self.logical_size();
                status.db_size_in_use = size as i64;
                status.db_size = self.physical_size().await?.max(size) as i64;
                status.db_size_quota = self.quota as i64;
                status.raft_applied_index = self.applied;
                status.errors = self
                    .alarms
                    .iter()
                    .map(|a| format!("alarm:{}", a.alarm))
                    .collect();
                Ok(serde_json::to_value(status)?)
            }
            Command::Hash(revision) => self.hash_kv(*revision).await,
            Command::HashAll => {
                let mut hash = 0;
                let mut rows = self.db.scan_prefix(b"etcd/", ..).await?;
                while let Some(row) = rows.next().await? {
                    hash = crc32c::crc32c_append(hash, &row.key);
                    hash = crc32c::crc32c_append(hash, &row.value);
                }
                Ok(json!({"header": mvcc::header(self.revision), "hash": hash}))
            }
            Command::Defragment => self
                .defragment()
                .await
                .map_err(KvError::storage)
                .map(|()| json!({"header": mvcc::header(self.revision)})),
            Command::Snapshot => {
                let bytes = self.snapshot_data().await?;
                if bytes.len() + 32 > 64 * 1024 * 1024 {
                    Err(KvError::new(
                        Code::ResourceExhausted,
                        "native snapshot exceeds 64 MiB",
                    ))
                } else {
                    Ok(json!({"header": mvcc::header(self.revision), "blob": bytes}))
                }
            }
            _ => Err(KvError::new(
                Code::InvalidArgument,
                "invalid local maintenance operation",
            )),
        };
        Ok(Outcome { result })
    }

    pub async fn apply_maintenance(
        &mut self,
        index: u64,
        command: &Command,
        credentials: &crate::auth::Credentials,
    ) -> Result<Outcome<Value>> {
        ensure!(
            index == self.applied + 1,
            "non-contiguous maintenance apply"
        );
        let mut batch = WriteBatch::new();
        let mut alarms = self.alarms.clone();
        let result = self.auth.admin(credentials).and_then(|()| match command {
            Command::Alarm(request) => {
                use alarm_request::AlarmAction;
                let matches = |a: &AlarmMember| {
                    (request.member_id == 0 || request.member_id == a.member_id)
                        && (request.alarm == 0 || request.alarm == a.alarm)
                };
                let selected: Vec<_> = alarms.iter().filter(|a| matches(a)).cloned().collect();
                match AlarmAction::try_from(request.action) {
                    Ok(AlarmAction::Get) => (),
                    Ok(AlarmAction::Activate) if request.alarm == 1 || request.alarm == 2 => {
                        if selected.is_empty() {
                            alarms.push(AlarmMember {
                                member_id: request.member_id,
                                alarm: request.alarm,
                            });
                        }
                    }
                    Ok(AlarmAction::Deactivate) => alarms.retain(|a| !matches(a)),
                    _ => return Err(KvError::new(Code::InvalidArgument, "invalid alarm request")),
                }
                let response = if request.action == AlarmAction::Deactivate as i32 {
                    selected
                } else {
                    alarms.iter().filter(|a| matches(a)).cloned().collect()
                };
                Ok(json!({"header": mvcc::header(self.revision), "alarms": response}))
            }
            Command::Downgrade(_) => Err(KvError::new(
                Code::Unimplemented,
                "binary downgrade is not supported by the SlateDB storage format",
            )),
            _ => Err(KvError::new(
                Code::InvalidArgument,
                "invalid maintenance command",
            )),
        });
        batch.put(ALARMS, serde_json::to_vec(&alarms)?);
        batch.put(APPLIED, index.to_be_bytes());
        self.write_batch(batch, index).await?;
        self.applied = index;
        self.alarms = alarms;
        Ok(Outcome { result })
    }

    pub(super) async fn hash_cache(&self) -> Result<Vec<Value>> {
        match self.db.get(b"etcd/hash-cache").await? {
            Some(bytes) => Ok(serde_json::from_slice(&bytes)?),
            None => Ok(vec![]),
        }
    }

    async fn physical_size(&self) -> Result<u64> {
        let manifest = self
            .admin
            .read_manifest(None)
            .await?
            .context("missing manifest")?;
        Ok(manifest
            .all_sst_views()
            .map(|v| v.sst.estimate_size())
            .sum())
    }

    async fn defragment(&mut self) -> Result<()> {
        self.db.flush().await?;
        self.durable_applied = self.applied;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(8);
        loop {
            let manifest = self
                .admin
                .read_manifest(None)
                .await?
                .context("missing manifest")?;
            let mut sources: Vec<_> = manifest
                .l0()
                .iter()
                .map(|v| SourceId::SstView(v.id))
                .collect();
            sources.extend(
                manifest
                    .compacted()
                    .iter()
                    .map(|r| SourceId::SortedRun(r.id)),
            );
            if sources.is_empty() {
                return Ok(());
            }
            let destination = manifest.compacted().iter().map(|r| r.id).min().unwrap_or(0);
            let job = self
                .admin
                .submit_compaction(CompactionSpec::new(sources, destination))
                .await?;
            loop {
                ensure!(
                    tokio::time::Instant::now() < deadline,
                    "defragmentation timed out"
                );
                tokio::time::sleep(Duration::from_millis(50)).await;
                if let Some(job) = self.admin.read_compaction(job.id(), None).await? {
                    match job.status() {
                        CompactionStatus::Completed => return Ok(()),
                        CompactionStatus::Failed => break,
                        _ => (),
                    }
                } else {
                    break;
                }
            }
        }
    }

    pub(super) async fn hash_kv(&self, revision: i64) -> std::result::Result<Value, KvError> {
        if revision > 0 {
            for mut cached in self.hash_cache().await.map_err(KvError::storage)? {
                if cached["hash_revision"].as_i64() == Some(revision) {
                    cached["header"] = json!(mvcc::header(self.revision));
                    return Ok(cached);
                }
            }
        }
        let revision = if revision == 0 {
            self.revision
        } else {
            revision
        };
        if revision < self.compact_revision {
            return Err(KvError::new(
                Code::OutOfRange,
                "mvcc: required revision has been compacted",
            ));
        }
        if revision > self.revision {
            return Err(KvError::new(
                Code::OutOfRange,
                "mvcc: required revision is a future revision",
            ));
        }
        // etcd excludes obsolete records below the compaction boundary at the requested revision.
        let mut latest = std::collections::BTreeMap::new();
        let mut scan = self
            .db
            .scan_prefix(HASH, ..)
            .await
            .map_err(KvError::storage)?;
        while let Some(row) = scan.next().await.map_err(KvError::storage)? {
            let key = &row.key[HASH.len()..];
            let main = i64::from_be_bytes(key[..8].try_into().map_err(KvError::storage)?);
            if main > revision {
                break;
            }
            let kv = crate::proto::mvccpb::KeyValue::decode(row.value.clone())
                .map_err(KvError::storage)?;
            if key.len() == 18 {
                latest.remove(&kv.key);
            } else {
                latest.insert(kv.key, key.to_vec());
            }
        }
        let keep: std::collections::BTreeSet<_> = latest.into_values().collect();
        let mut hash = crc32c::crc32c(b"key");
        let mut rows = self
            .db
            .scan_prefix(HASH, ..)
            .await
            .map_err(KvError::storage)?;
        while let Some(row) = rows.next().await.map_err(KvError::storage)? {
            let key = &row.key[HASH.len()..];
            let main = i64::from_be_bytes(key[..8].try_into().map_err(KvError::storage)?);
            if main > revision {
                break;
            }
            if main <= self.compact_revision && !keep.is_empty() && !keep.contains(key) {
                continue;
            }
            if main == self.compact_revision && key.len() == 18 {
                continue;
            }
            hash = crc32c::crc32c_append(hash, key);
            hash = crc32c::crc32c_append(hash, &row.value);
        }
        Ok(json!({"header": mvcc::header(self.revision), "hash": hash,
            "compact_revision": self.compact_revision, "hash_revision": revision}))
    }
}

pub(crate) fn record_hash(
    revision: i64,
    events: &[crate::proto::mvccpb::Event],
    batch: &mut WriteBatch,
) {
    for (sub, event) in events.iter().enumerate() {
        let mut key = HASH.to_vec();
        key.extend_from_slice(&revision.to_be_bytes());
        key.push(b'_');
        key.extend_from_slice(&(sub as i64).to_be_bytes());
        let mut kv = event.kv.clone().unwrap();
        if event.r#type == 1 {
            key.push(b't');
            kv = crate::proto::mvccpb::KeyValue {
                key: kv.key,
                ..Default::default()
            };
        }
        batch.put_new(key, kv.encode_to_vec());
    }
}

pub(crate) async fn compact_hash(
    db: &Db,
    revision: i64,
    batch: &mut WriteBatch,
) -> std::result::Result<(), KvError> {
    let mut scan = db.scan_prefix(HASH, ..).await.map_err(KvError::storage)?;
    let mut latest = std::collections::BTreeMap::new();
    while let Some(row) = scan.next().await.map_err(KvError::storage)? {
        let main = i64::from_be_bytes(
            row.key[HASH.len()..HASH.len() + 8]
                .try_into()
                .map_err(KvError::storage)?,
        );
        if main > revision {
            break;
        }
        let kv =
            crate::proto::mvccpb::KeyValue::decode(row.value.clone()).map_err(KvError::storage)?;
        if let Some(old) = latest.insert(kv.key, row.key.clone()) {
            batch.delete(old);
        }
        if row.key.len() == HASH.len() + 18 && main < revision {
            batch.delete(row.key);
        }
    }
    Ok(())
}

// Rebuild derived accounting when opening stored state.
pub(super) async fn count_logical_size(db: &Db) -> Result<u64> {
    let mut scan = db.scan_prefix(b"etcd/", ..).await?;
    let mut size = 0_u64;
    while let Some(row) = scan.next().await? {
        size = size
            .checked_add((row.key.len() + row.value.len()) as u64)
            .context("logical size counter overflow")?;
    }
    Ok(size)
}
