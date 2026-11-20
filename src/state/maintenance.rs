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
                let _timer = self.metrics.hash.start_timer();
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
                let _timer = self.metrics.snapshot.start_timer();
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
        Self::manifest_size(&self.admin).await
    }

    pub(crate) async fn manifest_size(admin: &slatedb::admin::Admin) -> Result<u64> {
        let manifest = admin
            .read_manifest(None)
            .await?
            .context("missing manifest")?;
        Ok(manifest
            .all_sst_views()
            .map(|v| v.sst.estimate_size())
            .sum())
    }

    async fn defragment(&mut self) -> Result<()> {
        let _timer = self.metrics.defrag.start_timer();
        let _active = crate::metrics::Active::new(&self.metrics.defrag_active);
        self.metrics.flush_db(&self.db).await?;
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
        let _timer = self.metrics.hash_rev.start_timer();
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
        let compact = self.compact_revision;
        // A pending physical pass leaves rows that it removes later. Hash the rows that remain.
        let pending = self.compaction.is_some();
        let mut latest = std::collections::HashMap::new();
        let mut survivors = std::collections::HashMap::new();
        if compact >= 0 {
            let mut scan = hash_rows(&self.db, revision).await?;
            while let Some(row) = scan.next().await.map_err(KvError::storage)? {
                let kv = HashUserKey::decode(row.value).map_err(KvError::storage)?;
                if pending && hash_main(&row.key)? <= compact {
                    survivors.insert(kv.key.clone(), row.key.clone());
                }
                if row.key.len() == HASH.len() + 18 {
                    latest.remove(&kv.key);
                } else {
                    latest.insert(kv.key, row.key[HASH.len()..].to_vec());
                }
            }
        }
        let keep = latest
            .into_values()
            .collect::<std::collections::HashSet<_>>();
        // The pass keeps the latest record at or below the boundary, except older tombstones.
        let survivors = survivors
            .into_values()
            .filter(|key| key.len() != HASH.len() + 18 || hash_main(key).ok() == Some(compact))
            .collect::<std::collections::HashSet<_>>();
        self.hash_retained(
            revision,
            |row| keep.contains(&row[HASH.len()..]),
            |row, main| !pending || main > compact || survivors.contains(row),
        )
        .await
    }

    pub(super) async fn hash_retained(
        &self,
        revision: i64,
        retained: impl Fn(&[u8]) -> bool,
        exists: impl Fn(&[u8], i64) -> bool,
    ) -> std::result::Result<Value, KvError> {
        let mut hash = crc32c::crc32c(b"key");
        // etcd includes obsolete records when the retention set is empty.
        let mut empty_hash = Some(hash).filter(|_| self.compact_revision >= 0);
        let mut rows = hash_rows(&self.db, revision).await?;
        while let Some(row) = rows.next().await.map_err(KvError::storage)? {
            let key = &row.key[HASH.len()..];
            let main = i64::from_be_bytes(key[..8].try_into().map_err(KvError::storage)?);
            let tombstone = key.len() == 18;
            if !exists(&row.key, main) || (main == self.compact_revision && tombstone) {
                continue;
            }
            let recent = main > self.compact_revision;
            let kept = (!recent || empty_hash.is_some()) && retained(&row.key);
            if kept {
                empty_hash = None;
            }
            let append = |crc| crc32c::crc32c_append(crc32c::crc32c_append(crc, key), &row.value);
            if recent || kept {
                hash = append(hash);
            }
            empty_hash = empty_hash.map(append);
        }
        let hash = empty_hash.unwrap_or(hash);
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
        let kv = event.kv.as_ref().unwrap();
        let deleted = event.r#type == 1;
        let value = if deleted {
            crate::proto::mvccpb::KeyValue {
                key: kv.key.clone(),
                ..Default::default()
            }
            .encode_to_vec()
        } else {
            kv.encode_to_vec()
        };
        batch.put_new(hash_key(revision, sub as i64, deleted), value);
    }
}

pub(crate) fn hash_key(revision: i64, sub: i64, deleted: bool) -> Vec<u8> {
    let mut key = Vec::with_capacity(HASH.len() + 17 + usize::from(deleted));
    key.extend_from_slice(HASH);
    key.extend_from_slice(&revision.to_be_bytes());
    key.push(b'_');
    key.extend_from_slice(&sub.to_be_bytes());
    if deleted {
        key.push(b't');
    }
    key
}

// Hash records use the etcd KeyValue wire format. Retention only needs field one.
#[derive(prost::Message)]
struct HashUserKey {
    #[prost(bytes = "vec", tag = "1")]
    key: Vec<u8>,
}

fn hash_main(key: &[u8]) -> std::result::Result<i64, KvError> {
    key.get(HASH.len()..HASH.len() + 8)
        .and_then(|main| main.try_into().ok())
        .map(i64::from_be_bytes)
        .ok_or_else(|| KvError::storage("invalid hash key"))
}

async fn hash_rows(db: &Db, revision: i64) -> std::result::Result<slatedb::DbIterator, KvError> {
    db.scan_with_options(
        HASH.to_vec()..=hash_key(revision, i64::MAX, true),
        &mvcc::maintenance_scan(),
    )
    .await
    .map_err(KvError::storage)
}

// Rebuild derived accounting when opening stored state.
pub(crate) async fn count_logical_size(db: &Db) -> Result<u64> {
    let mut scan = db.scan_prefix(b"etcd/", ..).await?;
    let mut size = 0_u64;
    while let Some(row) = scan.next().await? {
        size = size
            .checked_add((row.key.len() + row.value.len()) as u64)
            .context("logical size counter overflow")?;
    }
    Ok(size)
}
