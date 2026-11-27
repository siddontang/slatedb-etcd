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
        self.logical_bytes.total()
    }

    /// Logical bytes by row kind.
    pub fn logical_usage(&self) -> crate::batch::Usage {
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
                let mut rows = self
                    .applied_state()
                    .scan_under(b"etcd/", &Default::default())
                    .await?;
                while let Some(row) = rows.next().await? {
                    hash = crc32c::crc32c_append(hash, &row.key);
                    hash = crc32c::crc32c_append(hash, &row.value);
                }
                Ok(json!({"header": mvcc::header(self.revision), "hash": hash}))
            }
            // The shared database belongs to the leader writer. The node runs Defragment there.
            Command::Defragment => Err(KvError::new(
                Code::Unimplemented,
                "Defragment runs on the leader writer, not in apply",
            )),
            // gRPC Snapshot streams a backup through `Event::Snapshot`. This command keeps the
            // bounded in-memory image for Rust callers of the maintenance interface.
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
        self.mark_applied(&mut batch, index);
        self.write_batch(batch, index).await?;
        self.applied = index;
        self.alarms = alarms;
        Ok(Outcome { result })
    }

    pub(super) async fn hash_cache(&self) -> Result<Vec<Value>> {
        match self.applied_state().get(b"etcd/hash-cache").await? {
            Some(bytes) => Ok(serde_json::from_slice(&bytes)?),
            None => Ok(vec![]),
        }
    }

    async fn physical_size(&self) -> Result<u64> {
        Self::manifest_size(&self.admin).await
    }

    pub(crate) async fn manifest_size(admin: &slatedb::admin::Admin) -> Result<u64> {
        // A shared database has no manifest until its first leader opens the writer.
        let Some(manifest) = admin.read_manifest(None).await? else {
            return Ok(0);
        };
        Ok(manifest
            .all_sst_views()
            .map(|v| v.sst.estimate_size())
            .sum())
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
            let mut scan = hash_rows(self.applied_state(), revision).await?;
            while let Some(row) = scan.next().await.map_err(KvError::storage)? {
                let user_key = hash_record_key(&row.value).map_err(KvError::storage)?;
                let main = hash_main(&row.key)?;
                if pending && main <= compact {
                    survivors.insert(user_key.clone(), row.key.clone());
                }
                if row.key.len() == HASH.len() + 18 {
                    latest.remove(&user_key);
                } else {
                    latest.insert(user_key, row.key[HASH.len()..].to_vec());
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
        let mut rows = hash_rows(self.applied_state(), revision).await?;
        while let Some(row) = rows.next().await.map_err(KvError::storage)? {
            // A short record key is corrupt data. It fails this request and keeps the member.
            let main = hash_main(&row.key)?;
            let key = &row.key[HASH.len()..];
            let tombstone = key.len() == 18;
            if !exists(&row.key, main) || (main == self.compact_revision && tombstone) {
                continue;
            }
            let recent = main > self.compact_revision;
            let kept = (!recent || empty_hash.is_some()) && retained(&row.key);
            if kept {
                empty_hash = None;
            }
            let append = |crc| -> std::result::Result<u32, KvError> {
                let crc = crc32c::crc32c_append(crc, key);
                Ok(hash_append(crc, &row.value).map_err(KvError::storage)?.0)
            };
            if recent || kept {
                hash = append(hash)?;
            }
            empty_hash = empty_hash.map(append).transpose()?;
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
            hash_record(&kv.key, &kv.encode_to_vec())
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

/// The first byte of a hash digest record.
///
/// A record is either an etcd `KeyValue` encoding (a tombstone) or a digest.
/// A protobuf field number is never zero, so no `KeyValue` encoding, now or after etcd adds
/// fields, starts with a zero byte. The marker does not depend on which fields exist.
const DIGEST: u8 = 0;
/// The digest layout after [`DIGEST`]: version, CRC-32C (big endian), length (big endian), key.
const DIGEST_V1: u8 = 1;
const DIGEST_HEADER: usize = 2 + 4 + 8;

/// Encode the hash record of a live `KeyValue`. `kv` is its etcd wire encoding.
///
/// HashKV feeds the record through [`hash_append`], which gives the same CRC as the full bytes.
/// The record does not repeat the value, so large values are not written a further time.
pub(crate) fn hash_record(key: &[u8], kv: &[u8]) -> Vec<u8> {
    let mut record = Vec::with_capacity(DIGEST_HEADER + key.len());
    record.extend_from_slice(&[DIGEST, DIGEST_V1]);
    record.extend_from_slice(&crc32c::crc32c(kv).to_be_bytes());
    record.extend_from_slice(&(kv.len() as u64).to_be_bytes());
    record.extend_from_slice(key);
    record
}

/// A decoded hash record.
enum HashRecord<'a> {
    /// The CRC-32C and length of a `KeyValue` encoding.
    Digest { key: Vec<u8>, crc: u32, len: u64 },
    /// The full bytes of a `KeyValue` encoding.
    Full(&'a [u8]),
}

fn parse_hash_record(record: &[u8]) -> Result<HashRecord<'_>> {
    if record.first() == Some(&DIGEST) {
        anyhow::ensure!(
            record.len() >= DIGEST_HEADER && record[1] == DIGEST_V1,
            "unsupported hash record"
        );
        return Ok(HashRecord::Digest {
            key: record[DIGEST_HEADER..].to_vec(),
            crc: u32::from_be_bytes(record[2..6].try_into()?),
            len: u64::from_be_bytes(record[6..14].try_into()?),
        });
    }
    Ok(HashRecord::Full(record))
}

/// Return the user key of a hash record.
pub(crate) fn hash_record_key(record: &[u8]) -> Result<Vec<u8>> {
    Ok(match parse_hash_record(record)? {
        HashRecord::Digest { key, .. } => key,
        HashRecord::Full(bytes) => HashUserKey::decode(bytes)?.key,
    })
}

/// Append the bytes that a hash record stands for to `crc`.
/// Returns the new CRC and the number of bytes that it added.
pub(crate) fn hash_append(crc: u32, record: &[u8]) -> Result<(u32, u64)> {
    Ok(match parse_hash_record(record)? {
        HashRecord::Digest { crc: part, len, .. } => {
            (crate::crc::combine(crc, part, usize::try_from(len)?), len)
        }
        HashRecord::Full(bytes) => (crc32c::crc32c_append(crc, bytes), bytes.len() as u64),
    })
}

fn hash_main(key: &[u8]) -> std::result::Result<i64, KvError> {
    // The whole key must have the form of `hash_key`: the main revision, `_`, the
    // subrevision, and an optional tombstone mark.
    let record = key.get(HASH.len()..).unwrap_or_default();
    let valid = match record.len() {
        17 => true,
        18 => record[17] == b't',
        _ => false,
    } && record[8] == b'_';
    match record.get(..8) {
        Some(main) if valid => Ok(i64::from_be_bytes(main.try_into().expect("eight bytes"))),
        _ => Err(KvError::corrupt("invalid hash key")),
    }
}

/// Scan the hash records through `revision`.
async fn hash_rows<R: StateRead>(
    db: &R,
    revision: i64,
) -> std::result::Result<R::Scan<'_>, KvError> {
    // The first key after the last record of `revision` ends the range.
    let mut end = hash_key(revision, i64::MAX, true);
    end.push(0);
    db.scan_range(HASH.to_vec()..end, &mvcc::maintenance_scan())
        .await
        .map_err(KvError::storage)
}

// Rebuild derived quota accounting by row kind when opening stored state.
pub(crate) async fn count_logical_usage(db: &impl StateRead) -> Result<crate::batch::Usage> {
    let mut scan = db.scan_under(b"etcd/", &Default::default()).await?;
    let mut size = crate::batch::Usage::default();
    while let Some(row) = scan.next().await? {
        size.add_row(&row.key, row.value.len())?;
    }
    Ok(size)
}

/// Compact every L0 SST and sorted run into one sorted run, and wait up to 8 s for the end.
/// The compactor of the database writer runs the job. Defragment uses this.
/// Return the version of the manifest that shows compaction is complete.
///
/// `allowed` runs before each submission. A Defragment of a writer whose term ended must not add
/// a job record, because SlateDB checks no writer epoch at a submission.
pub(crate) async fn compact_all(
    admin: &slatedb::admin::Admin,
    allowed: &(dyn Fn() -> Result<()> + Send + Sync),
) -> Result<u64> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(8);
    loop {
        let manifest = admin
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
            return Ok(manifest.id());
        }
        let destination = manifest.compacted().iter().map(|r| r.id).min().unwrap_or(0);
        // SlateDB can reuse the destination run ID, so the input SST views identify the input.
        let input: std::collections::HashSet<_> =
            manifest.all_sst_views().map(|view| view.id).collect();
        allowed()?;
        let job = admin
            .submit_compaction(CompactionSpec::new(sources, destination))
            .await?;
        loop {
            ensure!(
                tokio::time::Instant::now() < deadline,
                "defragmentation timed out"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
            if let Some(job) = admin.read_compaction(job.id(), None).await? {
                match job.status() {
                    // The compactor commits the manifest after the job. A checkpoint after this
                    // call must see the new layout: one sorted run and none of the input SSTs.
                    CompactionStatus::Completed => loop {
                        let manifest = admin
                            .read_manifest(None)
                            .await?
                            .context("missing manifest")?;
                        if manifest.compacted().len() <= 1
                            && !manifest
                                .all_sst_views()
                                .any(|view| input.contains(&view.id))
                        {
                            return Ok(manifest.id());
                        }
                        ensure!(
                            tokio::time::Instant::now() < deadline,
                            "defragmentation timed out"
                        );
                        tokio::time::sleep(Duration::from_millis(20)).await;
                    },
                    CompactionStatus::Failed => break,
                    _ => (),
                }
            } else {
                break;
            }
        }
    }
}

#[cfg(test)]
mod hash_record_tests {
    use super::*;
    use crate::proto::mvccpb::KeyValue;

    #[test]
    fn a_digest_hashes_like_the_full_bytes() {
        let seed = crc32c::crc32c(b"seed");
        for (key, value) in [
            (b"k".to_vec(), vec![]),
            (b"key".to_vec(), vec![7; 300]),
            (vec![0xff; 200], vec![1; 70_000]),
        ] {
            let kv = KeyValue {
                key: key.clone(),
                create_revision: 2,
                mod_revision: 9,
                version: 3,
                value,
                lease: 5,
            }
            .encode_to_vec();
            let expected = (crc32c::crc32c_append(seed, &kv), kv.len() as u64);
            let digest = hash_record(&key, &kv);
            assert_eq!(digest[..2], [DIGEST, DIGEST_V1]);
            for record in [digest, kv.clone()] {
                assert_eq!(hash_append(seed, &record).unwrap(), expected);
                assert_eq!(hash_record_key(&record).unwrap(), key);
            }
        }
        // Tombstones keep the KeyValue bytes with only the key.
        let tombstone = KeyValue {
            key: b"gone".to_vec(),
            ..Default::default()
        }
        .encode_to_vec();
        assert_eq!(hash_record_key(&tombstone).unwrap(), b"gone");
        assert_eq!(
            hash_append(seed, &tombstone).unwrap(),
            (
                crc32c::crc32c_append(seed, &tombstone),
                tombstone.len() as u64
            )
        );
    }

    #[test]
    fn digests_do_not_depend_on_keyvalue_fields() {
        // A protobuf field number is never zero, so no KeyValue encoding starts with DIGEST.
        // This holds for fields that etcd may add later too, unlike a check for fields 14 and 15.
        for field in [1_u32, 5, 6, 7, 14, 15, 16, 1000, (1 << 29) - 1] {
            for wire in [0_u32, 1, 2, 5] {
                let tag = (field << 3) | wire;
                let mut encoded = vec![];
                prost::encoding::encode_varint(u64::from(tag), &mut encoded);
                assert_ne!(encoded[0], DIGEST, "field {field}, wire type {wire}");
            }
        }
        assert!(!KeyValue::default().encode_to_vec().starts_with(&[DIGEST]));
        // A later digest layout fails explicitly instead of being misread.
        let mut later = hash_record(b"k", b"kv");
        later[1] = DIGEST_V1 + 1;
        assert!(hash_append(0, &later).is_err());
        assert!(hash_record_key(&later).is_err());
        assert!(hash_append(0, &[DIGEST, DIGEST_V1, 0]).is_err());
    }
}
