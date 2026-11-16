use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{ensure, Context, Result};
use raft::eraftpb::Snapshot;
use serde::{Deserialize, Serialize};
use slatedb::config::{Settings, WriteOptions};
use slatedb::object_store::local::LocalFileSystem;
use slatedb::{Db, WriteBatch};

use crate::mvcc::{self, Outcome};
use crate::proto::etcdserverpb::{CompactionRequest, CompactionResponse, RequestOp};
use crate::{Command, Operation, Reply};

const APPLIED: &[u8] = b"meta/applied";
const DATA: &[u8] = b"data/";

#[derive(Deserialize, Serialize)]
struct SnapshotData {
    version: u32,
    applied: u64,
    rows: Vec<(Vec<u8>, Vec<u8>)>,
}

pub struct StateMachine {
    db: Arc<Db>,
    pub applied: u64,
    pub durable_applied: u64,
    pub revision: i64,
    pub compact_revision: i64,
}

impl StateMachine {
    pub async fn open(dir: &Path) -> Result<Self> {
        std::fs::create_dir_all(dir)?;
        let object_store = Arc::new(LocalFileSystem::new_with_prefix(dir)?);
        let mut settings = Settings {
            wal_enabled: false,
            flush_interval: None,
            manifest_poll_interval: Duration::from_millis(100),
            ..Settings::default()
        };
        // Frequent checkpoints need prompt L0 compaction and manifest refreshes.
        if let Some(compactor) = settings.compactor_options.as_mut() {
            compactor.poll_interval = Duration::from_millis(100);
        }
        let db = Db::builder("state", object_store)
            .with_settings(settings)
            .build()
            .await?;
        let applied = match db.get(APPLIED).await? {
            Some(bytes) => {
                u64::from_be_bytes(bytes.as_ref().try_into().context("invalid apply marker")?)
            }
            None => 0,
        };
        ensure!(
            db.snapshot()?.seq() == applied,
            "SlateDB sequence and apply marker differ"
        );
        let revision = read_revision(&db).await?;
        let compact_revision = read_compact_revision(&db).await?;
        ensure!(
            compact_revision <= revision,
            "compaction exceeds current revision"
        );
        Ok(Self {
            db: Arc::new(db),
            applied,
            durable_applied: applied,
            revision,
            compact_revision,
        })
    }

    pub async fn apply(&mut self, index: u64, command: Option<&Command>) -> Result<Reply> {
        ensure!(
            index == self.applied + 1,
            "non-contiguous state machine apply"
        );
        let mut batch = WriteBatch::new();
        let mut values = vec![];
        // A small overlay preserves request order within an atomic multi-operation batch.
        let mut overlay = std::collections::BTreeMap::<String, Option<String>>::new();
        if let Some(command) = command {
            for operation in &command.request.operations {
                match operation {
                    Operation::Put { key, value } => {
                        batch.put(data_key(key), value.as_bytes());
                        overlay.insert(key.clone(), Some(value.clone()));
                        values.push(None);
                    }
                    Operation::Delete { key } => {
                        batch.delete(data_key(key));
                        overlay.insert(key.clone(), None);
                        values.push(None);
                    }
                    Operation::Get { key } => {
                        let value = if let Some(value) = overlay.get(key) {
                            value.clone()
                        } else {
                            self.db
                                .get(data_key(key))
                                .await?
                                .map(|v| String::from_utf8(v.to_vec()))
                                .transpose()?
                        };
                        values.push(value);
                    }
                }
            }
        }
        batch.put(APPLIED, index.to_be_bytes());
        // Raft already made this command durable. Waiting for SlateDB here would wait for L0.
        self.db
            .write_with_options(batch, &WriteOptions { seqnum: index })
            .await?;
        self.applied = index;
        Ok(Reply { index, values })
    }

    pub async fn snapshot_data(&mut self) -> Result<Vec<u8>> {
        let data = self.snapshot_job()?.await?;
        self.durable_applied = self.applied;
        Ok(data)
    }

    pub async fn read_etcd(&self, request: &RequestOp) -> Result<Outcome> {
        let result = mvcc::prepare(&self.db, self.revision, self.compact_revision, request).await;
        if let Err(error) = &result {
            ensure!(
                error.code != tonic::Code::Internal as i32,
                "{}",
                error.message
            );
        }
        Ok(Outcome {
            result: result.map(|(response, _, _)| response),
        })
    }

    pub async fn apply_etcd(&mut self, index: u64, request: &RequestOp) -> Result<Outcome> {
        ensure!(
            index == self.applied + 1,
            "non-contiguous state machine apply"
        );
        let (result, mut batch, revision) =
            match mvcc::prepare(&self.db, self.revision, self.compact_revision, request).await {
                Ok((response, batch, revision)) => (Ok(response), batch, revision),
                Err(error) => {
                    ensure!(
                        error.code != tonic::Code::Internal as i32,
                        "{}",
                        error.message
                    );
                    (Err(error), WriteBatch::new(), self.revision)
                }
            };
        batch.put(APPLIED, index.to_be_bytes());
        self.db
            .write_with_options(batch, &WriteOptions { seqnum: index })
            .await?;
        self.applied = index;
        self.revision = revision;
        Ok(Outcome { result })
    }

    pub async fn apply_compaction(
        &mut self,
        index: u64,
        request: &CompactionRequest,
    ) -> Result<Outcome<CompactionResponse>> {
        ensure!(
            index == self.applied + 1,
            "non-contiguous state machine apply"
        );
        let (result, mut batch) =
            match mvcc::prepare_compaction(&self.db, self.revision, self.compact_revision, request)
                .await
            {
                Ok((response, batch)) => (Ok(response), batch),
                Err(error) => {
                    ensure!(
                        error.code != tonic::Code::Internal as i32,
                        "{}",
                        error.message
                    );
                    (Err(error), WriteBatch::new())
                }
            };
        batch.put(APPLIED, index.to_be_bytes());
        self.db
            .write_with_options(batch, &WriteOptions { seqnum: index })
            .await?;
        self.applied = index;
        if result.is_ok() {
            self.compact_revision = request.revision;
        }
        Ok(Outcome { result })
    }

    /// Pin the applied prefix now, then flush and serialize it outside the Raft loop.
    pub fn snapshot_job(
        &self,
    ) -> Result<impl std::future::Future<Output = Result<Vec<u8>>> + Send + 'static> {
        let snapshot = self.db.snapshot()?;
        let db = Arc::clone(&self.db);
        let applied = self.applied;
        ensure!(
            snapshot.seq() == applied,
            "snapshot does not match the apply marker"
        );
        Ok(async move {
            db.flush().await?;
            let mut rows = vec![];
            let mut iterator = snapshot.scan_prefix(b"", ..).await?;
            while let Some(row) = iterator.next().await? {
                rows.push((row.key.to_vec(), row.value.to_vec()));
            }
            Ok(bincode::serialize(&SnapshotData {
                version: 2,
                applied,
                rows,
            })?)
        })
    }

    pub async fn install_snapshot(&mut self, snapshot: &Snapshot) -> Result<()> {
        let index = snapshot.get_metadata().index;
        if index <= self.applied {
            return Ok(());
        }
        let data: SnapshotData = bincode::deserialize(snapshot.get_data())?;
        ensure!(
            (data.version == 1 || data.version == 2) && data.applied == index,
            "invalid snapshot metadata"
        );
        let mut batch = WriteBatch::new();
        let mut iterator = self.db.scan_prefix(b"", ..).await?;
        while let Some(row) = iterator.next().await? {
            batch.delete(row.key);
        }
        for (key, value) in data.rows {
            ensure!(
                key.starts_with(DATA) || key.starts_with(b"etcd/") || key == APPLIED,
                "invalid snapshot key"
            );
            batch.put(key, value);
        }
        batch.put(APPLIED, index.to_be_bytes());
        self.db
            .write_with_options(batch, &WriteOptions { seqnum: index })
            .await?;
        // The durable Raft snapshot lets a restart repeat this atomic replacement.
        self.db.flush().await?;
        self.applied = index;
        self.durable_applied = index;
        self.revision = read_revision(&self.db).await?;
        self.compact_revision = read_compact_revision(&self.db).await?;
        ensure!(
            self.compact_revision <= self.revision,
            "compaction exceeds current revision"
        );
        Ok(())
    }

    pub async fn local_value(&self, key: &str) -> Result<Option<String>> {
        self.db
            .get(data_key(key))
            .await?
            .map(|value| String::from_utf8(value.to_vec()).map_err(Into::into))
            .transpose()
    }
}

async fn read_revision(db: &Db) -> Result<i64> {
    Ok(match db.get(mvcc::REVISION).await? {
        Some(bytes) => i64::from_be_bytes(bytes.as_ref().try_into().context("invalid revision")?),
        None => 1,
    })
}

async fn read_compact_revision(db: &Db) -> Result<i64> {
    Ok(match db.get(mvcc::COMPACT_REVISION).await? {
        Some(bytes) => i64::from_be_bytes(
            bytes
                .as_ref()
                .try_into()
                .context("invalid compact revision")?,
        ),
        None => -1,
    })
}

fn data_key(key: &str) -> Vec<u8> {
    [DATA, key.as_bytes()].concat()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::etcdserverpb::request_op::Request as Op;
    use crate::proto::etcdserverpb::response_op::Response as Resp;
    use crate::proto::etcdserverpb::{DeleteRangeRequest, PutRequest, RangeRequest};

    fn put(key: &[u8], value: &[u8]) -> RequestOp {
        RequestOp {
            request: Some(Op::RequestPut(PutRequest {
                key: key.to_vec(),
                value: value.to_vec(),
                ..Default::default()
            })),
        }
    }

    async fn read(state: &StateMachine, key: &[u8], revision: i64) -> Outcome {
        state
            .read_etcd(&RequestOp {
                request: Some(Op::RequestRange(RangeRequest {
                    key: key.to_vec(),
                    revision,
                    ..Default::default()
                })),
            })
            .await
            .unwrap()
    }

    async fn history_len(state: &StateMachine) -> usize {
        let mut scan = state.db.scan_prefix(b"etcd/history/", ..).await.unwrap();
        let mut count = 0;
        while scan.next().await.unwrap().is_some() {
            count += 1;
        }
        count
    }

    #[tokio::test]
    async fn compaction_removes_history_and_preserves_boundary_versions() {
        let dir = tempfile::tempdir().unwrap();
        let mut state = StateMachine::open(dir.path()).await.unwrap();
        let delete = RequestOp {
            request: Some(Op::RequestDeleteRange(DeleteRangeRequest {
                key: b"a".to_vec(),
                ..Default::default()
            })),
        };
        for (index, request) in [
            put(b"a", b"first"),
            put(b"b", b"stable"),
            put(b"a", b"second"),
            delete,
            put(b"a", b"third"),
            put(b"a", b"fourth"),
        ]
        .iter()
        .enumerate()
        {
            state
                .apply_etcd(index as u64 + 1, request)
                .await
                .unwrap()
                .result
                .unwrap();
        }
        assert_eq!(history_len(&state).await, 6);
        state
            .apply_compaction(
                7,
                &CompactionRequest {
                    revision: 5,
                    physical: false,
                },
            )
            .await
            .unwrap()
            .result
            .unwrap();
        assert_eq!(history_len(&state).await, 4);
        let deleted = read(&state, b"a", 5).await.result.unwrap();
        assert!(matches!(deleted.response, Some(Resp::ResponseRange(r)) if r.kvs.is_empty()));
        let stable = read(&state, b"b", 5).await.result.unwrap();
        assert!(matches!(stable.response, Some(Resp::ResponseRange(r))
            if r.kvs[0].value == b"stable" && r.kvs[0].mod_revision == 3));
        let compacted = read(&state, b"a", 4).await.result.unwrap_err();
        assert_eq!(compacted.code, tonic::Code::OutOfRange as i32);
        let future = state
            .apply_compaction(
                8,
                &CompactionRequest {
                    revision: 8,
                    physical: false,
                },
            )
            .await
            .unwrap();
        assert!(future.result.is_err());
        assert_eq!(state.compact_revision, 5);
        assert_eq!(history_len(&state).await, 4);
        state
            .apply_compaction(
                9,
                &CompactionRequest {
                    revision: 7,
                    physical: false,
                },
            )
            .await
            .unwrap()
            .result
            .unwrap();
        assert_eq!(state.revision, 7);
        assert_eq!(history_len(&state).await, 2);
        let current = read(&state, b"a", 7).await.result.unwrap();
        assert!(matches!(current.response, Some(Resp::ResponseRange(r))
            if r.kvs[0].value == b"fourth" && r.kvs[0].create_revision == 6));
        state.db.flush().await.unwrap();
        state.db.close().await.unwrap();
        drop(state);
        let state = StateMachine::open(dir.path()).await.unwrap();
        assert_eq!(
            (state.applied, state.revision, state.compact_revision),
            (9, 7, 7)
        );
        assert_eq!(history_len(&state).await, 2);
        assert!(read(&state, b"a", 6).await.result.is_err());
        assert!(read(&state, b"a", 7).await.result.is_ok());
    }

    #[tokio::test]
    async fn snapshot_keeps_the_boundary_at_its_pinned_apply_position() {
        let dir = tempfile::tempdir().unwrap();
        let other = tempfile::tempdir().unwrap();
        let mut state = StateMachine::open(dir.path()).await.unwrap();
        state
            .apply_etcd(1, &put(b"key", b"first"))
            .await
            .unwrap()
            .result
            .unwrap();
        state
            .apply_etcd(2, &put(b"key", b"second"))
            .await
            .unwrap()
            .result
            .unwrap();
        state
            .apply_compaction(
                3,
                &CompactionRequest {
                    revision: 2,
                    physical: false,
                },
            )
            .await
            .unwrap()
            .result
            .unwrap();
        let snapshot_job = state.snapshot_job().unwrap();
        state
            .apply_compaction(
                4,
                &CompactionRequest {
                    revision: 3,
                    physical: false,
                },
            )
            .await
            .unwrap()
            .result
            .unwrap();
        let mut snapshot = Snapshot::default();
        snapshot.mut_metadata().index = 3;
        snapshot.data = snapshot_job.await.unwrap().into();
        let mut restored = StateMachine::open(other.path()).await.unwrap();
        restored.install_snapshot(&snapshot).await.unwrap();
        assert_eq!((restored.applied, restored.compact_revision), (3, 2));
        assert_eq!(history_len(&restored).await, 2);
        assert_eq!(history_len(&state).await, 1);
        assert!(read(&restored, b"key", 2).await.result.is_ok());
        assert!(read(&state, b"key", 2).await.result.is_err());
        restored
            .apply_compaction(
                4,
                &CompactionRequest {
                    revision: 3,
                    physical: false,
                },
            )
            .await
            .unwrap()
            .result
            .unwrap();
        assert_eq!(history_len(&restored).await, 1);
        assert!(read(&restored, b"key", 2).await.result.is_err());
    }
}
