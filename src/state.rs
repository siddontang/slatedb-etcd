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
use crate::proto::etcdserverpb::RequestOp;
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
        Ok(Self {
            db: Arc::new(db),
            applied,
            durable_applied: applied,
            revision,
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
        let result = mvcc::prepare(&self.db, self.revision, request).await;
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
            match mvcc::prepare(&self.db, self.revision, request).await {
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

fn data_key(key: &str) -> Vec<u8> {
    [DATA, key.as_bytes()].concat()
}
