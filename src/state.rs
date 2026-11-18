use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use crate::WriteBatch;
use anyhow::{ensure, Context, Result};
use raft::eraftpb::Snapshot;
use serde::{Deserialize, Serialize};
use slatedb::config::{Settings, WriteOptions};
use slatedb::db_cache::moka::{MokaCache, MokaCacheOptions};
use slatedb::object_store::local::LocalFileSystem;
use slatedb::Db;

use crate::mvcc::{self, Outcome};
use crate::proto::etcdserverpb::{CompactionRequest, CompactionResponse, RequestOp};
use crate::{Command, Operation, Reply, STORAGE_FORMAT};

mod auth;
mod backup;
mod checkpoint;
mod cluster;
pub(crate) mod current;
mod diagnostic;
mod kv;
mod lease;
pub(crate) mod put_batch;
mod read;
mod snapshot;
pub(crate) use read::ReadView;
pub(crate) mod maintenance;
#[cfg(test)]
mod quota_tests;

const APPLIED: &[u8] = b"meta/applied";
const DATA: &[u8] = b"data/";

#[derive(Deserialize, Serialize)]
struct SnapshotData {
    version: u32,
    applied: u64,
    rows: Vec<(Vec<u8>, Vec<u8>)>,
}

pub struct StateMachine {
    pub(crate) metrics: Arc<crate::metrics::StateMeters>,
    pub(crate) db: Arc<Db>,
    pub auth: Arc<crate::auth::Store>,
    pub members: Arc<crate::cluster::Store>,
    pub(crate) admin: Arc<slatedb::admin::Admin>,
    pub quota: u64,
    logical_bytes: u64,
    pub(crate) current: current::CurrentRows,
    checkpoint_delta: Option<checkpoint::Delta>,
    pub(crate) write_batches: u64,
    pub alarms: Vec<crate::proto::etcdserverpb::AlarmMember>,
    pub applied: u64,
    pub durable_applied: u64,
    pub revision: i64,
    pub compact_revision: i64,
    pub leases: Arc<std::collections::BTreeMap<i64, crate::lease::LeaseRecord>>,
}

impl StateMachine {
    pub async fn open(dir: &Path) -> Result<Self> {
        std::fs::create_dir_all(dir)?;
        let object_store = Arc::new(LocalFileSystem::new_with_prefix(dir)?);
        Self::open_store(object_store, "state").await
    }

    pub async fn open_store(
        object_store: Arc<dyn slatedb::object_store::ObjectStore>,
        prefix: &str,
    ) -> Result<Self> {
        let mut settings = Settings {
            wal_enabled: false,
            flush_interval: None,
            object_store_max_retries: Some(3),
            manifest_poll_interval: Duration::from_millis(100),
            ..Settings::default()
        };
        // Frequent checkpoints need prompt L0 compaction and manifest refreshes.
        if let Some(compactor) = settings.compactor_options.as_mut() {
            compactor.poll_interval = Duration::from_millis(100);
            compactor.commit_compacted_interval = Duration::from_millis(100);
            if let Some(worker) = compactor.worker.as_mut() {
                worker.compactions_poll_interval = Duration::from_millis(100);
            }
        }
        let admin = slatedb::admin::Admin::builder(prefix, object_store.clone()).build();
        // Keep state cache weights separate from Raft log memory budgets.
        let cache = MokaCache::new_with_opts(MokaCacheOptions {
            max_capacity: 64 * 1024 * 1024,
            time_to_live: None,
            time_to_idle: None,
        });
        let db = Db::builder(prefix, object_store)
            .with_db_cache(Arc::new(cache), 0)
            .with_settings(settings)
            .build()
            .await?;
        let db = Arc::new(db);
        let result = async {
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
            let leases = crate::lease::load(&db).await?;
            let auth = match db.get(crate::auth::KEY).await? {
                Some(data) => serde_json::from_slice(&data)?,
                None => crate::auth::Store::default(),
            };
            let alarms = match db.get(crate::maintenance::ALARMS).await? {
                Some(data) => serde_json::from_slice(&data)?,
                None => vec![],
            };
            let members = match db.get(crate::cluster::KEY).await? {
                Some(data) => serde_json::from_slice(&data)?,
                None => crate::cluster::Store::default(),
            };
            Ok(Self {
                members: Arc::new(members),
                admin: Arc::new(admin),
                metrics: Arc::new(crate::metrics::StateMeters::new()?),
                quota: 0,
                current: current::CurrentRows::default(),
                checkpoint_delta: None,
                logical_bytes: maintenance::count_logical_size(&db).await?,
                write_batches: 0,
                alarms,
                auth: Arc::new(auth),
                db: Arc::clone(&db),
                applied,
                durable_applied: applied,
                revision,
                compact_revision,
                leases: Arc::new(leases),
            })
        }
        .await;
        if result.is_err() {
            let _ = db
                .close_with_options(slatedb::config::CloseOptions { flush_type: None })
                .await;
        }
        result
    }

    async fn write_batch(&mut self, batch: WriteBatch, index: u64) -> Result<()> {
        let size = batch.logical_size(&self.db, self.logical_bytes).await?;
        self.store_batch(batch, index).await?;
        self.logical_bytes = size;
        Ok(())
    }

    async fn store_batch(&mut self, batch: WriteBatch, index: u64) -> Result<()> {
        self.db
            .write_with_options(batch.to_storage(), &WriteOptions { seqnum: index })
            .await?;
        self.write_batches += 1;
        for (key, value) in batch.into_rows() {
            self.current.publish(&key, value.as_ref());
            if let Some(delta) = &mut self.checkpoint_delta {
                if !delta.record(key, value) {
                    self.checkpoint_delta = None;
                }
            }
        }
        Ok(())
    }

    pub(crate) async fn close(&self) -> Result<()> {
        // Raft keeps acknowledged writes durable. Shutdown does not require a new checkpoint.
        self.db
            .close_with_options(slatedb::config::CloseOptions { flush_type: None })
            .await?;
        Ok(())
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
mod tests;
