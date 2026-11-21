use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use crate::WriteBatch;
use anyhow::{ensure, Context, Result};
use raft::eraftpb::Snapshot;
use serde::{Deserialize, Serialize};
use slatedb::config::{Settings, WriteOptions};
use slatedb::db_cache::moka::{MokaCache, MokaCacheOptions};
use slatedb::db_cache::{DbCache, SplitCache};
use slatedb::object_store::local::LocalFileSystem;
use slatedb::Db;

use crate::mvcc::{self, Outcome};
use crate::proto::etcdserverpb::{CompactionRequest, CompactionResponse, RequestOp};
use crate::{Command, Operation, Reply, STORAGE_FORMAT};

mod auth;
mod backup;
mod checkpoint;
mod cluster;
pub(crate) mod compaction;
#[cfg(test)]
pub(crate) mod compaction_tests;
pub(crate) mod current;
mod diagnostic;
pub(crate) mod key_index;
mod kv;
mod lease;
pub(crate) mod put_batch;
mod read;
mod snapshot;
pub(crate) use read::ReadView;
#[cfg(test)]
mod key_index_tests;
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
    pub(crate) key_index: key_index::KeyIndex,
    pub(crate) write_batches: u64,
    pub alarms: Vec<crate::proto::etcdserverpb::AlarmMember>,
    pub applied: u64,
    pub durable_applied: u64,
    pub revision: i64,
    pub compact_revision: i64,
    /// Physical removal that follows the logical boundary.
    pub(crate) compaction: Option<compaction::Progress>,
    pub leases: Arc<std::collections::BTreeMap<i64, crate::lease::LeaseRecord>>,
    /// Journal rows of the current apply job for the in-memory Watch feed.
    pub(crate) watch_capture: Option<crate::watch::Capture>,
}

/// Weight budgets of the state cache. Both are separate from Raft log memory.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StateCache {
    /// Budget for cached SST data blocks.
    pub block_bytes: u64,
    /// Budget for cached SST indexes, filters, and stats.
    pub meta_bytes: u64,
}

impl Default for StateCache {
    fn default() -> Self {
        Self {
            block_bytes: 64 * 1024 * 1024,
            meta_bytes: 64 * 1024 * 1024,
        }
    }
}

impl StateCache {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.block_bytes > 0 && self.meta_bytes > 0,
            "state cache budgets must be positive"
        );
        Ok(())
    }

    /// Build the cache. SST metadata needs its own cache.
    ///
    /// Moka admits a new entry only when its frequency exceeds the summed frequency of the
    /// entries it would evict. An SST index is often several MiB and would displace hundreds
    /// of 4 KiB data blocks. In a shared cache that is full of blocks, indexes and filters are
    /// therefore always rejected, and every scan reads and verifies them again.
    pub(crate) fn build(self) -> Arc<dyn DbCache> {
        let moka = |max_capacity| {
            Some(Arc::new(MokaCache::new_with_opts(MokaCacheOptions {
                max_capacity,
                time_to_live: None,
                time_to_idle: None,
            })) as Arc<dyn DbCache>)
        };
        Arc::new(
            SplitCache::new()
                .with_block_cache(moka(self.block_bytes))
                .with_meta_cache(moka(self.meta_bytes))
                .build(),
        )
    }
}

/// Read-ahead buffers per compaction input SST.
const COMPACTION_FETCH_TASKS: usize = 2;
/// Bytes of each compaction read-ahead buffer.
const COMPACTION_FETCH_BYTES: usize = 512 * 1024;

/// Subcompactions per compaction job. SlateDB defaults to 4.
///
/// Each subcompaction holds one SST writer, which buffers up to 90 MiB of SST data until the
/// paced store writes it, and the read-ahead of every input SST. With 4 jobs of 4
/// subcompactions this held about 185 MiB in put-v4096 on a server with 4 CPU threads.
const SUBCOMPACTIONS: usize = 2;

/// Cache SST indexes, filters, and current-row data blocks after a memtable flush.
///
/// SlateDB inserts every data block of a flushed L0 SST by default. Each flush then filled the
/// block cache with history and Watch journal rows that latest reads do not touch. Puts use the
/// in-memory key index. Range reads at the latest revision read `etcd/current/` rows, so a flush
/// still caches those blocks. Other blocks are cached when a read loads them.
fn block_cache_policy() -> slatedb::BlockCachePolicy {
    use slatedb::CacheTarget;
    slatedb::BlockCachePolicy::default().with_flush_targets(&[
        CacheTarget::data(crate::mvcc::CURRENT..CURRENT_END),
        CacheTarget::Index,
        CacheTarget::Filters,
    ])
}

/// SlateDB settings for the state machine.
fn slatedb_settings() -> Settings {
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
            worker.max_subcompactions = SUBCOMPACTIONS;
            // Each input SST of each subcompaction reads ahead `max_fetch_tasks` buffers
            // of `bytes_to_fetch`. The 4 x 2 MiB default held about 256 MiB with 8 L0
            // inputs and 4 subcompactions. Local objects need little read-ahead.
            worker.max_fetch_tasks = COMPACTION_FETCH_TASKS;
            worker.bytes_to_fetch = COMPACTION_FETCH_BYTES;
        }
    }
    settings
}

/// The first key after every `etcd/current/` key.
const CURRENT_END: &[u8] = b"etcd/current0";

impl StateMachine {
    pub async fn open(dir: &Path) -> Result<Self> {
        Self::open_with_cache(dir, StateCache::default()).await
    }

    pub async fn open_with_cache(dir: &Path, cache: StateCache) -> Result<Self> {
        std::fs::create_dir_all(dir)?;
        // Log truncation relies on flushed state. Sync local objects like a remote object store,
        // and pace SST writeback so a large flush does not stall Raft log syncs.
        let local = LocalFileSystem::new_with_prefix(dir)?.with_fsync(true);
        #[cfg(target_os = "linux")]
        let object_store = Arc::new(crate::object::paced::PacedStore::new(local));
        #[cfg(not(target_os = "linux"))]
        let object_store = Arc::new(local);
        Self::open_store_with_cache(object_store, "state", cache).await
    }

    pub async fn open_store(
        object_store: Arc<dyn slatedb::object_store::ObjectStore>,
        prefix: &str,
    ) -> Result<Self> {
        Self::open_store_with_cache(object_store, prefix, StateCache::default()).await
    }

    pub async fn open_store_with_cache(
        object_store: Arc<dyn slatedb::object_store::ObjectStore>,
        prefix: &str,
        cache: StateCache,
    ) -> Result<Self> {
        cache.validate()?;
        let cache = cache.build();
        let settings = slatedb_settings();
        let admin = slatedb::admin::Admin::builder(prefix, object_store.clone()).build();
        let db = Db::builder(prefix, object_store)
            .with_db_cache(cache, 0)
            .with_settings(settings)
            .with_block_cache_policy(block_cache_policy())
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
            let compaction = compaction::load(&db).await?;
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
                key_index: key_index::KeyIndex::load(&db).await?,
                logical_bytes: maintenance::count_logical_size(&db).await?,
                write_batches: 0,
                alarms,
                auth: Arc::new(auth),
                db: Arc::clone(&db),
                applied,
                durable_applied: applied,
                revision,
                compact_revision,
                compaction,
                leases: Arc::new(leases),
                watch_capture: None,
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
        let size = batch
            .logical_size(&self.db, &self.key_index, self.logical_bytes)
            .await?;
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
            if let Some(capture) = &mut self.watch_capture {
                capture.record(&key, value.as_ref());
            }
            self.current.publish(&key, value.as_ref());
            self.key_index.publish(&key, value.as_ref())?;
        }
        self.key_index.commit(index);
        Ok(())
    }

    /// Return the Watch events of the work since `previous`, if capture is enabled.
    pub(crate) fn take_watch_batch(&mut self, previous: i64) -> Option<crate::watch::Batch> {
        let revision = self.revision;
        self.watch_capture.as_mut()?.take(previous, revision)
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
