use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use crate::WriteBatch;
use anyhow::{ensure, Context, Result};
use raft::eraftpb::Snapshot;
use serde::{Deserialize, Serialize};
use slatedb::config::Settings;
use slatedb::db_cache::moka::{MokaCache, MokaCacheOptions};
use slatedb::db_cache::{DbCache, SplitCache};

use crate::mvcc::{self, Outcome};
use crate::proto::etcdserverpb::{CompactionRequest, CompactionResponse, RequestOp};
use crate::{Command, Operation, Reply};

mod auth;
pub(crate) mod backup;
mod checkpoint;
pub(crate) mod cluster;
pub(crate) mod compaction;
#[cfg(test)]
pub(crate) mod compaction_tests;
pub(crate) mod current;
mod diagnostic;
pub(crate) mod key_index;
mod kv;
mod lease;
pub(crate) mod overlay;
pub(crate) mod put_batch;
pub(crate) mod quota;
mod read;
pub(crate) mod rows;
#[cfg(test)]
mod rows_tests;
pub(crate) mod shared;
mod snapshot;
mod source;
pub(crate) mod transfer;
pub(crate) mod view;
pub(crate) use read::ReadView;
pub use shared::ViewRetention;
pub use source::{Row, StateRead, StateScan};
use view::StateView;
#[cfg(test)]
mod history_tests;
#[cfg(test)]
mod key_index_tests;
#[cfg(test)]
mod lease_index_tests;
pub(crate) mod maintenance;
#[cfg(test)]
mod quota_tests;
#[cfg(test)]
mod range_tests;

pub(crate) const APPLIED: &[u8] = b"meta/applied";
/// The apply position with its Raft term: `(index, term)`, two big-endian `u64` values.
/// Every state batch writes it with `meta/applied`. Binaries before this row advance
/// `meta/applied` alone, so the term is valid only while its index equals `meta/applied`.
/// Logical images and native backups omit the row. See `read_applied_term`.
pub(crate) const APPLIED_TERM: &[u8] = b"meta/applied-term";
/// The logical size counter. Each state batch updates it with the apply marker.
const LOGICAL_SIZE: &[u8] = b"meta/logical-size";
const DATA: &[u8] = b"data/";

#[cfg(test)]
#[derive(Deserialize, Serialize)]
struct SnapshotData {
    version: u32,
    applied: u64,
    rows: Vec<(Vec<u8>, Vec<u8>)>,
}

pub struct StateMachine {
    pub(crate) metrics: Arc<crate::metrics::StateMeters>,
    pub auth: Arc<crate::auth::Store>,
    pub members: Arc<crate::cluster::Store>,
    pub(crate) admin: Arc<slatedb::admin::Admin>,
    /// The quota that apply uses, from the cluster claim. Zero disables it. See `quota`.
    pub quota: u64,
    /// The effective flag value of this member. It only shows a difference from the claim.
    quota_flag: i64,
    logical_bytes: crate::batch::Usage,
    pub(crate) current: current::CurrentRows,
    pub(crate) key_index: key_index::KeyIndex,
    /// Current rows shared by pinned point reads.
    pub(crate) rows: Arc<rows::RowCache>,
    pub(crate) write_batches: u64,
    pub alarms: Vec<crate::proto::etcdserverpb::AlarmMember>,
    pub applied: u64,
    /// The Raft term of the entry at `applied`. Zero means that the term is unknown: the database
    /// is empty or predates `meta/applied-term`.
    pub applied_term: u64,
    /// The Raft term of the entry that the next state batch marks as applied. The apply worker
    /// sets it before each entry or group, and snapshot install sets it from the snapshot.
    pub(crate) entry_term: u64,
    pub durable_applied: u64,
    pub revision: i64,
    pub compact_revision: i64,
    /// Physical removal that follows the logical boundary.
    pub(crate) compaction: Option<compaction::Progress>,
    /// The next physical compaction step, read in the background.
    pub(crate) compaction_plan: compaction::Planner,
    pub leases: Arc<std::collections::BTreeMap<i64, crate::lease::LeaseRecord>>,
    /// Journal rows of the current apply job for the in-memory Watch feed.
    pub(crate) watch_capture: Option<crate::watch::Capture>,
    /// The object namespace and current database path. Snapshot installs replace the database.
    pub(crate) location: Arc<transfer::Location>,
    /// The stored `etcd/members` row, if any. See `cluster::MembersRow`.
    pub(crate) members_row: Option<cluster::MembersRow>,
    /// The leader of the last `SeedMembers` entry whose row digest differed from this row.
    pub(crate) members_mismatch: Option<u64>,
    /// The term and index of the last `SeedMembers` entry that compared the row.
    pub(crate) members_checked: Option<(u64, u64)>,
    /// The state that apply reads: the overlay and base.
    reads: StateView,
    /// The overlay and base. Only the in-memory state of a test has none.
    pub(crate) shared: Option<shared::Shared>,
}

/// Weight budgets of the state cache. Both are separate from Raft log memory.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StateCache {
    /// Budget for cached SST data blocks.
    pub block_bytes: u64,
    /// Budget for cached SST indexes, filters, and stats.
    pub meta_bytes: u64,
    /// Budget for current rows of linearizable point reads. Zero disables the row cache.
    pub row_bytes: u64,
}

impl Default for StateCache {
    fn default() -> Self {
        Self {
            block_bytes: 64 * 1024 * 1024,
            meta_bytes: 64 * 1024 * 1024,
            row_bytes: 64 * 1024 * 1024,
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
        Arc::new(crate::metrics::reads::Counted(Arc::new(
            SplitCache::new()
                .with_block_cache(moka(self.block_bytes))
                .with_meta_cache(moka(self.meta_bytes))
                .build(),
        )))
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

/// The SST entries that a memtable flush caches: indexes, filters, and current-row data blocks.
///
/// SlateDB inserts every data block of a flushed L0 SST by default. Each flush then filled the
/// block cache with history and Watch journal rows that latest reads do not touch. Puts use the
/// in-memory key index. Range reads at the latest revision read `etcd/current/` rows, so a flush
/// still caches those blocks. Other blocks are cached when a read loads them. A shared-mode base
/// loads the same entries for the L0 SSTs that the member did not flush. See `view::Base::warm`.
pub(crate) fn flush_targets() -> [slatedb::CacheTarget; 3] {
    use slatedb::CacheTarget;
    [
        CacheTarget::data(crate::mvcc::CURRENT..CURRENT_END),
        CacheTarget::Index,
        CacheTarget::Filters,
    ]
}

fn block_cache_policy() -> slatedb::BlockCachePolicy {
    slatedb::BlockCachePolicy::default().with_flush_targets(&flush_targets())
}

/// SlateDB settings for the state machine.
pub(crate) fn slatedb_settings() -> Settings {
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

/// Read the stored logical size, or count it when the counter does not match the apply marker.
///
/// An entry that changes no state writes no batch, so it moves the apply marker past the
/// counter. The next open then counts again. A counter without the split by row kind is
/// counted again too.
async fn load_logical_size(db: &impl StateRead, applied: u64) -> Result<crate::batch::Usage> {
    use prost::Message;
    let stored = match db.get(LOGICAL_SIZE).await? {
        Some(row) => {
            let counter =
                crate::proto::state::LogicalSize::decode(row).context("invalid size counter")?;
            stored_usage(&counter, applied)?
        }
        None => None,
    };
    let Some(size) = stored else {
        return maintenance::count_logical_usage(db).await;
    };
    if cfg!(debug_assertions) {
        let counted = maintenance::count_logical_usage(db).await?;
        ensure!(
            counted == size,
            "stored logical size {size:?} differs from the counted size {counted:?}"
        );
    }
    Ok(size)
}

/// The usage in `counter` if it belongs to `applied` and carries the split by row kind.
fn stored_usage(
    counter: &crate::proto::state::LogicalSize,
    applied: u64,
) -> Result<Option<crate::batch::Usage>> {
    if counter.applied != applied {
        return Ok(None);
    }
    let (Some(current), Some(history), Some(journal), Some(other)) = (
        counter.current,
        counter.history,
        counter.journal,
        counter.other,
    ) else {
        return Ok(None);
    };
    let size = crate::batch::Usage {
        current,
        history,
        journal,
        other,
    };
    ensure!(
        size.checked_total() == Some(counter.bytes),
        "logical size split {size:?} does not add up to {}",
        counter.bytes
    );
    Ok(Some(size))
}

/// Encode the logical size row for `size` after `applied`.
fn logical_size_row(applied: u64, size: crate::batch::Usage) -> Vec<u8> {
    use prost::Message;
    crate::proto::state::LogicalSize {
        applied,
        bytes: size.total(),
        current: Some(size.current),
        history: Some(size.history),
        journal: Some(size.journal),
        other: Some(size.other),
    }
    .encode_to_vec()
}

/// The first key after every `etcd/current/` key.
const CURRENT_END: &[u8] = b"etcd/current0";

impl StateMachine {
    /// Build the derived state from `reads`, the applied state of a shared base.
    async fn load(
        admin: Arc<slatedb::admin::Admin>,
        location: Arc<transfer::Location>,
        rows: Arc<rows::RowCache>,
        reads: StateView,
        shared: Option<shared::Shared>,
    ) -> Result<Self> {
        let mut state = Self::load_rows(admin, location, rows, reads).await?;
        state.shared = shared;
        Ok(state)
    }

    /// Build the derived state from `reads` without the overlay. The caller adds it.
    async fn load_rows(
        admin: Arc<slatedb::admin::Admin>,
        location: Arc<transfer::Location>,
        rows: Arc<rows::RowCache>,
        reads: StateView,
    ) -> Result<Self> {
        let applied = decode_applied(reads.get(APPLIED).await?)?;
        let applied_term = read_applied_term(&reads, applied).await?;
        let revision = read_revision(&reads).await?;
        let compact_revision = read_compact_revision(&reads).await?;
        ensure!(
            compact_revision <= revision,
            "compaction exceeds current revision"
        );
        let compaction = compaction::load(&reads).await?;
        let leases = crate::lease::load(&reads).await?;
        let auth = match reads.get(crate::auth::KEY).await? {
            Some(data) => serde_json::from_slice(&data)?,
            None => crate::auth::Store::default(),
        };
        let alarms = match reads.get(crate::maintenance::ALARMS).await? {
            Some(data) => serde_json::from_slice(&data)?,
            None => vec![],
        };
        let (members, members_row) = cluster::load(&reads, applied).await?;
        // Each batch stores the counter row with the apply marker, so a base needs no full scan.
        let logical_bytes = load_logical_size(&reads, applied).await?;
        Ok(Self {
            members: Arc::new(members),
            admin,
            metrics: Arc::new(crate::metrics::StateMeters::new()?),
            quota: 0,
            quota_flag: 0,
            current: current::CurrentRows::default(),
            key_index: key_index::KeyIndex::load(&reads).await?,
            rows,
            logical_bytes,
            write_batches: 0,
            alarms,
            auth: Arc::new(auth),
            applied,
            applied_term,
            entry_term: applied_term,
            durable_applied: applied,
            revision,
            compact_revision,
            compaction,
            compaction_plan: compaction::Planner::default(),
            leases: Arc::new(leases),
            watch_capture: None,
            location,
            members_row,
            members_mismatch: None,
            members_checked: None,
            reads,
            shared: None,
        })
    }

    /// The fields that `load` builds from the rows, in one text. Two states at one index must
    /// give the same text, whether they applied the entries or loaded a base. The membership is
    /// its row digest, because the in-memory set before the seed is member-local.
    #[cfg(test)]
    pub(crate) fn loaded_fields(&self) -> String {
        format!(
            "{} {} {} {} {:?} {} {} {:?} {:?} {:?} {}",
            self.applied,
            self.applied_term,
            self.revision,
            self.compact_revision,
            self.compaction,
            serde_json::to_string(self.leases.as_ref()).unwrap_or_default(),
            serde_json::to_string(self.auth.as_ref()).unwrap_or_default(),
            self.alarms,
            self.members_row.as_ref().map(|row| &row.digest),
            self.logical_bytes,
            self.key_index.len(),
        )
    }

    /// The applied state that apply and the apply owner read.
    pub(crate) fn applied_state(&self) -> &StateView {
        &self.reads
    }

    /// Open an empty in-memory state for the reference of the deterministic simulation.
    ///
    /// Apply writes each batch into one sorted map at once. The state has no SlateDB database,
    /// no overlay, no base, no materializer, and no checkpoint. Apply uses the rules of shared
    /// state mode, because the members of a shared-mode simulation use them. The checkpoint
    /// namespace is in an unused in-memory object store, and no apply path reads it.
    #[cfg(test)]
    pub(crate) async fn open_memory() -> Result<Self> {
        let store = Arc::new(slatedb::object_store::memory::InMemory::new());
        let location = transfer::Location::new("state", StateCache::default());
        let admin = Arc::new(slatedb::admin::Admin::builder(location.root(), store).build());
        let rows = Arc::new(rows::RowCache::new(0));
        let reads = StateView::Memory(view::MemoryView::default());
        Self::load(admin, location, rows, reads, None).await
    }

    /// Open a state for a test on the in-memory store of `dir`. A later open of the same `dir`
    /// opens the same store, at its newest checkpoint.
    #[cfg(test)]
    pub(crate) async fn open_test(dir: &Path) -> Result<Self> {
        Self::open_test_store(crate::object::test_store(dir), "cluster").await
    }

    /// Open a state for a test at `root` in `store`, as the only voter of its cluster.
    #[cfg(test)]
    pub(crate) async fn open_test_store(
        store: Arc<dyn slatedb::object_store::ObjectStore>,
        root: &str,
    ) -> Result<Self> {
        let cache = StateCache::default();
        Self::open_shared(store, root, "test", cache, &shared::tests::options()).await
    }

    /// Write `batch` into a state from `open_memory`, outside apply. A test changes the rows
    /// with this.
    #[cfg(test)]
    pub(crate) fn write_memory(&mut self, batch: &WriteBatch) {
        let StateView::Memory(rows) = &mut self.reads else {
            panic!("the state is not in memory");
        };
        rows.write(batch);
    }

    /// Store `rows` as the state at the next index, outside apply. They reach no derived state,
    /// so a test stores corrupt rows with this.
    #[cfg(test)]
    pub(crate) fn write_raw<K: AsRef<[u8]>, V: AsRef<[u8]>>(&mut self, rows: &[(K, V)]) {
        let index = self.applied + 1;
        let mut batch = WriteBatch::new();
        for (key, value) in rows {
            batch.put(key.as_ref(), value.as_ref());
        }
        mark_applied(&mut batch, index, self.entry_term);
        let shared = self.shared.as_mut().expect("a shared state");
        shared.overlay.push(index, Arc::new(batch));
        self.applied = index;
    }

    /// Pin the applied state for a read that runs outside apply.
    pub(crate) fn pinned_state(&self) -> Result<StateView> {
        let shared = self
            .shared
            .as_ref()
            .context("the state has no shared view")?;
        Ok(shared.pinned())
    }

    /// Mark `index` applied in `batch`, with the term of its entry from `entry_term`.
    fn mark_applied(&self, batch: &mut WriteBatch, index: u64) {
        mark_applied(batch, index, self.entry_term);
    }

    async fn write_batch(&mut self, batch: WriteBatch, index: u64) -> Result<()> {
        let size = batch
            .logical_size(self.applied_state(), &self.key_index, self.logical_bytes)
            .await
            .map_err(crate::mvcc::storage_fault::read_cause)?;
        let size = self.store_batch(batch, index, size).await?;
        self.logical_bytes = size;
        self.applied_term = self.entry_term;
        Ok(())
    }

    /// Store `batch` and return `size`.
    async fn store_batch(
        &mut self,
        mut batch: WriteBatch,
        index: u64,
        size: crate::batch::Usage,
    ) -> Result<crate::batch::Usage> {
        if batch
            .keys()
            .all(|key| key == APPLIED || key == APPLIED_TERM)
        {
            // An entry that changes no state writes no batch. Otherwise each announcement would
            // dirty the memtable, and its flush would cause the next announcement. The overlay
            // still records that the state at `index` is the state after its newest batch.
            if let Some(shared) = &mut self.shared {
                shared.overlay.cover(index);
            }
            return Ok(size);
        }
        // Drop shared rows before SlateDB may expose the change. See `rows`.
        for key in batch.keys() {
            if let Some(key) = key.strip_prefix(mvcc::CURRENT) {
                self.rows.invalidate(key, index);
            }
        }
        self.put_logical_size(&mut batch, index, size);
        let batch = Arc::new(batch);
        match (&mut self.shared, &mut self.reads) {
            #[cfg(test)]
            (None, StateView::Memory(rows)) => rows.write(&batch),
            (None, _) => anyhow::bail!("the state has no overlay"),
            // Apply reads the overlay at once. The leader materializer writes the `Db` later.
            (Some(shared), _) => shared.overlay.push(index, batch.clone()),
        }
        self.write_batches += 1;
        for (key, value) in batch.rows() {
            if let Some(capture) = &mut self.watch_capture {
                capture.record(key, value);
            }
            self.current.publish(key, value);
            self.key_index.publish(key, value)?;
        }
        self.key_index.commit(index);
        Ok(size)
    }

    /// Store the logical size `size` at `index` in `batch`.
    fn put_logical_size(&self, batch: &mut WriteBatch, index: u64, size: crate::batch::Usage) {
        // The counter is outside `etcd/`, so it does not count itself.
        batch.put(LOGICAL_SIZE, logical_size_row(index, size));
    }

    /// Return the Watch events of the work since `previous`, if capture is enabled.
    pub(crate) fn take_watch_batch(&mut self, previous: i64) -> Option<crate::watch::Batch> {
        let revision = self.revision;
        self.watch_capture.as_mut()?.take(previous, revision)
    }

    pub(crate) async fn close(&self) -> Result<()> {
        // Raft keeps acknowledged writes durable. A member has no writer of its own here.
        Ok(())
    }
}

/// Store the apply position: the index and the Raft term of the last entry of a batch.
fn mark_applied(batch: &mut WriteBatch, index: u64, term: u64) {
    batch.put(APPLIED, index.to_be_bytes());
    batch.put(APPLIED_TERM, applied_term_row(index, term));
}

/// Encode the `meta/applied-term` row.
pub(crate) fn applied_term_row(index: u64, term: u64) -> [u8; 16] {
    let mut row = [0; 16];
    row[..8].copy_from_slice(&index.to_be_bytes());
    row[8..].copy_from_slice(&term.to_be_bytes());
    row
}

pub(crate) use crate::mvcc::storage_fault::stored;

/// Decode the `meta/applied` row. A state without the row has applied nothing.
fn decode_applied(row: Option<bytes::Bytes>) -> Result<u64> {
    Ok(match row {
        Some(bytes) => {
            u64::from_be_bytes(bytes.as_ref().try_into().context("invalid apply marker")?)
        }
        None => 0,
    })
}

/// Read the term of the entry at `applied`. Zero means that the term is unknown: the row is
/// missing, or does not hold the applied index.
async fn read_applied_term(db: &impl StateRead, applied: u64) -> Result<u64> {
    let Some(row) = db.get(APPLIED_TERM).await? else {
        return Ok(0);
    };
    let row: [u8; 16] = row.as_ref().try_into().context("invalid applied term")?;
    let index = u64::from_be_bytes(row[..8].try_into()?);
    let term = u64::from_be_bytes(row[8..].try_into()?);
    Ok(if index == applied { term } else { 0 })
}

async fn read_revision(db: &impl StateRead) -> Result<i64> {
    Ok(match db.get(mvcc::REVISION).await? {
        Some(bytes) => i64::from_be_bytes(bytes.as_ref().try_into().context("invalid revision")?),
        None => 1,
    })
}

async fn read_compact_revision(db: &impl StateRead) -> Result<i64> {
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

/// Return true when a native snapshot or backup can hold `key`.
///
/// This binary never writes a derived meta row to an image or backup. Readers accept the derived
/// rows, and restore drops them.
fn snapshot_key(key: &[u8]) -> bool {
    key.starts_with(DATA) || key.starts_with(b"etcd/") || key == APPLIED || derived_key(key)
}

/// Return true for a meta row that each state derives again: the applied term, the logical
/// size counter, and the progress of a shared restore. Format 4 images and backups omit these
/// rows.
fn derived_key(key: &[u8]) -> bool {
    key == APPLIED_TERM || key == LOGICAL_SIZE || key == shared::PROGRESS
}

fn data_key(key: &str) -> Vec<u8> {
    [DATA, key.as_bytes()].concat()
}

#[cfg(test)]
mod tests;
