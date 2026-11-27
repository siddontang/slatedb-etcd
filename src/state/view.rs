//! The state that apply, read views, Watch catch-up, and the compaction planner read.
//!
//! `StateView` is an enum over the views, not a generic parameter or a trait object.
//! A generic `StateMachine<S>` would spread a type parameter through the node, the apply worker,
//! read views, and Watch. A trait object would box a future for each read. The enum costs one
//! match for each call on the hot path.
//!
//! A shared-mode view is `(base, segments, seq)`: a read-only SlateDB checkpoint plus the overlay
//! batches at or below `seq`. See `docs/shared-state.md`.

use std::collections::HashSet;
use std::ops::{Bound, Range};
use std::sync::Arc;

use bytes::Bytes;
use slatedb::config::{DbReaderOptions, ScanOptions};
use slatedb::manifest::SsTableId;
use slatedb::{CacheTarget, DbCacheManagerOps, DbIterator, DbReader, DbReaderMode};

use super::overlay::{End, Segment};
use super::{Row, StateRead, StateScan};

/// A read-only state.
#[derive(Clone)]
pub(crate) enum StateView {
    /// A shared-mode view.
    Shared(SharedView),
    /// The in-memory state of a test. See `StateMachine::open_memory`.
    #[cfg(test)]
    Memory(MemoryView),
}

/// A checkpoint under shared-mode views. An empty base precedes the first checkpoint.
pub(crate) struct Base {
    reader: Option<DbReader>,
    /// The checkpoint ID. `None` for the empty base.
    pub checkpoint: Option<uuid::Uuid>,
    /// `F`: the index of the last state change in the checkpoint.
    pub applied: u64,
    /// The SSTs of the checkpoint. The flag is true for an L0 SST.
    ssts: Vec<(SsTableId, bool)>,
}

impl Base {
    pub(crate) fn empty() -> Self {
        Self {
            reader: None,
            checkpoint: None,
            applied: 0,
            ssts: vec![],
        }
    }

    /// Open checkpoint `id` of the database at `root`. The reader writes nothing and does not
    /// poll, and it shares the member's block cache. A read ends after the retries of the
    /// member's database, so apply and client reads see a storage error instead of a hang.
    pub(crate) async fn open(
        store: Arc<dyn slatedb::object_store::ObjectStore>,
        root: &str,
        id: uuid::Uuid,
        cache: Arc<dyn slatedb::db_cache::DbCache>,
    ) -> anyhow::Result<Self> {
        let reader = DbReader::builder(root.to_owned(), store)
            .with_reader_mode(DbReaderMode::Checkpoint(id))
            .with_options(DbReaderOptions {
                skip_wal_replay: true,
                object_store_max_retries: super::slatedb_settings().object_store_max_retries,
                ..Default::default()
            })
            .with_db_cache(cache, 0)
            .build()
            .await?;
        Self::of(reader, id).await
    }

    /// Open checkpoint `id` for one sequential scan, as a backup does. The reader keeps no
    /// cache, and it ends a read after the retries of the member's database. The default
    /// retries forever while a member pins the checkpoint.
    pub(crate) async fn open_for_scan(
        store: Arc<dyn slatedb::object_store::ObjectStore>,
        root: &str,
        id: uuid::Uuid,
    ) -> anyhow::Result<Self> {
        let reader = DbReader::builder(root.to_owned(), store)
            .with_reader_mode(DbReaderMode::Checkpoint(id))
            .with_options(DbReaderOptions {
                skip_wal_replay: true,
                object_store_max_retries: super::slatedb_settings().object_store_max_retries,
                ..Default::default()
            })
            .with_db_cache_disabled()
            .build()
            .await?;
        Self::of(reader, id).await
    }

    async fn of(reader: DbReader, id: uuid::Uuid) -> anyhow::Result<Self> {
        let applied = super::decode_applied(reader.get(super::APPLIED).await?)
            .map_err(|error| super::stored(format!("checkpoint {id}: {error:#}")))?;
        let manifest = reader.manifest();
        let l0 = manifest.l0().iter().map(|view| (view.sst.id, true));
        let runs = manifest.compacted().iter().flat_map(|run| run.sst_views());
        let mut ssts: Vec<_> = l0.chain(runs.map(|view| (view.sst.id, false))).collect();
        let mut seen = HashSet::new();
        ssts.retain(|(id, _)| seen.insert(*id));
        Ok(Self {
            reader: Some(reader),
            checkpoint: Some(id),
            applied,
            ssts,
        })
    }

    pub(crate) async fn get(&self, key: &[u8]) -> Result<Option<Bytes>, slatedb::Error> {
        match &self.reader {
            Some(reader) => reader.get(key).await,
            None => Ok(None),
        }
    }

    /// The SSTs of the checkpoint. The flag is true for an L0 SST.
    pub(crate) fn ssts(&self) -> &[(SsTableId, bool)] {
        &self.ssts
    }

    /// Load the cache entries of SST `id` that the member would cache if it wrote the SST.
    ///
    /// A writer caches the entries of `flush_targets` when it flushes an L0 SST, and SlateDB
    /// caches the index and filters of compaction output. A member that does not write an SST
    /// has no such entries, so without this step each first read of a new block is an object store GET. On a
    /// local MinIO, these cold reads cut point read throughput by more than half and raised the
    /// p99 latency ten times. See `Source::warm` and `docs/shared-state.md`.
    pub(crate) async fn warm_sst(&self, id: SsTableId, l0: bool) -> Result<(), slatedb::Error> {
        let Some(reader) = &self.reader else {
            return Ok(());
        };
        if l0 {
            reader.warm_sst(id, &super::flush_targets()).await
        } else {
            reader
                .warm_sst(id, &[CacheTarget::Index, CacheTarget::Filters])
                .await
        }
    }
}

/// The message of a read that held its view past the pin limit.
pub(crate) const EXPIRED: &str = "read view expired; retry";

/// The error of a read that held its view past the pin limit. Its source marks the class.
fn expired_error() -> slatedb::Error {
    slatedb::Error::unavailable(EXPIRED.to_owned())
        .with_source(Box::new(crate::storage_fault::Expired))
}

/// A base plus the overlay batches at or below `seq`.
#[derive(Clone)]
pub(crate) struct SharedView {
    pub(super) base: Arc<Base>,
    /// Oldest first.
    pub(super) segments: Arc<[Arc<Segment>]>,
    pub(super) seq: u64,
    /// The end of one read's pin. Each read takes its own deadline on a shared view.
    deadline: Option<std::time::Instant>,
}

impl SharedView {
    pub(crate) fn new(base: Arc<Base>, segments: Arc<[Arc<Segment>]>, seq: u64) -> Self {
        Self {
            base,
            segments,
            seq,
            deadline: None,
        }
    }

    /// Run one base read within the deadline. A slow object read cannot hold the pin longer.
    ///
    /// The read is boxed. The read futures of both modes share one type, and an inline timeout
    /// made them large enough to overflow the stack of deep apply calls.
    async fn bounded<'a, T>(
        &self,
        read: std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<T, slatedb::Error>> + Send + 'a>,
        >,
    ) -> Result<T, slatedb::Error> {
        match self.deadline {
            Some(deadline) => tokio::time::timeout_at(deadline.into(), read)
                .await
                .map_err(|_| expired_error())?,
            None => read.await,
        }
    }

    /// Fail when the read holds the view past its deadline. Gets and each scan step check it.
    fn check(&self) -> Result<(), slatedb::Error> {
        match self.deadline {
            Some(deadline) if std::time::Instant::now() >= deadline => Err(expired_error()),
            _ => Ok(()),
        }
    }

    /// Return the first overlay key after `lower` with its newest version at `seq`.
    ///
    /// The search goes from the oldest segment to the newest, and a newer segment replaces an
    /// equal key. Each segment then searches only up to the best key so far. Only the open
    /// segment holds versions above `seq`, and it comes last. So it skips only the hidden keys
    /// below the next row, and a scan visits each hidden key once.
    fn next_overlay(&self, lower: Bound<&[u8]>, end: &End<'_>) -> Option<(Bytes, Option<Bytes>)> {
        let mut best: Option<(Bytes, Option<Bytes>)> = None;
        for segment in self.segments.iter() {
            let limit = best.as_ref().map(|(key, _)| key.as_ref());
            if let Some(found) = segment.first_after(lower, end, self.seq, limit) {
                best = Some(found);
            }
        }
        best
    }

    /// Return the last overlay key in `[start, upper)` with its newest version at `seq`. Like
    /// `next_overlay`, the search goes from the oldest segment to the newest.
    fn prev_overlay(
        &self,
        start: &[u8],
        upper: Bound<&[u8]>,
        end: &End<'_>,
    ) -> Option<(Bytes, Option<Bytes>)> {
        let mut best: Option<(Bytes, Option<Bytes>)> = None;
        for segment in self.segments.iter() {
            let limit = best.as_ref().map(|(key, _)| key.as_ref());
            if let Some(found) = segment.last_before(start, upper, end, self.seq, limit) {
                best = Some(found);
            }
        }
        best
    }
}

/// The upper end of a scan that the scan owns.
enum Limit {
    Before(Vec<u8>),
    Prefix(Vec<u8>),
}

impl Limit {
    fn end(&self) -> End<'_> {
        match self {
            Self::Before(end) => End::Before(end),
            Self::Prefix(prefix) => End::Prefix(prefix),
        }
    }

    /// The exclusive upper bound of a descending scan. A prefix scan walks down from the end of
    /// the keys and skips the keys past the prefix.
    fn upper(&self) -> Bound<&[u8]> {
        match self {
            Self::Before(end) => Bound::Excluded(end),
            Self::Prefix(_) => Bound::Unbounded,
        }
    }
}

/// Merge a base scan with the overlay rows of a view. An overlay delete hides the base row.
pub(crate) struct SharedScan<'a> {
    view: &'a SharedView,
    start: Vec<u8>,
    limit: Limit,
    base: Option<DbIterator>,
    base_row: Option<Row>,
    /// Rows come in descending key order.
    descending: bool,
    /// The last overlay key that the scan used.
    after: Option<Bytes>,
    overlay_row: Option<(Bytes, Option<Bytes>)>,
    overlay_done: bool,
    /// Merge steps, for `YIELD_EVERY`.
    steps: u32,
}

/// A scan yields to the runtime after this many merge steps. A long run of overlay deletes, or
/// rows that come from memory, then cannot hold the thread, and a cancelled scan ends soon.
const YIELD_EVERY: u32 = 256;

impl StateScan for SharedScan<'_> {
    async fn next(&mut self) -> Result<Option<Row>, slatedb::Error> {
        loop {
            self.steps = self.steps.wrapping_add(1);
            if self.steps.is_multiple_of(YIELD_EVERY) {
                tokio::task::yield_now().await;
            }
            self.view.check()?;
            if self.base_row.is_none() {
                if let Some(base) = &mut self.base {
                    self.base_row = self.view.bounded(Box::pin(StateScan::next(base))).await?;
                    if self.base_row.is_none() {
                        self.base = None;
                    }
                }
            }
            if self.overlay_row.is_none() && !self.overlay_done {
                let end = self.limit.end();
                self.overlay_row = if self.descending {
                    let upper = match &self.after {
                        Some(key) => Bound::Excluded(key.as_ref()),
                        None => self.limit.upper(),
                    };
                    self.view.prev_overlay(&self.start, upper, &end)
                } else {
                    let lower = match &self.after {
                        Some(key) => Bound::Excluded(key.as_ref()),
                        None => Bound::Included(self.start.as_slice()),
                    };
                    self.view.next_overlay(lower, &end)
                };
                self.overlay_done = self.overlay_row.is_none();
            }
            let overlay_first = match (&self.base_row, &self.overlay_row) {
                (None, None) => return Ok(None),
                (Some(_), None) => false,
                (None, Some(_)) => true,
                (Some(base), Some((key, _))) => match self.order(key, &base.key) {
                    std::cmp::Ordering::Less => true,
                    std::cmp::Ordering::Equal => {
                        // The overlay holds a newer version of this row.
                        self.base_row = None;
                        true
                    }
                    std::cmp::Ordering::Greater => false,
                },
            };
            if !overlay_first {
                return Ok(self.base_row.take());
            }
            let (key, value) = self.overlay_row.take().unwrap();
            self.after = Some(key.clone());
            if let Some(value) = value {
                return Ok(Some(Row { key, value }));
            }
        }
    }

    /// Move forward to the first merged row at or after `key`, like `DbIterator::seek`.
    /// A buffered row at or after `key` is kept; the base and overlay never move backward.
    async fn seek(&mut self, key: &[u8]) -> Result<(), slatedb::Error> {
        if self.descending {
            return Err(slatedb::Error::invalid(
                "seek on a descending shared scan".to_owned(),
            ));
        }
        self.view.check()?;
        if self
            .base_row
            .as_ref()
            .is_none_or(|row| row.key.as_ref() < key)
        {
            self.base_row = None;
            if let Some(base) = &mut self.base {
                self.view
                    .bounded(Box::pin(StateScan::seek(base, key)))
                    .await?;
            }
        }
        if self
            .overlay_row
            .as_ref()
            .is_none_or(|(k, _)| k.as_ref() < key)
        {
            self.overlay_row = None;
            if self.after.as_ref().is_none_or(|after| after.as_ref() < key) {
                self.after = None;
                if self.start.as_slice() < key {
                    self.start = key.to_vec();
                }
            }
            self.overlay_done = false;
        }
        Ok(())
    }
}

impl SharedScan<'_> {
    /// Compare two keys in the order of the scan.
    fn order(&self, a: &[u8], b: &[u8]) -> std::cmp::Ordering {
        if self.descending {
            b.cmp(a)
        } else {
            a.cmp(b)
        }
    }
}

impl StateRead for SharedView {
    type Scan<'a> = SharedScan<'a>;

    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>, slatedb::Error> {
        self.check()?;
        let meters = &crate::metrics::reads::READS;
        for segment in self.segments.iter().rev() {
            if let Some(value) = segment.get(key, self.seq) {
                meters.overlay_reads.inc();
                return Ok(value);
            }
        }
        meters.base_reads.inc();
        self.bounded(Box::pin(self.base.get(key))).await
    }

    async fn scan_range(
        &self,
        range: Range<Vec<u8>>,
        options: &ScanOptions,
    ) -> Result<SharedScan<'_>, slatedb::Error> {
        let base = match &self.base.reader {
            Some(reader) => Some(
                self.bounded(Box::pin(reader.scan_with_options(range.clone(), options)))
                    .await?,
            ),
            None => None,
        };
        Ok(SharedScan {
            view: self,
            start: range.start,
            limit: Limit::Before(range.end),
            base,
            base_row: None,
            descending: matches!(options.order, slatedb::IterationOrder::Descending),
            after: None,
            overlay_row: None,
            overlay_done: false,
            steps: 0,
        })
    }

    async fn scan_under(
        &self,
        prefix: &[u8],
        options: &ScanOptions,
    ) -> Result<SharedScan<'_>, slatedb::Error> {
        let base = match &self.base.reader {
            Some(reader) => Some(
                self.bounded(Box::pin(reader.scan_prefix_with_options(
                    prefix,
                    ..,
                    options,
                )))
                .await?,
            ),
            None => None,
        };
        Ok(SharedScan {
            view: self,
            start: prefix.to_vec(),
            limit: Limit::Prefix(prefix.to_vec()),
            base,
            base_row: None,
            descending: matches!(options.order, slatedb::IterationOrder::Descending),
            after: None,
            overlay_row: None,
            overlay_done: false,
            steps: 0,
        })
    }
}

/// The rows of an in-memory state. A clone keeps the rows as they are: the next write copies
/// the map.
#[cfg(test)]
#[derive(Clone, Default)]
pub(crate) struct MemoryView {
    rows: Arc<std::collections::BTreeMap<Bytes, Bytes>>,
}

#[cfg(test)]
impl MemoryView {
    /// Write the puts and deletes of `batch`.
    pub(super) fn write(&mut self, batch: &crate::WriteBatch) {
        let rows = Arc::make_mut(&mut self.rows);
        for (key, value) in batch.rows() {
            match value {
                Some(value) => {
                    rows.insert(key.clone(), value.clone());
                }
                None => {
                    rows.remove(key);
                }
            }
        }
    }

    /// Copy the rows from `start` while `keep` is true, in the order of `options`.
    fn scan(
        &self,
        start: &[u8],
        keep: impl Fn(&[u8]) -> bool,
        options: &ScanOptions,
    ) -> MemoryScan {
        let mut rows: std::collections::VecDeque<Row> = self
            .rows
            .range::<[u8], _>((Bound::Included(start), Bound::Unbounded))
            .take_while(|(key, _)| keep(key))
            .map(|(key, value)| Row {
                key: key.clone(),
                value: value.clone(),
            })
            .collect();
        let descending = matches!(options.order, slatedb::IterationOrder::Descending);
        if descending {
            rows.make_contiguous().reverse();
        }
        MemoryScan { rows, descending }
    }
}

/// The rows of one scan of a `MemoryView`, copied when the scan starts.
#[cfg(test)]
pub(crate) struct MemoryScan {
    rows: std::collections::VecDeque<Row>,
    descending: bool,
}

#[cfg(test)]
impl StateScan for MemoryScan {
    async fn next(&mut self) -> Result<Option<Row>, slatedb::Error> {
        Ok(self.rows.pop_front())
    }

    async fn seek(&mut self, key: &[u8]) -> Result<(), slatedb::Error> {
        if self.descending {
            return Err(slatedb::Error::invalid(
                "seek on a descending memory scan".to_owned(),
            ));
        }
        while self.rows.front().is_some_and(|row| row.key.as_ref() < key) {
            self.rows.pop_front();
        }
        Ok(())
    }
}

#[cfg(test)]
impl StateRead for MemoryView {
    type Scan<'a> = MemoryScan;

    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>, slatedb::Error> {
        Ok(self.rows.get(key).cloned())
    }

    async fn scan_range(
        &self,
        range: Range<Vec<u8>>,
        options: &ScanOptions,
    ) -> Result<MemoryScan, slatedb::Error> {
        let end = range.end;
        Ok(self.scan(&range.start, |key| key < end.as_slice(), options))
    }

    async fn scan_under(
        &self,
        prefix: &[u8],
        options: &ScanOptions,
    ) -> Result<MemoryScan, slatedb::Error> {
        Ok(self.scan(prefix, |key| key.starts_with(prefix), options))
    }
}

impl StateView {
    /// The same state for one read that must end by `deadline`. Only shared views pin a base.
    pub(crate) fn until(&self, deadline: Option<std::time::Instant>) -> Self {
        match self {
            Self::Shared(view) => Self::Shared(SharedView {
                deadline,
                ..view.clone()
            }),
            #[cfg(test)]
            Self::Memory(_) => self.clone(),
        }
    }
}

/// Return true when an error message comes from a read that held its view too long.
#[cfg(test)]
pub(crate) fn expired(message: &str) -> bool {
    message.contains(EXPIRED)
}

/// The rows of a `StateView` scan.
///
/// A shared scan holds its SlateDB iterator inline. It lives on the stack of one request, and a
/// box would cost an allocation for each scan.
#[allow(clippy::large_enum_variant)]
pub(crate) enum ViewScan<'a> {
    Shared(SharedScan<'a>),
    #[cfg(test)]
    Memory(MemoryScan),
}

impl StateScan for ViewScan<'_> {
    async fn next(&mut self) -> Result<Option<Row>, slatedb::Error> {
        match self {
            Self::Shared(scan) => scan.next().await,
            #[cfg(test)]
            Self::Memory(scan) => scan.next().await,
        }
    }

    async fn seek(&mut self, key: &[u8]) -> Result<(), slatedb::Error> {
        match self {
            Self::Shared(scan) => scan.seek(key).await,
            #[cfg(test)]
            Self::Memory(scan) => scan.seek(key).await,
        }
    }
}

impl StateRead for StateView {
    type Scan<'a> = ViewScan<'a>;

    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>, slatedb::Error> {
        match self {
            Self::Shared(view) => view.get(key).await,
            #[cfg(test)]
            Self::Memory(view) => view.get(key).await,
        }
    }

    async fn scan_range(
        &self,
        range: Range<Vec<u8>>,
        options: &ScanOptions,
    ) -> Result<ViewScan<'_>, slatedb::Error> {
        Ok(match self {
            Self::Shared(view) => ViewScan::Shared(view.scan_range(range, options).await?),
            #[cfg(test)]
            Self::Memory(view) => ViewScan::Memory(view.scan_range(range, options).await?),
        })
    }

    async fn scan_under(
        &self,
        prefix: &[u8],
        options: &ScanOptions,
    ) -> Result<ViewScan<'_>, slatedb::Error> {
        Ok(match self {
            Self::Shared(view) => ViewScan::Shared(view.scan_under(prefix, options).await?),
            #[cfg(test)]
            Self::Memory(view) => ViewScan::Memory(view.scan_under(prefix, options).await?),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::super::overlay::Overlay;
    use super::super::source::rows;
    use super::*;
    use slatedb::config::{CheckpointOptions, CheckpointScope, WriteOptions};
    use slatedb::Db;

    fn batch(rows: &[(&str, Option<&str>)]) -> Arc<crate::WriteBatch> {
        let mut batch = crate::WriteBatch::new();
        for (key, value) in rows {
            match value {
                Some(value) => batch.put(key, value),
                None => batch.delete(key),
            }
        }
        Arc::new(batch)
    }

    fn row(key: &str, value: &str) -> Row {
        Row {
            key: Bytes::copy_from_slice(key.as_bytes()),
            value: Bytes::copy_from_slice(value.as_bytes()),
        }
    }

    async fn scan(view: &impl StateRead, range: Range<&str>) -> Vec<Row> {
        let range = range.start.as_bytes().to_vec()..range.end.as_bytes().to_vec();
        let mut scan = view
            .scan_range(range, &ScanOptions::default())
            .await
            .unwrap();
        let mut found = vec![];
        while let Some(row) = scan.next().await.unwrap() {
            found.push(row);
        }
        found
    }

    /// Write `a/1..a/4` at sequence 3 and return a checkpoint base of them.
    async fn checkpoint_base(
        store: Arc<dyn slatedb::object_store::ObjectStore>,
    ) -> (Db, Arc<Base>) {
        let db = Db::builder("state", store.clone())
            .with_settings(super::super::slatedb_settings())
            .build()
            .await
            .unwrap();
        let mut rows = crate::WriteBatch::new();
        for key in ["a/1", "a/2", "a/3", "a/4"] {
            rows.put(key, format!("base-{key}"));
        }
        rows.put(super::super::APPLIED, 3_u64.to_be_bytes());
        db.write_with_options(rows.to_storage(), &WriteOptions { seqnum: 3 })
            .await
            .unwrap();
        db.flush().await.unwrap();
        let id = db
            .create_checkpoint(CheckpointScope::Durable, &CheckpointOptions::default())
            .await
            .unwrap()
            .id;
        let cache = crate::state::StateCache::default().build();
        let base = Base::open(store, "state", id, cache).await.unwrap();
        assert_eq!(base.applied, 3);
        (db, Arc::new(base))
    }

    /// Collect a scan of `range` after `next` rows and a seek to `to`.
    async fn scan_seek(view: &SharedView, range: Range<&str>, next: usize, to: &str) -> Vec<Row> {
        let range = range.start.as_bytes().to_vec()..range.end.as_bytes().to_vec();
        let mut scan = view
            .scan_range(range, &ScanOptions::default())
            .await
            .unwrap();
        let mut found = vec![];
        for _ in 0..next {
            found.push(scan.next().await.unwrap().unwrap());
        }
        scan.seek(to.as_bytes()).await.unwrap();
        while let Some(row) = scan.next().await.unwrap() {
            found.push(row);
        }
        found
    }

    #[tokio::test]
    async fn shared_scans_seek_forward_over_base_and_overlay() {
        let store: Arc<dyn slatedb::object_store::ObjectStore> =
            Arc::new(slatedb::object_store::memory::InMemory::new());
        let (_db, base) = checkpoint_base(store).await;
        let mut overlay = Overlay::new(3);
        overlay.push(
            5,
            batch(&[("a/0", Some("new")), ("a/2", None), ("a/35", Some("mid"))]),
        );
        let view = SharedView::new(base, overlay.segments(), 5);
        // Merged order: a/0 (overlay), a/1, a/3, a/35 (overlay), a/4. a/2 is deleted.
        // Seek from the start skips both sources to the first row at or after the key.
        assert_eq!(
            scan_seek(&view, "a/".."b", 0, "a/3").await,
            vec![
                row("a/3", "base-a/3"),
                row("a/35", "mid"),
                row("a/4", "base-a/4")
            ]
        );
        // A seek between rows lands on the next overlay row.
        assert_eq!(
            scan_seek(&view, "a/".."b", 1, "a/31").await,
            vec![
                row("a/0", "new"),
                row("a/35", "mid"),
                row("a/4", "base-a/4")
            ]
        );
        // After a/0 from the overlay, the base row a/1 is buffered. A seek to a key before it
        // keeps the buffered row instead of seeking the base iterator backward.
        assert_eq!(
            scan_seek(&view, "a/".."b", 1, "a/05").await,
            vec![
                row("a/0", "new"),
                row("a/1", "base-a/1"),
                row("a/3", "base-a/3"),
                row("a/35", "mid"),
                row("a/4", "base-a/4"),
            ]
        );
        // A seek onto a deleted key skips it.
        assert_eq!(
            scan_seek(&view, "a/".."b", 0, "a/2").await,
            vec![
                row("a/3", "base-a/3"),
                row("a/35", "mid"),
                row("a/4", "base-a/4")
            ]
        );
        // A seek past the end yields nothing.
        assert!(scan_seek(&view, "a/".."b", 0, "a/9").await.is_empty());
    }

    #[tokio::test]
    async fn descending_shared_scans_merge_base_and_overlay_in_reverse() {
        let store: Arc<dyn slatedb::object_store::ObjectStore> =
            Arc::new(slatedb::object_store::memory::InMemory::new());
        let (_db, base) = checkpoint_base(store).await;
        let mut overlay = Overlay::new(3);
        overlay.push(5, batch(&[("a/0", Some("old")), ("a/35", Some("mid"))]));
        overlay.close_segment();
        overlay.push(6, batch(&[("a/0", Some("new")), ("a/2", None)]));
        let view = SharedView::new(base, overlay.segments(), 6);
        let options = ScanOptions {
            order: slatedb::IterationOrder::Descending,
            ..ScanOptions::default()
        };
        let mut scan = view
            .scan_range(b"a/".to_vec()..b"a/4".to_vec(), &options)
            .await
            .unwrap();
        let mut found = vec![];
        while let Some(row) = scan.next().await.unwrap() {
            found.push(row);
        }
        // a/4 is past the end, a/2 is deleted, and the newer segment holds a/0.
        assert_eq!(
            found,
            vec![
                row("a/35", "mid"),
                row("a/3", "base-a/3"),
                row("a/1", "base-a/1"),
                row("a/0", "new"),
            ]
        );
    }

    #[tokio::test]
    async fn views_merge_the_overlay_over_a_checkpoint_at_their_sequence() {
        let store: Arc<dyn slatedb::object_store::ObjectStore> =
            Arc::new(slatedb::object_store::memory::InMemory::new());
        let (db, base) = checkpoint_base(store).await;
        let mut overlay = Overlay::new(3);
        overlay.push(5, batch(&[("a/2", None), ("a/0", Some("new"))]));
        let at_five = SharedView::new(base.clone(), overlay.segments(), 5);
        overlay.close_segment();
        overlay.push(
            7,
            batch(&[("a/2", Some("back")), ("a/3", None), ("a/9", Some("x"))]),
        );
        let at_seven = SharedView::new(base.clone(), overlay.segments(), 7);
        let at_three = SharedView::new(base.clone(), overlay.segments(), 3);

        // Point reads take the newest overlay version at the view sequence, then the base.
        assert_eq!(at_five.get(b"a/2").await.unwrap(), None);
        assert_eq!(at_seven.get(b"a/2").await.unwrap(), Some("back".into()));
        assert_eq!(at_three.get(b"a/2").await.unwrap(), Some("base-a/2".into()));
        assert_eq!(at_seven.get(b"a/1").await.unwrap(), Some("base-a/1".into()));
        assert_eq!(at_five.get(b"a/9").await.unwrap(), None);

        // Scans merge both sources key by key. Deletes hide base rows.
        let base_rows = vec![
            row("a/1", "base-a/1"),
            row("a/2", "base-a/2"),
            row("a/3", "base-a/3"),
            row("a/4", "base-a/4"),
        ];
        assert_eq!(scan(&at_three, "a/".."b").await, base_rows);
        assert_eq!(
            scan(&at_five, "a/".."b").await,
            vec![
                row("a/0", "new"),
                row("a/1", "base-a/1"),
                row("a/3", "base-a/3"),
                row("a/4", "base-a/4"),
            ]
        );
        let latest = vec![
            row("a/0", "new"),
            row("a/1", "base-a/1"),
            row("a/2", "back"),
            row("a/4", "base-a/4"),
            row("a/9", "x"),
        ];
        assert_eq!(scan(&at_seven, "a/".."b").await, latest);
        assert_eq!(scan(&at_seven, "a/1".."a/4").await, latest[1..3]);
        let view = StateView::Shared(at_seven);
        assert_eq!(rows(&view, b"a/").await, latest);
        assert_eq!(rows(&view, b"a/0").await, latest[..1]);
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn an_empty_base_reads_only_the_overlay() {
        let mut overlay = Overlay::new(0);
        overlay.push(1, batch(&[("k/1", Some("v")), ("k/2", None)]));
        let view = StateView::Shared(SharedView::new(
            Arc::new(Base::empty()),
            overlay.segments(),
            1,
        ));
        assert_eq!(view.get(b"k/1").await.unwrap(), Some("v".into()));
        assert_eq!(view.get(b"k/2").await.unwrap(), None);
        assert_eq!(rows(&view, b"k/").await, vec![row("k/1", "v")]);
    }

    /// Collect a scan of `view` in `order`.
    async fn scan_all(view: &SharedView, order: slatedb::IterationOrder) -> Vec<Row> {
        let options = ScanOptions {
            order,
            ..ScanOptions::default()
        };
        let mut scan = view.scan_under(b"k/", &options).await.unwrap();
        let mut found = vec![];
        while let Some(row) = scan.next().await.unwrap() {
            found.push(row);
        }
        found
    }

    /// The newer segment wins for an equal key, and its delete hides the older value. Rows that
    /// the open segment gets after the view's sequence stay hidden, in both scan orders.
    #[tokio::test]
    async fn a_newer_segment_wins_for_an_equal_key() {
        let mut overlay = Overlay::new(0);
        overlay.push(
            1,
            batch(&[
                ("k/a", Some("old")),
                ("k/b", Some("old")),
                ("k/c", Some("old")),
            ]),
        );
        overlay.close_segment();
        overlay.push(2, batch(&[("k/a", Some("new")), ("k/b", None)]));
        overlay.push(
            3,
            batch(&[("k/a", Some("late")), ("k/c", None), ("k/d", Some("late"))]),
        );
        let view = SharedView::new(Arc::new(Base::empty()), overlay.segments(), 2);
        let expected = vec![row("k/a", "new"), row("k/c", "old")];
        let ascending = scan_all(&view, slatedb::IterationOrder::Ascending).await;
        assert_eq!(ascending, expected);
        let mut descending = scan_all(&view, slatedb::IterationOrder::Descending).await;
        descending.reverse();
        assert_eq!(descending, expected);
    }

    /// A scan visits each key of each segment about once, also when the open segment holds many
    /// keys above the view's sequence. A search from the newest segment visited the hidden keys
    /// again for each visible key of an older segment.
    #[tokio::test]
    async fn a_scan_visits_hidden_keys_once() {
        const VISIBLE: usize = 2_000;
        const HIDDEN: usize = 4_000;
        let mut overlay = Overlay::new(0);
        let visible: Vec<_> = (0..VISIBLE).map(|n| format!("k/{:06}", n * 2)).collect();
        let rows: Vec<_> = visible
            .iter()
            .map(|key| (key.as_str(), Some("v")))
            .collect();
        overlay.push(1, batch(&rows));
        overlay.close_segment();
        // The open segment has a visible tombstone and many later keys.
        overlay.push(2, batch(&[("k/000000", None)]));
        let hidden: Vec<_> = (0..HIDDEN).map(|n| format!("k/{:06}", n * 2 + 1)).collect();
        let rows: Vec<_> = hidden
            .iter()
            .map(|key| (key.as_str(), Some("late")))
            .collect();
        overlay.push(3, batch(&rows));
        let segments = overlay.segments();
        let view = SharedView::new(Arc::new(Base::empty()), segments.clone(), 2);
        let visits = || {
            segments
                .iter()
                .map(|segment| segment.visits.load(std::sync::atomic::Ordering::Relaxed))
                .sum::<u64>()
        };
        for order in [
            slatedb::IterationOrder::Ascending,
            slatedb::IterationOrder::Descending,
        ] {
            let before = visits();
            let found = scan_all(&view, order).await;
            assert_eq!(found.len(), VISIBLE - 1);
            assert!(found.iter().all(|row| row.value == "v"));
            let visited = visits() - before;
            let bound = 3 * (VISIBLE + HIDDEN) as u64;
            assert!(
                visited <= bound,
                "{visited} visits for {VISIBLE} + {HIDDEN} keys"
            );
        }
    }

    /// A warm base serves current rows of its L0 SSTs from the cache. Other rows still need GETs.
    #[tokio::test]
    async fn a_warm_base_reads_current_rows_without_sst_gets() {
        use crate::object::faults::{FaultStore, FAIL_SST_GET};
        let faults = Arc::new(FaultStore::new(Arc::new(
            slatedb::object_store::memory::InMemory::new(),
        )));
        let store: Arc<dyn slatedb::object_store::ObjectStore> = faults.clone();
        let db = Db::builder("state", store.clone())
            .with_settings(super::super::slatedb_settings())
            .build()
            .await
            .unwrap();
        // Many rows, so the current rows, history rows, and apply marker are in separate blocks.
        let mut rows = crate::WriteBatch::new();
        for i in 0..300 {
            rows.put(format!("etcd/current/{i:04}"), vec![b'c'; 200]);
            rows.put(format!("etcd/history/{i:04}"), vec![b'h'; 200]);
        }
        rows.put(super::super::APPLIED, 3_u64.to_be_bytes());
        db.write_with_options(rows.to_storage(), &WriteOptions { seqnum: 3 })
            .await
            .unwrap();
        db.flush().await.unwrap();
        let id = db
            .create_checkpoint(CheckpointScope::Durable, &CheckpointOptions::default())
            .await
            .unwrap()
            .id;
        let open = || async {
            let cache = crate::state::StateCache::default().build();
            Base::open(store.clone(), "state", id, cache).await.unwrap()
        };
        let (warm, cold) = (open().await, open().await);
        assert_eq!(warm.ssts().len(), 1);
        let (id, l0) = warm.ssts()[0];
        assert!(l0);
        warm.warm_sst(id, l0).await.unwrap();

        faults.set(FAIL_SST_GET);
        for i in [0, 150, 299] {
            let key = format!("etcd/current/{i:04}");
            assert_eq!(warm.get(key.as_bytes()).await.unwrap().unwrap().len(), 200);
            assert!(cold.get(key.as_bytes()).await.is_err());
        }
        assert!(warm.get(b"etcd/history/0150").await.is_err());
        faults.set(0);
        db.close().await.unwrap();
    }
    #[tokio::test]
    async fn memory_scans_follow_the_range_prefix_and_order() {
        let mut view = MemoryView::default();
        let rows = [
            ("a", Some("1")),
            ("b/1", Some("2")),
            ("b/2", Some("3")),
            ("c", Some("4")),
        ];
        view.write(&batch(&rows));
        let pinned = view.clone();
        view.write(&batch(&[("b/1", None), ("b/3", Some("5"))]));
        let keys = |rows: Vec<Row>| rows.into_iter().map(|row| row.key).collect::<Vec<_>>();
        let all = [row("a", "1"), row("b/2", "3"), row("b/3", "5")];
        assert_eq!(scan(&view, "a".."c").await, all);
        assert!(scan(&view, "c".."a").await.is_empty());
        // A clone keeps the rows of its time.
        assert_eq!(pinned.get(b"b/1").await.unwrap(), Some("2".into()));
        assert_eq!(view.get(b"b/1").await.unwrap(), None);
        let descending = ScanOptions {
            order: slatedb::IterationOrder::Descending,
            ..ScanOptions::default()
        };
        let mut scan = view.scan_under(b"b/", &descending).await.unwrap();
        let mut found = vec![];
        while let Some(row) = scan.next().await.unwrap() {
            found.push(row);
        }
        assert_eq!(keys(found), ["b/3", "b/2"]);
        let range = b"a".to_vec()..b"d".to_vec();
        let mut scan = view
            .scan_range(range, &ScanOptions::default())
            .await
            .unwrap();
        scan.seek(b"b/3").await.unwrap();
        let mut found = vec![];
        while let Some(row) = scan.next().await.unwrap() {
            found.push(row);
        }
        assert_eq!(keys(found), ["b/3", "c"]);
    }
}
