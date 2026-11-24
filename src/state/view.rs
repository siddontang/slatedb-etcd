//! The state that apply, read views, Watch catch-up, and the compaction planner read.
//!
//! `StateView` is an enum over the two state modes, not a generic parameter or a trait object.
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
use slatedb::{CacheTarget, Db, DbCacheManagerOps, DbIterator, DbReader, DbReaderMode, DbSnapshot};

use super::overlay::{End, Segment};
use super::{Row, StateRead, StateScan};

/// A read-only state.
#[derive(Clone)]
pub(crate) enum StateView {
    /// The live replicated database. Apply and the replicated compaction planner read it.
    Live(Arc<Db>),
    /// A pinned snapshot of the replicated database.
    Snapshot(Arc<DbSnapshot>),
    /// A shared-mode view.
    Shared(SharedView),
}

/// A checkpoint under shared-mode views. An empty base precedes the first checkpoint.
pub(crate) struct Base {
    reader: Option<DbReader>,
    /// The checkpoint ID. `None` for the empty base.
    pub checkpoint: Option<uuid::Uuid>,
    /// `F`: the index of the last state change in the checkpoint.
    pub applied: u64,
}

impl Base {
    pub(crate) fn empty() -> Self {
        Self {
            reader: None,
            checkpoint: None,
            applied: 0,
        }
    }

    /// Open checkpoint `id` of the database at `root`. The reader writes nothing and does not
    /// poll, and it shares the member's block cache.
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
                ..Default::default()
            })
            .with_db_cache(cache, 0)
            .build()
            .await?;
        let applied = super::decode_applied(reader.get(super::APPLIED).await?)?;
        Ok(Self {
            reader: Some(reader),
            checkpoint: Some(id),
            applied,
        })
    }

    pub(crate) async fn get(&self, key: &[u8]) -> Result<Option<Bytes>, slatedb::Error> {
        match &self.reader {
            Some(reader) => reader.get(key).await,
            None => Ok(None),
        }
    }

    /// Load cache entries for the SSTs of the base that are not in `known`. Return all SSTs of
    /// the base.
    ///
    /// A replicated member caches the entries of `flush_targets` when it flushes an L0 SST, and
    /// SlateDB caches the index and filters of compaction output. A shared-mode member writes no
    /// SST, so without this step each first read of a new block is an object store GET. On a
    /// local MinIO, these cold reads cut point read throughput by more than half and raised the
    /// p99 latency ten times. This step loads the same entries as replicated mode. A failure only
    /// leaves the cache cold. See `docs/shared-state.md`.
    pub(crate) async fn warm(&self, known: &HashSet<SsTableId>) -> HashSet<SsTableId> {
        let Some(reader) = &self.reader else {
            return HashSet::new();
        };
        let manifest = reader.manifest();
        let flushed = super::flush_targets();
        let compacted = [CacheTarget::Index, CacheTarget::Filters];
        let mut ssts = HashSet::new();
        let mut cold = vec![];
        let l0 = manifest.l0().iter().map(|view| (view.sst.id, true));
        let runs = manifest.compacted().iter().flat_map(|run| run.sst_views());
        for (id, flush) in l0.chain(runs.map(|view| (view.sst.id, false))) {
            if ssts.insert(id) && !known.contains(&id) {
                cold.push((id, flush));
            }
        }
        for (id, flush) in cold {
            let targets = if flush { &flushed[..] } else { &compacted[..] };
            if let Err(error) = reader.warm_sst(id, targets).await {
                tracing::warn!(%error, sst = %id.value(), "cannot warm the cache for a base SST");
            }
        }
        ssts
    }
}

/// The message of a read that held its view past the pin limit.
pub(crate) const EXPIRED: &str = "read view expired; retry";

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
                .map_err(|_| slatedb::Error::unavailable(EXPIRED.to_owned()))?,
            None => read.await,
        }
    }

    /// Fail when the read holds the view past its deadline. Gets and each scan step check it.
    fn check(&self) -> Result<(), slatedb::Error> {
        match self.deadline {
            Some(deadline) if std::time::Instant::now() >= deadline => {
                Err(slatedb::Error::unavailable(EXPIRED.to_owned()))
            }
            _ => Ok(()),
        }
    }

    /// Return the first overlay key after `lower` with its newest version at `seq`.
    /// The newest segment wins for equal keys.
    fn next_overlay(&self, lower: Bound<&[u8]>, end: &End<'_>) -> Option<(Bytes, Option<Bytes>)> {
        let mut best: Option<(Bytes, Option<Bytes>)> = None;
        for segment in self.segments.iter().rev() {
            let limit = best.as_ref().map(|(key, _)| key.as_ref());
            if let Some(found) = segment.first_after(lower, end, self.seq, limit) {
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
}

/// Merge a base scan with the overlay rows of a view. An overlay delete hides the base row.
pub(crate) struct SharedScan<'a> {
    view: &'a SharedView,
    start: Vec<u8>,
    limit: Limit,
    base: Option<DbIterator>,
    base_row: Option<Row>,
    /// The last overlay key that the scan used.
    after: Option<Bytes>,
    overlay_row: Option<(Bytes, Option<Bytes>)>,
    overlay_done: bool,
}

impl StateScan for SharedScan<'_> {
    async fn next(&mut self) -> Result<Option<Row>, slatedb::Error> {
        loop {
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
                let lower = match &self.after {
                    Some(key) => Bound::Excluded(key.as_ref()),
                    None => Bound::Included(self.start.as_slice()),
                };
                self.overlay_row = self.view.next_overlay(lower, &self.limit.end());
                self.overlay_done = self.overlay_row.is_none();
            }
            let overlay_first = match (&self.base_row, &self.overlay_row) {
                (None, None) => return Ok(None),
                (Some(_), None) => false,
                (None, Some(_)) => true,
                (Some(base), Some((key, _))) => match key.as_ref().cmp(base.key.as_ref()) {
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
            after: None,
            overlay_row: None,
            overlay_done: false,
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
            after: None,
            overlay_row: None,
            overlay_done: false,
        })
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
            other => other.clone(),
        }
    }
}

/// Return true when an error message comes from a read that held its view too long.
pub(crate) fn expired(message: &str) -> bool {
    message.contains(EXPIRED)
}

/// The rows of a `StateView` scan.
///
/// Both variants hold a SlateDB iterator inline. A scan lives on the stack of one request, and a
/// box would cost an allocation for each scan.
#[allow(clippy::large_enum_variant)]
pub(crate) enum ViewScan<'a> {
    Db(DbIterator),
    Shared(SharedScan<'a>),
}

impl StateScan for ViewScan<'_> {
    async fn next(&mut self) -> Result<Option<Row>, slatedb::Error> {
        match self {
            Self::Db(scan) => StateScan::next(scan).await,
            Self::Shared(scan) => scan.next().await,
        }
    }
}

impl StateRead for StateView {
    type Scan<'a> = ViewScan<'a>;

    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>, slatedb::Error> {
        match self {
            Self::Live(db) => StateRead::get(db.as_ref(), key).await,
            Self::Snapshot(snapshot) => StateRead::get(snapshot.as_ref(), key).await,
            Self::Shared(view) => view.get(key).await,
        }
    }

    async fn scan_range(
        &self,
        range: Range<Vec<u8>>,
        options: &ScanOptions,
    ) -> Result<ViewScan<'_>, slatedb::Error> {
        Ok(match self {
            Self::Live(db) => ViewScan::Db(db.scan_range(range, options).await?),
            Self::Snapshot(snapshot) => ViewScan::Db(snapshot.scan_range(range, options).await?),
            Self::Shared(view) => ViewScan::Shared(view.scan_range(range, options).await?),
        })
    }

    async fn scan_under(
        &self,
        prefix: &[u8],
        options: &ScanOptions,
    ) -> Result<ViewScan<'_>, slatedb::Error> {
        Ok(match self {
            Self::Live(db) => ViewScan::Db(db.scan_under(prefix, options).await?),
            Self::Snapshot(snapshot) => ViewScan::Db(snapshot.scan_under(prefix, options).await?),
            Self::Shared(view) => ViewScan::Shared(view.scan_under(prefix, options).await?),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::super::overlay::Overlay;
    use super::super::source::rows;
    use super::*;
    use slatedb::config::{CheckpointOptions, CheckpointScope, WriteOptions};

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
        let ssts = warm.warm(&HashSet::new()).await;
        assert_eq!(ssts.len(), 1);
        // Known SSTs are not read again.
        assert_eq!(warm.warm(&ssts).await, ssts);

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
}
