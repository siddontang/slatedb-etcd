//! Counters of the state read path: SlateDB cache lookups, row cache lookups, and point reads.
//!
//! The counters are process-wide. A standalone member has one process, so they describe that
//! member. Embedded members of one process share them. The SlateDB cache outlives a snapshot
//! install, and shared-mode bases share it, so the counters are not part of one state machine.

use std::sync::{Arc, LazyLock};

use anyhow::Result;
use async_trait::async_trait;
use prometheus::{
    exponential_buckets, Histogram, HistogramOpts, IntCounter, IntCounterVec, Opts, Registry,
};
use slatedb::db_cache::{CacheFetch, CacheLoader, CacheLookup, CachedEntry, CachedKey, DbCache};

/// Hit and miss counters of one entry kind.
pub(crate) struct Lookups {
    pub hit: IntCounter,
    pub miss: IntCounter,
}

impl Lookups {
    fn of(vec: &IntCounterVec, kind: &str) -> Self {
        Self {
            hit: vec.with_label_values(&[kind, "hit"]),
            miss: vec.with_label_values(&[kind, "miss"]),
        }
    }

    fn record(&self, hit: bool) {
        if hit {
            self.hit.inc();
        } else {
            self.miss.inc();
        }
    }
}

pub(crate) struct ReadMeters {
    cache: IntCounterVec,
    pub data: Lookups,
    pub index: Lookups,
    pub filter: Lookups,
    pub stats: Lookups,
    rows: IntCounterVec,
    /// Row cache lookups of linearizable and serializable point reads.
    pub row: Lookups,
    /// Storage reads of current rows that the row cache did not serve.
    pub row_read: Histogram,
    shared: IntCounterVec,
    /// Shared-mode point reads that the overlay served.
    pub overlay_reads: IntCounter,
    /// Shared-mode point reads that went to the base checkpoint.
    pub base_reads: IntCounter,
}

impl ReadMeters {
    fn new() -> Result<Self> {
        let cache = IntCounterVec::new(
            Opts::new(
                "slatedb_etcd_state_cache_lookups_total",
                "SlateDB block cache lookups of this process by entry kind and result.",
            ),
            &["kind", "result"],
        )?;
        let rows = IntCounterVec::new(
            Opts::new(
                "slatedb_etcd_row_cache_lookups_total",
                "Current-row cache lookups of point reads in this process, by result.",
            ),
            &["kind", "result"],
        )?;
        let shared = IntCounterVec::new(
            Opts::new(
                "slatedb_etcd_shared_point_reads_total",
                "Shared-mode point reads in this process, by the source that served them.",
            ),
            &["source"],
        )?;
        let row_read = Histogram::with_opts(
            HistogramOpts::new(
                "slatedb_etcd_row_read_duration_seconds",
                "Time of storage reads of current rows that the row cache did not serve.",
            )
            .buckets(exponential_buckets(0.00001, 2.0, 20)?),
        )?;
        Ok(Self {
            data: Lookups::of(&cache, "data"),
            index: Lookups::of(&cache, "index"),
            filter: Lookups::of(&cache, "filter"),
            stats: Lookups::of(&cache, "stats"),
            row: Lookups::of(&rows, "current"),
            overlay_reads: shared.with_label_values(&["overlay"]),
            base_reads: shared.with_label_values(&["base"]),
            cache,
            rows,
            row_read,
            shared,
        })
    }

    /// Add the read counters to a member registry.
    pub(crate) fn register(&self, registry: &Registry) -> Result<()> {
        registry.register(Box::new(self.cache.clone()))?;
        registry.register(Box::new(self.rows.clone()))?;
        registry.register(Box::new(self.row_read.clone()))?;
        registry.register(Box::new(self.shared.clone()))?;
        Ok(())
    }
}

pub(crate) static READS: LazyLock<ReadMeters> =
    LazyLock::new(|| ReadMeters::new().expect("read meter options are valid"));

/// A SlateDB cache that counts the hits and misses of its lookups.
///
/// SlateDB counts them in its own recorder, but each `Db` and `DbReader` has a separate one.
/// This wrapper sees the lookups of the writer and of every shared-mode base.
pub(crate) struct Counted(pub Arc<dyn DbCache>);

fn counted(
    lookups: &Lookups,
    result: Result<Option<CachedEntry>, slatedb::Error>,
) -> Result<Option<CachedEntry>, slatedb::Error> {
    if let Ok(entry) = &result {
        lookups.record(entry.is_some());
    }
    result
}

fn fetched(
    lookups: &Lookups,
    result: Result<CacheFetch, slatedb::Error>,
) -> Result<CacheFetch, slatedb::Error> {
    if let Ok(fetch) = &result {
        lookups.record(fetch.lookup == CacheLookup::Hit);
    }
    result
}

#[async_trait]
impl DbCache for Counted {
    async fn get_block(&self, key: &CachedKey) -> Result<Option<CachedEntry>, slatedb::Error> {
        counted(&READS.data, self.0.get_block(key).await)
    }

    async fn get_index(&self, key: &CachedKey) -> Result<Option<CachedEntry>, slatedb::Error> {
        counted(&READS.index, self.0.get_index(key).await)
    }

    async fn get_filter(&self, key: &CachedKey) -> Result<Option<CachedEntry>, slatedb::Error> {
        counted(&READS.filter, self.0.get_filter(key).await)
    }

    async fn get_stats(&self, key: &CachedKey) -> Result<Option<CachedEntry>, slatedb::Error> {
        counted(&READS.stats, self.0.get_stats(key).await)
    }

    async fn insert(&self, key: CachedKey, value: CachedEntry) {
        self.0.insert(key, value).await
    }

    async fn remove(&self, key: &CachedKey) {
        self.0.remove(key).await
    }

    fn entry_count(&self) -> u64 {
        self.0.entry_count()
    }

    async fn close(&self) -> Result<(), slatedb::Error> {
        self.0.close().await
    }

    async fn flush_scope(&self, db_cache_id: u64) -> Result<(), slatedb::Error> {
        self.0.flush_scope(db_cache_id).await
    }

    async fn flush_to_disk(&self) -> Result<(), slatedb::Error> {
        self.0.flush_to_disk().await
    }

    async fn fetch_block(
        &self,
        key: CachedKey,
        loader: CacheLoader,
    ) -> Result<CacheFetch, slatedb::Error> {
        fetched(&READS.data, self.0.fetch_block(key, loader).await)
    }

    async fn fetch_index(
        &self,
        key: CachedKey,
        loader: CacheLoader,
    ) -> Result<CacheFetch, slatedb::Error> {
        fetched(&READS.index, self.0.fetch_index(key, loader).await)
    }

    async fn fetch_filter(
        &self,
        key: CachedKey,
        loader: CacheLoader,
    ) -> Result<CacheFetch, slatedb::Error> {
        fetched(&READS.filter, self.0.fetch_filter(key, loader).await)
    }

    async fn fetch_stats(
        &self,
        key: CachedKey,
        loader: CacheLoader,
    ) -> Result<CacheFetch, slatedb::Error> {
        fetched(&READS.stats, self.0.fetch_stats(key, loader).await)
    }
}
