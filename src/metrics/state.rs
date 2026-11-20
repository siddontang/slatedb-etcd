//! Measurements from the state machine and pinned read views.

use anyhow::Result;
use prometheus::{
    core::Collector, exponential_buckets, Histogram, HistogramOpts, HistogramVec, IntCounter,
    IntGauge, Registry,
};

pub(super) fn register<T: Collector + Clone + 'static>(
    registry: &Registry,
    metric: T,
) -> Result<T> {
    registry.register(Box::new(metric.clone()))?;
    Ok(metric)
}

pub(crate) struct StateMeters {
    pub registry: Registry,
    pub puts: IntCounter,
    pub deletes: IntCounter,
    pub put_bytes: IntGauge,
    pub grants: IntCounter,
    pub revokes: IntCounter,
    pub ttl: Histogram,
    pub flush: Histogram,
    pub defrag: Histogram,
    pub defrag_active: IntGauge,
    pub snapshot: Histogram,
    pub hash: Histogram,
    pub hash_rev: Histogram,
    pub range_ok: Histogram,
    pub range_error: Histogram,
}

impl StateMeters {
    pub async fn flush_db(&self, db: &slatedb::Db) -> Result<()> {
        let _timer = self.flush.start_timer();
        db.flush().await?;
        Ok(())
    }

    pub fn new() -> Result<Self> {
        let registry = Registry::new();
        let histogram = |name, help, start, count| -> Result<Histogram> {
            register(
                &registry,
                Histogram::with_opts(
                    HistogramOpts::new(name, help).buckets(exponential_buckets(start, 2.0, count)?),
                )?,
            )
        };
        let ranges = register(
            &registry,
            HistogramVec::new(
                HistogramOpts::new(
                    "etcd_server_range_duration_seconds",
                    "Time spent executing a range.",
                )
                .buckets(exponential_buckets(0.0001, 2.0, 20)?),
                &["success"],
            )?,
        )?;
        Ok(Self {
            puts: register(
                &registry,
                IntCounter::new("etcd_mvcc_put_total", "MVCC Put operations.")?,
            )?,
            deletes: register(
                &registry,
                IntCounter::new("etcd_mvcc_delete_total", "MVCC DeleteRange operations.")?,
            )?,
            put_bytes: register(
                &registry,
                IntGauge::new(
                    "etcd_debugging_mvcc_total_put_size_in_bytes",
                    "Total Put key and value bytes.",
                )?,
            )?,
            grants: register(
                &registry,
                IntCounter::new("etcd_debugging_lease_granted_total", "Granted leases.")?,
            )?,
            revokes: register(
                &registry,
                IntCounter::new("etcd_debugging_lease_revoked_total", "Revoked leases.")?,
            )?,
            ttl: histogram(
                "etcd_debugging_lease_ttl_total",
                "Granted lease TTL in seconds.",
                1.0,
                24,
            )?,
            flush: histogram(
                "etcd_disk_backend_commit_duration_seconds",
                "Time spent flushing state to backend storage.",
                0.001,
                14,
            )?,
            defrag: histogram(
                "etcd_disk_backend_defrag_duration_seconds",
                "Backend defragmentation time.",
                0.1,
                13,
            )?,
            defrag_active: register(
                &registry,
                IntGauge::new(
                    "etcd_disk_defrag_inflight",
                    "Active defragmentation operations.",
                )?,
            )?,
            snapshot: histogram(
                "etcd_disk_backend_snapshot_duration_seconds",
                "Time spent building a backend backup.",
                0.01,
                17,
            )?,
            hash: histogram(
                "etcd_mvcc_hash_duration_seconds",
                "Time spent hashing the backend.",
                0.01,
                15,
            )?,
            hash_rev: histogram(
                "etcd_mvcc_hash_rev_duration_seconds",
                "Time spent hashing an MVCC revision.",
                0.01,
                15,
            )?,
            range_ok: ranges.with_label_values(&["true"]),
            range_error: ranges.with_label_values(&["false"]),
            registry,
        })
    }
}

pub(crate) struct Active(IntGauge);

impl Active {
    pub fn new(gauge: &IntGauge) -> Self {
        gauge.inc();
        Self(gauge.clone())
    }
}

impl Drop for Active {
    fn drop(&mut self) {
        self.0.dec();
    }
}

pub(crate) struct WatchMeters {
    pub watchers: IntGauge,
    pub streams: IntGauge,
    pub events: IntCounter,
}

impl WatchMeters {
    pub fn new(registry: &Registry) -> Result<Self> {
        Ok(Self {
            watchers: register(
                registry,
                IntGauge::new("etcd_debugging_mvcc_watcher_total", "Active watchers.")?,
            )?,
            streams: register(
                registry,
                IntGauge::new(
                    "etcd_debugging_mvcc_watch_stream_total",
                    "Active Watch streams.",
                )?,
            )?,
            events: register(
                registry,
                IntCounter::new(
                    "etcd_debugging_mvcc_events_total",
                    "Events sent to watchers.",
                )?,
            )?,
        })
    }
}

#[derive(Default)]
pub(crate) struct Writes {
    puts: u64,
    bytes: usize,
    pub deletes: u64,
}

impl Writes {
    pub fn put(&mut self, key: &[u8], value: &[u8]) {
        self.puts += 1;
        self.bytes += key.len() + value.len();
    }

    pub fn merge(&mut self, other: Self) {
        self.puts += other.puts;
        self.bytes += other.bytes;
        self.deletes += other.deletes;
    }

    pub fn publish(self, meters: &StateMeters) {
        if self.puts > 0 {
            meters.puts.inc_by(self.puts);
            meters.put_bytes.add(self.bytes as i64);
        }
        if self.deletes > 0 {
            meters.deletes.inc_by(self.deletes);
        }
    }
}
