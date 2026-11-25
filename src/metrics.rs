//! Prometheus metrics for one embedded or standalone member.

mod peer;
pub(crate) use peer::PeerTraffic;
pub(crate) mod reads;
mod rpc;
pub(crate) use rpc::{Observation as RpcObservation, ObservedBody, Observer as RpcObserver};
mod sample;
mod state;
pub(crate) use state::{Active, StateMeters, WatchMeters, Writes};

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use axum::extract::State;
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use prometheus::{
    exponential_buckets, Encoder, Histogram, HistogramOpts, IntCounter, IntGauge, Opts, Registry,
    TextEncoder,
};
use tokio::sync::{mpsc, oneshot};

use crate::node::Event;

#[derive(Clone)]
pub(crate) struct Wal {
    pub fsync: Histogram,
    pub write: Histogram,
    pub bytes: IntGauge,
}

impl Wal {
    pub fn new() -> Result<Self> {
        let histogram = |name, help| {
            Histogram::with_opts(
                HistogramOpts::new(name, help).buckets(exponential_buckets(0.001, 2.0, 14)?),
            )
        };
        Ok(Self {
            fsync: histogram(
                "etcd_disk_wal_fsync_duration_seconds",
                "Time in WAL file sync calls.",
            )?,
            write: histogram(
                "etcd_disk_wal_write_duration_seconds",
                "Time in WAL file writes.",
            )?,
            bytes: IntGauge::new(
                "etcd_disk_wal_write_bytes_total",
                "Bytes written to WAL files.",
            )?,
        })
    }
}

pub(crate) struct Member {
    registry: Registry,
    admin: std::sync::RwLock<Arc<slatedb::admin::Admin>>,
    permits: Arc<tokio::sync::Semaphore>,
    scraping: IntGauge,
    scraped: IntCounter,
    unavailable: IntCounter,
    pub leader_changes: IntCounter,
    pub failed_proposals: IntCounter,
    pub failed_read_indexes: IntCounter,
}

/// Raft and applied state from one turn of the member's event loop.
pub struct Sample {
    pub(crate) id: u64,
    pub(crate) leader: u64,
    pub(crate) learner: bool,
    pub(crate) committed: u64,
    pub(crate) applied: u64,
    pub(crate) pending: usize,
    pub(crate) quota: i64,
    /// Logical bytes by row kind. The total is the quota usage.
    pub(crate) usage: crate::batch::Usage,
    /// The quota usage that raises the early warning. `None` disables the warning.
    pub(crate) warning_bytes: Option<u64>,
    pub(crate) revision: i64,
    pub(crate) compact: i64,
    pub(crate) auth_revision: u64,
    pub(crate) peers: Vec<(u64, bool)>,
    pub(crate) traffic: Vec<(u64, Arc<PeerTraffic>)>,
}

impl Member {
    pub fn new(wal: Wal, admin: Arc<slatedb::admin::Admin>, registry: Registry) -> Result<Self> {
        registry.register(Box::new(wal.fsync))?;
        registry.register(Box::new(wal.write))?;
        registry.register(Box::new(wal.bytes))?;
        #[cfg(target_os = "linux")]
        registry.register(Box::new(
            prometheus::process_collector::ProcessCollector::for_self(),
        ))?;
        let scraping = state::register(
            &registry,
            IntGauge::new(
                "promhttp_metric_handler_requests_in_flight",
                "Scrapes in progress.",
            )?,
        )?;
        let scrapes = state::register(
            &registry,
            prometheus::IntCounterVec::new(
                Opts::new(
                    "promhttp_metric_handler_requests_total",
                    "Completed scrapes by HTTP status.",
                ),
                &["code"],
            )?,
        )?;
        scrapes.with_label_values(&["500"]);
        let counter = |name, help| -> Result<IntCounter> {
            let value = IntCounter::new(name, help)?;
            registry.register(Box::new(value.clone()))?;
            Ok(value)
        };
        Ok(Self {
            leader_changes: counter(
                "etcd_server_leader_changes_seen_total",
                "Leader changes seen.",
            )?,
            failed_proposals: counter(
                "etcd_server_proposals_failed_total",
                "Failed consensus proposals.",
            )?,
            failed_read_indexes: counter(
                "etcd_server_read_indexes_failed_total",
                "Failed Raft read proofs.",
            )?,
            registry,
            admin: std::sync::RwLock::new(admin),
            permits: Arc::new(tokio::sync::Semaphore::new(2)),
            scraping,
            scraped: scrapes.with_label_values(&["200"]),
            unavailable: scrapes.with_label_values(&["503"]),
        })
    }

    /// Read the manifest of the database that a snapshot install opened.
    pub fn rebind(&self, admin: Arc<slatedb::admin::Admin>) {
        *self.admin.write().unwrap() = admin;
    }

    fn admin(&self) -> Arc<slatedb::admin::Admin> {
        self.admin.read().unwrap().clone()
    }

    /// Register the per-RPC and watch meters of the client listener.
    pub(crate) fn rpc(
        &self,
        level: crate::server::MetricsLevel,
    ) -> Result<(Arc<WatchMeters>, Arc<RpcObserver>)> {
        let watch = Arc::new(WatchMeters::new(&self.registry)?);
        let rpc = rpc::Observer::new(&self.registry, level)?;
        Ok((watch, Arc::new(rpc)))
    }

    pub fn scrape_router(self: &Arc<Self>, events: mpsc::Sender<Event>) -> axum::Router {
        axum::Router::new()
            .route("/metrics", axum::routing::get(scrape))
            .with_state((self.clone(), events))
    }
}

async fn scrape(State((member, events)): State<(Arc<Member>, mpsc::Sender<Event>)>) -> Response {
    let _active = Active::new(&member.scraping);
    let collect_member = member.clone();
    let collect = async move {
        let member = collect_member;
        let permit = member.permits.clone().try_acquire_owned()?;
        let (tx, rx) = oneshot::channel();
        events
            .send(Event::Metrics(tx))
            .await
            .map_err(|_| anyhow::anyhow!("member stopped"))?;
        let sample = rx.await?;
        let physical = tokio::time::timeout(
            Duration::from_secs(1),
            crate::state::StateMachine::manifest_size(&member.admin()),
        )
        .await
        .ok()
        .and_then(Result::ok);
        tokio::task::spawn_blocking(move || -> Result<Vec<u8>> {
            let _permit = permit;
            let mut families = member.registry.gather();
            sample.collect(physical, &mut families);
            let mut body = Vec::new();
            TextEncoder::new().encode(&families, &mut body)?;
            Ok(body)
        })
        .await?
    };
    match tokio::time::timeout(Duration::from_secs(5), collect).await {
        Ok(Ok(body)) => {
            member.scraped.inc();
            (
                [(
                    header::CONTENT_TYPE,
                    "text/plain; version=0.0.4; charset=utf-8",
                )],
                body,
            )
                .into_response()
        }
        _ => {
            member.unavailable.inc();
            (StatusCode::SERVICE_UNAVAILABLE, "metrics unavailable\n").into_response()
        }
    }
}
