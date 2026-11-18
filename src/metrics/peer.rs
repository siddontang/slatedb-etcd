//! Export measurements for current peer routes.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use anyhow::Result;
use prometheus::{exponential_buckets, Histogram, HistogramOpts, IntCounterVec, Opts, Registry};

#[derive(Default)]
pub(crate) struct PeerTraffic {
    pub sent: AtomicU64,
    pub received: AtomicU64,
    failures: AtomicU64,
    pub disconnected: AtomicU64,
    rtt: OnceLock<Histogram>,
}

impl PeerTraffic {
    pub fn failed(&self, messages: usize) {
        self.failures.fetch_add(messages as u64, Ordering::Relaxed);
    }

    pub fn observe_rtt(&self, peer: u64, elapsed: Duration) {
        let meter = self.rtt.get_or_init(|| {
            let options = HistogramOpts::new(
                "etcd_network_peer_round_trip_time_seconds",
                "Time from a sampled Raft frame to its transport receipt.",
            )
            .const_label("To", format!("{peer:x}"))
            .buckets(exponential_buckets(0.0001, 2.0, 16).expect("valid RTT buckets"));
            Histogram::with_opts(options).expect("valid RTT histogram")
        });
        meter.observe(elapsed.as_secs_f64());
    }
}

pub(super) fn collect(
    local: &str,
    peers: &[(u64, Arc<PeerTraffic>)],
    registry: &Registry,
) -> Result<()> {
    let counter = |name, help, label| {
        super::state::register(
            registry,
            IntCounterVec::new(Opts::new(name, help), &[label])?,
        )
    };
    let sent = counter(
        "etcd_network_peer_sent_bytes_total",
        "Raft payload bytes accepted by the peer.",
        "To",
    )?;
    let received = counter(
        "etcd_network_peer_received_bytes_total",
        "Received Raft payload bytes.",
        "From",
    )?;
    let failures = counter(
        "etcd_network_peer_sent_failures_total",
        "Failed outbound Raft messages.",
        "To",
    )?;
    let disconnected = super::state::register(
        registry,
        IntCounterVec::new(
            Opts::new(
                "etcd_network_disconnected_peers_total",
                "Losses of an active peer connection.",
            ),
            &["Local", "Remote"],
        )?,
    )?;
    for (id, meter) in peers {
        let label = format!("{id:x}");
        sent.with_label_values(&[&label])
            .inc_by(meter.sent.load(Ordering::Relaxed));
        received
            .with_label_values(&[&label])
            .inc_by(meter.received.load(Ordering::Relaxed));
        let lost = meter.failures.load(Ordering::Relaxed);
        if lost > 0 {
            failures.with_label_values(&[&label]).inc_by(lost);
        }
        let changes = meter.disconnected.load(Ordering::Relaxed);
        if changes > 0 {
            disconnected
                .with_label_values(&[local, &label])
                .inc_by(changes);
        }
        if let Some(rtt) = meter.rtt.get() {
            registry.register(Box::new(rtt.clone()))?;
        }
    }
    Ok(())
}
