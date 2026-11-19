//! Export measurements for current peer routes.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use prometheus::{exponential_buckets, Histogram, HistogramOpts};

const RTT_NAME: &str = "etcd_network_peer_round_trip_time_seconds";
const RTT_HELP: &str = "Time from a sampled Raft frame to its transport receipt.";

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
            let options = HistogramOpts::new(RTT_NAME, RTT_HELP)
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
    families: &mut Vec<prometheus::proto::MetricFamily>,
) {
    use super::sample::{append, counter};
    use prometheus::{core::Metric, proto::MetricType};

    let mut sent = Vec::with_capacity(peers.len());
    let mut received = Vec::with_capacity(peers.len());
    let mut failures = Vec::new();
    let mut disconnected = Vec::new();
    let mut times = Vec::new();
    for (id, meter) in peers {
        let label = format!("{id:x}");
        sent.push(counter(
            meter.sent.load(Ordering::Relaxed),
            [("To", &label)],
        ));
        received.push(counter(
            meter.received.load(Ordering::Relaxed),
            [("From", &label)],
        ));
        let lost = meter.failures.load(Ordering::Relaxed);
        if lost > 0 {
            failures.push(counter(lost, [("To", &label)]));
        }
        let changes = meter.disconnected.load(Ordering::Relaxed);
        if changes > 0 {
            disconnected.push(counter(changes, [("Local", local), ("Remote", &label)]));
        }
        if let Some(rtt) = meter.rtt.get() {
            times.push(rtt.metric());
        }
    }
    for (name, help, metrics) in [
        (
            "etcd_network_peer_sent_bytes_total",
            "Raft payload bytes accepted by the peer.",
            sent,
        ),
        (
            "etcd_network_peer_received_bytes_total",
            "Received Raft payload bytes.",
            received,
        ),
        (
            "etcd_network_peer_sent_failures_total",
            "Failed outbound Raft messages.",
            failures,
        ),
        (
            "etcd_network_disconnected_peers_total",
            "Losses of an active peer connection.",
            disconnected,
        ),
    ] {
        append(families, name, help, MetricType::COUNTER, metrics);
    }
    append(families, RTT_NAME, RTT_HELP, MetricType::HISTOGRAM, times);
}
