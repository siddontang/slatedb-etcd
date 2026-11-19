//! Build metric families from one member snapshot.

use prometheus::proto::{Counter, Gauge, LabelPair, Metric, MetricFamily, MetricType};

use super::Sample;

fn labels<const N: usize>(pairs: [(&str, &str); N]) -> Vec<LabelPair> {
    pairs
        .into_iter()
        .map(|(name, value)| {
            let mut pair = LabelPair::default();
            pair.set_name(name.to_owned());
            pair.set_value(value.to_owned());
            pair
        })
        .collect()
}

pub(super) fn gauge<const N: usize>(value: i64, pairs: [(&str, &str); N]) -> Metric {
    let mut gauge = Gauge::default();
    gauge.set_value(value as f64);
    let mut metric = Metric::from_label(labels(pairs));
    metric.set_gauge(gauge);
    metric
}

pub(super) fn counter<const N: usize>(value: u64, pairs: [(&str, &str); N]) -> Metric {
    let mut counter = Counter::default();
    counter.set_value(value as f64);
    let mut metric = Metric::from_label(labels(pairs));
    metric.set_counter(counter);
    metric
}

pub(super) fn append(
    families: &mut Vec<MetricFamily>,
    name: &str,
    help: &str,
    kind: MetricType,
    samples: Vec<Metric>,
) {
    if samples.is_empty() {
        return;
    }
    let mut family = MetricFamily::default();
    family.set_name(name.to_owned());
    family.set_help(help.to_owned());
    family.set_field_type(kind);
    family.set_metric(samples);
    families.push(family);
}

impl Sample {
    pub(super) fn collect(&self, physical: Option<u64>, families: &mut Vec<MetricFamily>) {
        for (name, help, value) in [
            (
                "etcd_server_has_leader",
                "Member knows its leader.",
                i64::from(self.leader != 0),
            ),
            (
                "etcd_server_is_leader",
                "Member is the leader.",
                i64::from(self.leader == self.id),
            ),
            (
                "etcd_server_is_learner",
                "Member is a learner.",
                i64::from(self.learner),
            ),
            (
                "etcd_server_proposals_committed_total",
                "Committed Raft index.",
                self.committed as i64,
            ),
            (
                "etcd_server_proposals_applied_total",
                "Applied Raft index.",
                self.applied as i64,
            ),
            (
                "etcd_server_proposals_pending",
                "Outstanding local proposals.",
                self.pending as i64,
            ),
            (
                "etcd_server_quota_backend_bytes",
                "Backend quota in bytes.",
                self.quota,
            ),
            (
                "etcd_mvcc_db_total_size_in_use_in_bytes",
                "Logical backend bytes in use.",
                self.used as i64,
            ),
            (
                "etcd_debugging_mvcc_current_revision",
                "Current MVCC revision.",
                self.revision,
            ),
            (
                "etcd_debugging_mvcc_compact_revision",
                "Last compacted MVCC revision.",
                self.compact,
            ),
            (
                "etcd_debugging_auth_revision",
                "Current authorization revision.",
                self.auth_revision as i64,
            ),
        ] {
            append(
                families,
                name,
                help,
                MetricType::GAUGE,
                vec![gauge(value, [])],
            );
        }
        if let Some(size) = physical {
            append(
                families,
                "etcd_mvcc_db_total_size_in_bytes",
                "Backend size in bytes.",
                MetricType::GAUGE,
                vec![gauge(size.max(self.used) as i64, [])],
            );
        }
        let local = format!("{:x}", self.id);
        for (name, label, value) in [
            ("etcd_server_id", "server_id", local.as_str()),
            ("etcd_cluster_version", "cluster_version", "3.6"),
            (
                "etcd_server_version",
                "server_version",
                crate::ETCD_API_VERSION,
            ),
        ] {
            append(
                families,
                name,
                "Member identity or supported API version.",
                MetricType::GAUGE,
                vec![gauge(1, [(label, value)])],
            );
        }
        let mut known = Vec::with_capacity(self.peers.len());
        let mut active = Vec::with_capacity(self.peers.len());
        for (peer, connected) in &self.peers {
            let remote = format!("{peer:x}");
            let labels = [("Local", local.as_str()), ("Remote", remote.as_str())];
            known.push(gauge(1, labels));
            if *peer != self.id {
                active.push(gauge(i64::from(*connected), labels));
            }
        }
        for (name, metrics) in [
            ("etcd_network_known_peers", known),
            ("etcd_network_active_peers", active),
        ] {
            append(
                families,
                name,
                "Peer membership or transport activity.",
                MetricType::GAUGE,
                metrics,
            );
        }
        super::peer::collect(&local, &self.traffic, families);
    }
}
