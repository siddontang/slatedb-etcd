//! Keep the outgoing link to each peer: its message queue, its next dial address, its transport
//! task, its traffic meter, and its health.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

use raft::eraftpb::{Message, MessageType};
use tokio::sync::{mpsc, watch};

use super::Event;
use crate::metrics::PeerTraffic;
use crate::peer::{transport_to, Identity};

/// The peer links of one member. The Raft owner sends through them and reports their results to
/// Raft.
#[derive(Default)]
pub(super) struct PeerLinks {
    senders: BTreeMap<u64, mpsc::Sender<Message>>,
    /// The URL that each peer sender uses on its next dial.
    addresses: BTreeMap<u64, watch::Sender<String>>,
    transports: tokio::task::JoinSet<()>,
    traffic: BTreeMap<u64, Arc<PeerTraffic>>,
    /// For each healthy peer, the time of its first success report after its last failure.
    health: BTreeMap<u64, Instant>,
    /// The peer and the snapshot flag of each message that did not go into a queue.
    failures: Vec<(u64, bool)>,
}

impl PeerLinks {
    /// Links that put messages in the given queues. No transport task reads them. DST and tests
    /// read the queues themselves.
    pub(super) fn manual(senders: BTreeMap<u64, mpsc::Sender<Message>>) -> Self {
        Self {
            senders,
            ..Self::default()
        }
    }

    /// Start one transport task for each peer other than `local`.
    pub(super) fn dial(
        local: u64,
        peers: &BTreeMap<u64, String>,
        identity: &Identity,
        events: &mpsc::Sender<Event>,
        transport: &crate::tls::Transport,
    ) -> Self {
        let mut links = Self::default();
        links.sync(local, peers, identity, events, transport);
        links
    }

    /// Keep a link for each peer in `peers` other than `local`, and stop the others.
    pub(super) fn sync(
        &mut self,
        local: u64,
        peers: &BTreeMap<u64, String>,
        identity: &Identity,
        events: &mpsc::Sender<Event>,
        transport: &crate::tls::Transport,
    ) {
        while self.transports.try_join_next().is_some() {}
        self.traffic.retain(|id, _| peers.contains_key(id));
        self.senders.retain(|id, _| peers.contains_key(id));
        self.addresses.retain(|id, _| peers.contains_key(id));
        for (&id, address) in peers {
            if id == local {
                continue;
            }
            if self.senders.contains_key(&id) {
                // A running sender keeps its connection and dials the new URL after a failure.
                if let Some(sender) = self.addresses.get(&id) {
                    sender.send_if_modified(|old| {
                        let changed = crate::cluster::url(old) != crate::cluster::url(address);
                        if changed {
                            *old = address.clone();
                        }
                        changed
                    });
                    continue;
                }
            }
            let (tx, rx) = mpsc::channel(256);
            self.senders.insert(id, tx);
            self.health.remove(&id);
            let (address_tx, address_rx) = watch::channel(address.clone());
            self.addresses.insert(id, address_tx);
            self.transports.spawn(transport_to(
                id,
                address_rx,
                identity.clone(),
                rx,
                events.clone(),
                transport.clone(),
                self.traffic.entry(id).or_default().clone(),
            ));
        }
    }

    /// Put a message in the queue of its peer. A blocked peer, a peer without a link, and a full
    /// queue count as a failure. See `take_failures`.
    pub(super) fn send(&mut self, message: Message, blocked: &BTreeSet<u64>) {
        let peer = message.to;
        let snapshot = message.get_msg_type() == MessageType::MsgSnapshot;
        let sent = !blocked.contains(&peer)
            && self
                .senders
                .get(&peer)
                .is_some_and(|sender| sender.try_send(message).is_ok());
        if !sent {
            if let Some(meter) = self.traffic.get(&peer) {
                meter.failed(1);
            }
            self.failures.push((peer, snapshot));
        }
    }

    /// Give back the peer and the snapshot flag of each failed send since the last call.
    pub(super) fn take_failures(&mut self) -> Vec<(u64, bool)> {
        std::mem::take(&mut self.failures)
    }

    /// Count the bytes of a frame that came from `peer`.
    pub(super) fn received(&self, peer: u64, bytes: usize) {
        if let Some(meter) = self.traffic.get(&peer) {
            meter.received.fetch_add(bytes as u64, Ordering::Relaxed);
        }
    }

    /// Record the result that the transport task of `peer` reports. A failure after a good send
    /// counts as a disconnect.
    pub(super) fn reported(&mut self, peer: u64, success: bool) {
        if success {
            self.health.entry(peer).or_insert_with(Instant::now);
        } else if self.health.remove(&peer).is_some() {
            if let Some(meter) = self.traffic.get(&peer) {
                meter.disconnected.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// The time of the first success report of `peer` after its last failure, or `None` while
    /// the link is down.
    pub(super) fn connected_since(&self, peer: u64) -> Option<Instant> {
        self.health.get(&peer).copied()
    }

    /// The traffic meter of each peer link.
    pub(super) fn traffic(&self) -> Vec<(u64, Arc<PeerTraffic>)> {
        self.traffic
            .iter()
            .map(|(&id, meter)| (id, meter.clone()))
            .collect()
    }

    /// Stop the transport tasks.
    pub(super) async fn shutdown(&mut self) {
        self.transports.shutdown().await;
    }

    /// Tests: put the messages to `peer` in this queue.
    #[cfg(test)]
    pub(super) fn connect(&mut self, peer: u64, sender: mpsc::Sender<Message>) {
        self.senders.insert(peer, sender);
    }
}
