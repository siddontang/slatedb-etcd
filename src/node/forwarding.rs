//! Publish the applied revision and current routes for forwarded requests.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::watch;

pub(super) struct Publisher {
    revision: Arc<AtomicI64>,
    leader: Arc<AtomicU64>,
    compact: watch::Receiver<i64>,
    peers: watch::Sender<Arc<BTreeMap<u64, String>>>,
}

/// Read the member routes and the latest locally applied revision.
pub struct ForwardView {
    revision: Arc<AtomicI64>,
    leader: Arc<AtomicU64>,
    pub(crate) compact: watch::Receiver<i64>,
    pub(crate) peers: watch::Receiver<Arc<BTreeMap<u64, String>>>,
}

impl Publisher {
    pub(super) fn new(
        peers: &BTreeMap<u64, String>,
        revision: i64,
        compact: watch::Receiver<i64>,
    ) -> Self {
        Self {
            revision: Arc::new(AtomicI64::new(revision)),
            leader: Arc::default(),
            compact,
            peers: watch::channel(Arc::new(routes(peers))).0,
        }
    }

    pub(super) fn subscribe(&self) -> ForwardView {
        ForwardView {
            revision: self.revision.clone(),
            leader: self.leader.clone(),
            compact: self.compact.clone(),
            peers: self.peers.subscribe(),
        }
    }

    pub(super) fn apply(&self, revision: i64) {
        self.revision.store(revision, Ordering::Release);
    }

    pub(super) fn set_leader(&self, id: u64) {
        self.leader.store(id, Ordering::Relaxed);
    }

    pub(super) fn update_peers(&self, peers: &BTreeMap<u64, String>) {
        self.peers.send_replace(Arc::new(routes(peers)));
    }
}

impl ForwardView {
    pub(crate) fn leader(&self) -> u64 {
        self.leader.load(Ordering::Relaxed)
    }

    pub(crate) fn applied(&self, revision: i64) -> bool {
        self.revision.load(Ordering::Acquire) >= revision
    }
}

fn routes(peers: &BTreeMap<u64, String>) -> BTreeMap<u64, String> {
    peers
        .iter()
        .map(|(&id, url)| (id, crate::cluster::url(url)))
        .collect()
}
