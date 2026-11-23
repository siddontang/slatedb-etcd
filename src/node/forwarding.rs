//! Publish the applied revision and current routes for forwarded requests, and the applied
//! view that serves serializable point reads without the Raft owner.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use tokio::sync::watch;

/// The latest applied view and response header for serializable reads.
///
/// The owner replaces it in apply order, so a reader never sees an older view after a newer
/// one. It holds only applied state, which contains committed entries only.
pub(crate) struct LocalRead {
    view: Arc<crate::state::ReadView>,
    header: super::apply::Header,
}

impl LocalRead {
    pub(super) fn new(view: Arc<crate::state::ReadView>, header: super::apply::Header) -> Self {
        Self { view, header }
    }

    /// Evaluate a serializable read on this view, like the owner's point-read executor.
    pub(crate) async fn read(
        &self,
        request: &crate::proto::etcdserverpb::RequestOp,
        credentials: Option<&crate::auth::Credentials>,
    ) -> anyhow::Result<crate::mvcc::Outcome> {
        let mut outcome = self.view.read(request, credentials).await?;
        self.header.outcome(&mut outcome);
        Ok(outcome)
    }

    #[cfg(test)]
    pub(super) fn applied_revision(&self) -> i64 {
        self.view.revision
    }
}

type LocalSlot = Arc<RwLock<Option<Arc<LocalRead>>>>;

pub(super) struct Publisher {
    local: LocalSlot,
    revision: Arc<AtomicI64>,
    leader: Arc<AtomicU64>,
    compact: watch::Receiver<i64>,
    peers: watch::Sender<Arc<BTreeMap<u64, String>>>,
}

/// Read the member routes and the latest locally applied revision.
pub struct ForwardView {
    local: LocalSlot,
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
            local: Arc::default(),
            revision: Arc::new(AtomicI64::new(revision)),
            leader: Arc::default(),
            compact,
            peers: watch::channel(Arc::new(routes(peers))).0,
        }
    }

    pub(super) fn subscribe(&self) -> ForwardView {
        ForwardView {
            local: self.local.clone(),
            revision: self.revision.clone(),
            leader: self.leader.clone(),
            compact: self.compact.clone(),
            peers: self.peers.subscribe(),
        }
    }

    pub(super) fn apply(&self, revision: i64) {
        self.revision.store(revision, Ordering::Release);
    }

    /// Replace the view for serializable reads. `None` sends them through the owner.
    pub(super) fn publish_local(&self, local: Option<LocalRead>) {
        let local = local.map(Arc::new);
        let old = std::mem::replace(
            &mut *self.local.write().unwrap_or_else(|e| e.into_inner()),
            local,
        );
        // Release the old snapshot outside the lock.
        drop(old);
    }

    #[cfg(test)]
    pub(super) fn local(&self) -> Option<Arc<LocalRead>> {
        self.local.read().unwrap_or_else(|e| e.into_inner()).clone()
    }

    pub(super) fn set_leader(&self, id: u64) {
        self.leader.store(id, Ordering::Relaxed);
    }

    pub(super) fn update_peers(&self, peers: &BTreeMap<u64, String>) {
        self.peers.send_replace(Arc::new(routes(peers)));
    }
}

impl ForwardView {
    /// The applied view for a serializable read, if the owner publishes one.
    pub(crate) fn local_read(&self) -> Option<Arc<LocalRead>> {
        self.local.read().unwrap_or_else(|e| e.into_inner()).clone()
    }

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
