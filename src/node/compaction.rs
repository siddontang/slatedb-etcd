//! Flush physical compactions while the apply worker accepts more entries.

use super::*;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::watch;

type FlushJob = Pin<Box<dyn Future<Output = Result<(u64, i64)>> + Send>>;

pub(super) struct Flush {
    db: Arc<slatedb::Db>,
    meters: Arc<crate::metrics::StateMeters>,
    running: Option<FlushJob>,
    queued: Option<(u64, i64)>,
    pub durable: watch::Sender<i64>,
    // Each reply belongs to a live proposal. Proposal expiry also removes this reply.
    pub replies: HashMap<String, (u64, Value)>,
}

impl Flush {
    pub fn new(state: &StateMachine) -> Self {
        Self {
            db: state.db.clone(),
            meters: state.metrics.clone(),
            running: None,
            queued: None,
            durable: watch::channel(state.compact_revision).0,
            replies: HashMap::new(),
        }
    }

    pub fn cancel(&mut self) {
        self.running = None;
        self.queued = None;
    }

    pub fn active(&self) -> bool {
        self.running.is_some()
    }

    pub fn request(&mut self, index: u64, revision: i64) {
        if self.active() {
            // Apply submits requests in log order. One later flush covers the queued prefix.
            self.queued = Some((index, revision));
            return;
        }
        let db = self.db.clone();
        let meters = self.meters.clone();
        self.running = Some(Box::pin(async move {
            crate::failpoint::hit("compact-before-flush");
            meters.flush_db(&db).await?;
            crate::failpoint::hit("compact-after-flush");
            Ok((index, revision))
        }));
    }

    pub async fn next(&mut self) -> Result<(u64, i64)> {
        // The Raft loop can select another event while this future waits for storage.
        self.running
            .as_mut()
            .expect("active compaction flush")
            .await
    }

    pub fn publish(&self, revision: i64) {
        self.durable.send_if_modified(|old| {
            if *old >= revision {
                return false;
            }
            *old = revision;
            true
        });
    }
}

impl Node {
    pub(super) fn finish_compaction_flush(&mut self, (index, revision): (u64, i64)) -> Result<()> {
        self.compaction.running = None;
        self.state.durable_applied = self.state.durable_applied.max(index);
        self.compaction.publish(revision);
        for (id, (_, value)) in self
            .compaction
            .replies
            .extract_if(|_, (at, _)| *at <= index)
        {
            if let Some(pending) = self.pending.remove(&id) {
                pending.response.json(value)?;
            }
        }
        if let Some((index, revision)) = self.compaction.queued.take() {
            self.compaction.request(index, revision);
        }
        Ok(())
    }
}
