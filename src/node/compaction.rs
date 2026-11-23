//! Drive physical MVCC compaction through small replicated steps.
//!
//! Each member flushes SlateDB after a physical target finishes. The flush runs while the apply
//! worker accepts more entries.

use super::*;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::watch;

/// The pause between steps lets client entries use the apply worker.
const STEP_PAUSE: Duration = Duration::from_millis(2);

type FlushJob = Pin<Box<dyn Future<Output = Result<(u64, i64)>> + Send>>;

pub(super) struct Flush {
    db: Arc<slatedb::Db>,
    meters: Arc<crate::metrics::StateMeters>,
    running: Option<FlushJob>,
    queued: Option<(u64, i64)>,
    /// The physical boundary that has reached object storage.
    pub durable: watch::Sender<i64>,
    /// Physical Compact replies. A long pass can exceed the proposal timeout, so these replies
    /// leave the pending map and wait until the client disconnects.
    pub replies: Vec<Waiter>,
}

/// The newest ready background compaction step. Manual test nodes leave it unset, so their
/// step proposals do not depend on task timing.
pub(super) struct PlanReady(Option<watch::Receiver<u64>>);

impl PlanReady {
    pub fn new(state: &StateMachine, manual: bool) -> Self {
        Self((!manual).then(|| state.compaction_plan.subscribe()))
    }

    /// Return true when this replica has read the step for `generation`.
    pub fn ready(&self, generation: u64) -> bool {
        self.0
            .as_ref()
            .is_none_or(|ready| *ready.borrow() >= generation)
    }

    /// Wait until another background step is ready.
    pub async fn changed(&mut self) {
        if let Some(ready) = &mut self.0 {
            if ready.changed().await.is_ok() {
                return;
            }
        }
        std::future::pending().await
    }
}

pub(super) struct Waiter {
    /// The reply needs a flush through the Compact entry.
    index: u64,
    /// The reply needs the physical pass through this boundary.
    revision: i64,
    response: ClientResponse,
    value: Value,
}

impl Waiter {
    pub fn closed(&self) -> bool {
        self.response.is_closed()
    }
}

impl Flush {
    pub fn new(state: &StateMachine) -> Self {
        Self {
            db: state.db.clone(),
            meters: state.metrics.clone(),
            running: None,
            queued: None,
            durable: watch::channel(state.physical_revision()).0,
            replies: vec![],
        }
    }

    /// Use the database that a snapshot install opened. A running flush finishes on the old one.
    pub fn rebind(&mut self, db: std::sync::Arc<slatedb::Db>) {
        self.db = db;
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
        let durable = *self.compaction.durable.borrow();
        for waiter in self
            .compaction
            .replies
            .extract_if(.., |w| w.index <= index && w.revision <= durable)
        {
            waiter.response.json(waiter.value)?;
        }
        if let Some((index, revision)) = self.compaction.queued.take() {
            self.compaction.request(index, revision);
        }
        Ok(())
    }

    pub(super) fn wait_physical_compact(
        &mut self,
        id: &str,
        (index, revision, value): (u64, i64, Value),
    ) {
        if let Some(pending) = self.pending.remove(id) {
            self.compaction.replies.push(Waiter {
                index,
                revision,
                response: pending.response,
                value,
            });
        }
    }

    /// Propose the next compaction step. Only the leader proposes, and one step at a time.
    pub(super) fn drive_compaction(&mut self) -> Result<()> {
        if self.raw.raft.state != StateRole::Leader || !self.state.compaction_pending {
            self.compact_step = None;
            self.compact_ready_at = None;
            return Ok(());
        }
        if let Some((index, term)) = self.compact_step {
            if self.state.applied < index && term == self.raw.raft.term {
                return Ok(());
            }
            self.compact_step = None;
            if self.compaction.replies.is_empty() {
                self.compact_ready_at = Some(Instant::now() + STEP_PAUSE);
            }
        }
        if self.compaction.replies.is_empty()
            && self
                .compact_ready_at
                .is_some_and(|ready| ready > Instant::now())
        {
            return Ok(());
        }
        self.compact_ready_at = None;
        // The step reads storage in the background. Proposing it earlier makes apply wait.
        if self.backlogged() || !self.compact_plan.ready(self.state.compaction_plan) {
            return Ok(());
        }
        let data = RaftCommand::CompactStep {
            id: uuid::Uuid::new_v4().to_string(),
        }
        .encode()?;
        if !self.raw.store().admits(
            self.raw.raft.raft_log.unstable_entries(),
            &[],
            data.len() + cache::ENTRY_OVERHEAD + 64,
        ) {
            return Ok(());
        }
        if self.raw.propose(vec![], data).is_ok() {
            self.compact_step = Some((self.raw.raft.raft_log.last_index(), self.raw.raft.term));
        }
        Ok(())
    }
}
