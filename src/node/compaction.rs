//! Drive physical MVCC compaction through small replicated steps.
//!
//! Each member flushes SlateDB after a physical target finishes. The flush runs while the apply
//! worker accepts more entries. In shared mode a member has no database of its own. It waits
//! until its base checkpoint covers the target instead.

use super::*;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::watch;

/// The pause between steps lets client entries use the apply worker.
const STEP_PAUSE: Duration = Duration::from_millis(2);

type FlushJob = Pin<Box<dyn Future<Output = Result<(u64, i64)>> + Send>>;

pub(super) struct Flush {
    db: Option<Arc<slatedb::Db>>,
    meters: Arc<crate::metrics::StateMeters>,
    running: Option<FlushJob>,
    queued: Option<(u64, i64)>,
    /// Shared mode: the index `F` of the base. A durable target needs a covering base.
    shared: Option<watch::Receiver<u64>>,
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
            db: state.database(),
            meters: state.metrics.clone(),
            running: None,
            queued: None,
            shared: None,
            durable: watch::channel(state.physical_revision()).0,
            replies: vec![],
        }
    }

    /// Use the database that a snapshot install opened. A running flush finishes on the old one.
    pub fn rebind(&mut self, db: Option<std::sync::Arc<slatedb::Db>>) {
        self.db = db;
    }

    /// Wait for a covering base instead of a flush. `based` follows the base index `F`.
    pub fn share(&mut self, based: watch::Receiver<u64>) {
        self.shared = Some(based);
    }

    pub fn cancel(&mut self) {
        self.running = None;
        self.queued = None;
    }

    pub fn active(&self) -> bool {
        self.running.is_some()
    }

    /// Flush through `index`. In shared mode `wait` is the batch sequence that holds `index`,
    /// and the job waits for a base at or above it.
    pub fn request(&mut self, index: u64, revision: i64, wait: Option<u64>) {
        if self.active() {
            // Apply submits requests in log order. One later flush covers the queued prefix.
            self.queued = Some((index, revision));
            return;
        }
        let db = self.db.clone();
        let meters = self.meters.clone();
        let shared = self.shared.clone().zip(wait);
        self.running = Some(Box::pin(async move {
            if let Some((mut based, seq)) = shared {
                based.wait_for(|base| *base >= seq).await?;
                return Ok((index, revision));
            }
            crate::failpoint::hit("compact-before-flush");
            meters
                .flush_db(db.as_ref().context("replicated state has a database")?)
                .await?;
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
            self.flush_compaction(index, revision);
        }
        Ok(())
    }

    /// Flush storage through `index` for the physical boundary `revision`.
    pub(super) fn flush_compaction(&mut self, index: u64, revision: i64) {
        let wait = self.writer.as_ref().map(|writer| writer.covering(index));
        // The leader publishes a covering checkpoint now instead of at the age limit.
        if let Some(writer) = &self.writer {
            writer.request_publication(index);
        }
        self.compaction.request(index, revision, wait);
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
        // Simulation holds steps here, as an unfinished background read does.
        #[cfg(test)]
        if self.hold_compact_steps {
            return Ok(());
        }
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
