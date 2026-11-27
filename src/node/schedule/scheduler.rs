//! Keep the jobs that the apply worker runs, in order, and the state reads that wait for it.

use std::collections::VecDeque;
use std::time::Duration;

use anyhow::{ensure, Context, Result};
use raft::eraftpb::{Entry, EntryType};
use serde_json::Value;

use crate::node::apply::{self, Completion, Header, Job, Query, Work, Worker};
use crate::ApiError;

/// The apply jobs of one member, from submission to completion.
///
/// The worker runs the jobs in submission order. Only a plain committed-entry job lets another
/// Entries job queue behind it, up to `apply::PIPELINE_DEPTH` jobs. `Node` decides which job
/// comes next. It gives the worker, the response header, and its applied index to each method.
pub(in crate::node) struct ApplyScheduler {
    /// The worker has a job.
    busy: bool,
    /// The target index of the last submitted job.
    target: u64,
    /// Submitted apply jobs in order: target index, and whether a later Entries job may follow.
    jobs: VecDeque<(u64, bool)>,
    /// The highest commit index that Raft delivered in a Ready. Apply does not pass it.
    committed: u64,
    /// State reads that wait for the worker.
    queries: VecDeque<Query>,
    /// The next free worker goes to a state read before more entries. See `take_preferred_query`.
    prefer_query: bool,
    delay_ms: u64,
}

impl ApplyScheduler {
    /// Start at the applied index `applied`.
    pub(in crate::node) fn new(applied: u64) -> Self {
        Self {
            busy: false,
            target: applied,
            jobs: VecDeque::new(),
            committed: applied,
            queries: VecDeque::new(),
            prefer_query: false,
            delay_ms: 0,
        }
    }

    pub(in crate::node) fn busy(&self) -> bool {
        self.busy
    }

    /// The highest commit index that Raft delivered.
    pub(in crate::node) fn committed(&self) -> u64 {
        self.committed
    }

    /// Record that Raft delivered entries or a snapshot through `index`.
    pub(in crate::node) fn deliver(&mut self, index: u64) {
        self.committed = self.committed.max(index);
    }

    pub(in crate::node) fn set_delay(&mut self, delay_ms: u64) {
        self.delay_ms = delay_ms;
    }

    /// Queue a state read for the worker, or fail it if the queue is full.
    pub(in crate::node) fn queue_query(&mut self, query: Query, leader_id: u64) {
        if self.queries.len() >= apply::MAX_QUERIES {
            query.reject(ApiError {
                error: "too many queued state reads".to_owned(),
                leader_id,
            });
        } else {
            self.queries.push_back(query);
        }
    }

    pub(in crate::node) fn has_queries(&self) -> bool {
        !self.queries.is_empty()
    }

    pub(in crate::node) fn next_query(&mut self) -> Option<Job> {
        self.queries.pop_front().map(Job::Query)
    }

    /// The next state read, if an Entries job ran since the last preferred read. Then reads and
    /// entries take turns.
    pub(in crate::node) fn take_preferred_query(&mut self) -> Option<Job> {
        if !self.prefer_query {
            return None;
        }
        let job = self.next_query()?;
        self.prefer_query = false;
        Some(job)
    }

    /// The last target and the delivered commit index, if another Entries job may queue behind
    /// the submitted jobs.
    pub(in crate::node) fn chainable(&self) -> Option<(u64, u64)> {
        if self.jobs.len() >= apply::PIPELINE_DEPTH {
            return None;
        }
        let &(last, true) = self.jobs.back()? else {
            return None;
        };
        (last < self.committed).then_some((last, self.committed))
    }

    /// Submit a job to an idle worker.
    pub(in crate::node) fn submit(
        &mut self,
        worker: &Worker,
        job: Job,
        target: u64,
        header: Header,
    ) -> Result<()> {
        ensure!(!self.busy, "apply worker already has a job");
        self.enqueue(worker, job, target, header)
    }

    /// Queue a batch of delivered committed entries behind any running jobs.
    pub(in crate::node) fn submit_entries(
        &mut self,
        worker: &Worker,
        entries: Vec<Entry>,
        header: Header,
    ) -> Result<()> {
        let target = entries.last().context("empty apply batch")?.index;
        ensure!(target <= self.committed, "apply exceeds delivered commit");
        self.prefer_query = true;
        self.enqueue(worker, Job::Entries(entries), target, header)
    }

    /// Queue a job behind any running jobs. The worker runs jobs in submission order.
    fn enqueue(&mut self, worker: &Worker, job: Job, target: u64, header: Header) -> Result<()> {
        // Only a plain committed-entry job lets another Entries job queue behind it.
        // A configuration change must reach Raft before the worker applies later entries.
        let chainable = match &job {
            Job::Entries(entries) => entries
                .iter()
                .all(|entry| entry.entry_type == EntryType::EntryNormal as i32),
            _ => false,
        };
        worker.submit(Work {
            job,
            header,
            delay: Duration::from_millis(self.delay_ms),
        })?;
        self.busy = true;
        self.target = target;
        self.jobs.push_back((target, chainable));
        Ok(())
    }

    /// Take the front job for its completion. `applied` is the applied index before it.
    pub(in crate::node) fn complete(&mut self, completed: &Completion, applied: u64) -> Result<()> {
        ensure!(self.busy, "unexpected apply completion");
        let (target, _) = self
            .jobs
            .pop_front()
            .context("unexpected apply completion")?;
        // A covering snapshot can end a job at its applied prefix. See `preempt_paused_apply`.
        ensure!(
            completed.applied == target || completed.early && completed.applied < target,
            "apply completion has the wrong index"
        );
        ensure!(completed.applied >= applied, "apply position regressed");
        ensure!(
            completed.applied <= self.committed.max(applied),
            "apply completion exceeds delivered commit"
        );
        self.busy = !self.jobs.is_empty();
        Ok(())
    }

    pub(in crate::node) fn status(&self, status: &mut Value) {
        status["apply_busy"] = self.busy.into();
        status["apply_target"] = self.target.into();
        status["queued_state_reads"] = self.queries.len().into();
        status["apply_delay_ms"] = self.delay_ms.into();
    }

    #[cfg(test)]
    pub(in crate::node) fn target(&self) -> u64 {
        self.target
    }

    /// The targets of the submitted jobs, in order.
    #[cfg(test)]
    pub(in crate::node) fn targets(&self) -> impl Iterator<Item = u64> + '_ {
        self.jobs.iter().map(|(target, _)| *target)
    }

    #[cfg(test)]
    pub(in crate::node) fn queued(&self) -> usize {
        self.queries.len()
    }
}
