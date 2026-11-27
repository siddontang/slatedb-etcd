use super::*;

impl Node {
    pub(super) fn enqueue_query(&mut self, query: Query) {
        if matches!(query, Query::Read(..) | Query::Linear(..)) {
            if self.read_queries.len() >= apply::MAX_QUERIES {
                query.reject(self.error("too many queued reads"));
            } else {
                self.read_queries.push_back(query);
            }
            return;
        }
        if self.queries.len() >= apply::MAX_QUERIES {
            query.reject(self.error("too many queued state reads"));
        } else {
            self.queries.push_back(query);
        }
    }

    pub(super) fn next_query_job(&mut self) -> Option<Job> {
        self.queries.pop_front().map(Job::Query)
    }

    pub(super) fn submit_apply(&mut self, job: Job, target: u64) -> Result<()> {
        ensure!(!self.apply_busy, "apply worker already has a job");
        self.enqueue_apply(job, target)
    }

    /// Queue a job behind any running jobs. The worker runs jobs in submission order.
    fn enqueue_apply(&mut self, job: Job, target: u64) -> Result<()> {
        // Only a plain committed-entry job lets another Entries job queue behind it.
        // A configuration change must reach Raft before the worker applies later entries.
        let chainable = match &job {
            Job::Entries(entries) => entries
                .iter()
                .all(|entry| entry.entry_type == EntryType::EntryNormal as i32),
            _ => false,
        };
        self.worker.submit(Work {
            job,
            header: self.header(),
            delay: Duration::from_millis(self.apply_delay_ms),
        })?;
        self.apply_busy = true;
        self.apply_target = target;
        self.apply_jobs.push_back((target, chainable));
        Ok(())
    }

    /// The checkpoint to start once `applied` entries have been applied, if one is due.
    /// `Some(true)` builds a complete image; `Some(false)` only flushes state and truncates.
    pub(super) fn checkpoint_due(&self, applied: u64) -> Option<bool> {
        // No checkpoint below the record can replace it. `certify_base` raises the base first.
        if self.base_below_record() {
            return None;
        }
        // Shared mode records its base `F`. The log stays above the base, so the record needs
        // no flush, and any member can send it.
        let applied = self.checkpointable(applied);
        let store = self.raw.store();
        let snapshot_index = store.snapshot_index();
        // A peer snapshot or a manual checkpoint needs a complete image.
        let image_can_advance =
            applied > snapshot_index || (snapshot_index > 0 && !store.has_image());
        let image_due = image_can_advance
            && (store.image_wanted()
                || self
                    .checkpoint_waiters
                    .iter()
                    .any(|(index, _)| *index <= applied));
        // Automatic checkpoints only truncate the local log. A membership change needs a
        // checkpoint at its index.
        let forced = self.membership_checkpoint;
        let truncate_due = ((forced > snapshot_index && applied >= forced)
            || (self.next_checkpoint.is_some_and(|index| applied >= index)
                && self.appended_bytes >= self.options.snapshot_min_bytes))
            && applied > snapshot_index
            && !store.image_wanted()
            && self.checkpoint_waiters.is_empty()
            && !self.sending_snapshot();
        (image_due || truncate_due).then_some(image_due)
    }

    /// The index that a checkpoint can cover: `applied`, or the base `F` in shared mode.
    pub(super) fn checkpointable(&self, applied: u64) -> u64 {
        match self.state.view {
            Some((base, _, _)) if self.writer.is_some() => base,
            _ => applied,
        }
    }

    /// Submit committed entries in `(applied, committed]`, up to one apply batch.
    fn submit_entries(&mut self, applied: u64, committed: u64) -> Result<()> {
        let low = applied + 1;
        let high = (low + apply::APPLY_ENTRIES).min(committed + 1);
        if high <= low {
            return Ok(());
        }
        let entries = self.raw.store().entries(
            low,
            high,
            Some(apply::BATCH_BYTES),
            raft::GetEntriesContext::empty(false),
        )?;
        let target = entries.last().context("empty apply batch")?.index;
        ensure!(
            target <= self.committed_ready,
            "apply exceeds delivered commit"
        );
        self.prefer_query = true;
        self.enqueue_apply(Job::Entries(entries), target)
    }

    /// Keep the worker busy with committed entries while it runs earlier Entries jobs.
    /// Any other work waits until the queue drains, so it still sees the state it expects.
    fn pipeline_apply(&mut self) -> Result<()> {
        while self.apply_jobs.len() < apply::PIPELINE_DEPTH {
            let Some(&(last, true)) = self.apply_jobs.back() else {
                return Ok(());
            };
            let committed = self.committed_ready;
            if last >= committed
                || self.apply_paused()
                || self.raw.store().snapshot_pending()
                || self.installing()
                || self.prepared_puts.is_some()
                || !self.queries.is_empty()
                || (!self.checkpoint_running && self.checkpoint_due(last).is_some())
            {
                return Ok(());
            }
            self.submit_entries(last, committed)?;
        }
        Ok(())
    }

    /// Keep the election timer past the leader lease, so the member still votes, but below the
    /// largest randomized timeout, so it does not campaign. See `tick_raft`.
    pub(super) fn hold_campaign(&mut self) {
        let raft = &mut self.raw.raft;
        let timeout = 2 * raft.election_timeout() - 1;
        raft.set_randomized_election_timeout(timeout);
        raft.election_elapsed = raft.election_elapsed.min(timeout - 2);
    }

    /// Return true from the time Raft accepts a snapshot until apply installs it: while Raft
    /// holds it as unstable, while the log cache stages it, and while it waits for apply. The
    /// log cache moves its first index at staging, and Raft drops its unstable snapshot before
    /// the write ends, so each step needs its own index.
    pub(super) fn installing(&self) -> bool {
        // Raft scans from its own applied index. `complete_apply` keeps that index at or above
        // `state.applied`, also for a waiting member that starts with Raft ahead. So Raft's
        // index is below a snapshot only when this one is, and the guard holds then.
        self.accepted_snapshot() > 0
    }

    pub(super) fn schedule_apply(&mut self) -> Result<()> {
        self.preempt_paused_apply();
        if self.apply_busy {
            return self.pipeline_apply();
        }
        // Raft can hold an accepted snapshot as unstable while the log queue is full, and the log
        // cache then stages it. Other apply work waits until the record is durable, so the
        // install goes first.
        let snapshot_index = self.raw.store().snapshot_index();
        if self.raw.store().snapshot_pending() || self.accepted_snapshot() > snapshot_index {
            return Ok(());
        }
        if self.state.applied < snapshot_index {
            // The incoming snapshot covers the prepared prefix or establishes a new base.
            // Apply any remaining suffix through the normal committed log path.
            self.prepared_puts.take();
            self.held_puts.take();
            return self.submit_apply(
                Job::Snapshot(self.raw.store().recovery_snapshot().clone()),
                snapshot_index,
            );
        }
        if self.take_raise() {
            let record = self.raw.store().recovery_snapshot().clone();
            return self.submit_apply(Job::RaiseBase(record), self.state.applied);
        }
        // A switch frees overlay memory, so it goes before other work.
        if self.prepared_puts.is_none() && self.take_switch() {
            return self.submit_apply(Job::SwitchBase, self.state.applied);
        }
        if let Some((base, term)) = self.adoptable() {
            let index = base.applied;
            return self.submit_apply(Job::AdoptBase { base, term }, index);
        }
        // Raft delivers committed entries only after local persistence. The commit marker in the
        // log is volatile, so apply need not wait for the log thread to write it.
        let committed = self.committed_ready;
        let released = self.release_held_puts();
        if let Some(batch) = &self.prepared_puts {
            let last = batch.last;
            if last <= committed && self.held_puts.is_none() {
                let batch = self.prepared_puts.take().unwrap();
                self.submit_apply(Job::PublishPuts(batch), last)?;
            }
            // Send the replies after the PublishPuts wake, so a handler runs on this worker.
            if let Some(replies) = released {
                self.expire_pending();
                self.finish_output(Output::Entries(replies))?;
            }
            return Ok(());
        }
        if !self.checkpoint_running {
            if let Some(image) = self.checkpoint_due(self.state.applied) {
                return self.start_checkpoint(image);
            }
        }
        if self.prefer_query {
            if let Some(job) = self.next_query_job() {
                self.prefer_query = false;
                return self.submit_apply(job, self.state.applied);
            }
        }
        let paused = self.apply_paused();
        if self.state.applied < committed && !paused {
            if self.prepare_local_puts()? {
                return Ok(());
            }
            self.submit_entries(self.state.applied, committed)?;
            return self.pipeline_apply();
        }
        if !paused && self.prepare_quorum_puts()? {
            return Ok(());
        }
        if let Some(job) = self.next_query_job() {
            return self.submit_apply(job, self.state.applied);
        }
        Ok(())
    }

    pub(super) fn complete_apply(&mut self, completed: Completion) -> Result<()> {
        ensure!(self.apply_busy, "unexpected apply completion");
        let (target, _) = self
            .apply_jobs
            .pop_front()
            .context("unexpected apply completion")?;
        // A covering snapshot can end a job at its applied prefix. See `preempt_paused_apply`.
        ensure!(
            completed.applied == target || completed.early && completed.applied < target,
            "apply completion has the wrong index"
        );
        ensure!(
            completed.applied >= self.state.applied,
            "apply position regressed"
        );
        ensure!(
            completed.applied <= self.committed_ready.max(self.state.applied),
            "apply completion exceeds delivered commit"
        );
        self.apply_busy = !self.apply_jobs.is_empty();
        self.expire_reads();
        // A job that a snapshot ended during a rebase replay sends only its prefix boundary. The
        // snapshot replaces the state. See `apply_retrying`.
        let Some(state) = completed.state else {
            ensure!(
                completed.applied == self.state.applied || completed.early,
                "read changed the apply position"
            );
            return self.finish_output(completed.output);
        };
        let durable = self.state.durable_applied.max(state.durable_applied);
        let membership_changed = !std::sync::Arc::ptr_eq(&self.state.members, &state.members);
        let mut state = state;
        let batch = state.watch.take();
        self.state = state;
        self.observe_members();
        self.forwarding.apply(self.state.revision);
        self.publish_local_reads();
        self.publish_watch(batch);
        self.state.durable_applied = durable;
        self.finish_revision_waiters();

        self.sync_lease_deadlines();
        self.expire_pending();
        self.finish_output(completed.output)?;
        // A member that waits for a snapshot can start with Raft ahead of its state. See
        // `node::quota`.
        if self.state.applied >= self.raw.raft.raft_log.applied {
            self.raw.advance_apply_to(self.state.applied);
        }
        self.raw.mut_store().applied_to(self.state.applied);
        self.note_base();
        if membership_changed {
            self.sync_members();
            // Each checkpoint must hold the membership that Raft uses.
            if let Some(writer) = &self.writer {
                writer.request_publication(self.state.applied);
            }
        }
        Ok(())
    }

    pub(super) fn finish_output(&mut self, output: Output) -> Result<()> {
        match output {
            Output::SharedLinear(ids, outcome) => self.finish_shared_reads(ids, outcome)?,
            Output::Reads(reads) => {
                for read in reads {
                    self.finish_output(read)?;
                }
            }
            Output::PreparedPuts(batch, replies) => {
                // A term change can replace uncommitted entries at these indices. The held group
                // is then dropped before any reply leaves.
                let raft = &self.raw.raft;
                if batch.last > raft.raft_log.committed
                    || raft.term != self.prepare_term
                    || raft.state != StateRole::Leader
                {
                    self.hold_puts(batch, replies);
                    return Ok(());
                }
                self.quorum_reply_index = batch.last;
                // Withdraw the published view before any of these replies leaves.
                self.publish_local_reads();
                self.prepared_puts = Some(batch);
                self.expire_pending();
                self.finish_output(Output::Entries(replies))?;
            }
            Output::Unprepared(last) => self.unprepared = last,
            Output::Entries(replies) => {
                // Raft takes the ConfState of a snapshot when it accepts it. A snapshot is above
                // the commit index, so it covers each entry of these replies, and its ConfState
                // holds or undoes each of their changes. Raft must not apply them again.
                let superseded = self.installing();
                for command in replies {
                    if let Some(change) = command.change.as_ref().filter(|_| !superseded) {
                        self.raw.apply_conf_change(change)?;
                        self.membership_checkpoint = self.state.applied;
                        self.tick_clock(false);
                        self.prune_write_proofs();
                    }
                    if let Some(LeaseCommand::Expire { id, generation }) = command.lease {
                        self.lease_expiring.remove(&(id, generation));
                    }
                    match command.reply {
                        Reply::PhysicalCompact {
                            index,
                            revision,
                            value,
                            flush,
                        } => {
                            self.wait_physical_compact(&command.id, (index, revision, value));
                            if let Some(physical) = flush {
                                self.flush_compaction(index, physical);
                            }
                            continue;
                        }
                        Reply::CompactFlush { index, revision } => {
                            self.flush_compaction(index, revision);
                            continue;
                        }
                        Reply::Encoded(_) | Reply::Kv(_) | Reply::Lease(_) => {}
                    }
                    if let Some(pending) = self.pending.remove(&command.id) {
                        let reply = match command.reply {
                            Reply::Kv(outcome) => {
                                pending.response.kv(outcome)?;
                                continue;
                            }
                            Reply::Encoded(value) => value,
                            Reply::PhysicalCompact { .. } | Reply::CompactFlush { .. } => {
                                unreachable!()
                            }
                            Reply::Lease(mut outcome) => {
                                if let Ok(response) = &mut outcome.result {
                                    if let LeaseReply::TimeToLive(response) = response {
                                        if response.ttl >= 0 {
                                            if let Some((_, deadline)) =
                                                self.lease_deadlines.get(&response.id)
                                            {
                                                response.ttl = deadline
                                                    .saturating_duration_since(Instant::now())
                                                    .as_secs()
                                                    as i64;
                                            }
                                        }
                                    }
                                    self.fill_header(response.header_mut());
                                }
                                serde_json::to_value(outcome)?
                            }
                        };
                        pending.response.json(reply)?;
                    }
                }
            }
            Output::Snapshot(rebound) => {
                self.metrics.rebind(rebound.admin);
                self.compaction.publish(self.state.physical_revision);
                self.installed_snapshots += 1;
                self.options.join = false;
            }
            Output::Query {
                response,
                value,
                require_leader,
            } => {
                let result = if require_leader && self.raw.raft.leader_id == 0 {
                    Err(self.error("no leader"))
                } else {
                    value
                };
                let _ = response.send(result);
            }
            Output::Read { response, outcome } => response.kv(outcome)?,
            Output::Linear { id, outcome } => self.finish_read(id, outcome)?,
            // Raft took the ConfState of a snapshot that it accepted above the adopted base.
            Output::Adopted(conf) if self.installing() => drop(conf),
            Output::Adopted(conf) => {
                shared::reconcile_conf(&mut self.raw, &conf)?;
                self.tick_clock(false);
                self.prune_write_proofs();
            }
            Output::RejectedBase(rejected) => self.reject_base(rejected),
            Output::Checkpoint(built) => {
                if let Some(built) = built {
                    self.checkpoint_built(built)?;
                }
            }
        }
        Ok(())
    }

    #[cfg(test)]
    pub(super) async fn drain_apply(&mut self) -> Result<()> {
        loop {
            let mut progress = false;
            if let Some(completed) = self.worker.step().await? {
                self.complete_apply(completed)?;
                progress = true;
            }
            if let Some(completed) = self.read_executor.step().await? {
                self.complete_read(completed)?;
                progress = true;
            }
            if !progress {
                return Ok(());
            }
            self.drive_ready().await?;
        }
    }

    #[cfg(test)]
    pub(super) async fn drain_state(&mut self) -> Result<()> {
        while let Some(completed) = self.worker.step().await? {
            self.complete_apply(completed)?;
            self.drive_ready().await?;
        }
        Ok(())
    }

    #[cfg(test)]
    pub(super) async fn drain_reads(&mut self) -> Result<()> {
        while let Some(completed) = self.read_executor.step().await? {
            self.complete_read(completed)?;
            self.drive_ready().await?;
        }
        Ok(())
    }
}

// Serve a member whose apply waits on a storage error. See "Read faults" in
// `docs/shared-state.md`.
//
// A transient error pauses apply, and the job retries. Corrupt stored data holds apply on its
// entry. The member stays up and serves reads of its earlier state. A held follower asks its
// leader for a snapshot above the held entry, and a snapshot that Raft accepts ends the
// waiting job at its applied prefix. A linearizable read that needs a later entry fails at
// once instead of waiting for the read timeout.

/// Log entries above a held entry after which the member logs an alarm. A long hold keeps the
/// log, so an operator must replace the member's state.
const HOLD_LOG_ALARM: u64 = 100_000;

impl Node {
    /// The index of the snapshot that Raft accepted and apply did not install yet, or zero.
    pub(super) fn accepted_snapshot(&self) -> u64 {
        let store = self.raw.store();
        let unstable = self.raw.raft.raft_log.unstable.snapshot.as_ref();
        let accepted = unstable.map_or(0, |snapshot| snapshot.get_metadata().index);
        let stored = store.staged_snapshot_index().max(store.snapshot_index());
        let index = accepted.max(stored);
        if index > self.state.applied {
            index
        } else {
            0
        }
    }

    /// End a waiting apply job when an accepted snapshot covers its entry. Raft already took the
    /// ConfState of the snapshot, so the job's prefix delivers its replies but not its
    /// membership changes. See `finish_output`.
    pub(super) fn preempt_paused_apply(&mut self) {
        let accepted = self.accepted_snapshot();
        if accepted > 0 {
            self.worker.control.preempt(accepted);
        }
    }

    /// Fail each linearizable read whose read index is above the applied index while apply
    /// holds. The read can serve nothing below its read index.
    pub(super) fn fence_reads(&mut self) {
        // A transient pause ends soon, so its reads wait as usual.
        let Some(paused) = self.worker.control.paused().filter(|paused| paused.held) else {
            return;
        };
        let applied = self.state.applied;
        let fenced: Vec<_> = self
            .reads
            .iter()
            .filter(|(_, read)| read.index().is_some_and(|index| index > applied))
            .map(|(id, _)| *id)
            .collect();
        let message = format!(
            "storage unavailable; apply holds at entry {}: {}",
            paused.index, paused.reason
        );
        for id in fenced {
            if let Some(read) = self.reads.remove(&id) {
                read.into_response().error(self.error(&message));
            }
        }
    }

    /// Run the periodic work of a paused apply: the metrics, the alarm, and the snapshot request
    /// of a held follower.
    pub(super) fn tick_apply_pause(&mut self) {
        let paused = self.worker.control.paused();
        let held = paused.as_ref().filter(|paused| paused.held);
        // The mark stays after a covering snapshot ends the job, until the install passes it.
        match held {
            Some(paused) => self.apply_held_at = Some(paused.index),
            None if self
                .apply_held_at
                .is_some_and(|index| self.state.applied >= index) =>
            {
                self.apply_held_at = None;
            }
            None => {}
        }
        let last = self.raw.raft.raft_log.last_index();
        let growth = held.map_or(0, |paused| last.saturating_sub(paused.index));
        self.metrics.apply_held.set(held.is_some().into());
        self.metrics.apply_held_entries.set(growth as i64);
        if growth >= HOLD_LOG_ALARM && !self.hold_alarm {
            tracing::error!(
                growth,
                "the log grows behind held apply; replace the state of this member"
            );
        }
        self.hold_alarm = growth >= HOLD_LOG_ALARM;
        // The leader serves the request with a base above the held entry. See `certify_base`.
        if held.is_some() && self.raw.raft.state != StateRole::Leader {
            let _ = self.raw.request_snapshot();
        }
        self.fence_reads();
    }

    /// The `/status` report of a paused apply.
    pub(super) fn pause_status(&self) -> Value {
        match self.worker.control.paused() {
            Some(paused) => json!({
                "index": paused.index,
                "held": paused.held,
                "reason": paused.reason,
                "paused_ms": paused.since.elapsed().as_millis() as u64,
                "log_alarm": self.hold_alarm,
            }),
            None => Value::Null,
        }
    }
}

#[cfg(test)]
#[path = "shared/pause_tests.rs"]
mod pause_tests;
