use super::*;
pub(super) mod pipeline;

impl Node {
    pub(super) async fn drive_ready(&mut self) -> Result<()> {
        // Queued client writes enter Raft below, after the term-start entries of this term.
        self.propose_term_start()?;
        self.resume_proposals();
        self.flush_puts();
        self.drive_compaction()?;
        self.expire_reads();
        self.issue_reads();
        self.expire_renewals();
        self.issue_renewals();
        if self.log.has_room() {
            if let Some((index, conf, image)) = self.checkpoints.take_built() {
                self.log.queue(
                    log::Job::Checkpoint { index, conf, image },
                    None,
                    Some(index),
                    vec![],
                    vec![],
                );
            }
        }
        // At a limit, the owner takes no Ready. Then a leader sends no heartbeat until the log
        // worker completes the job that keeps the queue full.
        while self.raw.has_ready() && self.log.takes_ready() {
            let mut ready = self.raw.ready();
            if let Some(state) = ready.ss() {
                if state.leader_id != 0 && self.last_observed_leader != state.leader_id {
                    self.metrics.leader_changes.inc();
                }
                self.last_observed_leader = state.leader_id;
                self.forwarding.set_leader(state.leader_id);
            }
            #[cfg(feature = "write-profile")]
            crate::write_profile::Stage::Raft.batch(ready.entries().len());
            // Raft can restore a requested snapshot at the follower's commit index. When a record
            // is already at that index, the log ends there too: Raft asks only at the last
            // index, and its own snapshot is not below its commit. Storing the same boundary
            // again changes nothing, and the stores reject it, so it is skipped. Raft still
            // clears the pending request.
            let staged = self.raw.store().staged_snapshot_index();
            let snapshot = (!ready.snapshot().is_empty()
                && ready.snapshot().get_metadata().index > staged)
                .then(|| ready.snapshot().clone());
            // A requested snapshot can arrive when the commit index already covers it. Raft then
            // has no HardState change, but the snapshot record needs one.
            let hs = ready
                .hs()
                .cloned()
                .or_else(|| snapshot.as_ref().map(|_| self.raw.raft.hard_state()));
            self.raw
                .mut_store()
                .stage(ready.entries(), hs.as_ref(), snapshot.as_ref())?;
            self.checkpoints.appended(ready.entries());
            let job = log::Job::Ready {
                entries: ready.take_entries(),
                hs,
                snapshot,
            };
            let early = ready.take_messages();
            let messages = ready.take_persisted_messages();
            let reads = ready.take_read_states();
            let number = ready.number();
            // Apply only entries that Raft has delivered, even if disk commit is ahead.
            self.apply.deliver(ready.snapshot().get_metadata().index);
            if let Some(entry) = ready.take_committed_entries().last() {
                self.apply.deliver(entry.index);
            }
            // Cached entries must be readable before Raft can advance without disk completion.
            self.raw.advance_append_async(ready);
            self.log.queue(job, Some(number), None, messages, reads);
            self.log.join_queued_ready()?;
            // raft-rs permits leader replication before local persistence.
            self.send(early);
            for (peer, snapshot) in self.links.take_failures() {
                self.raw.report_unreachable(peer);
                if snapshot {
                    self.raw.report_snapshot(peer, SnapshotStatus::Failure);
                }
            }
        }
        if !self.log.jobs_full() {
            if let Some(fetch) = self.raw.mut_store().take_fetch() {
                self.log
                    .queue(log::Job::Fetch(fetch), None, None, vec![], vec![]);
            }
        }
        if !self.raw.has_ready() {
            self.log.fence_peer_waiters();
        }
        self.start_log()?;
        self.schedule_reads();
        self.finish_renewals()?;
        self.schedule_apply()?;
        Ok(())
    }

    pub(super) fn start_log(&mut self) -> Result<()> {
        // Empty Ready batches only fence earlier writes. They need no storage work.
        while let Some(task) = self.log.next_barrier() {
            self.finish_log_task(task)?;
        }
        self.log.submit(&self.log_worker)
    }

    pub(super) fn complete_log(&mut self, completed: log::Completion) -> Result<()> {
        let mut task = self.log.complete(completed.id)?;
        let saved = completed.durable.snapshot.get_metadata().index;
        if saved > self.raw.store().snapshot_index() {
            self.checkpoints
                .set_next(self.options.next_checkpoint(saved));
        }
        self.raw.mut_store().complete(completed.durable)?;
        if let Some(stats) = completed.stats {
            if let Some(response) = task.take_stats_response() {
                let _ = response.send(Ok(serde_json::to_value(stats)?));
            }
        }
        if let Some((fetch, entries)) = completed.fetched {
            self.raw.mut_store().fetched(&fetch, entries)?;
            self.raw.on_entries_fetched(fetch.context);
        }
        self.finish_log_task(task)?;
        self.start_log()?;
        Ok(())
    }

    pub(super) fn finish_log_task(&mut self, task: pipeline::LogTask) -> Result<()> {
        // An empty job can hold message bytes, and it completes here without the worker.
        let task = self.log.release(task);
        if let Some(number) = task.ready {
            crate::failpoint::hit("raft-before-advance");
            self.raw.on_persist_ready(number);
        }
        if let Some(index) = task.checkpoint {
            self.checkpoints.stored();
            self.state.durable_applied = self.state.durable_applied.max(index);
        }
        self.confirm_reads(task.reads);
        self.send(task.messages);
        for response in task.responses {
            let _ = response.send(Ok(json!({"accepted": true})));
        }
        Ok(())
    }

    #[cfg(test)]
    pub(super) async fn drain_log(&mut self) -> Result<()> {
        while let Some(completed) = self.log_worker.step()? {
            self.complete_log(completed)?;
            self.drive_ready().await?;
        }
        Ok(())
    }
}
