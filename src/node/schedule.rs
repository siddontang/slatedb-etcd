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
    fn checkpoint_due(&self, applied: u64) -> Option<bool> {
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
        // Automatic checkpoints only flush state and truncate the local log.
        let truncate_due = ((self.membership_checkpoint > snapshot_index
            && applied >= self.membership_checkpoint)
            || (self.next_checkpoint.is_some_and(|index| applied >= index)
                && self.appended_bytes >= self.options.snapshot_min_bytes))
            && applied > snapshot_index
            && !store.image_wanted()
            && self.checkpoint_waiters.is_empty()
            && !self.sending_snapshot();
        (image_due || truncate_due).then_some(image_due)
    }

    /// Submit committed entries in `(applied, committed]`, up to one apply batch.
    fn submit_entries(&mut self, applied: u64, committed: u64) -> Result<()> {
        let low = applied + 1;
        let high = (low + apply::APPLY_ENTRIES).min(committed + 1);
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
                || self.raw.store().snapshot_pending()
                || last < self.raw.store().snapshot_index()
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

    pub(super) fn schedule_apply(&mut self) -> Result<()> {
        if self.apply_busy {
            return self.pipeline_apply();
        }
        if self.raw.store().snapshot_pending() {
            return Ok(());
        }
        let snapshot_index = self.raw.store().snapshot_index();
        if self.state.applied < snapshot_index {
            // The incoming snapshot covers the prepared prefix or establishes a new base.
            // Apply any remaining suffix through the normal committed log path.
            self.prepared_puts.take();
            return self.submit_apply(
                Job::Snapshot(self.raw.store().recovery_snapshot().clone()),
                snapshot_index,
            );
        }
        // Raft delivers committed entries only after local persistence. The commit marker in the
        // log is volatile, so apply need not wait for the log thread to write it.
        let committed = self.committed_ready;
        if let Some(batch) = &self.prepared_puts {
            let last = batch.last;
            if last <= committed {
                let batch = self.prepared_puts.take().unwrap();
                return self.submit_apply(Job::PublishPuts(batch), last);
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
        if self.state.applied < committed {
            if self.prepare_local_puts()? {
                return Ok(());
            }
            self.submit_entries(self.state.applied, committed)?;
            return self.pipeline_apply();
        }
        if self.prepare_quorum_puts()? {
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
        ensure!(
            completed.applied == target,
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
        let Some(state) = completed.state else {
            ensure!(
                completed.applied == self.state.applied,
                "read changed the apply position"
            );
            return self.finish_output(completed.output);
        };
        let durable = self.state.durable_applied.max(state.durable_applied);
        let membership_changed = !std::sync::Arc::ptr_eq(&self.state.members, &state.members);
        let mut state = state;
        let batch = state.watch.take();
        self.state = state;
        self.forwarding.apply(self.state.revision);
        self.publish_watch(batch);
        self.state.durable_applied = durable;
        self.finish_revision_waiters();

        self.sync_lease_deadlines();
        self.expire_pending();
        self.finish_output(completed.output)?;
        self.raw.advance_apply_to(self.state.applied);
        self.raw.mut_store().applied_to(self.state.applied);
        if membership_changed {
            self.sync_members();
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
                self.quorum_reply_index = batch.last;
                self.prepared_puts = Some(batch);
                self.expire_pending();
                self.finish_output(Output::Entries(replies))?;
            }
            Output::Entries(replies) => {
                for command in replies {
                    if let Some(change) = &command.change {
                        self.raw.apply_conf_change(change)?;
                        self.membership_checkpoint = self.state.applied;
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
                                self.compaction.request(index, physical);
                            }
                            continue;
                        }
                        Reply::CompactFlush { index, revision } => {
                            self.compaction.request(index, revision);
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
            Output::Snapshot => {
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
                    Ok(value)
                };
                let _ = response.send(result);
            }
            Output::Read { response, outcome } => response.kv(outcome)?,
            Output::Linear { id, outcome } => self.finish_read(id, outcome)?,
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
