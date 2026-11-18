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
        self.worker.submit(Work {
            job,
            header: self.header(),
            delay: Duration::from_millis(self.apply_delay_ms),
        })?;
        self.apply_busy = true;
        self.apply_target = target;
        Ok(())
    }

    pub(super) fn schedule_apply(&mut self) -> Result<()> {
        if self.apply_busy || self.raw.store().snapshot_pending() {
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
        let committed = self.raw.store().commit_index().min(self.committed_ready);
        if let Some(batch) = &self.prepared_puts {
            let last = batch.last;
            if last <= committed {
                let batch = self.prepared_puts.take().unwrap();
                return self.submit_apply(Job::PublishPuts(batch), last);
            }
            return Ok(());
        }
        let checkpoint_due = (self.membership_checkpoint > snapshot_index
            && self.state.applied >= self.membership_checkpoint)
            || self
                .checkpoint_waiters
                .iter()
                .any(|(index, _)| *index <= self.state.applied)
            || (self.options.snapshot_every > 0
                && self.state.applied.saturating_sub(snapshot_index)
                    >= self.options.snapshot_every);
        if checkpoint_due && !self.checkpoint_running && self.state.applied > snapshot_index {
            return self.start_checkpoint();
        }
        if self.prefer_query {
            if let Some(job) = self.next_query_job() {
                self.prefer_query = false;
                return self.submit_apply(job, self.state.applied);
            }
        }
        if self.state.applied < committed {
            let low = self.state.applied + 1;
            let high = (low + apply::BATCH_ENTRIES).min(committed + 1);
            let entries = self.raw.store().entries(
                low,
                high,
                Some(apply::BATCH_BYTES),
                raft::GetEntriesContext::empty(false),
            )?;
            let target = entries.last().context("empty apply batch")?.index;
            ensure!(
                target <= self.raw.store().commit_index(),
                "apply exceeds durable commit"
            );
            self.prefer_query = true;
            return self.submit_apply(Job::Entries(entries), target);
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
        ensure!(
            completed.applied == self.apply_target,
            "apply completion has the wrong index"
        );
        ensure!(
            completed.applied >= self.state.applied,
            "apply position regressed"
        );
        ensure!(
            completed.applied <= self.raw.store().commit_index(),
            "apply completion exceeds durable commit"
        );
        self.apply_busy = false;
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
        self.state = state;
        if *self.applied_notify.borrow() != self.state.applied {
            self.applied_notify.send_replace(self.state.applied);
        }
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
                    if let Some(pending) = self.pending.remove(&command.id) {
                        let reply = match command.reply {
                            Reply::Kv(outcome) => {
                                pending.response.kv(outcome)?;
                                continue;
                            }
                            Reply::Encoded(value) => value,
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
            Output::Checkpoint => {}
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
