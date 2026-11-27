use super::*;

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
        if self.log_queue.len() < log::MAX_JOBS
            && self.log_bytes < log::MAX_BYTES
            && self.log_snapshot_bytes < log::MAX_SNAPSHOT_BYTES
        {
            if let Some((index, conf, image)) = self.built_checkpoint.take() {
                self.queue_log(
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
        while self.raw.has_ready()
            && self.log_queue.len() < log::MAX_JOBS
            && self.log_bytes < log::MAX_BYTES
            && self.log_snapshot_bytes < log::MAX_SNAPSHOT_BYTES
            && self.log_queue.iter().map(|task| task.readys).sum::<usize>() < log::MAX_READYS
        {
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
            self.appended_bytes = ready
                .entries()
                .iter()
                .fold(self.appended_bytes, |total, entry| {
                    total.saturating_add(entry.data.len() as u64)
                });
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
            self.committed_ready = self
                .committed_ready
                .max(ready.snapshot().get_metadata().index);
            if let Some(entry) = ready.take_committed_entries().last() {
                self.committed_ready = self.committed_ready.max(entry.index);
            }
            // Cached entries must be readable before Raft can advance without disk completion.
            self.raw.advance_append_async(ready);
            self.queue_log(job, Some(number), None, messages, reads);
            self.join_queued_ready()?;
            // raft-rs permits leader replication before local persistence.
            self.send(early);
            for (peer, snapshot) in std::mem::take(&mut self.send_failures) {
                self.raw.report_unreachable(peer);
                if snapshot {
                    self.raw.report_snapshot(peer, SnapshotStatus::Failure);
                }
            }
        }
        if self.log_queue.len() < log::MAX_JOBS {
            if let Some(fetch) = self.raw.mut_store().take_fetch() {
                self.queue_log(log::Job::Fetch(fetch), None, None, vec![], vec![]);
            }
        }
        if !self.raw.has_ready() {
            if let Some(task) = self.log_queue.back_mut() {
                task.responses.append(&mut self.peer_waiters);
            } else {
                for response in self.peer_waiters.drain(..) {
                    let _ = response.send(Ok(json!({"accepted": true})));
                }
            }
        }
        self.start_log()?;
        self.schedule_reads();
        self.finish_renewals()?;
        self.schedule_apply()?;
        Ok(())
    }

    pub(super) fn queue_log(
        &mut self,
        job: log::Job,
        ready: Option<u64>,
        checkpoint: Option<u64>,
        messages: Vec<Message>,
        reads: Vec<raft::ReadState>,
    ) {
        self.log_sequence += 1;
        // The messages and read states wait in memory with the job, so they count too.
        let messages_bytes: usize = messages.iter().map(ProtobufMessage::encoded_len).sum();
        let reads_bytes: usize = reads.iter().map(|read| read.request_ctx.len()).sum();
        let bytes = job.bytes() + messages_bytes + reads_bytes;
        let snapshot_bytes = job.snapshot_bytes();
        self.log_bytes += bytes;
        self.log_snapshot_bytes += snapshot_bytes;
        self.log_queue.push_back(LogTask {
            id: self.log_sequence,
            work: Some(job),
            bytes,
            snapshot_bytes,
            ready,
            readys: usize::from(ready.is_some()),
            checkpoint,
            messages,
            reads,
            responses: vec![],
            stats_response: None,
        });
    }

    pub(super) fn start_log(&mut self) -> Result<()> {
        if !self.log_busy {
            // Empty Ready batches only fence earlier writes. They need no storage work.
            while self
                .log_queue
                .front()
                .is_some_and(|task| task.work.as_ref().is_some_and(log::Job::is_barrier))
            {
                let task = self.log_queue.pop_front().unwrap();
                self.finish_log_task(task)?;
            }
            // Combine queued Ready work without adding a timer or crossing other job types.
            while self.log_queue.len() >= 2
                && self
                    .log_queue
                    .iter()
                    .take(2)
                    .all(|task| task.work.as_ref().is_some_and(log::Job::can_batch))
            {
                let mut task = self.log_queue.pop_front().unwrap();
                let next = self.log_queue.pop_front().unwrap();
                join_log(&mut task, next)?;
                self.log_queue.push_front(task);
            }
            if let Some(task) = self.log_queue.front_mut() {
                let job = task.work.take().context("log job already submitted")?;
                let reading = matches!(job, log::Job::Fetch(_));
                self.log_worker.submit(log::Work {
                    id: task.id,
                    job,
                    delay: Duration::from_millis(if reading {
                        self.log_read_delay_ms
                    } else {
                        self.log_delay_ms
                    }),
                    fail: if reading {
                        self.log_read_fail
                    } else {
                        self.log_fail
                    },
                })?;
                self.log_busy = true;
            }
        }
        Ok(())
    }

    /// Join the last queued Ready job to the queued Ready job before it, as `start_log` would.
    /// Thus consecutive Ready jobs without a snapshot use at most two queue slots behind a slow
    /// write, and the owner can take up to `log::MAX_READYS` Readys and send their heartbeats.
    /// The joined job keeps its place, so it completes after all earlier jobs. A job with disk
    /// work does not join an empty job before it: `start_log` completes the empty job without a
    /// wait for that write.
    fn join_queued_ready(&mut self) -> Result<()> {
        let count = self.log_queue.len();
        let joins = count >= 2
            && match (
                &self.log_queue[count - 2].work,
                &self.log_queue[count - 1].work,
            ) {
                (Some(job), Some(next)) => {
                    job.can_batch() && next.can_batch() && (next.is_barrier() || !job.is_barrier())
                }
                _ => false,
            };
        if joins {
            let next = self.log_queue.pop_back().unwrap();
            join_log(self.log_queue.back_mut().unwrap(), next)?;
        }
        Ok(())
    }

    pub(super) fn complete_log(&mut self, completed: log::Completion) -> Result<()> {
        ensure!(self.log_busy, "unexpected log completion");
        let mut task = self.log_queue.pop_front().context("missing log job")?;
        ensure!(task.id == completed.id, "log completion is out of order");
        self.log_busy = false;
        let saved = completed.durable.snapshot.get_metadata().index;
        if saved > self.raw.store().snapshot_index() {
            self.next_checkpoint = self.options.next_checkpoint(saved);
        }
        self.raw.mut_store().complete(completed.durable)?;
        if let Some(stats) = completed.stats {
            if let Some(response) = task.stats_response.take() {
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

    pub(super) fn finish_log_task(&mut self, task: LogTask) -> Result<()> {
        // An empty job can hold message bytes, and it completes here without the worker.
        self.log_bytes -= task.bytes;
        self.log_snapshot_bytes -= task.snapshot_bytes;
        if let Some(number) = task.ready {
            crate::failpoint::hit("raft-before-advance");
            self.raw.on_persist_ready(number);
        }
        if let Some(index) = task.checkpoint {
            self.checkpoint_running = false;
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

fn join_log(task: &mut LogTask, next: LogTask) -> Result<()> {
    task.work.as_mut().unwrap().append(next.work.unwrap())?;
    task.bytes += next.bytes;
    task.ready = next.ready.or(task.ready);
    task.readys += next.readys;
    task.messages.extend(next.messages);
    task.reads.extend(next.reads);
    task.responses.extend(next.responses);
    Ok(())
}
