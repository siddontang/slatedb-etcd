//! Keep the log jobs that wait for the log worker, in order, with their queued bytes and the
//! peer replies that wait for persistence.

use std::collections::VecDeque;
use std::time::Duration;

use anyhow::{ensure, Context, Result};
use prost::Message as ProtobufMessage;
use raft::eraftpb::Message;
use serde_json::{json, Value};

use crate::node::{log, Response};

/// The log jobs of one member, from the queue to the log worker and back.
///
/// The worker runs one job at a time, and the jobs complete in queue order. A job holds the
/// persisted messages, read states, and replies that wait for its write and for all earlier
/// jobs. `Node` gives the worker and each completion. It makes the Raft calls for each job that
/// this type gives back as finished.
pub(in crate::node) struct LogPipeline {
    queue: VecDeque<LogTask>,
    /// The front job is with the worker.
    busy: bool,
    sequence: u64,
    /// The bytes of the queued jobs, with their messages and read states.
    bytes: usize,
    snapshot_bytes: usize,
    delay_ms: u64,
    fail: bool,
    read_delay_ms: u64,
    read_fail: bool,
    /// Step replies that wait for the next log job. See `fence_peer_waiters`.
    peer_waiters: Vec<Response>,
}

pub(in crate::node) struct LogTask {
    id: u64,
    work: Option<log::Job>,
    bytes: usize,
    snapshot_bytes: usize,
    ready: Option<u64>,
    /// The Readys in this task. raft-rs keeps a record for each until `on_persist_ready`.
    readys: usize,
    checkpoint: Option<u64>,
    messages: Vec<Message>,
    reads: Vec<raft::ReadState>,
    responses: Vec<Response>,
    stats_response: Option<Response>,
}

/// What `Node` does for a finished job, in this order: it gives the Ready number to Raft, records
/// the stored checkpoint, confirms the read states, sends the messages, and accepts the replies.
pub(in crate::node) struct Finished {
    pub(in crate::node) ready: Option<u64>,
    pub(in crate::node) checkpoint: Option<u64>,
    pub(in crate::node) reads: Vec<raft::ReadState>,
    pub(in crate::node) messages: Vec<Message>,
    pub(in crate::node) responses: Vec<Response>,
}

impl LogTask {
    /// The reply of a `Stats` job.
    pub(in crate::node) fn take_stats_response(&mut self) -> Option<Response> {
        self.stats_response.take()
    }
}

impl LogPipeline {
    pub(in crate::node) fn new() -> Self {
        Self {
            queue: VecDeque::new(),
            busy: false,
            sequence: 0,
            bytes: 0,
            snapshot_bytes: 0,
            delay_ms: 0,
            fail: false,
            read_delay_ms: 0,
            read_fail: false,
            peer_waiters: vec![],
        }
    }

    /// The queue is under its job, log byte, and snapshot byte limits.
    pub(in crate::node) fn has_room(&self) -> bool {
        self.queue.len() < log::MAX_JOBS
            && self.bytes < log::MAX_BYTES
            && self.snapshot_bytes < log::MAX_SNAPSHOT_BYTES
    }

    /// The queue has room, and fewer than `log::MAX_READYS` Readys wait in it, the running job
    /// included. At a limit, the owner takes no Ready.
    pub(in crate::node) fn takes_ready(&self) -> bool {
        self.has_room()
            && self.queue.iter().map(|task| task.readys).sum::<usize>() < log::MAX_READYS
    }

    pub(in crate::node) fn jobs_full(&self) -> bool {
        self.queue.len() >= log::MAX_JOBS
    }

    pub(in crate::node) fn bytes_full(&self) -> bool {
        self.bytes >= log::MAX_BYTES
    }

    pub(in crate::node) fn queue(
        &mut self,
        job: log::Job,
        ready: Option<u64>,
        checkpoint: Option<u64>,
        messages: Vec<Message>,
        reads: Vec<raft::ReadState>,
    ) {
        self.sequence += 1;
        // The messages and read states wait in memory with the job, so they count too.
        let messages_bytes: usize = messages.iter().map(ProtobufMessage::encoded_len).sum();
        let reads_bytes: usize = reads.iter().map(|read| read.request_ctx.len()).sum();
        let bytes = job.bytes() + messages_bytes + reads_bytes;
        let snapshot_bytes = job.snapshot_bytes();
        self.bytes += bytes;
        self.snapshot_bytes += snapshot_bytes;
        self.queue.push_back(LogTask {
            id: self.sequence,
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

    /// Queue a `Stats` job that answers `response`. The caller checks `jobs_full` first.
    pub(in crate::node) fn queue_stats(&mut self, response: Response) {
        self.queue(log::Job::Stats, None, None, vec![], vec![]);
        self.queue.back_mut().unwrap().stats_response = Some(response);
    }

    /// Join the last queued Ready job to the queued Ready job before it, as `submit` would.
    /// Thus consecutive Ready jobs without a snapshot use at most two queue slots behind a slow
    /// write, and the owner can take up to `log::MAX_READYS` Readys and send their heartbeats.
    /// The joined job keeps its place, so it completes after all earlier jobs. A job with disk
    /// work does not join an empty job before it: `next_barrier` completes the empty job without
    /// a wait for that write.
    pub(in crate::node) fn join_queued_ready(&mut self) -> Result<()> {
        let count = self.queue.len();
        let joins = count >= 2
            && match (&self.queue[count - 2].work, &self.queue[count - 1].work) {
                (Some(job), Some(next)) => {
                    job.can_batch() && next.can_batch() && (next.is_barrier() || !job.is_barrier())
                }
                _ => false,
            };
        if joins {
            let next = self.queue.pop_back().unwrap();
            join_log(self.queue.back_mut().unwrap(), next)?;
        }
        Ok(())
    }

    /// While the worker is idle, take the empty Ready job at the front. It only fences earlier
    /// writes and needs no storage work. Give it to `release`.
    pub(in crate::node) fn next_barrier(&mut self) -> Option<LogTask> {
        if self.busy
            || !self
                .queue
                .front()
                .is_some_and(|task| task.work.as_ref().is_some_and(log::Job::is_barrier))
        {
            return None;
        }
        self.queue.pop_front()
    }

    /// While the worker is idle, give it the front job. Queued Ready jobs join first.
    pub(in crate::node) fn submit(&mut self, worker: &log::Worker) -> Result<()> {
        if self.busy {
            return Ok(());
        }
        // Combine queued Ready work without adding a timer or crossing other job types.
        while self.queue.len() >= 2
            && self
                .queue
                .iter()
                .take(2)
                .all(|task| task.work.as_ref().is_some_and(log::Job::can_batch))
        {
            let mut task = self.queue.pop_front().unwrap();
            let next = self.queue.pop_front().unwrap();
            join_log(&mut task, next)?;
            self.queue.push_front(task);
        }
        if let Some(task) = self.queue.front_mut() {
            let job = task.work.take().context("log job already submitted")?;
            let reading = matches!(job, log::Job::Fetch(_));
            worker.submit(log::Work {
                id: task.id,
                job,
                delay: Duration::from_millis(if reading {
                    self.read_delay_ms
                } else {
                    self.delay_ms
                }),
                fail: if reading { self.read_fail } else { self.fail },
            })?;
            self.busy = true;
        }
        Ok(())
    }

    /// Take the running job that the worker completed as `id`. Give it to `release`.
    pub(in crate::node) fn complete(&mut self, id: u64) -> Result<LogTask> {
        ensure!(self.busy, "unexpected log completion");
        let task = self.queue.pop_front().context("missing log job")?;
        ensure!(task.id == id, "log completion is out of order");
        self.busy = false;
        Ok(task)
    }

    /// Free the bytes of a finished job, and give back what `Node` does for it.
    pub(in crate::node) fn release(&mut self, task: LogTask) -> Finished {
        self.bytes -= task.bytes;
        self.snapshot_bytes -= task.snapshot_bytes;
        Finished {
            ready: task.ready,
            checkpoint: task.checkpoint,
            reads: task.reads,
            messages: task.messages,
            responses: task.responses,
        }
    }

    pub(in crate::node) fn set_fault(&mut self, delay_ms: u64, fail: bool) {
        self.delay_ms = delay_ms;
        self.fail = fail;
    }

    pub(in crate::node) fn set_read_fault(&mut self, delay_ms: u64, fail: bool) {
        self.read_delay_ms = delay_ms;
        self.read_fail = fail;
    }

    pub(in crate::node) fn add_peer_waiters(&mut self, responses: Vec<Response>) {
        self.peer_waiters.extend(responses);
    }

    pub(in crate::node) fn peer_waiters(&self) -> usize {
        self.peer_waiters.len()
    }

    /// The waiting step replies wait for the last queued job, or, with no job, are accepted now.
    pub(in crate::node) fn fence_peer_waiters(&mut self) {
        if let Some(task) = self.queue.back_mut() {
            task.responses.append(&mut self.peer_waiters);
        } else {
            for response in self.peer_waiters.drain(..) {
                let _ = response.send(Ok(json!({"accepted": true})));
            }
        }
    }

    /// Write the queue and fault fields of the member status.
    pub(in crate::node) fn status(&self, status: &mut Value) {
        status["log_busy"] = self.busy.into();
        status["queued_log_jobs"] = self.queue.len().into();
        status["queued_log_bytes"] = self.bytes.into();
        status["queued_snapshot_bytes"] = self.snapshot_bytes.into();
        status["log_delay_ms"] = self.delay_ms.into();
        status["log_fail"] = self.fail.into();
        status["log_read_delay_ms"] = self.read_delay_ms.into();
        status["log_read_fail"] = self.read_fail.into();
    }

    #[cfg(test)]
    pub(in crate::node) fn busy(&self) -> bool {
        self.busy
    }

    #[cfg(test)]
    pub(in crate::node) fn len(&self) -> usize {
        self.queue.len()
    }

    #[cfg(test)]
    pub(in crate::node) fn bytes(&self) -> usize {
        self.bytes
    }

    #[cfg(test)]
    pub(in crate::node) fn set_bytes(&mut self, bytes: usize) {
        self.bytes = bytes;
    }

    #[cfg(test)]
    pub(in crate::node) fn snapshot_bytes(&self) -> usize {
        self.snapshot_bytes
    }

    #[cfg(test)]
    pub(in crate::node) fn readys(&self) -> usize {
        self.queue.iter().map(|task| task.readys).sum()
    }

    /// Tests: accept `response` when the last queued job finishes.
    #[cfg(test)]
    pub(in crate::node) fn respond_after_last(&mut self, response: Response) {
        self.queue.back_mut().unwrap().responses.push(response);
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
