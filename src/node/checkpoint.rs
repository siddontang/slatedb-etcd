//! Decide when this member starts a checkpoint, and keep the checkpoint that waits for its log
//! job and the manual checkpoint requests.

use std::time::{Duration, Instant};

use anyhow::{ensure, Result};
use raft::eraftpb::{ConfState, Entry};

use super::Response;
use crate::storage::PreparedCheckpoint;

/// The wait after a failed checkpoint before the next one starts.
const RETRY: Duration = Duration::from_secs(1);

/// The checkpoint schedule of one member.
///
/// One checkpoint runs at a time: from `start` until its log job stores the record (`stored`),
/// or until it fails (`failed`). `Node` checks the Raft and store state, and asks `due` for the
/// rest.
pub(super) struct Checkpointer {
    running: bool,
    /// The time before which no checkpoint starts after a failed one.
    retry_at: Option<Instant>,
    /// The appended log bytes that the running checkpoint took. A failed one gives them back.
    taken_bytes: u64,
    /// Log bytes appended since the last checkpoint started.
    appended_bytes: u64,
    /// Count full images started for peers or manual checkpoints.
    images: u64,
    /// The index of the next automatic checkpoint.
    next: Option<u64>,
    /// The index of the last applied membership change. A checkpoint must cover it.
    membership: u64,
    /// The built checkpoint that waits for its log job.
    built: Option<(u64, ConfState, Option<PreparedCheckpoint>)>,
    /// Manual checkpoint requests: the commit index at the request, and the reply.
    waiters: Vec<(u64, Response)>,
    /// Fault injection: the wait before a checkpoint job builds its image.
    delay_ms: u64,
}

impl Checkpointer {
    pub(super) fn new(next: Option<u64>) -> Self {
        Self {
            running: false,
            retry_at: None,
            taken_bytes: 0,
            appended_bytes: 0,
            images: 0,
            next,
            membership: 0,
            built: None,
            waiters: vec![],
            delay_ms: 0,
        }
    }

    /// Return true while a checkpoint runs.
    pub(super) fn running(&self) -> bool {
        self.running
    }

    /// The number of full images that this member started.
    pub(super) fn images(&self) -> u64 {
        self.images
    }

    /// The wait before a checkpoint job builds its image.
    pub(super) fn delay(&self) -> Duration {
        Duration::from_millis(self.delay_ms)
    }

    pub(super) fn set_delay(&mut self, delay_ms: u64) {
        self.delay_ms = delay_ms;
    }

    /// Set the index of the next automatic checkpoint. `None` stops automatic checkpoints.
    pub(super) fn set_next(&mut self, next: Option<u64>) {
        self.next = next;
    }

    /// Raft applied a membership change at `index`. A checkpoint at that index is due.
    pub(super) fn membership_changed(&mut self, index: u64) {
        self.membership = index;
    }

    /// Count the log bytes of these appended entries.
    pub(super) fn appended(&mut self, entries: &[Entry]) {
        self.appended_bytes = entries.iter().fold(self.appended_bytes, |total, entry| {
            total.saturating_add(entry.data.len() as u64)
        });
    }

    /// Return true if the automatic checkpoint is due at `applied`: the index and the appended
    /// bytes both reached their trigger.
    pub(super) fn automatic_due(&self, applied: u64, min_bytes: u64) -> bool {
        self.next.is_some_and(|index| applied >= index) && self.appended_bytes >= min_bytes
    }

    /// The checkpoint to start at `applied` with the record at `record`, if one is due.
    /// `Some(true)` builds a complete image; `Some(false)` only flushes state and truncates.
    ///
    /// `held` tells that the automatic checkpoint is due and that a follower needs the log. The
    /// caller also checks that no peer receives an image before a truncation.
    pub(super) fn due(
        &self,
        applied: u64,
        record: u64,
        image_can_advance: bool,
        image_wanted: bool,
        min_bytes: u64,
        held: bool,
    ) -> Option<bool> {
        // A failed checkpoint lets other apply jobs run before the next one.
        if self.retry_at.is_some_and(|at| Instant::now() < at) {
            return None;
        }
        let image_due = image_can_advance
            && (image_wanted || self.waiters.iter().any(|(index, _)| *index <= applied));
        // Automatic checkpoints only truncate the local log. A membership change needs a
        // checkpoint at its index.
        // A membership checkpoint does not wait for a lagging follower: a new member needs it.
        let forced = self.membership;
        let truncate_due = ((forced > record && applied >= forced)
            || (self.automatic_due(applied, min_bytes) && !held))
            && applied > record
            && !image_wanted
            && self.waiters.is_empty();
        (image_due || truncate_due).then_some(image_due)
    }

    /// A checkpoint job started. It takes the appended bytes. `image` counts a full image.
    pub(super) fn start(&mut self, image: bool) {
        self.running = true;
        self.taken_bytes = std::mem::take(&mut self.appended_bytes);
        if image {
            self.images += 1;
        }
    }

    /// The running checkpoint is built. It waits for its log job.
    pub(super) fn built(
        &mut self,
        index: u64,
        conf: ConfState,
        image: Option<PreparedCheckpoint>,
    ) -> Result<()> {
        ensure!(self.built.is_none(), "overlapping checkpoint results");
        self.built = Some((index, conf, image));
        Ok(())
    }

    /// A storage error failed the running checkpoint. The record stays, and the next checkpoint
    /// starts after `RETRY`.
    pub(super) fn failed(&mut self) -> Result<()> {
        ensure!(self.built.is_none(), "overlapping checkpoint results");
        self.running = false;
        self.retry_at = Some(Instant::now() + RETRY);
        // The bytes count again, so an automatic checkpoint stays due after the wait.
        self.appended_bytes = self.appended_bytes.saturating_add(self.taken_bytes);
        Ok(())
    }

    /// Take the built checkpoint for its log job.
    pub(super) fn take_built(&mut self) -> Option<(u64, ConfState, Option<PreparedCheckpoint>)> {
        self.built.take()
    }

    /// The log job stored the checkpoint record.
    pub(super) fn stored(&mut self) {
        self.running = false;
    }

    /// The number of waiting manual checkpoint requests.
    pub(super) fn waiting(&self) -> usize {
        self.waiters.len()
    }

    /// Wait for a checkpoint through `index`.
    pub(super) fn wait(&mut self, index: u64, response: Response) {
        self.waiters.push((index, response));
    }

    /// Take the waiting requests. The caller calls `wait` again for each request that still
    /// waits.
    pub(super) fn take_waiters(&mut self) -> Vec<(u64, Response)> {
        std::mem::take(&mut self.waiters)
    }

    /// The highest index of a waiting request.
    pub(super) fn highest_waiter(&self) -> Option<u64> {
        self.waiters.iter().map(|(index, _)| *index).max()
    }
}

#[cfg(test)]
impl Checkpointer {
    pub(in crate::node) fn end_retry_wait(&mut self) {
        self.retry_at = None;
    }

    pub(in crate::node) fn appended_bytes(&self) -> u64 {
        self.appended_bytes
    }

    pub(in crate::node) fn set_appended_bytes(&mut self, bytes: u64) {
        self.appended_bytes = bytes;
    }
}
