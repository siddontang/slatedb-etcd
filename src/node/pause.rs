//! Serve a member whose apply waits on a storage error. See "Read faults" in
//! `docs/shared-state.md`.
//!
//! A transient error pauses apply, and the job retries. Corrupt stored data holds apply on its
//! entry. The member stays up and serves reads of its earlier state. A held follower asks its
//! leader for a snapshot above the held entry, and a snapshot that Raft accepts ends the
//! waiting job at its applied prefix. A linearizable read that needs a later entry fails at
//! once instead of waiting for the read timeout.

use super::*;

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
#[path = "pause_tests.rs"]
mod tests;
