//! Propose automatic Compact entries and report quota pressure.
//!
//! `crate::compactor` chooses the revision. The leader proposes it as an ordinary Compact
//! entry, so it uses the same apply path as a client Compact. Each Compact carries a fixed
//! revision that this member has applied. Raft order makes every replica apply the same
//! boundary, whichever leader proposed it. A stale proposal from an old leader can only fail
//! as already compacted.

use super::*;
use crate::compactor::{Compactor, Retention};
use crate::proto::etcdserverpb::CompactionResponse;

/// The warning clears below this share of its threshold, so the log does not repeat while
/// usage moves around the threshold.
const WARNING_CLEAR: f64 = 0.95;

pub(super) struct AutoCompaction {
    compactor: Compactor,
    reply: Option<oneshot::Receiver<Result<Value, ApiError>>>,
}

impl AutoCompaction {
    pub fn new(retention: Retention, now: Instant, revision: i64) -> Self {
        Self {
            compactor: Compactor::new(retention, now, revision),
            reply: None,
        }
    }

    /// Return true while a proposed Compact waits for its reply.
    #[cfg(test)]
    pub fn running(&self) -> bool {
        self.reply.is_some()
    }

    /// Return the result of the running Compact, or `None` while it runs.
    fn result(&mut self) -> Option<std::result::Result<(), String>> {
        let reply = self.reply.as_mut()?;
        let result = match reply.try_recv() {
            Err(oneshot::error::TryRecvError::Empty) => return None,
            Err(oneshot::error::TryRecvError::Closed) => Err("the reply was dropped".to_owned()),
            Ok(Err(error)) => Err(error.error),
            Ok(Ok(value)) => {
                match serde_json::from_value::<crate::mvcc::Outcome<CompactionResponse>>(value) {
                    Ok(outcome) => match outcome.result {
                        Ok(_) => Ok(()),
                        // Another Compact reached this revision first. etcd counts this as done.
                        Err(error) if error.message.ends_with(crate::mvcc::COMPACTED) => Ok(()),
                        Err(error) => Err(error.message),
                    },
                    Err(error) => Err(error.to_string()),
                }
            }
        };
        self.reply = None;
        Some(result)
    }
}

impl Node {
    /// Run one automatic compaction turn with the clock value `now`.
    pub(super) fn drive_auto_compaction(&mut self, now: Instant) -> Result<()> {
        let revision = self.state.revision;
        // A new Compact appends a target, which changes the input of the planned step and
        // restarts its read. While removal is pending, keep sampling but do not propose another
        // target, so a step read slower than the interval cannot be cancelled again and again.
        // A client Compact can still advance the boundary.
        let leader = self.raw.raft.state == StateRole::Leader && !self.state.compaction_pending;
        let node = self.options.id;
        let Some(auto) = &mut self.auto_compaction else {
            return Ok(());
        };
        if let Some(result) = auto.result() {
            let target = auto.compactor.finish(now, revision, result.is_ok());
            match result {
                Ok(()) => tracing::info!(node, revision = target, "Completed auto compaction"),
                Err(error) => {
                    tracing::warn!(node, revision = target, %error, "Failed auto compaction; retrying")
                }
            }
        }
        let Some(target) = auto.compactor.poll(now, revision, leader) else {
            return Ok(());
        };
        tracing::info!(node, revision = target, retention = ?auto.compactor.retention(),
            "Starting auto compaction");
        let (tx, rx) = oneshot::channel();
        auto.reply = Some(rx);
        // No credentials: this member issues the Compact itself, as etcd's compactor does.
        self.propose(
            RaftCommand::Compact {
                id: uuid::Uuid::new_v4().to_string(),
                compact: CompactionRequest {
                    revision: target,
                    physical: false,
                },
            },
            tx,
        )
    }

    /// The warning threshold of the enforced quota. Shared mode takes the quota from the
    /// cluster claim, which can differ from the local flag.
    pub(super) fn warning_bytes(&self) -> Option<u64> {
        super::warning_bytes(self.quota, self.options.quota_warning_fraction)
    }

    /// Log once when quota usage reaches the warning threshold, and once when it falls back.
    pub(super) fn check_quota_warning(&mut self) {
        let Some(threshold) = self.warning_bytes() else {
            return;
        };
        let usage = self.state.usage;
        let used = usage.total();
        if !self.quota_warned && used >= threshold {
            self.quota_warned = true;
            tracing::warn!(
                node = self.options.id,
                used,
                threshold,
                quota = self.quota,
                current = usage.current,
                history = usage.history,
                watch_journal = usage.journal,
                "Quota usage reached the warning threshold; compact history or raise the quota"
            );
        } else if self.quota_warned && (used as f64) < threshold as f64 * WARNING_CLEAR {
            self.quota_warned = false;
            tracing::info!(
                node = self.options.id,
                used,
                threshold,
                "Quota usage fell below the warning threshold"
            );
        }
    }
}
