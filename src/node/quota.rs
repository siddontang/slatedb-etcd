//! The quota flag of this member, and the Raft tick and step of the node.

use super::*;

/// The quota flag of this member.
pub(super) struct Quota {
    flag: i64,
}

impl Quota {
    /// The quota flag with the effective value `flag`. A non-positive value disables the quota.
    pub(super) fn new(flag: i64) -> Self {
        Self { flag }
    }
}

impl Node {
    /// Tick Raft. Between an accepted snapshot and its install, Raft scans the unapplied entries
    /// from its applied index at a campaign. The log starts above that index, so the scan fails
    /// and Raft stops the process. The install is local, so the member waits for it.
    pub(super) fn tick_raft(&mut self) -> Result<()> {
        if self.installing() {
            self.hold_campaign();
        }
        self.raw.tick();
        self.propose_term_start()
    }

    /// Step a peer message. A member that waits for a snapshot install ignores a leadership
    /// transfer.
    pub(super) fn step_peer(&mut self, message: Message) -> raft::Result<()> {
        if message.get_msg_type() == MessageType::MsgTimeoutNow && self.installing() {
            tracing::info!(
                from = message.from,
                "ignore a leadership transfer before a snapshot"
            );
            return Ok(());
        }
        self.raw.step(message)?;
        if let Err(error) = self.propose_term_start() {
            tracing::warn!(%error, "cannot propose the term-start entries");
        }
        Ok(())
    }

    /// Return true when this member leads.
    pub(super) fn acts_as_leader(&mut self) -> bool {
        self.raw.raft.state == StateRole::Leader
    }

    /// The quota fields of `/status`.
    pub(super) fn quota_status(&self) -> Value {
        json!({
            "bytes": self.state.quota_bytes,
            "flag_bytes": self.quota.flag,
            "flag_mismatch": self.state.quota_mismatch,
        })
    }

    /// The `db_size_quota` of Maintenance Status. Like etcd, it is the configured value, unless
    /// the quota that apply uses differs from it.
    pub(super) fn status_quota(&self) -> i64 {
        if self.state.quota_mismatch {
            self.state.quota_bytes
        } else {
            self.options.quota_bytes
        }
    }
}
