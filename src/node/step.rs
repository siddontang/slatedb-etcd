//! The Raft tick and step of the node.

use super::*;

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
}
