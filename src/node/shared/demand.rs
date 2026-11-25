//! Serve a demand for a base above the leader's base index. See `certify_base`.

use super::*;

/// Ticks before a leader proposes a certification again when the last one did not apply.
const CERTIFY_TICKS: u64 = 20;

impl Node {
    /// Serve a demand for a base above the base index of this leader: a follower's snapshot
    /// request at its last index, or a follower's manual checkpoint.
    ///
    /// The base holds every batch through its row. If this leader stored no batch above the row,
    /// the leader announces the same checkpoint again with its covered index. That costs one
    /// small entry and no checkpoint. The members then raise their base index, and the leader
    /// records the base at the new index for the snapshot. If a batch is above the row, the
    /// leader asks for a publication that holds it, and serves the demand with that checkpoint.
    ///
    /// The base row comes first, then one position read gives the last batch and the covered
    /// index. A switch between the reads makes the check stricter, not weaker. One announcement
    /// serves the demand at a time. A lost proposal is sent again after `CERTIFY_TICKS`. The
    /// demand ends when the base index reaches it, so an idle cluster stops announcing.
    pub(in crate::node) fn certify_base(&mut self) -> Result<()> {
        if self.raw.raft.state != StateRole::Leader {
            return Ok(());
        }
        let wanted = self.raw.store().wanted_index();
        let Some(writer) = &mut self.writer else {
            return Ok(());
        };
        writer.certify_ticks += 1;
        let demand = writer.demand.max(wanted);
        if demand <= *writer.applied.based.borrow() {
            writer.certified = None;
            return Ok(());
        }
        let Some((checkpoint, row)) = *writer.applied.base_id.borrow() else {
            return Ok(());
        };
        let position = writer.queue.position();
        if position.covered < demand {
            return Ok(());
        }
        if row < position.pushed {
            if writer.demand_publication < position.pushed {
                writer.demand_publication = position.pushed;
                writer.request_publication(position.pushed);
            }
            return Ok(());
        }
        let ticks = writer.certify_ticks;
        if writer
            .certified
            .is_some_and(|(covers, at)| covers >= demand && ticks < at + CERTIFY_TICKS)
        {
            return Ok(());
        }
        let covers = coverage(row, position);
        tracing::info!(%checkpoint, row, covers, demand, "certify the base for a demand");
        if self.propose_view(checkpoint, covers)? {
            if let Some(writer) = &mut self.writer {
                writer.certified = Some((covers, ticks));
            }
        }
        Ok(())
    }
}

/// The index through which a checkpoint at `row` holds the whole state of the leader at
/// `position`.
///
/// A checkpoint holds every batch through its row. When no batch of the leader is above the row,
/// no entry through the covered index changed state, so the checkpoint holds the state there
/// too. A batch above the row, materialized or not, keeps the coverage at the row. The position
/// gives the newest batch and the covered index from one applied state.
pub(super) fn coverage(row: u64, position: crate::state::overlay::Position) -> u64 {
    if row >= position.pushed {
        position.covered.max(row)
    } else {
        row
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Condition (a) of #96: a Put at 11 is still in the overlay when apply reaches 12. A
    /// checkpoint at 10 then covers only 10. With no batch above the row, it covers 12.
    #[test]
    fn a_checkpoint_covers_the_applied_index_only_without_a_later_batch() {
        use crate::state::overlay::Position;
        let position = |pushed, covered| Position { pushed, covered };
        assert_eq!(coverage(10, position(11, 12)), 10);
        assert_eq!(coverage(10, position(10, 12)), 12);
        assert_eq!(coverage(11, position(11, 12)), 12);
        // A checkpoint ahead of this member's apply covers its own row.
        assert_eq!(coverage(15, position(10, 12)), 15);
    }
}
