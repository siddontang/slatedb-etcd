//! Serve a demand for a base above the leader's base index. See `certify_base`.

use super::*;
use crate::state::overlay::Position;
use crate::state::shared::BaseRef;

/// Ticks before a leader proposes a certification again when the last one did not apply.
const CERTIFY_TICKS: u64 = 20;

impl Node {
    /// Serve a demand for a base above the base index of this leader: a follower's snapshot
    /// request at its last index, or a follower's manual checkpoint.
    ///
    /// The base holds every batch through its row. If no unreleased batch of this leader is
    /// above the row, the leader announces the same checkpoint again with its covered index.
    /// That costs one small entry and no checkpoint. The members then raise their base index,
    /// and the leader records the base at the new index for the snapshot. If a batch is above
    /// the row, the leader asks for a publication that holds it, and serves the demand with
    /// that checkpoint.
    ///
    /// The base comes first, then one position read gives the last batch and the covered
    /// index. A switch between the reads makes the check stricter, not weaker. One announcement
    /// serves the demand at a time. A lost proposal is sent again after `CERTIFY_TICKS`. The
    /// demand ends when the base index reaches it, so an idle cluster stops announcing.
    pub(in crate::node) fn certify_base(&mut self) -> Result<()> {
        if self.raw.raft.state != StateRole::Leader {
            return Ok(());
        }
        // A request of an earlier term can name a suffix that this leader replaced.
        let term = self.raw.raft.term;
        self.raw.store().lead_term(term);
        if let Some(writer) = &mut self.writer {
            if writer.demand_term != term {
                writer.demand_term = term;
                writer.demand = 0;
                writer.certified = None;
                writer.demand_publication = 0;
            }
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
        let Some(base) = *writer.applied.base_id.borrow() else {
            return Ok(());
        };
        let position = writer.queue.position();
        if position.covered < demand {
            return Ok(());
        }
        let change = last_change(Some(base), position);
        if base.row < change {
            if writer.demand_publication < change {
                writer.demand_publication = change;
                writer.request_publication(change);
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
        let covers = coverage(base.row, Some(base), position);
        let checkpoint = base.checkpoint;
        tracing::info!(%checkpoint, row = base.row, covers, demand, "certify the base for a demand");
        if self.propose_view(checkpoint, covers)? {
            if let Some(writer) = &mut self.writer {
                writer.certified = Some((covers, ticks));
            }
        }
        Ok(())
    }
}

/// The index after which the leader's batches hold no state change that its base lacks.
///
/// A batch at or below the base index was released: the base holds the state there. Such a
/// batch can be above the base row when this member grouped entries in another way, but it
/// changes nothing that the row lacks. The last real change is then at or below the row.
pub(super) fn last_change(base: Option<BaseRef>, position: Position) -> u64 {
    match base {
        Some(base) if position.pushed <= base.index => position.pushed.min(base.row),
        _ => position.pushed,
    }
}

/// The index through which a checkpoint at `row` holds the whole state of the leader at
/// `position`, with the leader's base `base`.
///
/// A checkpoint holds every batch through its row. When no state change of the leader is above
/// the row, no entry through the covered index changed state, so the checkpoint holds the state
/// there too. An unreleased batch above the row, materialized or not, keeps the coverage at the
/// row. The base comes before the position, so a later batch only makes the result smaller.
pub(super) fn coverage(row: u64, base: Option<BaseRef>, position: Position) -> u64 {
    if row >= last_change(base, position) {
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
        let position = |pushed, covered| Position { pushed, covered };
        assert_eq!(coverage(10, None, position(11, 12)), 10);
        assert_eq!(coverage(10, None, position(10, 12)), 12);
        assert_eq!(coverage(11, None, position(11, 12)), 12);
        // A checkpoint ahead of this member's apply covers its own row.
        assert_eq!(coverage(15, None, position(10, 12)), 15);
    }

    /// A member grouped entries 10 and 11 into a batch at 11. Its base has row 10 and index 11,
    /// so it released that batch. The batch changes no state, so the row covers 12. An
    /// unreleased batch at 13 still keeps the coverage at the row.
    #[test]
    fn a_released_batch_above_the_row_keeps_the_coverage() {
        let position = |pushed, covered| Position { pushed, covered };
        let base = Some(BaseRef {
            checkpoint: uuid::Uuid::nil(),
            row: 10,
            index: 11,
        });
        assert_eq!(last_change(base, position(11, 12)), 10);
        assert_eq!(coverage(10, base, position(11, 12)), 12);
        assert_eq!(coverage(10, base, position(13, 13)), 10);
        // A batch below the row was released with it. The last change is that batch.
        assert_eq!(last_change(base, position(9, 12)), 9);
    }
}
