//! Serve a demand for a base above the leader's base index, and keep records above a checkpoint
//! row away from members that cannot install them. See `certify_base`.

use crate::state::overlay::Position;
use crate::state::shared::BaseRef;

/// Ticks before a leader proposes a certification again when the last one did not apply.
pub(super) const CERTIFY_TICKS: u64 = 20;

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
