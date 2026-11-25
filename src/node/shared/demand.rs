//! The coverage that a shared leader certifies for its checkpoints. See `coverage`.

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
