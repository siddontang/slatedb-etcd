//! Serve a demand for a base above the leader's base index, and keep records above a checkpoint
//! row away from members that cannot install them. See `certify_base`.

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
    ///
    /// The leader certifies above a row only while every other member advertised that it
    /// installs such a base. Otherwise it serves no demand, as an earlier binary does.
    pub(in crate::node) fn certify_base(&mut self) -> Result<()> {
        self.note_base();
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
                writer.exact_through = 0;
            }
        }
        let extend = self.extension_allowed();
        let wanted = self.raw.store().wanted_index();
        let Some(writer) = &mut self.writer else {
            return Ok(());
        };
        writer.certify_ticks += 1;
        let demand = writer.demand.max(wanted);
        let unmet = demand > *writer.applied.based.borrow();
        // Without the capability on every member, a demand gets a checkpoint at its row.
        self.serve_exact(!extend && unmet);
        let Some(writer) = &mut self.writer else {
            return Ok(());
        };
        if !extend || !unmet {
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

    /// Return true when every other member of the configuration advertised, on its current
    /// process, that it installs a base above the checkpoint row. A member that did not report
    /// yet counts as an earlier binary.
    pub(in crate::node) fn extension_allowed(&self) -> bool {
        let store = self.raw.store();
        let conf = self.raw.raft.prs().conf().to_conf_state();
        conf.voters
            .iter()
            .chain(&conf.learners)
            .filter(|&&id| id != self.options.id)
            .all(|&id| store.coverage_capable(id))
    }

    /// An earlier binary installs a view snapshot only at its checkpoint row. The log cache does
    /// not send it a record above the row, and asks here for a record at a row instead. `unmet`
    /// asks too: a demand while some member lacks the capability, so no certification serves it.
    ///
    /// If the base is at its own row, above the record and at or above the demand, the next
    /// record is at that row. Otherwise the leader publishes a checkpoint whose row is its applied
    /// index. When no batch is at that index, apply first stores one that holds only the apply
    /// marker, so the checkpoint row is there. That costs one flush and one checkpoint.
    fn serve_exact(&mut self, unmet: bool) {
        if !unmet && !self.raw.store().exact_wanted() {
            return;
        }
        let wanted = self.raw.store().wanted_index();
        let record = self.raw.store().snapshot_index();
        let applied = self.state.applied;
        let Some(writer) = &mut self.writer else {
            return;
        };
        let Some(base) = *writer.applied.base_id.borrow() else {
            return;
        };
        if base.index == base.row && base.index > record && base.index >= wanted {
            return;
        }
        if writer.exact_through >= applied {
            return;
        }
        writer.exact_through = applied;
        if writer.queue.pushed() < applied {
            writer.mark_row_due = true;
        }
        writer.progress.request(applied);
        tracing::info!(
            applied,
            record,
            "publish a checkpoint at its row for an earlier binary"
        );
    }

    /// Tell the log cache which checkpoint and index the base has. See `Cache::has_image`.
    pub(in crate::node) fn note_base(&self) {
        if let Some(writer) = &self.writer {
            let base = writer
                .applied
                .base_id
                .borrow()
                .map(|base| (base.checkpoint, base.index));
            self.raw.store().note_base(base);
        }
    }

    /// Take a due job that stores the apply marker at the applied index.
    pub(in crate::node) fn take_mark_row(&mut self) -> bool {
        self.writer
            .as_mut()
            .is_some_and(|writer| std::mem::take(&mut writer.mark_row_due))
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
