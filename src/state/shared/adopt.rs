//! Adopt a new base: a rebase after a missing object, an opened base above the applied index,
//! and an installed snapshot. See "Rebase" and "Install" in `docs/shared-state.md`.

use super::*;

impl StateMachine {
    /// Make `base` the state at its index `F` and drop the overlay. The Raft log must already
    /// hold the matching boundary: a received snapshot, or a log that covers `F`.
    ///
    /// `term` is the Raft term at `F` when the caller knows it. A known checkpoint term must
    /// match it.
    ///
    /// The candidate loads and passes its checks on a view of the base alone first. Only then
    /// does the state drop its overlay and its materializer queue. A failed read leaves the
    /// state as it was, so the caller can try again. A storage error of a load read keeps its
    /// class, because the load changes nothing. Any other load error comes from the rows of the
    /// base, so it is corrupt stored data.
    pub(crate) async fn adopt_base(&mut self, base: Arc<Base>, term: u64) -> Result<()> {
        let reads = StateView::Shared(SharedView::new(
            base.clone(),
            Arc::from([Arc::default()]),
            u64::MAX,
        ));
        let rows = Arc::new(rows::RowCache::new(self.location.cache.row_bytes));
        let admin = self.admin.clone();
        let location = self.location.clone();
        // The candidate loads with a `Shared` of its own. Only a valid candidate takes the
        // current one, whose handles the writer holds.
        let shared = &self.shared;
        let own = Shared::new(
            base.clone(),
            shared.source.clone(),
            shared.store_id,
            shared.pin_max,
            shared.holders.clone(),
        );
        let mut next = Self::load(admin, location, rows, reads, own)
            .await
            .map_err(candidate_fault)?;
        if next.applied_term != 0 && term != 0 {
            ensure!(
                next.applied_term == term,
                "checkpoint term {} differs from Raft term {term}",
                next.applied_term
            );
        }
        // The candidate is valid. Nothing below fails, so the switch is whole.
        std::mem::swap(&mut next.shared, &mut self.shared);
        next.shared.reset(base);
        next.reads = next.shared.live();
        if next.applied_term == 0 {
            next.applied_term = term;
            next.entry_term = term;
        }
        next.metrics = self.metrics.clone();
        next.quota = self.quota;
        next.quota_flag = self.quota_flag;
        // A base before the seed has no row. The bootstrap membership then stays in memory.
        if next.members_row.is_none() {
            next.members = self.members.clone();
        }
        next.write_batches = self.write_batches;
        // The new rows reach watchers through storage catch-up.
        next.watch_capture = self.watch_capture.take();
        if let Some(capture) = &mut next.watch_capture {
            capture.invalidate();
        }
        *self = next;
        self.plan_compaction();
        Ok(())
    }

    /// Rebuild the state from the newest checkpoint at or below `limit` after a missing object.
    /// The caller replays the local log from the new `applied` to `limit`.
    ///
    /// The log can start above the newest checkpoint row: a member truncates through its base
    /// index. If no newer checkpoint reaches that index, the rebase opens the current base again
    /// and raises it to its index. `term` gives the Raft term at an index of the local log.
    ///
    /// The base index never goes down. So when the current base is gone and no checkpoint has
    /// its row from the base index to `limit`, the rebase fails with a missing object and
    /// changes nothing. An older checkpoint cannot serve, and a newer one needs a snapshot.
    pub(crate) async fn rebase(
        &mut self,
        limit: u64,
        term: impl FnOnce(u64) -> Result<u64>,
    ) -> Result<()> {
        let shared = &self.shared;
        let (current, row, index) = (
            shared.base.checkpoint,
            shared.base.applied,
            shared.base_index,
        );
        // The search reads manifests and opens a checkpoint. It changes nothing, so a storage
        // error keeps its class, and apply can run the rebase again.
        let mark = crate::mvcc::storage_fault::read_cause;
        let newest = newest_base_through(&self.admin, &shared.source, limit)
            .await
            .map_err(mark)?;
        let base = match (newest, current) {
            (Some(base), _) if base.applied >= index => base,
            (_, Some(current)) if index > row => {
                reopen_above_row(&self.admin, &shared.source, current, Some(row), index)
                    .await
                    .map_err(mark)?
            }
            _ => {
                return Err(anyhow::Error::new(crate::mvcc::storage_fault::Classified {
                    fault: crate::mvcc::storage_fault::StorageFault::Missing,
                    message: format!(
                        "the base is gone, and no view checkpoint has a row from the base \
                         index {index} to the rebase index {limit}"
                    ),
                }))
            }
        };
        tracing::warn!(
            base = base.applied,
            index,
            applied = self.applied,
            "rebase on a new checkpoint"
        );
        let extend = index > row && (row..index).contains(&base.applied);
        // Read the term first, so a failed log read leaves the state as it was.
        let term = extend.then(|| term(index)).transpose()?;
        self.adopt_base(base, 0).await?;
        if let Some(term) = term {
            self.extend_base(index, term)?;
        }
        Ok(())
    }

    /// Adopt the opened base above the applied index. A paused member uses this when its log
    /// commits the base index, so the Raft log needs no change. `term` is the log term there.
    pub(crate) async fn adopt_opened(&mut self, base: Arc<Base>, term: u64) -> Result<()> {
        ensure!(base.applied > self.applied, "the opened base is not ahead");
        self.adopt_base(base, term).await
    }

    /// Install a format 6 snapshot: the checkpoint that Raft sent. The receive handler opened it.
    ///
    /// `reader` is the base of the install job. The first try takes it from the receipt, or it
    /// opens the checkpoint. A failed try leaves it there, so the next try needs no new open. A
    /// later receipt of another snapshot cannot replace it then. The caller drops it when the
    /// job ends: after the install, a newer accepted snapshot, or a stop.
    pub(crate) async fn install_view(
        &mut self,
        descriptor: transfer::ViewDescriptor,
        index: u64,
        term: u64,
        reader: &mut Option<Arc<Base>>,
    ) -> Result<()> {
        let shared = &self.shared;
        let id = descriptor.checkpoint()?;
        if index <= self.applied {
            // Release the receipt, also for a snapshot that applies nothing.
            let received = shared.source.take_received(id);
            // The member applied past the record, so it installs nothing. The checkpoint holds
            // the state at `index`, so it can still raise the base there. The base index then
            // stays at or above the record index.
            if index > shared.base_index {
                let base = match received {
                    _ if shared.base.checkpoint == Some(id) => Ok(shared.base.clone()),
                    Some(base) => Ok(base),
                    None => shared.source.open_base(id).await,
                };
                match base {
                    Ok(base) => shared.opened.offer(base, index),
                    Err(error) => tracing::warn!(%error, %id, "cannot open a received checkpoint"),
                }
            }
            return Ok(());
        }
        // The job keeps the base across its tries, so a retry after a failed load still has
        // it. See "Receive" in `docs/shared-state.md`.
        let base = match reader.as_ref().filter(|base| base.checkpoint == Some(id)) {
            Some(base) => base.clone(),
            None => {
                let base = match shared.source.take_received(id) {
                    Some(base) => base,
                    // A storage error of the open keeps its class, so apply can retry.
                    None => shared
                        .source
                        .open_base(id)
                        .await
                        .map_err(crate::mvcc::storage_fault::read_cause)?,
                };
                *reader = Some(base.clone());
                base
            }
        };
        // The record index can be above the checkpoint row: the leader certified that no entry
        // in between changed state. The record gives the term at its own index only.
        ensure!(
            base.applied <= index,
            "checkpoint index {} is above snapshot index {index}",
            base.applied
        );
        let row_term = if base.applied == index { term } else { 0 };
        self.adopt_base(base, row_term).await?;
        // The state holds the base now, and the base pins its checkpoint.
        reader.take();
        self.extend_base(index, term)
    }
}

/// Mark an error of the load of a new base. A storage error keeps its class. Any other error is
/// a row of the base that does not decode or contradicts another row. The load also builds the
/// state meters, which fail only on invalid static options, so no member-local error comes here.
fn candidate_fault(error: anyhow::Error) -> anyhow::Error {
    match crate::mvcc::storage_fault::classify(&error) {
        Some(_) => crate::mvcc::storage_fault::read_cause(error),
        None => {
            crate::mvcc::storage_fault::stored(format!("the new base does not load: {error:#}"))
        }
    }
}

#[cfg(test)]
#[path = "adopt_tests.rs"]
mod tests;
