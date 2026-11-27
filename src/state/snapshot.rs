use super::*;

impl StateMachine {
    /// Build a row image in memory. A view scan makes nothing durable.
    pub async fn snapshot_data(&mut self) -> Result<Vec<u8>> {
        self.snapshot_job()?.await
    }

    /// Pin the applied prefix now, then serialize it outside the Raft loop.
    pub fn snapshot_job(
        &self,
    ) -> Result<impl std::future::Future<Output = Result<Vec<u8>>> + Send + 'static> {
        self.image_job(false)
    }

    /// Like `snapshot_job`. With `align`, pad the membership row for `etcdctl snapshot save`.
    /// See `backup::pad_members`.
    pub(super) fn image_job(
        &self,
        align: bool,
    ) -> Result<impl std::future::Future<Output = Result<Vec<u8>>> + Send + 'static> {
        // A view needs no flush. Its rows are in a checkpoint or in memory.
        let snapshot = self.pinned_state();
        let applied = self.applied;
        // A backup carries the membership in memory as its last row, padded for etcdctl. A
        // restore replaces it. A Raft image sends the stored row as it is. See `install_object`.
        let members = if align {
            Some(serde_json::to_vec(self.members.as_ref())?)
        } else {
            None
        };
        Ok(async move {
            let mut data = crate::codec::encode(&(crate::SNAPSHOT_FORMAT, applied))?;
            let count_offset = data.len();
            crate::codec::write(&mut data, &0_u64)?;
            let mut count = 0_u64;
            let mut iterator = snapshot.scan_under(b"", &Default::default()).await?;
            while let Some(row) = iterator.next().await? {
                // Format 4 readers reject the derived meta rows. Install takes the term from the
                // Raft snapshot metadata and counts the logical size from the rows. The stored
                // membership row goes as it is. A state without it sends none, so the install
                // stores no member-local row.
                let members_row = members.is_some() && row.key.as_ref() == crate::cluster::KEY;
                if !derived_key(&row.key) && !members_row {
                    crate::codec::write(&mut data, &(row.key, row.value))?;
                    count += 1;
                }
            }
            if let Some(mut members) = members {
                backup::pad_members(&mut members, data.len() as u64)?;
                crate::codec::write(&mut data, &(crate::cluster::KEY, members))?;
                count += 1;
            }
            // Replace the vector length after streaming its rows in the same binary format.
            crate::codec::write(&mut &mut data[count_offset..], &count)?;
            data.shrink_to_fit();
            Ok(data)
        })
    }

    /// Shared mode: raise the base to the checkpoint of a Raft snapshot record that this member
    /// applied past. Raft persisted the record, but apply installs nothing for it. The base
    /// index then normally reaches the record index. See `install_view`.
    pub async fn raise_to_record(&mut self, snapshot: &Snapshot) -> Result<()> {
        ensure!(
            snapshot.get_metadata().index <= self.applied,
            "the record is above the applied index"
        );
        self.install_snapshot(snapshot).await
    }

    pub async fn install_snapshot(&mut self, snapshot: &Snapshot) -> Result<()> {
        self.install_snapshot_with(snapshot, &mut None).await
    }

    /// Install `snapshot`. A shared-mode install keeps the base of its checkpoint in `reader`
    /// across tries. See `install_view`.
    pub(crate) async fn install_snapshot_with(
        &mut self,
        snapshot: &Snapshot,
        reader: &mut Option<std::sync::Arc<super::view::Base>>,
    ) -> Result<()> {
        let index = snapshot.get_metadata().index;
        let term = snapshot.get_metadata().term;
        // Known terms at the same index must agree, for every snapshot format.
        if index == self.applied {
            self.reconcile_term(term)?;
        }
        if let Some(view) = transfer::ViewDescriptor::decode(snapshot.get_data())? {
            return Box::pin(self.install_view(view, index, term, reader)).await;
        }
        ensure!(
            index <= self.applied,
            "shared state mode installs only format 6 snapshots"
        );
        Ok(())
    }

    /// Check the applied term against the term of a Raft snapshot at the applied index.
    ///
    /// A state without a valid term row gets the term only in memory. Startup passes the durable
    /// Raft snapshot again, and this fills the term.
    /// The row is not written here: a SlateDB write needs a sequence above `applied`. The
    /// durable snapshot stays until the log truncates at a later checkpoint, and the batch of
    /// any later entry stores the term.
    fn reconcile_term(&mut self, term: u64) -> Result<()> {
        if term == 0 {
            return Ok(());
        }
        if self.applied_term == 0 {
            self.applied_term = term;
            self.entry_term = term;
        }
        ensure!(
            self.applied_term == term,
            "applied term {} differs from snapshot term {term}",
            self.applied_term
        );
        Ok(())
    }
}
