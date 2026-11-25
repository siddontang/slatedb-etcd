use super::*;

impl StateMachine {
    pub async fn snapshot_data(&mut self) -> Result<Vec<u8>> {
        let data = self.snapshot_job()?.await?;
        // Only a replicated export flushes the database. A shared view scan makes nothing durable.
        if !self.is_shared() {
            self.durable_applied = self.applied;
        }
        Ok(data)
    }

    /// Pin the applied prefix now, then flush and serialize it outside the Raft loop.
    pub fn snapshot_job(
        &self,
    ) -> Result<impl std::future::Future<Output = Result<Vec<u8>>> + Send + 'static> {
        // A shared-mode view needs no flush. Its rows are in a checkpoint or in memory.
        let snapshot = self.pinned_state()?;
        let db = self.database.clone();
        let metrics = self.metrics.clone();
        let applied = self.applied;
        // Shared mode ignores a cluster quota row from a converted state. A replicated restore
        // of this image must not use that old value.
        let shared = self.is_shared();
        Ok(async move {
            if let Some(db) = db {
                metrics.flush_db(&db).await?;
            }
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
                let quota_row = shared && row.key.as_ref() == quota::KEY;
                if !derived_key(&row.key) && !quota_row {
                    crate::codec::write(&mut data, &(row.key, row.value))?;
                    count += 1;
                }
            }
            // Replace the vector length after streaming its rows in the same binary format.
            crate::codec::write(&mut &mut data[count_offset..], &count)?;
            data.shrink_to_fit();
            Ok(data)
        })
    }

    pub async fn install_snapshot(&mut self, snapshot: &Snapshot) -> Result<()> {
        let index = snapshot.get_metadata().index;
        let term = snapshot.get_metadata().term;
        // Known terms at the same index must agree, for every snapshot format.
        if index == self.applied {
            self.reconcile_term(term)?;
        }
        if let Some(view) = transfer::ViewDescriptor::decode(snapshot.get_data())? {
            return Box::pin(self.install_view(view, index, term)).await;
        }
        ensure!(
            !self.is_shared() || index <= self.applied,
            "shared state mode installs only format 6 snapshots"
        );
        if let Some(descriptor) = transfer::Descriptor::decode(snapshot.get_data())? {
            // Box the copy and database open. They are large futures on the apply stack.
            return Box::pin(self.install_object(descriptor, index, term)).await;
        }
        if index <= self.applied {
            return Ok(());
        }
        ensure!(
            !snapshot.data.is_empty(),
            "state storage is behind the log truncation point"
        );
        // The installed rows replace the rows that a planned compaction step reads.
        self.compaction_plan.clear();
        // Installed journal rows reach watchers through storage catch-up.
        if let Some(capture) = &mut self.watch_capture {
            capture.invalidate();
        }
        let data: SnapshotData = crate::codec::decode(snapshot.get_data())?;
        ensure!(
            crate::readable_snapshot(data.version) && data.applied == index,
            "invalid snapshot metadata"
        );
        let mut batch = WriteBatch::new();
        let mut rows = self
            .applied_state()
            .scan_under(b"", &Default::default())
            .await?;
        while let Some(row) = rows.next().await? {
            batch.delete(row.key);
        }
        drop(rows);
        for (key, value) in data.rows {
            ensure!(snapshot_key(&key), "invalid snapshot key");
            // This batch derives the size and term rows again from the installed rows.
            if !derived_key(&key) {
                batch.put(key, value);
            }
        }
        crate::failpoint::hit("snapshot-before-install");
        // The snapshot metadata holds the term of the entry at `index`. Images have no term
        // row, except from an older exporter. The metadata replaces any stored value.
        self.entry_term = snapshot.get_metadata().term;
        self.mark_applied(&mut batch, index);
        self.write_batch(batch, index).await?;
        crate::failpoint::hit("snapshot-after-install-write");
        // The durable Raft snapshot lets a restart repeat this atomic replacement.
        self.metrics.flush_db(self.db()).await?;
        crate::failpoint::hit("snapshot-after-install-flush");
        self.applied = index;
        self.durable_applied = index;
        self.revision = read_revision(self.applied_state()).await?;
        self.compact_revision = read_compact_revision(self.applied_state()).await?;
        self.compaction = compaction::load(self.applied_state()).await?;
        self.plan_compaction();
        self.leases = Arc::new(crate::lease::load(self.applied_state()).await?);
        self.alarms = match self.applied_state().get(crate::maintenance::ALARMS).await? {
            Some(data) => serde_json::from_slice(&data)?,
            None => vec![],
        };
        self.reload_members().await?;
        self.auth = Arc::new(match self.applied_state().get(crate::auth::KEY).await? {
            Some(data) => serde_json::from_slice(&data)?,
            None => crate::auth::Store::default(),
        });
        // The installed cluster quota replaces the current one. Without it, the flag applies.
        self.reload_quota().await?;
        ensure!(
            self.compact_revision <= self.revision,
            "compaction exceeds current revision"
        );
        Ok(())
    }

    /// Check the applied term against the term of a Raft snapshot at the applied index.
    ///
    /// A checkpoint from an older binary has no valid term row, so its install sets the term
    /// only in memory. Startup passes the durable Raft snapshot again, and this fills the term.
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

    /// Open the copied checkpoint as the new database and replace the current one.
    /// `term` is the Raft snapshot term. It must equal a known term of the checkpoint.
    async fn install_object(
        &mut self,
        descriptor: transfer::Descriptor,
        index: u64,
        term: u64,
    ) -> Result<()> {
        if index <= self.applied {
            // A restart installs its durable snapshot again. A checkpoint without the row keeps
            // the membership of the descriptor in memory only, as the first install did.
            if index == self.applied && self.members_row.is_none() {
                self.members = Arc::new(serde_json::from_slice(&descriptor.members)?);
            }
            return Ok(());
        }
        ensure!(descriptor.applied == index, "invalid snapshot metadata");
        // Installed journal rows reach watchers through storage catch-up.
        if let Some(capture) = &mut self.watch_capture {
            capture.invalidate();
        }
        let location = self.location.clone();
        let staging = location.prepare(&descriptor).await?;
        crate::failpoint::hit("snapshot-before-install");
        let mut next = StateMachine::open_at(location.clone(), &location.path(&staging)).await?;
        if next.applied != index {
            let _ = next.close().await;
            anyhow::bail!("copied state does not match the snapshot index");
        }
        // A checkpoint from an older binary has no valid term. The snapshot supplies it.
        if next.applied_term != 0 && next.applied_term != term {
            let _ = next.close().await;
            anyhow::bail!("copied state does not match the snapshot term");
        }
        next.applied_term = term;
        next.entry_term = term;
        let old_name = location.switch(&staging, index).await?;
        crate::failpoint::hit("snapshot-after-install-write");
        next.metrics = self.metrics.clone();
        // The copied state supplies the cluster quota, if any.
        next.set_quota_flag(self.quota_flag());
        next.write_batches = self.write_batches;
        next.row_marker = self.row_marker.take();
        next.watch_capture = self.watch_capture.take();
        // The copied checkpoint holds the row, if the sender stored one. Without it, the
        // membership of the sender stays in memory only, as the bootstrap membership does.
        if next.members_row.is_none() {
            next.members = Arc::new(serde_json::from_slice(&descriptor.members)?);
        }
        let old = std::mem::replace(self, next);
        location.retire(old.db().clone(), old_name);
        drop(old);
        crate::failpoint::hit("snapshot-after-install-flush");
        Ok(())
    }
}
