use super::*;

impl StateMachine {
    pub async fn snapshot_data(&mut self) -> Result<Vec<u8>> {
        let data = self.snapshot_job()?.await?;
        self.durable_applied = self.applied;
        Ok(data)
    }

    /// Pin the applied prefix now, then flush and serialize it outside the Raft loop.
    pub fn snapshot_job(
        &self,
    ) -> Result<impl std::future::Future<Output = Result<Vec<u8>>> + Send + 'static> {
        let snapshot = self.db.snapshot()?;
        let db = Arc::clone(&self.db);
        let metrics = self.metrics.clone();
        let applied = self.applied;
        let members = serde_json::to_vec(self.members.as_ref())?;
        ensure!(
            snapshot.seq() == applied,
            "snapshot does not match the apply marker"
        );
        Ok(async move {
            metrics.flush_db(&db).await?;
            let mut data = crate::codec::encode(&(crate::SNAPSHOT_FORMAT, applied))?;
            let count_offset = data.len();
            crate::codec::write(&mut data, &0_u64)?;
            let mut count = 0_u64;
            let mut iterator = snapshot.scan_prefix(b"", ..).await?;
            while let Some(row) = iterator.next().await? {
                if row.key.as_ref() != crate::cluster::KEY {
                    crate::codec::write(&mut data, &(row.key, row.value))?;
                    count += 1;
                }
            }
            crate::codec::write(&mut data, &(crate::cluster::KEY, members))?;
            count += 1;
            // Replace the vector length after streaming its rows in the same binary format.
            crate::codec::write(&mut &mut data[count_offset..], &count)?;
            data.shrink_to_fit();
            Ok(data)
        })
    }

    pub async fn install_snapshot(&mut self, snapshot: &Snapshot) -> Result<()> {
        let index = snapshot.get_metadata().index;
        if let Some(descriptor) = transfer::Descriptor::decode(snapshot.get_data())? {
            // Box the copy and database open. They are large futures on the apply stack.
            return Box::pin(self.install_object(descriptor, index)).await;
        }
        if index <= self.applied {
            return Ok(());
        }
        ensure!(
            !snapshot.data.is_empty(),
            "state storage is behind the log truncation point"
        );
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
        let mut iterator = self.db.scan_prefix(b"", ..).await?;
        while let Some(row) = iterator.next().await? {
            batch.delete(row.key);
        }
        for (key, value) in data.rows {
            ensure!(
                key.starts_with(DATA) || key.starts_with(b"etcd/") || key == APPLIED,
                "invalid snapshot key"
            );
            batch.put(key, value);
        }
        crate::failpoint::hit("snapshot-before-install");
        batch.put(APPLIED, index.to_be_bytes());
        self.write_batch(batch, index).await?;
        crate::failpoint::hit("snapshot-after-install-write");
        // The durable Raft snapshot lets a restart repeat this atomic replacement.
        self.metrics.flush_db(&self.db).await?;
        crate::failpoint::hit("snapshot-after-install-flush");
        self.applied = index;
        self.durable_applied = index;
        self.revision = read_revision(&self.db).await?;
        self.compact_revision = read_compact_revision(&self.db).await?;
        self.compaction = compaction::load(&self.db).await?;
        self.leases = Arc::new(crate::lease::load(&self.db).await?);
        self.alarms = match self.db.get(crate::maintenance::ALARMS).await? {
            Some(data) => serde_json::from_slice(&data)?,
            None => vec![],
        };
        self.members = Arc::new(match self.db.get(crate::cluster::KEY).await? {
            Some(data) => serde_json::from_slice(&data)?,
            None => crate::cluster::Store::default(),
        });
        self.auth = Arc::new(match self.db.get(crate::auth::KEY).await? {
            Some(data) => serde_json::from_slice(&data)?,
            None => crate::auth::Store::default(),
        });
        ensure!(
            self.compact_revision <= self.revision,
            "compaction exceeds current revision"
        );
        Ok(())
    }

    /// Open the copied checkpoint as the new database and replace the current one.
    async fn install_object(&mut self, descriptor: transfer::Descriptor, index: u64) -> Result<()> {
        if index <= self.applied {
            // A bootstrap membership is stored by the first write after the install.
            if index == self.applied && self.db.get(crate::cluster::KEY).await?.is_none() {
                self.members = Arc::new(serde_json::from_slice(&descriptor.members)?);
                self.members_unsaved = true;
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
        let old_name = location.switch(&staging).await?;
        crate::failpoint::hit("snapshot-after-install-write");
        next.metrics = self.metrics.clone();
        next.quota = self.quota;
        next.write_batches = self.write_batches;
        next.row_marker = self.row_marker.take();
        next.watch_capture = self.watch_capture.take();
        next.members = Arc::new(serde_json::from_slice(&descriptor.members)?);
        next.members_unsaved = true;
        let old = std::mem::replace(self, next);
        location.retire(old.db.clone(), old_name);
        drop(old);
        crate::failpoint::hit("snapshot-after-install-flush");
        Ok(())
    }
}
