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
            let mut data = crate::codec::encode(&(STORAGE_FORMAT, applied))?;
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
        if index <= self.applied {
            return Ok(());
        }
        self.checkpoint_delta = None;
        // Installed journal rows reach watchers through storage catch-up.
        if let Some(capture) = &mut self.watch_capture {
            capture.invalidate();
        }
        let data: SnapshotData = crate::codec::decode(snapshot.get_data())?;
        ensure!(
            data.version == STORAGE_FORMAT && data.applied == index,
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
}
