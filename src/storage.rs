use std::path::{Path, PathBuf};

use anyhow::{ensure, Context, Result};
use raft::eraftpb::{ConfState, Entry, HardState, Snapshot};
use raft::{GetEntriesContext, RaftState, Storage, StorageError};
use raft_engine::{Command as EngineCommand, Config, Engine, LogBatch, MessageExt, ReadableSize};
use serde::Serialize;

mod checkpoint;
pub use checkpoint::PreparedCheckpoint;
mod filesystem;
use filesystem::LogFileSystem;

const GROUP: u64 = 1;
const HARD_STATE: &[u8] = b"hard-state";
const SNAPSHOT: &[u8] = b"snapshot";
const IDENTITY: &[u8] = b"identity";

struct EntryCodec;

impl MessageExt for EntryCodec {
    type Entry = Entry;

    fn index(entry: &Entry) -> u64 {
        entry.index
    }
}

/// Stores Raft metadata, log entries, and a full recovery snapshot on local disk.
pub struct RaftStorage {
    engine: Engine<LogFileSystem>,
    pub(crate) wal_metrics: crate::metrics::Wal,
    hard_state: HardState,
    snapshot: Snapshot,
    identity: Vec<u8>,
    dir: PathBuf,
    ready_batch: Option<LogBatch>,
    pub sync_writes: u64,
}

#[derive(Debug, Serialize)]
pub struct LogStats {
    pub files: Vec<String>,
    pub bytes: u64,
}

impl RaftStorage {
    pub fn open(dir: &Path, node_id: u64, cluster_id: &str, voters: Vec<u64>) -> Result<Self> {
        let config = Config {
            dir: dir.to_string_lossy().into_owned(),
            target_file_size: ReadableSize::kb(256),
            purge_threshold: ReadableSize::kb(512),
            batch_compression_threshold: ReadableSize(0),
            enable_log_recycle: false,
            ..Config::default()
        };
        let wal_metrics = crate::metrics::Wal::new()?;
        let engine = Engine::open_with_file_system(
            config,
            std::sync::Arc::new(LogFileSystem(wal_metrics.clone())),
        )?;
        let identity = serde_json::to_vec(&(
            crate::STORAGE_FORMAT,
            node_id,
            cluster_id,
            &voters,
            "checkpoint-files",
        ))?;
        let stored = engine.get(GROUP, IDENTITY);
        let mut snapshot = engine
            .get_message::<Snapshot>(GROUP, SNAPSHOT)?
            .unwrap_or_default();
        if let Some(stored) = stored {
            ensure!(
                stored == identity
                    || stored
                        == serde_json::to_vec(&(
                            crate::STORAGE_FORMAT,
                            node_id,
                            cluster_id,
                            &voters
                        ))?,
                "data directory has another format, node, or cluster"
            );
        } else {
            ensure!(engine.last_index(GROUP).is_none(), "log has no identity");
            snapshot
                .mut_metadata()
                .set_conf_state(ConfState::from((voters, vec![])));
            let mut batch = LogBatch::default();
            batch.put(GROUP, IDENTITY.to_vec(), identity.clone())?;
            batch.put_message(GROUP, SNAPSHOT.to_vec(), &snapshot)?;
            engine.write(&mut batch, true)?;
        }
        let referenced = checkpoint::load(dir, &engine, &mut snapshot)?;
        let hard_state = engine.get_message(GROUP, HARD_STATE)?.unwrap_or_default();
        let this = Self {
            engine,
            wal_metrics,
            hard_state,
            snapshot,
            identity,
            dir: dir.to_owned(),
            ready_batch: None,
            sync_writes: 0,
        };
        ensure!(
            this.hard_state.commit >= this.snapshot_index(),
            "commit precedes snapshot"
        );
        ensure!(
            this.hard_state.commit <= this.last_index()?,
            "commit exceeds durable log"
        );
        checkpoint::remove_orphans(dir, referenced)?;
        Ok(this)
    }

    /// Write a Ready. Sync entries, snapshots, terms, and votes before Raft acknowledgements.
    pub fn persist(
        &mut self,
        entries: &[Entry],
        hard_state: Option<&HardState>,
        snapshot: Option<&Snapshot>,
    ) -> Result<()> {
        if entries.is_empty() && hard_state.is_none() && snapshot.is_none() {
            return Ok(());
        }
        // Raft commit progress is volatile. Entries, terms, and votes require stable storage.
        let must_sync = !entries.is_empty()
            || snapshot.is_some()
            || hard_state.is_some_and(|hs| {
                hs.term != self.hard_state.term || hs.vote != self.hard_state.vote
            });
        let mut batch = self
            .ready_batch
            .take()
            .unwrap_or_else(|| LogBatch::with_capacity(4));
        if let Some(snapshot) = snapshot {
            ensure!(
                snapshot.get_metadata().index > self.snapshot_index(),
                "stale snapshot"
            );
            // Replacing a snapshot discards the old log, including its divergent suffix.
            batch.add_command(GROUP, EngineCommand::Clean);
            batch.put(GROUP, IDENTITY.to_vec(), self.identity.clone())?;
            let hs = hard_state.context("snapshot Ready has no HardState")?;
            ensure!(
                hs.commit >= snapshot.get_metadata().index,
                "snapshot exceeds commit"
            );
            PreparedCheckpoint::write(&self.dir, snapshot.data.clone())?
                .publish(&self.dir, snapshot, &mut batch)?;
        }
        batch.add_entries::<EntryCodec>(GROUP, entries)?;
        if let Some(hs) = hard_state {
            batch.put_message(GROUP, HARD_STATE.to_vec(), hs)?;
        }
        if snapshot.is_some() {
            crate::failpoint::hit("snapshot-before-save");
        }
        if must_sync {
            crate::failpoint::hit("wal-before-sync");
        }
        let written = self.engine.write(&mut batch, must_sync)?;
        if written <= 512 * 1024 {
            self.ready_batch = Some(batch);
        }
        if must_sync {
            crate::failpoint::hit("wal-after-sync");
            self.sync_writes += 1;
        }
        if snapshot.is_some() {
            crate::failpoint::hit("snapshot-after-save");
        }
        if let Some(hs) = hard_state {
            self.hard_state = *hs;
        }
        if let Some(snapshot) = snapshot {
            checkpoint::retire(&self.dir, self.snapshot_index())?;
            self.snapshot = snapshot.clone();
        }
        Ok(())
    }

    pub fn persist_commit(&mut self, commit: u64) -> Result<()> {
        ensure!(commit >= self.hard_state.commit, "commit regressed");
        ensure!(commit <= self.last_index()?, "commit exceeds durable log");
        if commit != self.hard_state.commit {
            let mut hs = self.hard_state;
            hs.commit = commit;
            self.persist(&[], Some(&hs), None)?;
            self.engine.sync()?;
            self.sync_writes += 1;
        }
        Ok(())
    }

    /// Publish the replacement snapshot and truncate its covered log in one synced batch.
    pub fn checkpoint(&mut self, index: u64, data: Vec<u8>) -> Result<()> {
        self.checkpoint_with_conf(
            index,
            data,
            self.snapshot.get_metadata().get_conf_state().clone(),
        )
    }

    pub fn checkpoint_with_conf(
        &mut self,
        index: u64,
        data: Vec<u8>,
        conf: ConfState,
    ) -> Result<()> {
        let prepared = PreparedCheckpoint::write(&self.dir, data.into())?;
        self.publish_checkpoint(index, prepared, conf)
    }

    pub(crate) fn publish_checkpoint(
        &mut self,
        index: u64,
        prepared: PreparedCheckpoint,
        conf: ConfState,
    ) -> Result<()> {
        ensure!(
            index <= self.hard_state.commit,
            "cannot snapshot uncommitted data"
        );
        ensure!(index > self.snapshot_index(), "snapshot did not advance");
        let mut snapshot = Snapshot::default();
        snapshot.mut_metadata().index = index;
        snapshot.mut_metadata().term = self.term(index)?;
        snapshot.mut_metadata().set_conf_state(conf);
        snapshot.data = prepared.data.clone();
        let mut batch = LogBatch::default();
        prepared.publish(&self.dir, &snapshot, &mut batch)?;
        batch.put(GROUP, IDENTITY.to_vec(), self.identity.clone())?;
        batch.put_message(GROUP, HARD_STATE.to_vec(), &self.hard_state)?;
        batch.add_command(GROUP, EngineCommand::Compact { index: index + 1 });
        crate::failpoint::hit("checkpoint-before-save");
        self.engine.write(&mut batch, true)?;
        crate::failpoint::hit("checkpoint-after-save");
        self.sync_writes += 1;
        checkpoint::retire(&self.dir, self.snapshot_index())?;
        self.snapshot = snapshot;
        self.purge()?;
        crate::failpoint::hit("checkpoint-after-gc");
        Ok(())
    }

    pub fn purge(&self) -> Result<()> {
        self.engine.purge_expired_files()?;
        Ok(())
    }

    pub fn recovery_snapshot(&self) -> &Snapshot {
        &self.snapshot
    }

    pub fn snapshot_index(&self) -> u64 {
        self.snapshot.get_metadata().index
    }

    pub fn commit_index(&self) -> u64 {
        self.hard_state.commit
    }

    pub fn log_stats(&self) -> Result<LogStats> {
        let mut stats = LogStats {
            files: vec![],
            bytes: 0,
        };
        let data_file = checkpoint::path(&self.dir, self.snapshot_index());
        for entry in std::fs::read_dir(&self.dir)? {
            let entry = entry?;
            if entry.file_type()?.is_file() {
                let name = entry.file_name().to_string_lossy().into_owned();
                if name.ends_with(".raftlog") || entry.path() == data_file {
                    stats.bytes += entry.metadata()?.len();
                    stats.files.push(name);
                }
            }
        }
        stats.files.sort();
        Ok(stats)
    }
}

fn storage_error(error: raft_engine::Error) -> raft::Error {
    raft::Error::Store(StorageError::Other(Box::new(error)))
}

impl Storage for RaftStorage {
    fn initial_state(&self) -> raft::Result<RaftState> {
        Ok(RaftState {
            hard_state: self.hard_state,
            conf_state: self.snapshot.get_metadata().get_conf_state().clone(),
        })
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
        _context: GetEntriesContext,
    ) -> raft::Result<Vec<Entry>> {
        if low < self.first_index()? {
            return Err(StorageError::Compacted.into());
        }
        if high > self.last_index()? + 1 || high < low {
            return Err(StorageError::Unavailable.into());
        }
        let mut entries = vec![];
        if low == high {
            return Ok(entries);
        }
        self.engine
            .fetch_entries_to::<EntryCodec>(
                GROUP,
                low,
                high,
                max_size
                    .into()
                    .map(|size| usize::try_from(size).unwrap_or(usize::MAX)),
                &mut entries,
            )
            .map_err(storage_error)?;
        if entries.is_empty() {
            return Err(StorageError::Unavailable.into());
        }
        Ok(entries)
    }

    fn term(&self, index: u64) -> raft::Result<u64> {
        if index == self.snapshot_index() {
            return Ok(self.snapshot.get_metadata().term);
        }
        if index < self.snapshot_index() {
            return Err(StorageError::Compacted.into());
        }
        self.engine
            .get_entry::<EntryCodec>(GROUP, index)
            .map_err(storage_error)?
            .map(|entry| entry.term)
            .ok_or_else(|| StorageError::Unavailable.into())
    }

    fn first_index(&self) -> raft::Result<u64> {
        Ok(self.snapshot_index() + 1)
    }

    fn last_index(&self) -> raft::Result<u64> {
        Ok(self
            .engine
            .last_index(GROUP)
            .unwrap_or(0)
            .max(self.snapshot_index()))
    }

    fn snapshot(&self, request_index: u64, _to: u64) -> raft::Result<Snapshot> {
        if self.snapshot_index() == 0 || self.snapshot_index() < request_index {
            return Err(StorageError::SnapshotTemporarilyUnavailable.into());
        }
        Ok(self.snapshot.clone())
    }
}

#[cfg(test)]
mod checkpoint_tests {
    use super::*;

    #[test]
    fn an_inline_checkpoint_upgrades_with_the_first_file_reference() {
        let root = tempfile::tempdir().unwrap();
        let open = || RaftStorage::open(root.path(), 1, "test", vec![1]).unwrap();
        let store = open();
        let old_id = serde_json::to_vec(&(crate::STORAGE_FORMAT, 1, "test", vec![1])).unwrap();
        let mut old = store.recovery_snapshot().clone();
        old.mut_metadata().index = 1;
        old.mut_metadata().term = 1;
        old.data = bytes::Bytes::from_static(b"inline");
        let hs = HardState {
            commit: 1,
            term: 1,
            ..Default::default()
        };
        let mut batch = LogBatch::default();
        batch.put(GROUP, IDENTITY.to_vec(), old_id.clone()).unwrap();
        batch.put_message(GROUP, SNAPSHOT.to_vec(), &old).unwrap();
        batch.put_message(GROUP, HARD_STATE.to_vec(), &hs).unwrap();
        store.engine.write(&mut batch, true).unwrap();
        drop(store);
        let mut store = open();
        assert_eq!(store.recovery_snapshot(), &old);
        let entry = Entry {
            index: 2,
            term: 1,
            ..Default::default()
        };
        store
            .persist(&[entry], Some(&HardState { commit: 2, ..hs }), None)
            .unwrap();
        store.checkpoint(2, b"separate".to_vec()).unwrap();
        assert_ne!(store.engine.get(GROUP, IDENTITY).unwrap(), old_id);
        drop(store);
        assert_eq!(open().recovery_snapshot().data.as_ref(), b"separate");
    }
}
