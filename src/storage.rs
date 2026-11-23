use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use anyhow::{ensure, Context, Result};
use raft::eraftpb::{ConfState, Entry, HardState, Snapshot};
use raft::{GetEntriesContext, RaftState, Storage, StorageError};
use raft_engine::{
    Command as EngineCommand, Config, Engine, LogBatch, MessageExt, ProstCodec, ReadableSize,
};
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

impl MessageExt<ProstCodec> for EntryCodec {
    type Entry = Entry;

    fn index(entry: &Entry) -> u64 {
        entry.index
    }
}

/// Stores Raft metadata, log entries, and a full recovery snapshot on local disk.
pub struct RaftStorage {
    engine: Arc<Engine<LogFileSystem>>,
    pub(crate) wal_metrics: crate::metrics::Wal,
    hard_state: HardState,
    snapshot: Snapshot,
    rows: Arc<RowMarker>,
    dir: PathBuf,
    ready_batch: Option<LogBatch>,
    pub sync_writes: u64,
}

#[derive(Debug, Serialize)]
pub struct LogStats {
    pub files: Vec<String>,
    pub bytes: u64,
}

/// The identity marker of data directories whose state rows can omit Put values outside the
/// current and history rows. Binaries without it reject such a directory.
const ROW_MARKER: &str = "history-values";

/// The identity record of the data directory and whether it carries [`ROW_MARKER`].
///
/// A directory gets the marker just before it first holds a format 4 row or image, not when
/// this binary opens it. A directory that this binary only opened stays readable by older
/// binaries. The state machine calls [`RowMarker::mark`] before its first SlateDB write of a
/// hash or Watch journal row. Raft storage writes the marked record with every snapshot image.
pub struct RowMarker {
    engine: Arc<Engine<LogFileSystem>>,
    /// The record without the marker. Older binaries since file checkpoints accept it.
    unmarked: Vec<u8>,
    marked: Vec<u8>,
    /// Whether the stored record is `marked`. Writers of the record hold this lock across the
    /// synced write, so a batch never stores the unmarked record after the marked one.
    stored: Mutex<bool>,
    /// A lock-free copy of `stored` for the apply path.
    done: AtomicBool,
}

impl RowMarker {
    /// Return true when the stored identity carries the marker.
    pub(crate) fn is_marked(&self) -> bool {
        self.done.load(Ordering::Acquire)
    }

    /// Store the marked identity in one synced batch, unless it is already stored.
    ///
    /// Call this before writing a format 4 row. The record is durable when this returns, so a
    /// crash leaves no such row in a directory without the marker.
    pub(crate) fn mark(&self) -> Result<()> {
        let mut stored = self.lock();
        if !*stored {
            let mut batch = LogBatch::default();
            batch.put(GROUP, IDENTITY.to_vec(), self.marked.clone())?;
            self.engine.write(&mut batch, true)?;
            self.set(&mut stored);
        }
        Ok(())
    }

    fn lock(&self) -> MutexGuard<'_, bool> {
        self.stored
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// The record that a batch which rewrites the identity stores. `marking` is true when the
    /// batch also stores format 4 data, such as a snapshot image.
    fn record(&self, stored: &MutexGuard<'_, bool>, marking: bool) -> Vec<u8> {
        if **stored || marking {
            self.marked.clone()
        } else {
            self.unmarked.clone()
        }
    }

    fn set(&self, stored: &mut MutexGuard<'_, bool>) {
        **stored = true;
        self.done.store(true, Ordering::Release);
    }
}

/// Encode the identity record. Each marker names a storage change that older binaries cannot read.
fn identity_record(
    node_id: u64,
    cluster_id: &str,
    voters: &[u64],
    markers: &[&str],
) -> Result<Vec<u8>> {
    let mut fields = vec![
        serde_json::json!(crate::STORAGE_FORMAT),
        serde_json::json!(node_id),
        serde_json::json!(cluster_id),
        serde_json::json!(voters),
    ];
    fields.extend(markers.iter().map(|marker| serde_json::json!(marker)));
    Ok(serde_json::to_vec(&fields)?)
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
        let engine = Arc::new(Engine::open_with_file_system(
            config,
            std::sync::Arc::new(LogFileSystem(wal_metrics.clone())),
        )?);
        let identity = identity_record(
            node_id,
            cluster_id,
            &voters,
            &["checkpoint-files", ROW_MARKER],
        )?;
        let unmarked = identity_record(node_id, cluster_id, &voters, &["checkpoint-files"])?;
        let stored = engine.get(GROUP, IDENTITY);
        let mut marked = false;
        let mut snapshot = engine
            .get_value::<Snapshot, ProstCodec>(GROUP, SNAPSHOT)?
            .unwrap_or_default();
        if let Some(stored) = stored {
            // Earlier forms: before file checkpoints, and before history-backed rows. Opening
            // does not rewrite them. `RowMarker::mark` adds the marker before the first format 4
            // row, so a directory that this binary only opened stays readable by older binaries.
            let earlier = [
                identity_record(node_id, cluster_id, &voters, &[])?,
                unmarked.clone(),
            ];
            ensure!(
                stored == identity || earlier.contains(&stored),
                "data directory has another format, node, or cluster"
            );
            marked = stored == identity;
        } else {
            ensure!(engine.last_index(GROUP).is_none(), "log has no identity");
            snapshot
                .mut_metadata()
                .set_conf_state(ConfState::from((voters, vec![])));
            let mut batch = LogBatch::default();
            batch.put(GROUP, IDENTITY.to_vec(), unmarked.clone())?;
            batch.put_value::<ProstCodec, _>(GROUP, SNAPSHOT.to_vec(), &snapshot)?;
            engine.write(&mut batch, true)?;
        }
        let referenced = checkpoint::load(dir, &engine, &mut snapshot)?;
        let hard_state = engine
            .get_value::<HardState, ProstCodec>(GROUP, HARD_STATE)?
            .unwrap_or_default();
        let rows = Arc::new(RowMarker {
            engine: Arc::clone(&engine),
            unmarked,
            marked: identity,
            stored: Mutex::new(marked),
            done: AtomicBool::new(marked),
        });
        let this = Self {
            engine,
            wal_metrics,
            hard_state,
            snapshot,
            rows,
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
        // A snapshot image holds format 4 rows. Its batch stores the marked identity.
        let rows = Arc::clone(&self.rows);
        let mut identity = snapshot.map(|_| rows.lock());
        if let Some(snapshot) = snapshot {
            ensure!(
                snapshot.get_metadata().index > self.snapshot_index(),
                "stale snapshot"
            );
            ensure!(!snapshot.data.is_empty(), "received snapshot has no image");
            // Replacing a snapshot discards the old log, including its divergent suffix.
            batch.add_command(GROUP, EngineCommand::Clean);
            let record = rows.record(identity.as_ref().unwrap(), true);
            batch.put(GROUP, IDENTITY.to_vec(), record)?;
            let hs = hard_state.context("snapshot Ready has no HardState")?;
            ensure!(
                hs.commit >= snapshot.get_metadata().index,
                "snapshot exceeds commit"
            );
            PreparedCheckpoint::write(&self.dir, snapshot.data.clone().into())?
                .publish(&self.dir, snapshot, &mut batch)?;
        }
        batch.add_entries_with::<EntryCodec, ProstCodec>(GROUP, entries)?;
        if let Some(hs) = hard_state {
            batch.put_value::<ProstCodec, _>(GROUP, HARD_STATE.to_vec(), hs)?;
        }
        if snapshot.is_some() {
            crate::failpoint::hit("snapshot-before-save");
        }
        if must_sync {
            crate::failpoint::hit("wal-before-sync");
        }
        let written = self.engine.write(&mut batch, must_sync)?;
        if let Some(identity) = &mut identity {
            rows.set(identity);
        }
        drop(identity);
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
        self.publish_checkpoint(index, Some(prepared), conf)
    }

    /// Return true when the current snapshot record has no transferable image.
    pub fn metadata_only(&self) -> bool {
        self.snapshot_index() > 0 && self.snapshot.data.is_empty()
    }

    /// Publish a checkpoint and truncate its covered log in one synced batch.
    ///
    /// Without an image, the caller must first flush SlateDB through `index`. The flushed state
    /// and its apply marker then cover the removed entries. An image at the current index can
    /// replace a record without an image.
    pub(crate) fn publish_checkpoint(
        &mut self,
        index: u64,
        prepared: Option<PreparedCheckpoint>,
        conf: ConfState,
    ) -> Result<()> {
        ensure!(
            index <= self.hard_state.commit,
            "cannot snapshot uncommitted data"
        );
        let attach = prepared.is_some() && index == self.snapshot_index() && self.metadata_only();
        ensure!(
            index > self.snapshot_index() || attach,
            "snapshot did not advance"
        );
        let mut snapshot = Snapshot::default();
        snapshot.mut_metadata().index = index;
        snapshot.mut_metadata().term = self.term(index)?;
        snapshot.mut_metadata().set_conf_state(conf);
        let mut batch = LogBatch::default();
        let rows = Arc::clone(&self.rows);
        let mut identity = rows.lock();
        let image = prepared.is_some();
        match prepared {
            Some(prepared) => {
                snapshot.data = prepared.data.to_vec();
                prepared.publish(&self.dir, &snapshot, &mut batch)?;
            }
            None => checkpoint::publish_metadata(&snapshot, &mut batch)?,
        }
        // An image holds format 4 rows, so its batch stores the marked identity.
        batch.put(GROUP, IDENTITY.to_vec(), rows.record(&identity, image))?;
        batch.put_value::<ProstCodec, _>(GROUP, HARD_STATE.to_vec(), &self.hard_state)?;
        batch.add_command(GROUP, EngineCommand::Compact { index: index + 1 });
        crate::failpoint::hit("checkpoint-before-save");
        self.engine.write(&mut batch, true)?;
        if image {
            rows.set(&mut identity);
        }
        drop(identity);
        crate::failpoint::hit("checkpoint-after-save");
        self.sync_writes += 1;
        if !attach {
            checkpoint::retire(&self.dir, self.snapshot_index())?;
        }
        self.snapshot = snapshot;
        self.purge()?;
        crate::failpoint::hit("checkpoint-after-gc");
        Ok(())
    }

    /// The identity marker that the state machine sets before it writes format 4 rows.
    pub fn row_marker(&self) -> Arc<RowMarker> {
        Arc::clone(&self.rows)
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
            .fetch_entries_to_with::<EntryCodec, ProstCodec>(
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
            .get_entry_with::<EntryCodec, ProstCodec>(GROUP, index)
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
        if self.snapshot_index() == 0
            || self.snapshot_index() < request_index
            || self.snapshot.data.is_empty()
        {
            return Err(StorageError::SnapshotTemporarilyUnavailable.into());
        }
        Ok(self.snapshot.clone())
    }
}

#[cfg(test)]
mod checkpoint_tests {
    use super::*;

    /// The identity check of binaries before the row marker. They accept only these forms.
    fn older_binary_opens(stored: &[u8]) -> bool {
        let tuple4 = serde_json::to_vec(&(crate::STORAGE_FORMAT, 1, "test", vec![1])).unwrap();
        let tuple5 = serde_json::to_vec(&(
            crate::STORAGE_FORMAT,
            1,
            "test",
            vec![1],
            "checkpoint-files",
        ))
        .unwrap();
        stored == tuple4 || stored == tuple5
    }

    fn marked_record() -> serde_json::Value {
        serde_json::json!([
            crate::STORAGE_FORMAT,
            1,
            "test",
            [1],
            "checkpoint-files",
            ROW_MARKER
        ])
    }

    #[test]
    fn opening_keeps_an_older_directory_readable_until_the_first_row() {
        let tuple4 = serde_json::to_vec(&(crate::STORAGE_FORMAT, 1, "test", vec![1])).unwrap();
        // `None` is a new directory. Some(record) is a directory of an older binary.
        for old_id in [None, Some(tuple4)] {
            let root = tempfile::tempdir().unwrap();
            let store = RaftStorage::open(root.path(), 1, "test", vec![1]).unwrap();
            if let Some(old_id) = &old_id {
                let mut batch = LogBatch::default();
                batch.put(GROUP, IDENTITY.to_vec(), old_id.clone()).unwrap();
                store.engine.write(&mut batch, true).unwrap();
            }
            drop(store);
            let created = RaftStorage::open(root.path(), 1, "test", vec![1])
                .unwrap()
                .engine
                .get(GROUP, IDENTITY)
                .unwrap();
            // This binary opens and closes the directory twice without writing a row.
            for _ in 0..2 {
                let store = RaftStorage::open(root.path(), 1, "test", vec![1]).unwrap();
                assert!(!store.row_marker().is_marked());
                let stored = store.engine.get(GROUP, IDENTITY).unwrap();
                assert_eq!(stored, created);
                assert!(older_binary_opens(&stored));
                if let Some(old_id) = &old_id {
                    assert_eq!(&stored, old_id);
                }
            }
            // The first format 4 row marks the directory in one synced batch.
            let store = RaftStorage::open(root.path(), 1, "test", vec![1]).unwrap();
            let marker = store.row_marker();
            marker.mark().unwrap();
            assert!(marker.is_marked());
            marker.mark().unwrap();
            drop((store, marker));
            let store = RaftStorage::open(root.path(), 1, "test", vec![1]).unwrap();
            assert!(store.row_marker().is_marked());
            let stored = store.engine.get(GROUP, IDENTITY).unwrap();
            assert!(!older_binary_opens(&stored));
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&stored).unwrap(),
                marked_record()
            );
            drop(store);
            assert!(RaftStorage::open(root.path(), 2, "test", vec![1]).is_err());
        }
    }

    #[test]
    fn identity_rewrites_never_drop_the_row_marker() {
        let root = tempfile::tempdir().unwrap();
        let open = || RaftStorage::open(root.path(), 1, "test", vec![1]).unwrap();
        let entries = |range: std::ops::RangeInclusive<u64>| -> Vec<Entry> {
            range
                .map(|index| Entry {
                    index,
                    term: 1,
                    ..Default::default()
                })
                .collect()
        };
        let hs = |commit| HardState {
            commit,
            term: 1,
            ..Default::default()
        };
        let mut store = open();
        store.persist(&entries(1..=3), Some(&hs(3)), None).unwrap();
        // A checkpoint without an image keeps the directory readable by older binaries.
        store
            .publish_checkpoint(
                1,
                None,
                store.snapshot.get_metadata().get_conf_state().clone(),
            )
            .unwrap();
        assert!(older_binary_opens(
            &store.engine.get(GROUP, IDENTITY).unwrap()
        ));
        assert!(!store.row_marker().is_marked());
        // An image holds format 4 rows, so its checkpoint stores the marker.
        store.checkpoint(2, b"image".to_vec()).unwrap();
        assert!(store.row_marker().is_marked());
        drop(store);
        let mut store = open();
        assert!(store.row_marker().is_marked());
        // Later checkpoints without an image keep the marker.
        store
            .publish_checkpoint(
                3,
                None,
                store.snapshot.get_metadata().get_conf_state().clone(),
            )
            .unwrap();
        drop(store);
        let stored = open().engine.get(GROUP, IDENTITY).unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&stored).unwrap(),
            marked_record()
        );

        // A received snapshot replaces the log and stores the marker with its image.
        let root = tempfile::tempdir().unwrap();
        let mut store = RaftStorage::open(root.path(), 1, "test", vec![1]).unwrap();
        let mut snapshot = Snapshot::default();
        snapshot.mut_metadata().index = 5;
        snapshot.mut_metadata().term = 1;
        snapshot.data = b"image".to_vec();
        store.persist(&[], Some(&hs(5)), Some(&snapshot)).unwrap();
        assert!(store.row_marker().is_marked());
        drop(store);
        let store = RaftStorage::open(root.path(), 1, "test", vec![1]).unwrap();
        assert!(!older_binary_opens(
            &store.engine.get(GROUP, IDENTITY).unwrap()
        ));
    }

    #[test]
    fn an_inline_checkpoint_upgrades_with_the_first_file_reference() {
        let root = tempfile::tempdir().unwrap();
        let open = || RaftStorage::open(root.path(), 1, "test", vec![1]).unwrap();
        let store = open();
        let old_id = serde_json::to_vec(&(crate::STORAGE_FORMAT, 1, "test", vec![1])).unwrap();
        let mut old = store.recovery_snapshot().clone();
        old.mut_metadata().index = 1;
        old.mut_metadata().term = 1;
        old.data = b"inline".to_vec();
        let hs = HardState {
            commit: 1,
            term: 1,
            ..Default::default()
        };
        let mut batch = LogBatch::default();
        batch.put(GROUP, IDENTITY.to_vec(), old_id.clone()).unwrap();
        batch
            .put_value::<ProstCodec, _>(GROUP, SNAPSHOT.to_vec(), &old)
            .unwrap();
        batch
            .put_value::<ProstCodec, _>(GROUP, HARD_STATE.to_vec(), &hs)
            .unwrap();
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
        assert_eq!(open().recovery_snapshot().data.as_slice(), b"separate");
    }
}
