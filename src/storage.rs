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
    member: u64,
    /// The image file that the published reference names.
    checkpoint_file: Option<checkpoint::FileId>,
}

#[derive(Debug, Serialize)]
pub struct LogStats {
    pub files: Vec<String>,
    pub bytes: u64,
}

/// The identity marker of data directories whose state rows can omit Put values outside the
/// current and history rows. Binaries without it reject such a directory.
const ROW_MARKER: &str = "history-values";
/// Default size of one Raft log file, as etcd's 64 MiB WAL segment.
///
/// Each file rotation syncs the old file, the new file header, and the directory. With 256 KiB
/// files, 4 KiB-value puts rotated about every 130 writes, and rotation caused half of the
/// device flushes. Truncation deletes only whole files, so up to one file of old entries stays.
pub const DEFAULT_LOG_FILE_BYTES: u64 = 64 * 1024 * 1024;
/// Smallest Raft log file size. A file must hold its header and at least one write batch.
pub const MIN_LOG_FILE_BYTES: u64 = 64 * 1024;

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

/// Reads durable log entries outside the log worker. Shared-mode rebase replays them.
#[derive(Clone)]
pub struct LogReader {
    engine: Arc<Engine<LogFileSystem>>,
}

impl LogReader {
    /// Return the entries in `[low, high)`. A missing entry is an error.
    pub fn entries(&self, low: u64, high: u64) -> Result<Vec<Entry>> {
        let mut entries = vec![];
        self.engine
            .fetch_entries_to_with::<EntryCodec, ProstCodec>(
                GROUP,
                low,
                high,
                None,
                &mut entries,
            )?;
        ensure!(
            entries.len() as u64 == high - low,
            "the log has no entries in [{low}, {high})"
        );
        Ok(entries)
    }

    /// Return the term at `index`: the term of its entry, or of the snapshot record there.
    pub fn term(&self, index: u64) -> Result<u64> {
        if let Some(entry) = self
            .engine
            .get_entry_with::<EntryCodec, ProstCodec>(GROUP, index)?
        {
            return Ok(entry.term);
        }
        let record = self
            .engine
            .get_value::<Snapshot, ProstCodec>(GROUP, SNAPSHOT)?
            .unwrap_or_default();
        ensure!(
            record.get_metadata().index == index,
            "the log has no term at {index}"
        );
        Ok(record.get_metadata().term)
    }
}

impl RaftStorage {
    pub fn reader(&self) -> LogReader {
        LogReader {
            engine: Arc::clone(&self.engine),
        }
    }

    /// The member ID, the cluster ID, and the initial voters in the identity of an existing
    /// log. Offline tools open a log without the startup flags. The open fails while the member
    /// runs.
    pub fn stored_identity(dir: &Path) -> Result<(u64, String, Vec<u64>)> {
        let config = Config {
            dir: dir.to_string_lossy().into_owned(),
            enable_log_recycle: false,
            ..Config::default()
        };
        let engine = Engine::open(config)?;
        let stored = engine
            .get(GROUP, IDENTITY)
            .context("the log has no identity")?;
        let fields: Vec<serde_json::Value> = serde_json::from_slice(&stored)?;
        let field = |n: usize| fields.get(n).cloned().context("invalid log identity");
        Ok((
            serde_json::from_value(field(1)?)?,
            serde_json::from_value(field(2)?)?,
            serde_json::from_value(field(3)?)?,
        ))
    }

    /// Open an existing log with its stored identity.
    pub fn open_existing(dir: &Path) -> Result<Self> {
        let (member, cluster, voters) = Self::stored_identity(dir)?;
        Self::open(dir, member, &cluster, voters)
    }

    pub fn open(dir: &Path, node_id: u64, cluster_id: &str, voters: Vec<u64>) -> Result<Self> {
        Self::open_with_file_bytes(dir, node_id, cluster_id, voters, DEFAULT_LOG_FILE_BYTES)
    }

    /// Open the log with `file_bytes` per log file. Existing files of any size stay readable.
    pub fn open_with_file_bytes(
        dir: &Path,
        node_id: u64,
        cluster_id: &str,
        voters: Vec<u64>,
        file_bytes: u64,
    ) -> Result<Self> {
        Self::open_inner(dir, node_id, cluster_id, voters, file_bytes, true)
    }

    /// Open the log for a snapshot restore. The engine lock reserves the directory, but a new
    /// directory gets its identity only from `restore` or a row marker. The caller creates its
    /// restore marker first, so a crash never leaves a published identity without the marker.
    pub(crate) fn open_for_restore(
        dir: &Path,
        node_id: u64,
        cluster_id: &str,
        voters: Vec<u64>,
    ) -> Result<Self> {
        Self::open_inner(
            dir,
            node_id,
            cluster_id,
            voters,
            DEFAULT_LOG_FILE_BYTES,
            false,
        )
    }

    fn open_inner(
        dir: &Path,
        node_id: u64,
        cluster_id: &str,
        voters: Vec<u64>,
        file_bytes: u64,
        write_identity: bool,
    ) -> Result<Self> {
        ensure!(
            file_bytes >= MIN_LOG_FILE_BYTES,
            "log file size must be at least {MIN_LOG_FILE_BYTES} bytes"
        );
        let config = Config {
            dir: dir.to_string_lossy().into_owned(),
            target_file_size: ReadableSize(file_bytes),
            // raft-engine requires a purge threshold of at least one file.
            purge_threshold: ReadableSize(file_bytes),
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
            if write_identity {
                let mut batch = LogBatch::default();
                batch.put(GROUP, IDENTITY.to_vec(), unmarked.clone())?;
                batch.put_value::<ProstCodec, _>(GROUP, SNAPSHOT.to_vec(), &snapshot)?;
                engine.write(&mut batch, true)?;
            }
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
            member: node_id,
            checkpoint_file: referenced,
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
        let mut received = None;
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
            received = Some(
                PreparedCheckpoint::write(&self.dir, snapshot.data.clone().into())?
                    .publish(&self.dir, snapshot, 0, &mut batch)?,
            );
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
            checkpoint::retire(&self.dir, self.checkpoint_file)?;
            self.checkpoint_file = received;
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

    /// Adopt a shared-state checkpoint as the snapshot record, as a Raft snapshot restore does.
    ///
    /// - If the log holds the snapshot index with the snapshot term, keep the later entries and
    ///   the commit index when it is higher.
    /// - If the term differs and the index is above the commit index, discard the whole log. An
    ///   entry above a mismatched index can come from a deposed term.
    /// - If the term differs at or below the commit index, two committed prefixes disagree.
    ///   Return an error and change nothing.
    ///
    /// One synced batch stores the record, its ConfState, the commit `max(commit, index)`, and
    /// the log change. A crash leaves the old log or the new boundary.
    pub fn adopt(&mut self, snapshot: &Snapshot) -> Result<()> {
        self.adopt_at(snapshot, false)
    }

    /// Adopt `snapshot`, or replace the record at its own index when the terms match. The offline
    /// converter uses this, because a member's record can already be at the checkpoint index.
    pub fn replace_record(&mut self, snapshot: &Snapshot) -> Result<()> {
        self.adopt_at(snapshot, true)
    }

    fn adopt_at(&mut self, snapshot: &Snapshot, replace: bool) -> Result<()> {
        let index = snapshot.get_metadata().index;
        let term = snapshot.get_metadata().term;
        let same =
            replace && index == self.snapshot_index() && term == self.snapshot.get_metadata().term;
        ensure!(
            same || index > self.snapshot_index(),
            "stale adopted snapshot"
        );
        ensure!(
            !snapshot.data.is_empty(),
            "adopted snapshot has no descriptor"
        );
        let keep = same || (index <= self.last_index()? && self.term(index).ok() == Some(term));
        ensure!(
            keep || index > self.hard_state.commit,
            "committed log term at {index} differs from checkpoint term {term}"
        );
        let mut hard_state = self.hard_state;
        hard_state.commit = hard_state.commit.max(index);
        let mut batch = LogBatch::default();
        let rows = Arc::clone(&self.rows);
        let identity = rows.lock();
        if keep {
            batch.add_command(GROUP, EngineCommand::Compact { index: index + 1 });
        } else {
            batch.add_command(GROUP, EngineCommand::Clean);
        }
        // A clean removes the group's keys too, so the batch stores them again.
        batch.put(GROUP, IDENTITY.to_vec(), rows.record(&identity, false))?;
        batch.put_value::<ProstCodec, _>(GROUP, HARD_STATE.to_vec(), &hard_state)?;
        let file = PreparedCheckpoint::write(&self.dir, snapshot.data.clone().into())?
            .publish(&self.dir, snapshot, 0, &mut batch)?;
        crate::failpoint::hit("adopt-before-save");
        self.engine.write(&mut batch, true)?;
        drop(identity);
        self.sync_writes += 1;
        crate::failpoint::hit("adopt-after-save");
        if self.checkpoint_file != Some(file) {
            checkpoint::retire(&self.dir, self.checkpoint_file)?;
        }
        self.checkpoint_file = Some(file);
        self.snapshot = snapshot.clone();
        self.hard_state = hard_state;
        self.purge()?;
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

    /// Return true when an image at `index` replaces the record at the same index: the record
    /// has no transferable image, or it names another view checkpoint. A restart or a rebase can
    /// move the base to another checkpoint at the record index when retention deleted the one
    /// that the record names.
    pub(crate) fn replaces_at(&self, index: u64, prepared: Option<&PreparedCheckpoint>) -> bool {
        let Some(prepared) = prepared else {
            return false;
        };
        if index != self.snapshot_index() {
            return false;
        }
        let view = |data: &[u8]| {
            crate::state::transfer::ViewDescriptor::decode(data)
                .ok()
                .flatten()
                .map(|view| view.checkpoint)
        };
        self.metadata_only()
            || view(&self.snapshot.data)
                .zip(view(&prepared.data))
                .is_some_and(|(old, new)| old != new)
    }

    /// Return true when the current snapshot record has no transferable image.
    ///
    /// A received or aged checkpoint descriptor is not transferable. See `transfer::sendable`.
    pub fn metadata_only(&self) -> bool {
        self.snapshot_index() > 0
            && !crate::state::transfer::sendable(&self.snapshot.data, self.member)
    }

    pub fn member(&self) -> u64 {
        self.member
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
        let attach = self.replaces_at(index, prepared.as_ref());
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
        let file = match prepared {
            Some(prepared) => {
                snapshot.data = prepared.data.to_vec();
                // A replacement at the same index must not overwrite the referenced file.
                let generation = match self.checkpoint_file {
                    Some(old) if attach && old.index == index => old.generation + 1,
                    _ => 0,
                };
                Some(prepared.publish(&self.dir, &snapshot, generation, &mut batch)?)
            }
            None => {
                checkpoint::publish_metadata(&snapshot, &mut batch)?;
                None
            }
        };
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
        // The new reference is durable. Only now remove the file that the old one named.
        if self.checkpoint_file != file {
            checkpoint::retire(&self.dir, self.checkpoint_file)?;
        }
        self.checkpoint_file = file;
        self.snapshot = snapshot;
        self.purge()?;
        crate::failpoint::hit("checkpoint-after-gc");
        Ok(())
    }

    /// Return true for a directory without a snapshot, a vote, or log entries.
    pub(crate) fn is_new(&self) -> bool {
        self.snapshot_index() == 0
            && self.hard_state == HardState::default()
            && self.engine.last_index(GROUP).is_none()
    }

    /// Record a restored state at `index` in a new directory, with a snapshot without an image.
    ///
    /// The caller must first flush the restored SlateDB state with its apply marker at `index`.
    /// That state covers the log before `index`, as after `publish_checkpoint` without an image.
    pub(crate) fn restore(&mut self, index: u64, conf: ConfState) -> Result<()> {
        ensure!(self.is_new(), "restore requires a new Raft directory");
        let mut snapshot = Snapshot::default();
        snapshot.mut_metadata().index = index;
        snapshot.mut_metadata().term = 1;
        snapshot.mut_metadata().set_conf_state(conf);
        let hard_state = HardState {
            term: 1,
            commit: index,
            ..Default::default()
        };
        // The restored rows are format 4 rows. `open_for_restore` can leave the identity unset.
        let rows = Arc::clone(&self.rows);
        let mut stored = rows.lock();
        let mut batch = LogBatch::default();
        batch.put(GROUP, IDENTITY.to_vec(), rows.marked.clone())?;
        checkpoint::publish_metadata(&snapshot, &mut batch)?;
        batch.put_value::<ProstCodec, _>(GROUP, HARD_STATE.to_vec(), &hard_state)?;
        self.engine.write(&mut batch, true)?;
        rows.set(&mut stored);
        self.sync_writes += 1;
        self.hard_state = hard_state;
        self.snapshot = snapshot;
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
        let data_file = self
            .checkpoint_file
            .map(|id| checkpoint::path(&self.dir, id));
        for entry in std::fs::read_dir(&self.dir)? {
            let entry = entry?;
            if entry.file_type()?.is_file() {
                let name = entry.file_name().to_string_lossy().into_owned();
                if name.ends_with(".raftlog") || Some(entry.path()) == data_file {
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

    /// A descriptor that this member did not build. It does not count as a transferable image.
    fn foreign_image(index: u64, tag: &str) -> Vec<u8> {
        crate::state::transfer::Descriptor {
            version: crate::state::transfer::OBJECT_SNAPSHOT_FORMAT,
            applied: index,
            member: 9,
            namespace: tag.into(),
            backend: crate::state::transfer::Backend::Local,
            peer_url: None,
            root: "state".into(),
            checkpoint: uuid::Uuid::new_v4().to_string(),
            manifest: 1,
            ssts: vec![],
            members: vec![],
            created_ms: 0,
        }
        .encode()
        .unwrap()
    }

    /// A log of `last` entries in term 1 with commit `commit`.
    fn term_one_log(dir: &Path, last: u64, commit: u64) -> RaftStorage {
        let mut store = RaftStorage::open(dir, 1, "test", vec![1]).unwrap();
        let entries: Vec<Entry> = (1..=last)
            .map(|index| Entry {
                index,
                term: 1,
                ..Default::default()
            })
            .collect();
        let hs = HardState {
            term: 1,
            vote: 1,
            commit,
        };
        store.persist(&entries, Some(&hs), None).unwrap();
        store
    }

    fn view_snapshot(index: u64, term: u64) -> Snapshot {
        let mut snapshot = Snapshot::default();
        snapshot.mut_metadata().index = index;
        snapshot.mut_metadata().term = term;
        snapshot
            .mut_metadata()
            .set_conf_state(ConfState::from((vec![1], vec![])));
        snapshot.data = crate::state::transfer::ViewDescriptor::new(uuid::Uuid::new_v4())
            .encode()
            .unwrap();
        snapshot
    }

    fn bounds(store: &RaftStorage) -> (u64, u64, u64) {
        (
            store.first_index().unwrap(),
            store.last_index().unwrap(),
            store.commit_index(),
        )
    }

    /// Adopt checkpoint 100 with applied 90 and commit 110. The matching log keeps 101..=120 and
    /// the higher commit, also after a crash right after the batch.
    #[test]
    fn adopt_keeps_a_matching_suffix_and_a_higher_commit() {
        let root = tempfile::tempdir().unwrap();
        let mut store = term_one_log(root.path(), 120, 110);
        let snapshot = view_snapshot(100, 1);
        store.adopt(&snapshot).unwrap();
        assert_eq!(bounds(&store), (101, 120, 110));
        drop(store);
        let store = RaftStorage::open(root.path(), 1, "test", vec![1]).unwrap();
        assert_eq!(bounds(&store), (101, 120, 110));
        assert_eq!(store.recovery_snapshot(), &snapshot);
        assert_eq!(store.term(100).unwrap(), 1);
        assert_eq!(
            store
                .entries(101, 111, None, GetEntriesContext::empty(false))
                .unwrap()
                .len(),
            10
        );
    }

    /// A mismatched term above the commit discards the whole log. A crash after the batch keeps
    /// no discarded entry, so the member cannot vote with one or send one.
    #[test]
    fn adopt_discards_a_mismatched_log_above_the_commit() {
        let root = tempfile::tempdir().unwrap();
        let mut store = term_one_log(root.path(), 120, 90);
        store.adopt(&view_snapshot(100, 2)).unwrap();
        assert_eq!(bounds(&store), (101, 100, 100));
        drop(store);
        let store = RaftStorage::open(root.path(), 1, "test", vec![1]).unwrap();
        assert_eq!(bounds(&store), (101, 100, 100));
        assert_eq!(store.term(100).unwrap(), 2);
        assert!(store.term(101).is_err());
        // A checkpoint ahead of an empty log is an adopt with nothing to keep.
        let empty = tempfile::tempdir().unwrap();
        let mut store = RaftStorage::open(empty.path(), 1, "test", vec![1]).unwrap();
        store.adopt(&view_snapshot(7, 3)).unwrap();
        assert_eq!(bounds(&store), (8, 7, 7));
    }

    /// Two committed prefixes that disagree stop recovery and change nothing.
    #[test]
    fn adopt_refuses_a_mismatched_committed_index() {
        let root = tempfile::tempdir().unwrap();
        let mut store = term_one_log(root.path(), 120, 110);
        let error = store.adopt(&view_snapshot(100, 2)).unwrap_err();
        assert!(error.to_string().contains("differs"), "{error}");
        assert_eq!(bounds(&store), (1, 120, 110));
        drop(store);
        let store = RaftStorage::open(root.path(), 1, "test", vec![1]).unwrap();
        assert_eq!(bounds(&store), (1, 120, 110));
    }

    #[test]
    fn a_replacement_image_keeps_the_referenced_file_until_its_reference_is_durable() {
        let root = tempfile::tempdir().unwrap();
        let mut store = RaftStorage::open(root.path(), 1, "test", vec![1]).unwrap();
        let entries: Vec<Entry> = (1..=3)
            .map(|index| Entry {
                index,
                term: 1,
                ..Default::default()
            })
            .collect();
        let hs = HardState {
            term: 1,
            vote: 1,
            commit: 3,
        };
        store.persist(&entries, Some(&hs), None).unwrap();
        let first = foreign_image(3, "first");
        store.checkpoint(3, first.clone()).unwrap();
        assert!(store.metadata_only());
        // Model a stop after the replacement file exists but before its reference is saved.
        let snapshot = store.snapshot.clone();
        let staged = PreparedCheckpoint::write(root.path(), foreign_image(3, "lost").into())
            .unwrap()
            .publish(root.path(), &snapshot, 1, &mut LogBatch::default())
            .unwrap();
        drop(store);
        let mut store = RaftStorage::open(root.path(), 1, "test", vec![1]).unwrap();
        assert_eq!(store.recovery_snapshot().data, first);
        assert!(!checkpoint::path(root.path(), staged).exists());
        // A completed replacement uses a new file and then removes the old one.
        let second = foreign_image(3, "second");
        store.checkpoint(3, second.clone()).unwrap();
        assert!(!checkpoint::path(root.path(), checkpoint::FileId::first(3)).exists());
        drop(store);
        let store = RaftStorage::open(root.path(), 1, "test", vec![1]).unwrap();
        assert_eq!(store.recovery_snapshot().data, second);
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
