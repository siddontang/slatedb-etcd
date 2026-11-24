//! Native backups: stream a pinned checkpoint as a native image, and restore an image with
//! bounded memory. See `docs/administration.md#maintenance-and-storage`.
//!
//! The image is the legacy Raft snapshot format, `SnapshotData` in `codec` encoding, followed by
//! its SHA-256 digest. Its rows are in key order, and the membership row comes last.

use super::*;
use bytes::Bytes;
use sha2::{Digest, Sha256};
use slatedb::config::{CheckpointOptions, CheckpointScope, DbReaderOptions, ScanOptions};
use slatedb::{DbReader, DbReaderMode};
use std::io::{BufReader, Read};
use tokio::sync::mpsc;

/// The bytes of one Snapshot response message.
const CHUNK_BYTES: usize = 64 * 1024;
/// The messages buffered ahead of a slow client.
const CHUNKS_AHEAD: usize = 16;
/// A checkpoint of a failed process expires after this time. A running backup refreshes it.
const CHECKPOINT_LIFETIME: Duration = Duration::from_secs(10 * 60);
const REFRESH_EVERY: Duration = Duration::from_secs(60);
const CHECKPOINT_NAME: &str = "native-backup";
/// `etcdctl snapshot save` accepts a file of a multiple of this size plus the digest.
const ALIGN: u64 = 512;
const DIGEST: u64 = 32;
/// The largest image that a shared-mode backup builds in memory.
const MAX_VIEW_BACKUP_BYTES: usize = 64 * 1024 * 1024;
/// The largest row that a restore decodes. A Raft entry holds at most 8 MiB.
const MAX_ROW_BYTES: usize = 64 * 1024 * 1024;
/// The row bytes of one restore write batch.
const RESTORE_BATCH_BYTES: usize = 4 * 1024 * 1024;
/// The row bytes a restore writes between flushes. Without a flush, SlateDB keeps up to
/// `max_unflushed_bytes` in memtables. This matches the L0 SST size.
const RESTORE_FLUSH_BYTES: usize = 64 * 1024 * 1024;
/// A data directory with this file holds an interrupted restore. Startup rejects it.
pub(crate) const RESTORE_MARKER: &str = "restore-incomplete";

/// One part of a native image, and the image bytes after it.
pub struct Chunk {
    pub blob: Vec<u8>,
    pub remaining: u64,
}

/// A started backup: the response header of its applied boundary and the image chunks.
pub struct Stream {
    pub header: Option<crate::proto::etcdserverpb::ResponseHeader>,
    pub chunks: mpsc::Receiver<Result<Chunk>>,
}

impl StateMachine {
    /// Pin the applied prefix in a SlateDB checkpoint, then stream its image from a task.
    ///
    /// Call it between apply jobs. Only the checkpoint start runs here. Writes that apply later
    /// do not reach the checkpoint, so the image holds one applied state boundary.
    pub(crate) async fn backup(&self) -> Result<Stream> {
        if self.is_shared() {
            return self.view_backup().await;
        }
        ensure!(
            self.db().snapshot()?.seq() == self.applied,
            "backup does not match the apply marker"
        );
        // The membership row can exist only in memory. See `members_unsaved`.
        let members = serde_json::to_vec(self.members.as_ref())?;
        let handle = self
            .db()
            .begin_checkpoint(
                CheckpointScope::All,
                &CheckpointOptions {
                    lifetime: Some(CHECKPOINT_LIFETIME),
                    name: Some(CHECKPOINT_NAME.to_owned()),
                    ..Default::default()
                },
            )
            .await?;
        let timer = self.metrics.snapshot.start_timer();
        let source = Source {
            _db: self.db().clone(),
            admin: self.admin.clone(),
            store: self.location.store(),
            root: self.location.root(),
            applied: self.applied,
            members,
        };
        let (tx, rx) = mpsc::channel(CHUNKS_AHEAD);
        tokio::spawn(async move {
            let result = async {
                let checkpoint = handle.wait().await?.id;
                let result = source.stream(checkpoint, &tx).await;
                // Like etcd, the backend snapshot duration counts completed streams only.
                if result.is_ok() {
                    timer.observe_duration();
                } else {
                    timer.stop_and_discard();
                }
                if let Err(error) = source.admin.delete_checkpoint(checkpoint).await {
                    tracing::warn!(%error, "cannot delete a backup checkpoint");
                }
                result
            }
            .await;
            if let Err(error) = result {
                let _ = tx.send(Err(error)).await;
            }
        });
        Ok(Stream {
            header: mvcc::header(self.revision),
            chunks: rx,
        })
    }
}

impl StateMachine {
    /// Shared mode keeps applied rows in view checkpoints and in memory, not in `db`. Build the
    /// image from the pinned view in memory, as before streaming backups, and send it in chunks.
    async fn view_backup(&self) -> Result<Stream> {
        let timer = self.metrics.snapshot.start_timer();
        let mut bytes = self.snapshot_job()?.await?;
        ensure!(
            bytes.len() + DIGEST as usize <= MAX_VIEW_BACKUP_BYTES,
            "native snapshot exceeds 64 MiB"
        );
        timer.observe_duration();
        let digest = Sha256::digest(&bytes);
        bytes.extend_from_slice(&digest);
        let (tx, rx) = mpsc::channel(CHUNKS_AHEAD);
        tokio::spawn(async move {
            let mut remaining = bytes.len() as u64;
            for part in bytes.chunks(CHUNK_BYTES) {
                remaining -= part.len() as u64;
                let chunk = Chunk {
                    blob: part.to_vec(),
                    remaining,
                };
                if tx.send(Ok(chunk)).await.is_err() {
                    break;
                }
            }
        });
        Ok(Stream {
            header: mvcc::header(self.revision),
            chunks: rx,
        })
    }
}

struct Source {
    /// A replaced database keeps its objects while this handle exists. See `Location::retire`.
    _db: Arc<Db>,
    admin: Arc<slatedb::admin::Admin>,
    store: Arc<dyn slatedb::object_store::ObjectStore>,
    root: String,
    applied: u64,
    members: Vec<u8>,
}

impl Source {
    async fn stream(&self, checkpoint: uuid::Uuid, tx: &mpsc::Sender<Result<Chunk>>) -> Result<()> {
        let reader = DbReader::builder(self.root.as_str(), self.store.clone())
            .with_reader_mode(DbReaderMode::Checkpoint(checkpoint))
            .with_options(DbReaderOptions {
                skip_wal_replay: true,
                ..Default::default()
            })
            // One sequential pass does not reuse blocks. Keep the member caches for its reads.
            .with_db_cache_disabled()
            .build()
            .await?;
        let result = self.copy(&reader, checkpoint, tx).await;
        let _ = reader.close().await;
        result
    }

    async fn copy(
        &self,
        reader: &DbReader,
        checkpoint: uuid::Uuid,
        tx: &mpsc::Sender<Result<Chunk>>,
    ) -> Result<()> {
        let mut lease = Lease {
            admin: &self.admin,
            checkpoint,
            refreshed: tokio::time::Instant::now(),
        };
        let header = (crate::SNAPSHOT_FORMAT, self.applied);
        // The row count precedes the rows. Count them first; the checkpoint does not change.
        let mut rows = 0_u64;
        let mut size = encoded_size(&header)? + encoded_size(&0_u64)?;
        let mut scan = rows_of(reader).await?;
        while let Some(row) = scan.next().await? {
            lease.keep().await?;
            ensure!(!tx.is_closed(), "the snapshot client went away");
            if !omitted(&row.key) {
                rows += 1;
                size += encoded_size(&(row.key, row.value))?;
            }
        }
        // Pad the membership JSON with spaces so that `etcdctl snapshot save` accepts the size.
        let mut members = self.members.clone();
        size += encoded_size(&(crate::cluster::KEY, &members))?;
        let padding = (ALIGN - size % ALIGN) % ALIGN;
        members.resize(members.len() + padding as usize, b' ');
        let mut image = Image {
            tx,
            buffer: Vec::with_capacity(2 * CHUNK_BYTES),
            hash: Sha256::new(),
            remaining: size + padding + DIGEST,
        };
        image.write(&header).await?;
        image.write(&(rows + 1)).await?;
        let mut scan = rows_of(reader).await?;
        let mut written = 0_u64;
        while let Some(row) = scan.next().await? {
            lease.keep().await?;
            if !omitted(&row.key) {
                written += 1;
                image.write(&(row.key, row.value)).await?;
            }
        }
        ensure!(
            written == rows,
            "backup checkpoint changed during the stream"
        );
        image.write(&(crate::cluster::KEY, members)).await?;
        image.finish().await
    }
}

async fn rows_of(reader: &DbReader) -> Result<slatedb::DbIterator> {
    let options = ScanOptions {
        cache_blocks: false,
        ..Default::default()
    };
    Ok(reader.scan_prefix_with_options(b"", .., &options).await?)
}

/// Return true for the rows that the image leaves out. The membership row is written last, and
/// format 4 readers reject the derived meta rows. See `derived_key`.
fn omitted(key: &[u8]) -> bool {
    key == crate::cluster::KEY || derived_key(key)
}

fn encoded_size(value: &impl serde::Serialize) -> Result<u64> {
    Ok(crate::codec::write(&mut std::io::sink(), value)? as u64)
}

/// Keep the backup checkpoint alive while the stream runs.
struct Lease<'a> {
    admin: &'a slatedb::admin::Admin,
    checkpoint: uuid::Uuid,
    refreshed: tokio::time::Instant,
}

impl Lease<'_> {
    async fn keep(&mut self) -> Result<()> {
        if self.refreshed.elapsed() >= REFRESH_EVERY {
            self.admin
                .refresh_checkpoint(self.checkpoint, Some(CHECKPOINT_LIFETIME))
                .await?;
            self.refreshed = tokio::time::Instant::now();
        }
        Ok(())
    }
}

/// Encode the image into chunks, then send its digest.
struct Image<'a> {
    tx: &'a mpsc::Sender<Result<Chunk>>,
    buffer: Vec<u8>,
    hash: Sha256,
    remaining: u64,
}

impl Image<'_> {
    async fn write(&mut self, value: &impl serde::Serialize) -> Result<()> {
        let start = self.buffer.len();
        crate::codec::write(&mut self.buffer, value)?;
        self.hash.update(&self.buffer[start..]);
        let mut sent = 0;
        while self.buffer.len() - sent >= CHUNK_BYTES {
            let blob = self.buffer[sent..sent + CHUNK_BYTES].to_vec();
            sent += CHUNK_BYTES;
            self.send(blob).await?;
        }
        self.buffer.drain(..sent);
        Ok(())
    }

    async fn send(&mut self, blob: Vec<u8>) -> Result<()> {
        self.remaining = self
            .remaining
            .checked_sub(blob.len() as u64)
            .context("backup image exceeds its counted size")?;
        let remaining = self.remaining;
        self.tx
            .send(Ok(Chunk { blob, remaining }))
            .await
            .map_err(|_| anyhow::anyhow!("the snapshot client went away"))
    }

    async fn finish(mut self) -> Result<()> {
        let mut blob = std::mem::take(&mut self.buffer);
        blob.extend_from_slice(&self.hash.clone().finalize());
        for part in blob.chunks(CHUNK_BYTES) {
            self.send(part.to_vec()).await?;
        }
        ensure!(
            self.remaining == 0,
            "backup image is shorter than its counted size"
        );
        Ok(())
    }
}

/// Read a native image file row by row, and check its digest at the end.
struct ImageFile {
    payload: Hashing<std::io::Take<BufReader<std::fs::File>>>,
    version: u32,
    rows: u64,
    read: u64,
}

impl ImageFile {
    fn open(path: &Path) -> Result<Self> {
        let file = std::fs::File::open(path)?;
        let len = file.metadata()?.len();
        ensure!(len > DIGEST, "snapshot is missing its checksum");
        let payload = Hashing {
            inner: BufReader::with_capacity(1024 * 1024, file).take(len - DIGEST),
            hash: Sha256::new(),
        };
        Ok(Self {
            payload,
            version: 0,
            rows: 0,
            read: 0,
        })
    }

    fn header(&mut self) -> Result<()> {
        let (version, _applied, rows): (u32, u64, u64) =
            crate::codec::read::<_, MAX_ROW_BYTES>(&mut self.payload)
                .context("invalid native snapshot")?;
        ensure!(
            crate::readable_snapshot(version),
            "unsupported snapshot version"
        );
        self.version = version;
        self.rows = rows;
        Ok(())
    }

    /// Decode a row. `Bytes` reads each field at once, where `Vec<u8>` reads it bytewise.
    fn next(&mut self) -> Result<Option<(Bytes, Bytes)>> {
        if self.read == self.rows {
            return Ok(None);
        }
        self.read += 1;
        let row = crate::codec::read::<_, MAX_ROW_BYTES>(&mut self.payload)
            .context("invalid native snapshot")?;
        Ok(Some(row))
    }

    /// Read the rest of the file. Check its digest, then that the rows filled the payload.
    fn finish(mut self, rows: Result<()>) -> Result<()> {
        let trailing = std::io::copy(&mut self.payload, &mut std::io::sink())?;
        let mut checksum = [0; DIGEST as usize];
        self.payload.inner.into_inner().read_exact(&mut checksum)?;
        ensure!(
            self.payload.hash.finalize().as_slice() == checksum,
            "snapshot checksum mismatch"
        );
        rows?;
        ensure!(trailing == 0, "trailing native snapshot data");
        Ok(())
    }
}

struct Hashing<R> {
    inner: R,
    hash: Sha256,
}

impl<R: Read> Read for Hashing<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.hash.update(&buf[..n]);
        Ok(n)
    }
}

/// Return true for the rows that a restore replaces instead of copying.
fn replaced(key: &[u8]) -> bool {
    key == APPLIED || derived_key(key) || key == crate::cluster::KEY
}

/// Check every row in one pass. Return the number of copied rows.
fn check(path: &Path) -> Result<u64> {
    let mut image = ImageFile::open(path)?;
    let mut copied = 0;
    let rows = (|| {
        image.header()?;
        let mut last: Option<Bytes> = None;
        while let Some((key, value)) = image.next()? {
            ensure!(snapshot_key(&key), "invalid snapshot key");
            if replaced(&key) {
                continue;
            }
            // Images hold rows in key order, so order also rules out duplicates.
            ensure!(
                last.as_ref().is_none_or(|last| *last < key),
                "duplicate or unordered snapshot key"
            );
            restored(&key, value, 0)?;
            last = Some(key);
            copied += 1;
        }
        Ok(())
    })();
    image.finish(rows)?;
    Ok(copied)
}

/// The value that a restored cluster stores for `key`.
fn restored(key: &[u8], value: Bytes, member: u64) -> Result<Bytes> {
    if key == crate::auth::KEY {
        let mut auth: crate::auth::Store = serde_json::from_slice(&value)?;
        auth.reset_tokens();
        return Ok(serde_json::to_vec(&auth)?.into());
    }
    if key == crate::maintenance::ALARMS {
        let mut alarms: Vec<crate::proto::etcdserverpb::AlarmMember> =
            serde_json::from_slice(&value)?;
        // Keep alarm types, but attach them to a member of the restored cluster.
        for alarm in &mut alarms {
            alarm.member_id = member;
        }
        alarms.sort_by_key(|alarm| alarm.alarm);
        alarms.dedup();
        return Ok(serde_json::to_vec(&alarms)?.into());
    }
    Ok(value)
}

impl StateMachine {
    /// Restore a checked native backup with a new Raft identity and membership.
    ///
    /// The rows go to the state database in batches. Row `n` of the copied rows is written at
    /// sequence `n` or later, and the apply marker and membership follow at `rows + 1`. The
    /// restored index therefore depends only on the backup, so every member restores the same
    /// Raft position. The Raft directory records a snapshot without an image at that index.
    pub async fn restore_backup(
        backup: &Path,
        dir: &Path,
        id: u64,
        cluster_id: &str,
        members: &crate::cluster::Store,
        objects: &crate::object::Config,
        cache: StateCache,
    ) -> Result<()> {
        ensure!(
            members.members.contains_key(&id),
            "restore member is missing"
        );
        ensure!(
            !dir.exists() || std::fs::read_dir(dir)?.next().is_none(),
            "snapshot restore requires a new, empty data directory"
        );
        let started = std::time::Instant::now();
        let copied = check(backup)?;
        tracing::info!(rows = copied, elapsed = ?started.elapsed(), "checked the snapshot to restore");
        std::fs::create_dir_all(dir)?;
        let marker = dir.join(RESTORE_MARKER);
        std::fs::File::create(&marker)?.sync_all()?;
        sync_dir(dir)?;
        let mut storage = crate::storage::RaftStorage::open(
            &dir.join("raft"),
            id,
            cluster_id,
            members.conf().voters,
        )?;
        // Startup rejects a restore in shared mode, so the restored state is replicated.
        let mut state = objects
            .open(
                dir,
                cluster_id,
                id,
                cache,
                StateMode::Replicated,
                // Replicated mode ignores the shared-mode settings.
                &crate::state::shared::Options {
                    quota: 0,
                    voters: members.conf().voters.clone(),
                    join: false,
                    empty_log: false,
                    pin_max: std::time::Duration::from_secs(300),
                },
            )
            .await?;
        state.row_marker = Some(storage.row_marker());
        let member = *members.members.keys().next().unwrap();
        let result = async {
            let mut image = ImageFile::open(backup)?;
            let mut written = 0;
            let rows = async {
                image.header()?;
                let mut batch = WriteBatch::new();
                let mut bytes = 0;
                let mut unflushed = 0;
                while let Some((key, value)) = image.next()? {
                    if replaced(&key) {
                        continue;
                    }
                    let value = restored(&key, value, member)?;
                    bytes += key.len() + value.len();
                    // The database starts empty, and the check rules out duplicate keys.
                    batch.put_new(key, value);
                    written += 1;
                    if bytes >= RESTORE_BATCH_BYTES {
                        state
                            .write_batch(std::mem::take(&mut batch), written)
                            .await?;
                        unflushed += bytes;
                        bytes = 0;
                        if unflushed >= RESTORE_FLUSH_BYTES {
                            state.metrics.flush_db(state.db()).await?;
                            unflushed = 0;
                        }
                    }
                }
                if !batch.is_empty() {
                    state.write_batch(batch, written).await?;
                }
                Ok(())
            }
            .await;
            // The file can change after the check. Check its digest again before the marker.
            image.finish(rows)?;
            ensure!(written == copied, "snapshot changed during restore");
            let index = written + 1;
            let mut batch = WriteBatch::new();
            batch.put(crate::cluster::KEY, serde_json::to_vec(members)?);
            // The Raft directory records the snapshot at `index` with term 1.
            mark_applied(&mut batch, index, 1);
            state.write_batch(batch, index).await?;
            state.metrics.flush_db(state.db()).await?;
            tracing::info!(index, elapsed = ?started.elapsed(), "restored the snapshot rows");
            Ok(index)
        }
        .await;
        state.close().await?;
        storage.restore(result?, members.conf())?;
        std::fs::remove_file(&marker)?;
        sync_dir(dir)
    }
}

fn sync_dir(dir: &Path) -> Result<()> {
    std::fs::File::open(dir)?.sync_all()?;
    Ok(())
}
