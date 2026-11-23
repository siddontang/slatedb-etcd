//! Publish checkpoint bytes before the WAL can reference them or discard covered entries.

mod encoding;

use super::{Engine, LogBatch, LogFileSystem, ProstCodec, Snapshot, GROUP, SNAPSHOT};
use anyhow::{ensure, Context, Result};
use std::fs::{self, File};
use std::io::ErrorKind;
use std::path::{Path, PathBuf};

const REFERENCE: &[u8] = b"checkpoint-file";
const PENDING: &str = "checkpoint.pending";

const STAGING_PREFIX: &str = "checkpoint-stage-";
const STAGING_SUFFIX: &str = ".tmp";
const RANDOM_CHARS: usize = 12;

/// The file of one checkpoint image. A replacement at the same index gets a new generation,
/// so the previous file stays intact until the new reference is durable.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct FileId {
    pub index: u64,
    pub generation: u64,
}

impl FileId {
    #[cfg(test)]
    pub fn first(index: u64) -> Self {
        Self {
            index,
            generation: 0,
        }
    }
}

/// A reference field that marks a generation suffix after the original fields.
const GENERATION_TAG: u8 = 1;

pub(super) fn path(dir: &Path, id: FileId) -> PathBuf {
    match id.generation {
        0 => dir.join(format!("checkpoint-{:016x}.data", id.index)),
        generation => dir.join(format!(
            "checkpoint-{:016x}-{generation:016x}.data",
            id.index
        )),
    }
}

fn parse(name: &str) -> Option<FileId> {
    let id = name.strip_prefix("checkpoint-")?.strip_suffix(".data")?;
    let (index, generation) = match id.split_once('-') {
        Some((index, generation)) => (index, u64::from_str_radix(generation, 16).ok()?),
        None => (id, 0),
    };
    Some(FileId {
        index: u64::from_str_radix(index, 16).ok()?,
        generation,
    })
}

fn remove(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
        result => Ok(result?),
    }
}

/// A synced image that the WAL owner can publish or discard.
pub struct PreparedCheckpoint {
    pub(crate) data: bytes::Bytes,
    file: tempfile::NamedTempFile,
    checksum: u32,
    compressed_len: Option<u64>,
}

impl PreparedCheckpoint {
    pub(crate) fn write(dir: &Path, data: bytes::Bytes) -> Result<Self> {
        let mut file = tempfile::Builder::new()
            .prefix(STAGING_PREFIX)
            .suffix(STAGING_SUFFIX)
            .rand_bytes(RANDOM_CHARS)
            .tempfile_in(dir)?;
        let compressed_len = encoding::write(file.as_file_mut(), &data)?;
        file.as_file().sync_all()?;
        let checksum = crc32c::crc32c(&data);
        crate::failpoint::hit("checkpoint-after-file-sync");
        Ok(Self {
            data,
            file,
            checksum,
            compressed_len,
        })
    }

    pub(super) fn publish(
        self,
        dir: &Path,
        snapshot: &Snapshot,
        generation: u64,
        batch: &mut LogBatch,
    ) -> Result<FileId> {
        let id = FileId {
            index: snapshot.get_metadata().index,
            generation,
        };
        self.file.persist(path(dir, id))?;
        File::open(dir)?.sync_all()?;
        crate::failpoint::hit("checkpoint-file-before-reference");

        let mut reference = (self.data.len() as u64).to_le_bytes().to_vec();
        reference.extend_from_slice(&self.checksum.to_le_bytes());
        if let Some(len) = self.compressed_len {
            reference.extend_from_slice(&len.to_le_bytes());
        }
        if generation != 0 {
            reference.push(GENERATION_TAG);
            reference.extend_from_slice(&generation.to_le_bytes());
        }
        let mut metadata = snapshot.clone();
        metadata.data = Vec::new();
        batch.put_value::<ProstCodec, _>(GROUP, SNAPSHOT.to_vec(), &metadata)?;
        batch.put(GROUP, REFERENCE.to_vec(), reference)?;
        Ok(id)
    }
}

/// Publish snapshot metadata without an image. Flushed SlateDB state covers the removed log.
pub(super) fn publish_metadata(snapshot: &Snapshot, batch: &mut LogBatch) -> Result<()> {
    ensure!(snapshot.data.is_empty(), "metadata snapshot has data");
    batch.put_value::<ProstCodec, _>(GROUP, SNAPSHOT.to_vec(), snapshot)?;
    batch.delete(GROUP, REFERENCE.to_vec());
    Ok(())
}

fn staged_name(name: &str) -> bool {
    name.strip_prefix(STAGING_PREFIX)
        .and_then(|name| name.strip_suffix(STAGING_SUFFIX))
        .is_some_and(|id| id.len() == RANDOM_CHARS && id.bytes().all(|b| b.is_ascii_alphanumeric()))
}

pub(super) fn load(
    dir: &Path,
    engine: &Engine<LogFileSystem>,
    snapshot: &mut Snapshot,
) -> Result<Option<FileId>> {
    if let Some(reference) = engine.get(GROUP, REFERENCE) {
        ensure!(
            matches!(reference.len(), 12 | 20 | 21 | 29) && snapshot.data.is_empty(),
            "invalid checkpoint reference"
        );
        let (reference, generation) = match reference.len() {
            21 | 29 => {
                let (fields, suffix) = reference.split_at(reference.len() - 9);
                ensure!(suffix[0] == GENERATION_TAG, "invalid checkpoint reference");
                (fields, u64::from_le_bytes(suffix[1..].try_into()?))
            }
            _ => (&reference[..], 0),
        };
        let len = u64::from_le_bytes(reference[..8].try_into()?);
        let checksum = u32::from_le_bytes(reference[8..12].try_into()?);
        let compressed_len = reference[12..].try_into().ok().map(u64::from_le_bytes);
        let id = FileId {
            index: snapshot.get_metadata().index,
            generation,
        };
        let file = File::open(path(dir, id)).context("missing checkpoint file")?;
        let data = encoding::read(file, len, compressed_len)?;
        ensure!(
            crc32c::crc32c(&data) == checksum,
            "checkpoint checksum mismatch"
        );
        snapshot.data = data;
        return Ok(Some(id));
    }
    Ok(None)
}

/// Remove a file that the published reference no longer names.
pub(super) fn retire(dir: &Path, id: Option<FileId>) -> Result<()> {
    if let Some(id) = id.filter(|id| id.index != 0) {
        remove(&path(dir, id))?;
    }
    Ok(())
}

pub(super) fn remove_orphans(dir: &Path, current: Option<FileId>) -> Result<()> {
    remove(&dir.join(PENDING))?;
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        if name.to_str().is_some_and(staged_name) {
            remove(&entry.path())?;
            continue;
        }
        let Some(id) = name.to_str().and_then(parse) else {
            continue;
        };
        if Some(id) != current && entry.path() == path(dir, id) {
            remove(&entry.path())?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn concurrent_images_cannot_replace_the_published_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let image = |bytes| {
            PreparedCheckpoint::write(dir.path(), bytes::Bytes::from_static(bytes)).unwrap()
        };
        let older = image(b"old local image");
        let orphan = older.file.path().to_owned();
        let incoming = image(b"from leader");
        let mut snapshot = Snapshot::default();
        snapshot.mut_metadata().index = 11;
        incoming
            .publish(dir.path(), &snapshot, 0, &mut LogBatch::default())
            .unwrap();
        drop(older);
        assert!(!orphan.exists());
        assert_eq!(
            fs::read(path(dir.path(), FileId::first(11))).unwrap(),
            b"from leader"
        );

        let abandoned = image(b"synced before process exit");
        let (_, abandoned) = abandoned.file.keep().unwrap();
        let unrelated = dir.path().join("checkpoint-stage-personal.tmp");
        fs::write(&unrelated, b"other file").unwrap();
        let replaced = path(
            dir.path(),
            FileId {
                index: 11,
                generation: 2,
            },
        );
        fs::write(&replaced, b"replaced image").unwrap();
        remove_orphans(dir.path(), Some(FileId::first(11))).unwrap();
        assert!(!abandoned.exists());
        assert!(!replaced.exists());
        assert!(unrelated.exists());
        assert_eq!(
            fs::read(path(dir.path(), FileId::first(11))).unwrap(),
            b"from leader"
        );
    }
}
