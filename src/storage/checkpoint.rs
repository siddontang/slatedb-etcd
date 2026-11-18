//! Publish checkpoint bytes before the WAL can reference them or discard covered entries.

use super::{Engine, LogBatch, LogFileSystem, Snapshot, GROUP, SNAPSHOT};
use anyhow::{ensure, Context, Result};
use std::fs::{self, File};
use std::io::{ErrorKind, Write};
use std::path::{Path, PathBuf};

const REFERENCE: &[u8] = b"checkpoint-file";
const PENDING: &str = "checkpoint.pending";

pub(super) fn path(dir: &Path, index: u64) -> PathBuf {
    dir.join(format!("checkpoint-{index:016x}.data"))
}

fn remove(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
        result => Ok(result?),
    }
}

pub(super) fn stage(dir: &Path, snapshot: &Snapshot, batch: &mut LogBatch) -> Result<()> {
    let pending = dir.join(PENDING);
    let mut file = File::create(&pending)?;
    file.write_all(&snapshot.data)?;
    file.sync_all()?;
    fs::rename(pending, path(dir, snapshot.get_metadata().index))?;
    File::open(dir)?.sync_all()?;
    crate::failpoint::hit("checkpoint-file-before-reference");

    let mut reference = (snapshot.data.len() as u64).to_le_bytes().to_vec();
    reference.extend_from_slice(&crc32c::crc32c(&snapshot.data).to_le_bytes());
    let mut metadata = snapshot.clone();
    metadata.data = bytes::Bytes::new();
    batch.put_message(GROUP, SNAPSHOT.to_vec(), &metadata)?;
    batch.put(GROUP, REFERENCE.to_vec(), reference)?;
    Ok(())
}

pub(super) fn load(
    dir: &Path,
    engine: &Engine<LogFileSystem>,
    snapshot: &mut Snapshot,
) -> Result<Option<u64>> {
    if let Some(reference) = engine.get(GROUP, REFERENCE) {
        ensure!(
            reference.len() == 12 && snapshot.data.is_empty(),
            "invalid checkpoint reference"
        );
        let len = u64::from_le_bytes(reference[..8].try_into()?);
        let checksum = u32::from_le_bytes(reference[8..].try_into()?);
        let file = path(dir, snapshot.get_metadata().index);
        ensure!(
            fs::metadata(&file)
                .context("missing checkpoint file")?
                .len()
                == len,
            "checkpoint length mismatch"
        );
        let data = fs::read(file)?;
        ensure!(
            crc32c::crc32c(&data) == checksum,
            "checkpoint checksum mismatch"
        );
        snapshot.data = data.into();
        return Ok(Some(snapshot.get_metadata().index));
    }
    Ok(None)
}

pub(super) fn retire(dir: &Path, index: u64) -> Result<()> {
    if index != 0 {
        remove(&path(dir, index))?;
    }
    Ok(())
}

pub(super) fn remove_orphans(dir: &Path, current: Option<u64>) -> Result<()> {
    remove(&dir.join(PENDING))?;
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(index) = name
            .to_str()
            .and_then(|s| s.strip_prefix("checkpoint-"))
            .and_then(|s| s.strip_suffix(".data"))
            .and_then(|s| u64::from_str_radix(s, 16).ok())
        else {
            continue;
        };
        if Some(index) != current && entry.path() == path(dir, index) {
            remove(&entry.path())?;
        }
    }
    Ok(())
}
