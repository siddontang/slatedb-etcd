#[cfg(target_os = "macos")]
use std::fs::{File, OpenOptions};
use std::io::{self, Seek, Write};
#[cfg(target_os = "macos")]
use std::os::fd::AsRawFd;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use raft_engine::env::{DefaultFileSystem, FileSystem, Handle, Permission, WriteExt};

pub(super) struct LogFileSystem(pub crate::metrics::Wal);

pub(super) struct LogHandle {
    inner: Arc<<DefaultFileSystem as FileSystem>::Handle>,
    metrics: crate::metrics::Wal,
    #[cfg(target_os = "macos")]
    file: File,
}

impl Handle for LogHandle {
    fn truncate(&self, offset: usize) -> io::Result<()> {
        self.inner.truncate(offset)
    }

    fn file_size(&self) -> io::Result<usize> {
        self.inner.file_size()
    }

    fn sync(&self) -> io::Result<()> {
        let start = Instant::now();
        let result = self.sync_file();
        self.metrics.fsync.observe(start.elapsed().as_secs_f64());
        result
    }
}

impl LogHandle {
    #[cfg(not(target_os = "macos"))]
    fn sync_file(&self) -> io::Result<()> {
        self.inner.sync()
    }

    #[cfg(target_os = "macos")]
    fn sync_file(&self) -> io::Result<()> {
        // Match etcd on macOS. Plain fsync does not flush the drive's write cache.
        // SAFETY: The owned file keeps this descriptor valid throughout the call.
        let result = unsafe { libc::fcntl(self.file.as_raw_fd(), libc::F_FULLFSYNC, 0) };
        if result == -1 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }
}

impl FileSystem for LogFileSystem {
    type Handle = LogHandle;
    type Reader = <DefaultFileSystem as FileSystem>::Reader;
    type Writer = LogWriter;

    fn create<P: AsRef<Path>>(&self, path: P) -> io::Result<Self::Handle> {
        let inner = Arc::new(DefaultFileSystem.create(&path)?);
        #[cfg(target_os = "macos")]
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        Ok(LogHandle {
            inner,
            metrics: self.0.clone(),
            #[cfg(target_os = "macos")]
            file,
        })
    }

    fn open<P: AsRef<Path>>(&self, path: P, permission: Permission) -> io::Result<Self::Handle> {
        let inner = Arc::new(DefaultFileSystem.open(&path, permission)?);
        #[cfg(target_os = "macos")]
        let file = OpenOptions::new()
            .read(true)
            .write(permission == Permission::ReadWrite)
            .open(path)?;
        Ok(LogHandle {
            inner,
            metrics: self.0.clone(),
            #[cfg(target_os = "macos")]
            file,
        })
    }

    fn delete<P: AsRef<Path>>(&self, path: P) -> io::Result<()> {
        DefaultFileSystem.delete(path)
    }

    fn rename<P: AsRef<Path>>(&self, source: P, target: P) -> io::Result<()> {
        DefaultFileSystem.rename(source, target)
    }

    fn new_reader(&self, handle: Arc<Self::Handle>) -> io::Result<Self::Reader> {
        DefaultFileSystem.new_reader(Arc::clone(&handle.inner))
    }

    fn new_writer(&self, handle: Arc<Self::Handle>) -> io::Result<Self::Writer> {
        Ok(LogWriter {
            inner: DefaultFileSystem.new_writer(Arc::clone(&handle.inner))?,
            file: Arc::clone(&handle.inner),
            metrics: self.0.clone(),
        })
    }
}

pub(super) struct LogWriter {
    inner: <DefaultFileSystem as FileSystem>::Writer,
    file: Arc<<DefaultFileSystem as FileSystem>::Handle>,
    metrics: crate::metrics::Wal,
}

/// Zero bytes for one write of log file preallocation.
static ZEROS: [u8; 64 * 1024] = [0; 64 * 1024];

impl Write for LogWriter {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        let start = Instant::now();
        let result = self.inner.write(data);
        self.metrics.write.observe(start.elapsed().as_secs_f64());
        if let Ok(count) = result {
            self.metrics.bytes.add(count as i64);
        }
        result
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

impl Seek for LogWriter {
    fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
        self.inner.seek(pos)
    }
}

impl WriteExt for LogWriter {
    fn truncate(&mut self, offset: usize) -> io::Result<()> {
        self.inner.truncate(offset)
    }

    /// Write zeros over the range instead of `fallocate`.
    ///
    /// `fallocate` makes unwritten extents. On ext4, each later `fdatasync` must then commit
    /// the journal to mark the written blocks, and that costs a second device flush. Written
    /// zeros make the blocks plain data, so a log sync flushes the device once. Recovery reads
    /// zeros after the last record in both cases.
    fn allocate(&mut self, offset: usize, size: usize) -> io::Result<()> {
        let end = offset + size;
        let mut at = offset;
        while at < end {
            let len = (end - at).min(ZEROS.len());
            let written = self.file.write(at, &ZEROS[..len])?;
            if written == 0 {
                return Err(io::ErrorKind::WriteZero.into());
            }
            at += written;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};

    #[test]
    fn full_sync_keeps_written_data_across_rename_and_reopen() {
        let directory = tempfile::tempdir().unwrap();
        let original = directory.path().join("original");
        let renamed = directory.path().join("renamed");
        let fs = LogFileSystem(crate::metrics::Wal::new().unwrap());
        let handle = Arc::new(fs.create(&original).unwrap());
        let mut writer = fs.new_writer(Arc::clone(&handle)).unwrap();
        writer.write_all(b"durable log").unwrap();
        fs.rename(&original, &renamed).unwrap();
        handle.sync().unwrap();
        assert_eq!(handle.file_size().unwrap(), 11);
        drop(writer);
        drop(handle);
        let handle = Arc::new(fs.open(&renamed, Permission::ReadOnly).unwrap());
        let mut reader = fs.new_reader(handle).unwrap();
        let mut data = Vec::new();
        reader.read_to_end(&mut data).unwrap();
        assert_eq!(data, b"durable log");
        fs.delete(renamed).unwrap();
    }

    #[test]
    fn allocation_writes_zeros_after_data() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("log");
        let fs = LogFileSystem(crate::metrics::Wal::new().unwrap());
        let handle = Arc::new(fs.create(&path).unwrap());
        let mut writer = fs.new_writer(Arc::clone(&handle)).unwrap();
        writer.write_all(b"record").unwrap();
        let size = ZEROS.len() * 2 + 7;
        writer.allocate(6, size).unwrap();
        writer.write_all(b"next").unwrap();
        handle.sync().unwrap();
        assert_eq!(handle.file_size().unwrap(), 6 + size);
        drop(writer);
        let mut data = Vec::new();
        fs.new_reader(handle)
            .unwrap()
            .read_to_end(&mut data)
            .unwrap();
        assert_eq!(&data[..10], b"recordnext");
        assert!(data[10..].iter().all(|&byte| byte == 0));
    }
}
