use std::fs::{File, OpenOptions};
use std::io;
use std::os::fd::AsRawFd;
use std::path::Path;
use std::sync::Arc;

use raft_engine::env::{DefaultFileSystem, FileSystem, Handle, Permission};

pub(super) struct FullSyncFileSystem;

pub(super) struct FullSyncHandle {
    inner: Arc<<DefaultFileSystem as FileSystem>::Handle>,
    file: File,
}

impl Handle for FullSyncHandle {
    fn truncate(&self, offset: usize) -> io::Result<()> {
        self.inner.truncate(offset)
    }

    fn file_size(&self) -> io::Result<usize> {
        self.inner.file_size()
    }

    fn sync(&self) -> io::Result<()> {
        // Match etcd on macOS. Plain fsync does not flush the drive's write cache.
        // SAFETY: The owned file keeps this descriptor valid throughout the call.
        let result = unsafe { libc::fcntl(self.file.as_raw_fd(), libc::F_FULLFSYNC, 0) };
        if result == -1 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }
}

impl FileSystem for FullSyncFileSystem {
    type Handle = FullSyncHandle;
    type Reader = <DefaultFileSystem as FileSystem>::Reader;
    type Writer = <DefaultFileSystem as FileSystem>::Writer;

    fn create<P: AsRef<Path>>(&self, path: P) -> io::Result<Self::Handle> {
        let inner = Arc::new(DefaultFileSystem.create(&path)?);
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        Ok(FullSyncHandle { inner, file })
    }

    fn open<P: AsRef<Path>>(&self, path: P, permission: Permission) -> io::Result<Self::Handle> {
        let inner = Arc::new(DefaultFileSystem.open(&path, permission)?);
        let file = OpenOptions::new()
            .read(true)
            .write(permission == Permission::ReadWrite)
            .open(path)?;
        Ok(FullSyncHandle { inner, file })
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
        DefaultFileSystem.new_writer(Arc::clone(&handle.inner))
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
        let fs = FullSyncFileSystem;
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
}
