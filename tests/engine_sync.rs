use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use raft_engine::env::{DefaultFileSystem, FileSystem, Handle, Permission};
use raft_engine::{Config, Engine, LogBatch};

struct RecordedHandle {
    inner: Arc<<DefaultFileSystem as FileSystem>::Handle>,
    syncs: Arc<AtomicUsize>,
}

impl Handle for RecordedHandle {
    fn truncate(&self, offset: usize) -> io::Result<()> {
        self.inner.truncate(offset)
    }

    fn file_size(&self) -> io::Result<usize> {
        self.inner.file_size()
    }

    fn sync(&self) -> io::Result<()> {
        self.inner.sync()?;
        self.syncs.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

struct RecordedFileSystem(Arc<AtomicUsize>);

impl FileSystem for RecordedFileSystem {
    type Handle = RecordedHandle;
    type Reader = <DefaultFileSystem as FileSystem>::Reader;
    type Writer = <DefaultFileSystem as FileSystem>::Writer;

    fn create<P: AsRef<Path>>(&self, path: P) -> io::Result<Self::Handle> {
        Ok(RecordedHandle {
            inner: Arc::new(DefaultFileSystem.create(path)?),
            syncs: self.0.clone(),
        })
    }

    fn open<P: AsRef<Path>>(&self, path: P, permission: Permission) -> io::Result<Self::Handle> {
        Ok(RecordedHandle {
            inner: Arc::new(DefaultFileSystem.open(path, permission)?),
            syncs: self.0.clone(),
        })
    }

    fn delete<P: AsRef<Path>>(&self, path: P) -> io::Result<()> {
        DefaultFileSystem.delete(path)
    }

    fn rename<P: AsRef<Path>>(&self, from: P, to: P) -> io::Result<()> {
        DefaultFileSystem.rename(from, to)
    }

    fn new_reader(&self, handle: Arc<Self::Handle>) -> io::Result<Self::Reader> {
        DefaultFileSystem.new_reader(handle.inner.clone())
    }

    fn new_writer(&self, handle: Arc<Self::Handle>) -> io::Result<Self::Writer> {
        DefaultFileSystem.new_writer(handle.inner.clone())
    }
}

#[test]
fn explicit_sync_and_empty_sync_batch_reach_the_file_system() {
    let dir = tempfile::tempdir().unwrap();
    let syncs = Arc::new(AtomicUsize::new(0));
    let engine = Engine::open_with_file_system(
        Config {
            dir: dir.path().to_string_lossy().into_owned(),
            ..Config::default()
        },
        Arc::new(RecordedFileSystem(syncs.clone())),
    )
    .unwrap();
    let mut batch = LogBatch::default();
    batch.put(1, b"key".to_vec(), b"value".to_vec()).unwrap();
    engine.write(&mut batch, false).unwrap();
    let before = syncs.load(Ordering::SeqCst);
    engine.sync().unwrap();
    assert_eq!(syncs.load(Ordering::SeqCst), before + 1);
    engine.write(&mut LogBatch::default(), true).unwrap();
    assert_eq!(syncs.load(Ordering::SeqCst), before + 2);
    engine.write(&mut LogBatch::default(), false).unwrap();
    assert_eq!(syncs.load(Ordering::SeqCst), before + 2);
}
