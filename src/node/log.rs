use super::*;
use std::sync::{Arc, Condvar, Mutex};

pub(super) use super::cache::Cache;
use raft::eraftpb::{HardState, Snapshot};
use raft::GetEntriesContext;

pub(super) const MAX_JOBS: usize = 64;
pub(super) const MAX_BYTES: usize = 16 * 1024 * 1024;
pub(super) const MAX_SNAPSHOT_BYTES: usize = 64 * 1024 * 1024;
/// The owner takes no Ready while this many wait for persistence. Joined jobs keep one Ready
/// number, but raft-rs keeps a 56-byte record for each Ready until `on_persist_ready`. With
/// one heartbeat Ready for each 100 ms tick, the limit is 102 s behind a slow write. Other
/// work also makes Readys, so the limit does not set a time.
pub(super) const MAX_READYS: usize = 1024;

pub(super) struct Durable {
    pub hard_state: HardState,
    pub last_index: u64,
    pub snapshot: Snapshot,
    pub sync_writes: u64,
}

impl Durable {
    pub fn read(storage: &RaftStorage) -> Result<Self> {
        Ok(Self {
            hard_state: storage.initial_state()?.hard_state,
            last_index: storage.last_index()?,
            snapshot: storage.recovery_snapshot().clone(),
            sync_writes: storage.sync_writes,
        })
    }
}

pub(super) enum Job {
    Stats,
    Fetch(super::cache::Fetch),
    Ready {
        entries: Vec<Entry>,
        hs: Option<HardState>,
        snapshot: Option<Snapshot>,
    },
    /// Truncate the log through `index`. An image is present only when a peer needs one.
    Checkpoint {
        conf: raft::eraftpb::ConfState,
        index: u64,
        image: Option<super::PreparedCheckpoint>,
    },
    #[cfg(test)]
    Panic,
}

impl Job {
    pub fn is_barrier(&self) -> bool {
        matches!(self, Self::Ready { entries, hs: None, snapshot: None } if entries.is_empty())
    }

    pub fn can_batch(&self) -> bool {
        matches!(self, Self::Ready { snapshot: None, .. })
    }

    pub fn append(&mut self, next: Self) -> Result<()> {
        let Self::Ready {
            entries,
            hs,
            snapshot: None,
        } = self
        else {
            anyhow::bail!("cannot batch this log job");
        };
        let Self::Ready {
            entries: newer,
            hs: newer_hs,
            snapshot: None,
        } = next
        else {
            anyhow::bail!("cannot batch this log job");
        };
        if let Some(first) = newer.first() {
            entries.truncate(entries.partition_point(|entry| entry.index < first.index));
            ensure!(
                entries
                    .last()
                    .is_none_or(|last| last.index + 1 == first.index),
                "batched log jobs have an index gap"
            );
            entries.extend(newer);
        }
        if newer_hs.is_some() {
            *hs = newer_hs;
        }
        Ok(())
    }

    pub fn bytes(&self) -> usize {
        match self {
            Self::Fetch(_) | Self::Stats => 0,
            Self::Ready { entries, .. } => entries.iter().map(|e| e.encoded_len()).sum(),
            Self::Checkpoint { .. } => 0,
            #[cfg(test)]
            Self::Panic => 0,
        }
    }
    pub fn snapshot_bytes(&self) -> usize {
        match self {
            Self::Ready {
                snapshot: Some(snapshot),
                ..
            } => snapshot.data.len(),
            Self::Checkpoint { image, .. } => image.as_ref().map_or(0, |image| image.data.len()),
            _ => 0,
        }
    }

    fn writes(&self) -> bool {
        match self {
            Self::Ready {
                entries,
                hs,
                snapshot,
            } => !entries.is_empty() || hs.is_some() || snapshot.is_some(),
            Self::Fetch(_) | Self::Stats => false,
            Self::Checkpoint { .. } => true,
            #[cfg(test)]
            Self::Panic => true,
        }
    }
}

pub(super) struct Work {
    pub id: u64,
    pub job: Job,
    pub delay: Duration,
    pub fail: bool,
}

pub(super) struct Completion {
    pub id: u64,
    pub durable: Durable,
    pub fetched: Option<(super::cache::Fetch, raft::Result<Vec<Entry>>)>,
    pub stats: Option<crate::storage::LogStats>,
}

struct Executor {
    storage: RaftStorage,
}

impl Executor {
    fn execute(&mut self, work: Work) -> Result<Completion> {
        ensure!(
            !work.fail || !work.job.writes(),
            "injected Raft log write failure"
        );
        let mut fetched = None;
        let mut stats = None;
        match work.job {
            Job::Stats => stats = Some(self.storage.log_stats()?),
            Job::Fetch(fetch) => {
                ensure!(!work.fail, "injected Raft log read failure");
                let entries = self.storage.entries(
                    fetch.low,
                    fetch.high,
                    Some(fetch.max_bytes),
                    GetEntriesContext::empty(false),
                );
                fetched = Some((fetch, entries));
            }
            Job::Ready {
                entries,
                hs,
                snapshot,
            } => self
                .storage
                .persist(&entries, hs.as_ref(), snapshot.as_ref())?,
            Job::Checkpoint { index, image, conf } => {
                let attach = replaces_at(&self.storage, index, image.as_ref());
                // A received snapshot can make this result obsolete.
                if index > self.storage.snapshot_index() || attach {
                    self.storage
                        .publish_checkpoint(index, image, conf, attach)?;
                }
            }
            #[cfg(test)]
            Job::Panic => panic!("injected log worker panic"),
        }
        Ok(Completion {
            id: work.id,
            durable: Durable::read(&self.storage)?,
            fetched,
            stats,
        })
    }
}

/// Return true when an image at `index` replaces the record at the same index: the record
/// has no transferable image, or it names another view checkpoint. A restart or a rebase can
/// move the base to another checkpoint at the record index when retention deleted the one
/// that the record names.
///
/// Only a view descriptor is transferable. See `transfer::sendable`.
fn replaces_at(
    storage: &RaftStorage,
    index: u64,
    image: Option<&super::PreparedCheckpoint>,
) -> bool {
    let Some(image) = image else {
        return false;
    };
    if index != storage.snapshot_index() {
        return false;
    }
    let record = &storage.recovery_snapshot().data;
    let view = |data: &[u8]| {
        crate::state::transfer::ViewDescriptor::decode(data)
            .ok()
            .flatten()
            .map(|view| view.checkpoint)
    };
    let metadata_only = index > 0 && !crate::state::transfer::sendable(record);
    metadata_only
        || view(record)
            .zip(view(&image.data))
            .is_some_and(|(old, new)| old != new)
}

pub(super) struct Worker {
    work: mpsc::Sender<Work>,
    done: mpsc::Receiver<Result<Completion>>,
    stop: Arc<(Mutex<bool>, Condvar)>,
    task: Option<std::thread::JoinHandle<()>>,
    #[cfg(test)]
    manual: Option<(Executor, mpsc::Receiver<Work>)>,
}

impl Drop for Worker {
    fn drop(&mut self) {
        *self.stop.0.lock().unwrap() = true;
        self.stop.1.notify_all();
    }
}

impl Worker {
    pub fn start(storage: RaftStorage, manual: bool) -> Result<Self> {
        let (work, mut jobs) = mpsc::channel::<Work>(1);
        let (tx, done) = mpsc::channel(1);
        let mut executor = Executor { storage };
        let stop = Arc::new((Mutex::new(false), Condvar::new()));
        #[cfg(test)]
        if manual {
            return Ok(Self {
                work,
                done,
                stop,
                task: None,
                manual: Some((executor, jobs)),
            });
        }
        assert!(!manual);
        let shutdown = Arc::clone(&stop);
        #[cfg(feature = "write-profile")]
        let runtime = tokio::runtime::Handle::current();
        let task = std::thread::Builder::new()
            .name("raft-log-io".into())
            .spawn(move || {
                #[cfg(feature = "write-profile")]
                let probe = crate::write_profile::Probe::new(crate::write_profile::Stage::Wal);
                #[cfg(feature = "write-profile")]
                let mut receive = || runtime.block_on(probe.run(jobs.recv()));
                #[cfg(not(feature = "write-profile"))]
                let mut receive = || jobs.blocking_recv();
                while let Some(work) = receive() {
                    #[cfg(feature = "write-profile")]
                    let _stage = crate::write_profile::Stage::Wal.enter();
                    #[cfg(feature = "write-profile")]
                    if let Job::Ready { entries, .. } = &work.job {
                        crate::write_profile::Stage::Wal.batch(entries.len());
                    }
                    #[cfg(feature = "write-profile")]
                    let guard = crate::write_profile::Lock::WalStop.acquire(&shutdown.0);
                    #[cfg(not(feature = "write-profile"))]
                    let guard = shutdown.0.lock();
                    let mut stopped = guard.unwrap();
                    if (work.job.writes() || matches!(work.job, Job::Fetch(_)))
                        && !work.delay.is_zero()
                    {
                        stopped = shutdown
                            .1
                            .wait_timeout_while(stopped, work.delay, |s| !*s)
                            .unwrap()
                            .0;
                    }
                    if *stopped {
                        break;
                    }
                    drop(stopped);
                    let result = executor.execute(work);
                    let failed = result.is_err();
                    if tx.blocking_send(result).is_err() || failed {
                        break;
                    }
                }
            })?;
        Ok(Self {
            work,
            done,
            stop,
            task: Some(task),
            #[cfg(test)]
            manual: None,
        })
    }
    pub async fn shutdown(&mut self) -> Result<()> {
        *self.stop.0.lock().unwrap() = true;
        self.stop.1.notify_all();
        self.done.close();
        self.work = mpsc::channel(1).0;
        if let Some(task) = self.task.take() {
            tokio::task::spawn_blocking(move || task.join())
                .await?
                .map_err(|_| anyhow::anyhow!("log worker panicked"))?;
        }
        Ok(())
    }

    pub fn submit(&self, work: Work) -> Result<()> {
        self.work
            .try_send(work)
            .map_err(|_| anyhow::anyhow!("log worker queue is unavailable"))
    }
    pub async fn next(&mut self) -> Result<Completion> {
        self.done.recv().await.context("log worker stopped")?
    }
    #[cfg(test)]
    pub fn storage(&self) -> &RaftStorage {
        &self.manual.as_ref().unwrap().0.storage
    }
    #[cfg(test)]
    pub fn step(&mut self) -> Result<Option<Completion>> {
        let (executor, jobs) = self.manual.as_mut().context("log worker is not manual")?;
        match jobs.try_recv() {
            Ok(work) => executor.execute(work).map(Some),
            Err(mpsc::error::TryRecvError::Empty) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }
}
