use super::*;
use std::sync::{Arc, Condvar, Mutex};

pub(super) use super::cache::Cache;
use raft::eraftpb::{HardState, Snapshot};
use raft::GetEntriesContext;

pub(super) const MAX_JOBS: usize = 64;
pub(super) const MAX_BYTES: usize = 16 * 1024 * 1024;

pub(super) struct Durable {
    pub hard_state: HardState,
    pub last_index: u64,
    pub snapshot: Snapshot,
    pub sync_writes: u64,
    pub stats: crate::storage::LogStats,
}

impl Durable {
    pub fn read(storage: &RaftStorage) -> Result<Self> {
        Ok(Self {
            hard_state: storage.initial_state()?.hard_state,
            last_index: storage.last_index()?,
            snapshot: storage.recovery_snapshot().clone(),
            sync_writes: storage.sync_writes,
            stats: storage.log_stats()?,
        })
    }
}

pub(super) enum Job {
    Fetch(super::cache::Fetch),
    Ready {
        entries: Vec<Entry>,
        hs: Option<HardState>,
        snapshot: Option<Snapshot>,
    },
    Checkpoint {
        conf: raft::eraftpb::ConfState,
        index: u64,
        data: Vec<u8>,
    },
    #[cfg(test)]
    Panic,
}

impl Job {
    pub fn bytes(&self) -> usize {
        match self {
            Self::Fetch(_) => 0,
            Self::Ready {
                entries, snapshot, ..
            } => {
                entries
                    .iter()
                    .map(|e| e.compute_size() as usize)
                    .sum::<usize>()
                    + snapshot.as_ref().map_or(0, |s| s.data.len())
            }
            Self::Checkpoint { data, .. } => data.len(),
            #[cfg(test)]
            Self::Panic => 0,
        }
    }
    fn writes(&self) -> bool {
        match self {
            Self::Ready {
                entries,
                hs,
                snapshot,
            } => !entries.is_empty() || hs.is_some() || snapshot.is_some(),
            Self::Fetch(_) => false,
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
        match work.job {
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
            Job::Checkpoint { index, data, conf } => {
                if index > self.storage.snapshot_index() {
                    self.storage.checkpoint_with_conf(index, data, conf)?;
                }
            }
            #[cfg(test)]
            Job::Panic => panic!("injected log worker panic"),
        }
        Ok(Completion {
            id: work.id,
            durable: Durable::read(&self.storage)?,
            fetched,
        })
    }
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
        let task = std::thread::Builder::new()
            .name("raft-log-io".into())
            .spawn(move || {
                while let Some(work) = jobs.blocking_recv() {
                    let mut stopped = shutdown.0.lock().unwrap();
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
