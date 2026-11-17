use super::*;
use std::sync::{Arc, Condvar, Mutex};

use raft::eraftpb::{HardState, Snapshot};
use raft::{GetEntriesContext, RaftState, StorageError};

pub(super) const MAX_JOBS: usize = 64;
pub(super) const MAX_BYTES: usize = 16 * 1024 * 1024;

pub(super) struct Durable {
    pub hard_state: HardState,
    pub last_index: u64,
    snapshot: Snapshot,
    pub sync_writes: u64,
    pub stats: crate::storage::LogStats,
}

impl Durable {
    fn read(storage: &RaftStorage) -> Result<Self> {
        Ok(Self {
            hard_state: storage.initial_state()?.hard_state,
            last_index: storage.last_index()?,
            snapshot: storage.recovery_snapshot().clone(),
            sync_writes: storage.sync_writes,
            stats: storage.log_stats()?,
        })
    }
}

// Only the Raft owner reads this cache. The disk worker never changes it.
pub(super) struct Cache {
    entries: VecDeque<Entry>,
    snapshot: Snapshot,
    hard_state: HardState,
    pub durable: Durable,
}

impl Cache {
    pub fn load(storage: &RaftStorage) -> Result<Self> {
        let durable = Durable::read(storage)?;
        let entries = storage.entries(
            storage.first_index()?,
            storage.last_index()? + 1,
            None,
            GetEntriesContext::empty(false),
        )?;
        Ok(Self {
            entries: entries.into(),
            snapshot: durable.snapshot.clone(),
            hard_state: durable.hard_state.clone(),
            durable,
        })
    }

    pub fn stage(
        &mut self,
        entries: &[Entry],
        hs: Option<&HardState>,
        snapshot: Option<&Snapshot>,
    ) -> Result<()> {
        if let Some(snapshot) = snapshot {
            ensure!(
                snapshot.get_metadata().index > self.snapshot.get_metadata().index,
                "stale cached snapshot"
            );
            self.snapshot = snapshot.clone();
            self.entries.clear();
        }
        if let Some(first) = entries.first() {
            let low = self.first_index()?;
            ensure!(
                first.index >= low && first.index <= self.last_index()? + 1,
                "cached log gap"
            );
            self.entries.truncate((first.index - low) as usize);
            self.entries.extend(entries.iter().cloned());
        }
        if let Some(hs) = hs {
            self.hard_state = hs.clone();
        }
        Ok(())
    }

    pub fn complete(&mut self, durable: Durable) -> Result<()> {
        ensure!(
            durable.hard_state.commit >= self.commit_index(),
            "durable commit regressed"
        );
        if durable.snapshot.get_metadata().index > self.snapshot.get_metadata().index {
            let end = durable.snapshot.get_metadata().index + 1;
            while self.entries.front().is_some_and(|e| e.index < end) {
                self.entries.pop_front();
            }
            self.snapshot = durable.snapshot.clone();
        }
        self.durable = durable;
        Ok(())
    }

    pub fn snapshot_pending(&self) -> bool {
        self.snapshot.get_metadata().index > self.snapshot_index()
    }

    pub fn commit_index(&self) -> u64 {
        self.durable.hard_state.commit
    }
    pub fn snapshot_index(&self) -> u64 {
        self.durable.snapshot.get_metadata().index
    }
    pub fn recovery_snapshot(&self) -> &Snapshot {
        &self.durable.snapshot
    }
}

impl Storage for Cache {
    fn initial_state(&self) -> raft::Result<RaftState> {
        Ok(RaftState {
            hard_state: self.hard_state.clone(),
            conf_state: self.snapshot.get_metadata().get_conf_state().clone(),
        })
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
        _: GetEntriesContext,
    ) -> raft::Result<Vec<Entry>> {
        let first = self.first_index()?;
        if low < first {
            return Err(StorageError::Compacted.into());
        }
        if high > self.last_index()? + 1 || high < low {
            return Err(StorageError::Unavailable.into());
        }
        let mut result = vec![];
        let mut bytes = 0;
        let limit = max_size.into().unwrap_or(u64::MAX);
        for entry in self
            .entries
            .range((low - first) as usize..(high - first) as usize)
        {
            let size = u64::from(entry.compute_size());
            if !result.is_empty() && bytes + size > limit {
                break;
            }
            result.push(entry.clone());
            bytes += size;
        }
        Ok(result)
    }

    fn term(&self, index: u64) -> raft::Result<u64> {
        if index == self.snapshot.get_metadata().index {
            return Ok(self.snapshot.get_metadata().term);
        }
        let first = self.first_index()?;
        if index < first {
            return Err(StorageError::Compacted.into());
        }
        self.entries
            .get((index - first) as usize)
            .map(|e| e.term)
            .ok_or_else(|| StorageError::Unavailable.into())
    }
    fn first_index(&self) -> raft::Result<u64> {
        Ok(self.snapshot.get_metadata().index + 1)
    }
    fn last_index(&self) -> raft::Result<u64> {
        Ok(self
            .entries
            .back()
            .map_or(self.snapshot.get_metadata().index, |e| e.index))
    }
    fn snapshot(&self, request_index: u64, _: u64) -> raft::Result<Snapshot> {
        if self.snapshot_index() == 0 || self.snapshot_index() < request_index {
            return Err(StorageError::SnapshotTemporarilyUnavailable.into());
        }
        Ok(self.durable.snapshot.clone())
    }
}

pub(super) enum Job {
    Ready {
        entries: Vec<Entry>,
        hs: Option<HardState>,
        snapshot: Option<Snapshot>,
    },
    Checkpoint {
        index: u64,
        data: Vec<u8>,
    },
    #[cfg(test)]
    Panic,
}

impl Job {
    pub fn bytes(&self) -> usize {
        match self {
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
            _ => true,
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
        match work.job {
            Job::Ready {
                entries,
                hs,
                snapshot,
            } => self
                .storage
                .persist(&entries, hs.as_ref(), snapshot.as_ref())?,
            Job::Checkpoint { index, data } => {
                if index > self.storage.snapshot_index() {
                    self.storage.checkpoint(index, data)?;
                }
            }
            #[cfg(test)]
            Job::Panic => panic!("injected log worker panic"),
        }
        Ok(Completion {
            id: work.id,
            durable: Durable::read(&self.storage)?,
        })
    }
}

pub(super) struct Worker {
    work: mpsc::Sender<Work>,
    done: mpsc::Receiver<Result<Completion>>,
    stop: Arc<(Mutex<bool>, Condvar)>,
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
                manual: Some((executor, jobs)),
            });
        }
        assert!(!manual);
        let shutdown = Arc::clone(&stop);
        std::thread::Builder::new()
            .name("raft-log-io".into())
            .spawn(move || {
                while let Some(work) = jobs.blocking_recv() {
                    let mut stopped = shutdown.0.lock().unwrap();
                    if work.job.writes() && !work.delay.is_zero() {
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
            #[cfg(test)]
            manual: None,
        })
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
    pub fn step(&mut self) -> Result<Option<Completion>> {
        let (executor, jobs) = self.manual.as_mut().context("log worker is not manual")?;
        match jobs.try_recv() {
            Ok(work) => executor.execute(work).map(Some),
            Err(mpsc::error::TryRecvError::Empty) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }
}
