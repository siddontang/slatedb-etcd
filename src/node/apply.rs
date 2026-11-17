use super::*;
use crate::mvcc::Outcome;
use raft::eraftpb::Snapshot;

pub(super) const BATCH_ENTRIES: u64 = 32;
pub(super) const BATCH_BYTES: u64 = 512 * 1024;
pub(super) const MAX_QUERIES: usize = 64;
pub(super) const MAX_APPLY_LAG: u64 = 1024;

pub(super) struct AppliedState {
    pub applied: u64,
    pub durable_applied: u64,
    pub revision: i64,
    pub compact_revision: i64,
    pub leases: BTreeMap<i64, crate::lease::LeaseRecord>,
}

impl From<&StateMachine> for AppliedState {
    fn from(state: &StateMachine) -> Self {
        Self {
            applied: state.applied,
            durable_applied: state.durable_applied,
            revision: state.revision,
            compact_revision: state.compact_revision,
            leases: state.leases.clone(),
        }
    }
}

#[derive(Clone, Copy)]
pub(super) struct Header {
    pub cluster: u64,
    pub member: u64,
    pub term: u64,
}

impl Header {
    pub fn fill(self, header: Option<&mut ResponseHeader>) {
        if let Some(header) = header {
            header.cluster_id = self.cluster;
            header.member_id = self.member;
            header.raft_term = self.term;
        }
    }

    fn outcome(self, outcome: &mut Outcome) {
        if let Ok(response) = &mut outcome.result {
            self.fill(crate::mvcc::response_header(response));
        }
    }
}

pub(super) enum Query {
    Read(RequestOp, Response),
    Linear(RequestOp, Vec<u8>),
    Watch(
        crate::proto::etcdserverpb::WatchCreateRequest,
        bool,
        Option<i64>,
        Response,
    ),
    Local(String, Response),
}

impl Query {
    pub fn reject(self, error: ApiError) {
        let response = match self {
            Self::Linear(..) => unreachable!("linear reads wait for queue capacity"),
            Self::Read(_, response) | Self::Watch(_, _, _, response) | Self::Local(_, response) => {
                response
            }
        };
        let _ = response.send(Err(error));
    }
}

pub(super) enum Job {
    #[cfg(test)]
    Panic,
    Entries(Vec<Entry>),
    Snapshot(Snapshot),
    Query(Query),
    Checkpoint(Duration),
}

pub(super) struct Work {
    pub job: Job,
    pub header: Header,
    pub delay: Duration,
}

pub(super) enum Reply {
    Encoded(Value),
    Lease(Outcome<LeaseReply>),
}

pub(super) struct AppliedCommand {
    pub id: String,
    pub lease: Option<LeaseCommand>,
    pub reply: Reply,
}

pub(super) enum Output {
    Linear {
        id: Vec<u8>,
        value: Value,
    },
    Entries(Vec<AppliedCommand>),
    Snapshot,
    Query {
        response: Response,
        value: Value,
        require_leader: bool,
    },
    Checkpoint,
}

pub(super) struct Completion {
    pub state: AppliedState,
    pub output: Output,
}

struct Executor {
    state: StateMachine,
    events: mpsc::Sender<Event>,
    snapshot_task: Option<tokio::task::JoinHandle<()>>,
}

impl Drop for Executor {
    fn drop(&mut self) {
        if let Some(task) = &self.snapshot_task {
            task.abort();
        }
    }
}

impl Executor {
    async fn execute(&mut self, work: Work) -> Result<Completion> {
        if matches!(work.job, Job::Entries(_) | Job::Snapshot(_)) && !work.delay.is_zero() {
            tokio::time::sleep(work.delay).await;
        }
        let output = match work.job {
            #[cfg(test)]
            Job::Panic => panic!("injected apply worker panic"),
            Job::Entries(entries) => {
                Output::Entries(apply_entries(&mut self.state, entries, work.header).await?)
            }
            Job::Snapshot(snapshot) => {
                self.state.install_snapshot(&snapshot).await?;
                Output::Snapshot
            }
            Job::Query(query) => match query {
                Query::Linear(request, id) => {
                    let mut outcome = self.state.read_etcd(&request).await?;
                    work.header.outcome(&mut outcome);
                    Output::Linear {
                        id,
                        value: serde_json::to_value(outcome)?,
                    }
                }
                Query::Read(request, response) => {
                    let mut outcome = self.state.read_etcd(&request).await?;
                    work.header.outcome(&mut outcome);
                    Output::Query {
                        response,
                        value: serde_json::to_value(outcome)?,
                        require_leader: false,
                    }
                }
                Query::Watch(request, require_leader, through, response) => {
                    let mut page = self.state.watch(&request, through).await?;
                    work.header.fill(page.header.as_mut());
                    for response in &mut page.responses {
                        work.header.fill(response.header.as_mut());
                    }
                    Output::Query {
                        response,
                        value: serde_json::to_value(page)?,
                        require_leader,
                    }
                }
                Query::Local(key, response) => {
                    let value = self.state.local_value(&key).await?;
                    Output::Query {
                        response,
                        value: json!({"value": value,
                        "applied_index": self.state.applied}),
                        require_leader: false,
                    }
                }
            },
            Job::Checkpoint(delay) => {
                let index = self.state.applied;
                let job = self.state.snapshot_job()?;
                let events = self.events.clone();
                self.snapshot_task = Some(tokio::spawn(async move {
                    if !delay.is_zero() {
                        tokio::time::sleep(delay).await;
                    }
                    let result = job.await;
                    let _ = events.send(Event::SnapshotBuilt { index, result }).await;
                }));
                Output::Checkpoint
            }
        };
        Ok(Completion {
            state: AppliedState::from(&self.state),
            output,
        })
    }
}

pub(super) struct Worker {
    work: mpsc::Sender<Work>,
    completed: mpsc::Receiver<Result<Completion>>,
    task: Option<tokio::task::JoinHandle<()>>,
    #[cfg(test)]
    manual: Option<(Executor, mpsc::Receiver<Work>)>,
}

impl Drop for Worker {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

impl Worker {
    pub fn start(state: StateMachine, events: mpsc::Sender<Event>, manual: bool) -> Self {
        let (work, mut jobs) = mpsc::channel::<Work>(1);
        let (done, completed) = mpsc::channel(1);
        let mut executor = Executor {
            state,
            events,
            snapshot_task: None,
        };
        #[cfg(test)]
        if manual {
            return Self {
                work,
                completed,
                task: None,
                manual: Some((executor, jobs)),
            };
        }
        assert!(!manual, "manual apply is only available in tests");
        let task = tokio::spawn(async move {
            while let Some(work) = jobs.recv().await {
                let result = executor.execute(work).await;
                let failed = result.is_err();
                if done.send(result).await.is_err() || failed {
                    break;
                }
            }
        });
        Self {
            work,
            completed,
            task: Some(task),
            #[cfg(test)]
            manual: None,
        }
    }

    pub fn submit(&self, work: Work) -> Result<()> {
        self.work
            .try_send(work)
            .map_err(|_| anyhow::anyhow!("apply worker queue is unavailable"))
    }

    pub async fn next(&mut self) -> Result<Completion> {
        self.completed
            .recv()
            .await
            .context("apply worker stopped")?
    }

    #[cfg(test)]
    pub fn abort_handle(&self) -> tokio::task::AbortHandle {
        self.task.as_ref().unwrap().abort_handle()
    }

    #[cfg(test)]
    pub async fn step(&mut self) -> Result<Option<Completion>> {
        let (executor, jobs) = self.manual.as_mut().context("worker is not manual")?;
        match jobs.try_recv() {
            Ok(work) => executor.execute(work).await.map(Some),
            Err(mpsc::error::TryRecvError::Empty) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    #[cfg(test)]
    pub fn state(&self) -> &StateMachine {
        &self.manual.as_ref().unwrap().0.state
    }
}

async fn apply_entries(
    state: &mut StateMachine,
    entries: Vec<Entry>,
    header: Header,
) -> Result<Vec<AppliedCommand>> {
    let mut replies = vec![];
    for entry in entries {
        ensure!(
            entry.index == state.applied + 1,
            "apply batch has an index gap"
        );
        ensure!(
            entry.get_entry_type() == EntryType::EntryNormal,
            "membership changes are not supported"
        );
        if entry.data.is_empty() {
            state.apply(entry.index, None).await?;
            continue;
        }
        let command: RaftCommand = serde_json::from_slice(&entry.data)?;
        let mut lease_command = None;
        let reply = match &command {
            RaftCommand::Legacy(c) => Reply::Encoded(serde_json::to_value(
                state.apply(entry.index, Some(c)).await?,
            )?),
            RaftCommand::Etcd { etcd, .. } => {
                let mut outcome = state.apply_etcd(entry.index, etcd).await?;
                header.outcome(&mut outcome);
                Reply::Encoded(serde_json::to_value(outcome)?)
            }
            RaftCommand::Lease { lease, .. } => {
                lease_command = Some(lease.clone());
                Reply::Lease(state.apply_lease(entry.index, lease).await?)
            }
            RaftCommand::Compact { compact, .. } => {
                let mut outcome = state.apply_compaction(entry.index, compact).await?;
                if let Ok(response) = &mut outcome.result {
                    header.fill(response.header.as_mut());
                }
                Reply::Encoded(serde_json::to_value(outcome)?)
            }
        };
        replies.push(AppliedCommand {
            id: command.id().to_owned(),
            lease: lease_command,
            reply,
        });
    }
    Ok(replies)
}

// Replay in bounded batches when recovery exceeds the pending-entry quota.
pub(super) async fn recover(
    state: &mut StateMachine,
    storage: &RaftStorage,
    header: Header,
) -> Result<()> {
    while state.applied < storage.commit_index() {
        let low = state.applied + 1;
        let entries = storage.entries(
            low,
            (low + BATCH_ENTRIES).min(storage.commit_index() + 1),
            Some(BATCH_BYTES),
            raft::GetEntriesContext::empty(false),
        )?;
        ensure!(!entries.is_empty(), "empty recovery batch");
        apply_entries(state, entries, header).await?;
    }
    Ok(())
}
