use super::*;
use crate::mvcc::Outcome;
use raft::eraftpb::Snapshot;

pub(super) const BATCH_ENTRIES: u64 = 32;
/// Committed-entry jobs that may wait in the apply queue behind the running job.
pub(super) const PIPELINE_DEPTH: usize = 4;
/// Committed entries in one apply job. A larger job shares the fixed cost of a state write.
pub(super) const APPLY_ENTRIES: u64 = 64;
pub(super) const BATCH_BYTES: u64 = 512 * 1024;
pub(super) const MAX_QUERIES: usize = 64;
pub(super) const READ_BATCH: usize = 16;
pub(super) const MAX_APPLY_LAG: u64 = 1024;

pub(super) struct AppliedState {
    pub applied: u64,
    pub durable_applied: u64,
    pub members: std::sync::Arc<crate::cluster::Store>,
    pub read: std::sync::Arc<crate::state::ReadView>,
    pub write_batches: u64,
    pub logical_size: u64,
    /// Watch events that this state publishes after the node installs it.
    pub watch: Option<std::sync::Arc<crate::watch::Batch>>,
    pub compaction_pending: bool,
    pub physical_revision: i64,
}

impl std::ops::Deref for AppliedState {
    type Target = crate::state::ReadView;
    fn deref(&self) -> &Self::Target {
        &self.read
    }
}

impl AppliedState {
    pub fn new(state: &StateMachine) -> Result<Self> {
        Ok(Self {
            applied: state.applied,
            durable_applied: state.durable_applied,
            members: state.members.clone(),
            read: state.read_view()?,
            write_batches: state.write_batches,
            logical_size: state.logical_size(),
            watch: None,
            compaction_pending: state.compaction_pending(),
            physical_revision: state.physical_revision(),
        })
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

    pub(super) fn outcome(self, outcome: &mut Outcome) {
        if let Ok(response) = &mut outcome.result {
            self.fill(crate::mvcc::response_header(response));
        }
    }
}

pub(super) enum Query {
    AuthLookup(String, Response),
    Maintenance(
        crate::maintenance::Command,
        crate::auth::Credentials,
        Box<crate::proto::etcdserverpb::StatusResponse>,
        Response,
    ),
    Read(RequestOp, Option<crate::auth::Credentials>, ClientResponse),
    Linear(RequestOp, Option<crate::auth::Credentials>, u64),
    Local(String, Response),
}

impl Query {
    pub fn same_linear_read(&self, other: &Self) -> bool {
        matches!((self, other), (Self::Linear(a, ca, _), Self::Linear(b, cb, _)) if ca == cb && a == b)
    }

    /// Return true for a current Range that streams rows and can run in a shared scan slot.
    pub fn is_streaming_range(&self) -> bool {
        let request = match self {
            Self::Read(request, ..) | Self::Linear(request, ..) => request,
            _ => return false,
        };
        matches!(
            &request.request,
            Some(crate::proto::etcdserverpb::request_op::Request::RequestRange(range))
                if crate::mvcc::streams_range(range)
        )
    }

    pub fn is_point_read(&self) -> bool {
        let request = match self {
            Self::Read(request, ..) | Self::Linear(request, ..) => request,
            _ => return false,
        };
        matches!(
            &request.request,
            Some(crate::proto::etcdserverpb::request_op::Request::RequestRange(range))
                if range.range_end.is_empty() && range.revision <= 0
        )
    }

    pub fn reject(self, error: ApiError) {
        let response = match self {
            Self::Linear(..) => unreachable!("linear reads wait for queue capacity"),
            Self::Read(_, _, response) => {
                response.error(error);
                return;
            }
            Self::Maintenance(_, _, _, response)
            | Self::AuthLookup(_, response)
            | Self::Local(_, response) => response,
        };
        let _ = response.send(Err(error));
    }
}

pub(super) enum Job {
    #[cfg(test)]
    Panic,
    Entries(Vec<Entry>),
    PreparePuts(Vec<(u64, RaftCommand)>),
    PublishPuts(crate::state::put_batch::PreparedPuts),
    Snapshot(Snapshot),
    Query(Query),
    /// Flush the applied prefix. Build a full image only when `image` is set.
    Checkpoint {
        delay: Duration,
        dir: PathBuf,
        image: bool,
    },
}

pub(super) struct Work {
    pub job: Job,
    pub header: Header,
    pub delay: Duration,
}

pub(super) enum Reply {
    Encoded(Value),
    /// Physical Compact replies after a flush through `index` makes `revision` physical.
    PhysicalCompact {
        index: u64,
        revision: i64,
        value: Value,
        /// The physical boundary when the pass already covers `revision`.
        flush: Option<i64>,
    },
    /// A step finished a physical target. Flush through `index` and publish `revision`.
    CompactFlush {
        index: u64,
        revision: i64,
    },
    Kv(Outcome),
    Lease(Outcome<LeaseReply>),
}

pub(super) struct AppliedCommand {
    pub id: String,
    pub change: Option<raft::eraftpb::ConfChange>,
    pub lease: Option<LeaseCommand>,
    pub reply: Reply,
}

pub(super) enum Output {
    Reads(Vec<Output>),
    SharedLinear(Vec<u64>, std::sync::Arc<Outcome>),
    Linear {
        id: u64,
        outcome: Outcome,
    },
    Read {
        response: ClientResponse,
        outcome: Outcome,
    },
    Entries(Vec<AppliedCommand>),
    PreparedPuts(crate::state::put_batch::PreparedPuts, Vec<AppliedCommand>),
    Snapshot,
    Query {
        response: Response,
        value: Value,
        require_leader: bool,
    },
    /// A test worker returns its checkpoint result inline to keep schedules deterministic.
    Checkpoint(Option<Built>),
}

pub struct Built {
    pub(crate) index: u64,
    pub(crate) conf: raft::eraftpb::ConfState,
    pub(crate) result: Result<Option<PreparedCheckpoint>>,
}

pub(super) struct Completion {
    pub applied: u64,
    pub state: Option<AppliedState>,
    pub output: Output,
}

struct Executor {
    state: StateMachine,
    events: mpsc::Sender<Event>,
    snapshot_task: Option<tokio::task::JoinHandle<()>>,
    inline_checkpoints: bool,
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
        #[cfg(feature = "write-profile")]
        match &work.job {
            Job::Entries(entries) => crate::write_profile::Stage::Apply.batch(entries.len()),
            Job::PublishPuts(batch) => {
                crate::write_profile::Stage::Apply.batch((batch.last - self.state.applied) as usize)
            }
            _ => {}
        }
        let changes_state = matches!(
            work.job,
            Job::Entries(_) | Job::Snapshot(_) | Job::PublishPuts(_)
        );
        let copy_state = changes_state || matches!(work.job, Job::Query(Query::Maintenance(..)));
        let previous_revision = self.state.revision;
        if (changes_state || matches!(work.job, Job::PreparePuts(_))) && !work.delay.is_zero() {
            tokio::time::sleep(work.delay).await;
        }
        let output = match work.job {
            #[cfg(test)]
            Job::Panic => panic!("injected apply worker panic"),
            Job::Entries(entries) => {
                Output::Entries(apply_entries(&mut self.state, entries, work.header).await?)
            }
            Job::PreparePuts(commands) => {
                let (batch, replies) =
                    super::quorum_put::prepare(&self.state, &commands, work.header).await?;
                Output::PreparedPuts(batch, replies)
            }
            Job::PublishPuts(batch) => {
                self.state.publish_puts(batch).await?;
                Output::Entries(Vec::new())
            }
            Job::Snapshot(snapshot) => {
                self.state.install_snapshot(&snapshot).await?;
                Output::Snapshot
            }
            Job::Query(query) => match query {
                Query::Maintenance(command, credentials, status, response) => {
                    let mut outcome = self
                        .state
                        .maintenance(&command, &credentials, *status)
                        .await?;
                    fill_value_header(work.header, &mut outcome)?;
                    Output::Query {
                        response,
                        value: serde_json::to_value(outcome)?,
                        require_leader: false,
                    }
                }
                Query::AuthLookup(name, response) => Output::Query {
                    response,
                    value: serde_json::to_value(self.state.auth.users.get(&name))?,
                    require_leader: false,
                },
                Query::Linear(..) | Query::Read(..) => {
                    anyhow::bail!("read reached the apply worker")
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
            Job::Checkpoint { delay, dir, image } => {
                let index = self.state.applied;
                let conf = self.state.members.conf();
                let job = self.state.checkpoint_job(image)?;
                let build = async move {
                    if !delay.is_zero() {
                        tokio::time::sleep(delay).await;
                    }
                    let result = async {
                        let Some(data) = job.await? else {
                            return Ok(None);
                        };
                        tokio::task::spawn_blocking(move || {
                            PreparedCheckpoint::write(&dir, data.into()).map(Some)
                        })
                        .await?
                    }
                    .await;
                    Built {
                        index,
                        conf,
                        result,
                    }
                };
                if self.inline_checkpoints {
                    Output::Checkpoint(Some(build.await))
                } else {
                    let events = self.events.clone();
                    self.snapshot_task = Some(tokio::spawn(async move {
                        let _ = events.send(Event::SnapshotBuilt(build.await)).await;
                    }));
                    Output::Checkpoint(None)
                }
            }
        };
        let state = match copy_state {
            true => {
                let mut state = AppliedState::new(&self.state)?;
                state.watch = self
                    .state
                    .take_watch_batch(previous_revision)
                    .map(std::sync::Arc::new);
                Some(state)
            }
            false => None,
        };
        Ok(Completion {
            applied: self.state.applied,
            state,
            output,
        })
    }
}

pub(super) struct Worker {
    database: std::sync::Arc<slatedb::Db>,
    work: mpsc::Sender<Work>,
    completed: mpsc::Receiver<Result<Completion>>,
    task: Option<tokio::task::JoinHandle<Result<()>>>,
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
    pub fn start(mut state: StateMachine, events: mpsc::Sender<Event>, manual: bool) -> Self {
        state.watch_capture = Some(Default::default());
        let (work, mut jobs) = mpsc::channel::<Work>(PIPELINE_DEPTH);
        let (done, completed) = mpsc::channel(PIPELINE_DEPTH);
        let database = state.db.clone();
        let mut executor = Executor {
            state,
            events,
            snapshot_task: None,
            inline_checkpoints: manual,
        };
        #[cfg(test)]
        if manual {
            return Self {
                database,
                work,
                completed,
                task: None,
                manual: Some((executor, jobs)),
            };
        }
        assert!(!manual, "manual apply is only available in tests");
        let worker = async move {
            while let Some(work) = jobs.recv().await {
                let result = executor.execute(work).await;
                let failed = result.is_err();
                if done.send(result).await.is_err() || failed {
                    break;
                }
            }
            if let Some(task) = executor.snapshot_task.take() {
                task.abort();
                let _ = task.await;
            }
            executor.state.close().await
        };
        #[cfg(feature = "write-profile")]
        let worker = async move {
            crate::write_profile::Probe::new(crate::write_profile::Stage::Apply)
                .run(worker)
                .await
        };
        let task = tokio::spawn(worker);
        Self {
            database,
            work,
            completed,
            task: Some(task),
            #[cfg(test)]
            manual: None,
        }
    }

    pub async fn shutdown(&mut self) -> Result<()> {
        self.completed.close();
        self.work = mpsc::channel(1).0;
        if let Some(task) = self.task.take() {
            match task.await {
                Ok(result) => result?,
                Err(error) => {
                    let _ = self
                        .database
                        .close_with_options(slatedb::config::CloseOptions { flush_type: None })
                        .await;
                    return Err(error.into());
                }
            }
        }
        Ok(())
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
    pub fn database(&self) -> std::sync::Arc<slatedb::Db> {
        self.database.clone()
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
    let mut entries = entries.into_iter().peekable();
    while let Some(entry) = entries.next() {
        ensure!(
            entry.index == state.applied + 1,
            "apply batch has an index gap"
        );
        let kind = EntryType::try_from(entry.entry_type)?;
        if entry.data.is_empty() {
            state.apply(entry.index, None).await?;
            continue;
        }
        let change = match kind {
            EntryType::EntryNormal => None,
            EntryType::EntryConfChange => {
                let change = raft::eraftpb::ConfChange::decode(entry.data.clone())?;
                raft::eraftpb::ConfChangeType::try_from(change.change_type)?;
                Some(change)
            }
            EntryType::EntryConfChangeV2 => anyhow::bail!("unsupported joint configuration entry"),
        };
        let command = RaftCommand::decode(
            change
                .as_ref()
                .map_or(entry.data.as_ref(), |change| change.context.as_ref()),
        )?;
        if change.is_none() && super::quorum_put::put_command(&command).is_some() {
            let mut commands = vec![(entry.index, command)];
            while let Some(next) = entries.peek() {
                if next.entry_type != EntryType::EntryNormal as i32 || next.data.is_empty() {
                    break;
                }
                let command = RaftCommand::decode(&next.data)?;
                if super::quorum_put::put_command(&command).is_none() {
                    break;
                }
                commands.push((entries.next().unwrap().index, command));
            }
            let (batch, outcomes) = super::quorum_put::prepare(state, &commands, header).await?;
            state.publish_puts(batch).await?;
            replies.extend(outcomes);
            continue;
        }
        let id = command.id().to_owned();
        let command = if let RaftCommand::Secured {
            credentials,
            command,
            ..
        } = &command
        {
            if let Err(error) = state.authorize(credentials, command).await {
                state.apply(entry.index, None).await?;
                replies.push(AppliedCommand {
                    id,
                    change: None,
                    lease: None,
                    reply: Reply::Encoded(serde_json::to_value(Outcome::<Value> {
                        result: Err(error),
                    })?),
                });
                continue;
            }
            command.as_ref()
        } else {
            &command
        };
        let mut lease_command = None;
        let mut change = change;
        crate::failpoint::hit("apply-before-entry");
        let reply = match command {
            RaftCommand::Secured { .. } => anyhow::bail!("nested secured command"),
            RaftCommand::Cluster {
                request,
                credentials,
                ..
            } => {
                let mut outcome = state
                    .apply_cluster(entry.index, request, credentials)
                    .await?;
                if outcome.result.is_err() {
                    if let Some(change) = &mut change {
                        change.node_id = 0;
                    }
                }
                fill_value_header(header, &mut outcome)?;
                Reply::Encoded(serde_json::to_value(outcome)?)
            }
            RaftCommand::Maintenance {
                request,
                credentials,
                ..
            } => {
                let mut outcome = state
                    .apply_maintenance(entry.index, request, credentials)
                    .await?;
                fill_value_header(header, &mut outcome)?;
                Reply::Encoded(serde_json::to_value(outcome)?)
            }
            RaftCommand::Auth {
                auth, credentials, ..
            } => {
                let mut outcome = state.apply_auth(entry.index, credentials, auth).await?;
                if let Ok(value) = &mut outcome.result {
                    let mut h: ResponseHeader = serde_json::from_value(value["header"].clone())?;
                    header.fill(Some(&mut h));
                    value["header"] = serde_json::to_value(h)?;
                }
                Reply::Encoded(serde_json::to_value(outcome)?)
            }
            RaftCommand::Diagnostic(c) => Reply::Encoded(serde_json::to_value(
                state.apply(entry.index, Some(c)).await?,
            )?),
            RaftCommand::Etcd { etcd, origin, .. } => {
                let mut outcome = state.apply_etcd_quota(entry.index, etcd, *origin).await?;
                header.outcome(&mut outcome);
                Reply::Kv(outcome)
            }
            RaftCommand::Lease { lease, origin, .. } => {
                lease_command = Some(lease.clone());
                Reply::Lease(state.apply_lease_from(entry.index, lease, *origin).await?)
            }
            RaftCommand::Compact { compact, .. } => {
                let mut outcome = state.apply_compaction(entry.index, compact).await?;
                if let Ok(response) = &mut outcome.result {
                    header.fill(response.header.as_mut());
                }
                let wait = state.physical_compact_revision(compact, &outcome);
                let value = serde_json::to_value(outcome)?;
                if let Some(revision) = wait {
                    let physical = state.physical_revision();
                    Reply::PhysicalCompact {
                        index: entry.index,
                        revision,
                        value,
                        flush: (physical >= revision).then_some(physical),
                    }
                } else {
                    Reply::Encoded(value)
                }
            }
            RaftCommand::CompactStep { .. } => {
                match state.apply_compaction_step(entry.index).await? {
                    Some(revision) => Reply::CompactFlush {
                        index: entry.index,
                        revision,
                    },
                    None => Reply::Encoded(Value::Null),
                }
            }
        };
        crate::failpoint::hit("apply-after-entry");
        replies.push(AppliedCommand {
            id: command.id().to_owned(),
            change,
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
    let mut physical = false;
    while state.applied < storage.commit_index() {
        let low = state.applied + 1;
        let entries = storage.entries(
            low,
            (low + BATCH_ENTRIES).min(storage.commit_index() + 1),
            Some(BATCH_BYTES),
            raft::GetEntriesContext::empty(false),
        )?;
        ensure!(!entries.is_empty(), "empty recovery batch");
        let replies = apply_entries(state, entries, header).await?;
        physical |= replies.iter().any(|r| {
            matches!(
                r.reply,
                Reply::PhysicalCompact { flush: Some(_), .. } | Reply::CompactFlush { .. }
            )
        });
    }
    if physical {
        state.metrics.flush_db(&state.db).await?;
        state.durable_applied = state.applied;
    }
    Ok(())
}

fn fill_value_header(header: Header, outcome: &mut Outcome<Value>) -> Result<()> {
    if let Ok(value) = &mut outcome.result {
        let mut h: ResponseHeader = serde_json::from_value(value["header"].clone())?;
        header.fill(Some(&mut h));
        value["header"] = serde_json::to_value(h)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::etcdserverpb::request_op::Request as Op;
    use crate::proto::etcdserverpb::response_op::Response as Resp;
    use crate::proto::etcdserverpb::*;

    #[tokio::test]
    async fn put_groups_stop_at_transactions_leases_and_empty_entries() {
        let directory = tempfile::tempdir().unwrap();
        let mut state = StateMachine::open(directory.path()).await.unwrap();
        let put = |key: &[u8], value: &[u8], lease| RequestOp {
            request: Some(Op::RequestPut(PutRequest {
                key: key.to_vec(),
                value: value.to_vec(),
                lease,
                ..Default::default()
            })),
        };
        let kv = |id: &str, etcd| RaftCommand::Etcd {
            id: id.into(),
            etcd,
            origin: 1,
        };
        let commands = [
            kv("one", put(b"a", b"one", 0)),
            kv("two", put(b"a", b"two", 0)),
            kv(
                "txn",
                RequestOp {
                    request: Some(Op::RequestTxn(TxnRequest {
                        compare: vec![Compare {
                            key: b"a".to_vec(),
                            target: 3,
                            target_union: Some(compare::TargetUnion::Value(b"two".to_vec())),
                            ..Default::default()
                        }],
                        success: vec![put(b"b", b"observed", 0)],
                        ..Default::default()
                    })),
                },
            ),
            RaftCommand::Lease {
                id: "lease".into(),
                lease: LeaseCommand::Grant { id: 77, ttl: 60 },
                origin: 1,
            },
            kv("leased", put(b"a", b"leased", 77)),
            kv("detach", put(b"a", b"detached", 0)),
            kv("last", put(b"c", b"last", 0)),
        ];
        let mut entries: Vec<_> = commands
            .iter()
            .enumerate()
            .map(|(offset, command)| Entry {
                index: offset as u64 + 1,
                term: 1,
                data: command.encode().unwrap().into(),
                ..Default::default()
            })
            .collect();
        entries.push(Entry {
            index: 8,
            term: 1,
            ..Default::default()
        });
        let replies = apply_entries(
            &mut state,
            entries,
            Header {
                cluster: 1,
                member: 1,
                term: 1,
            },
        )
        .await
        .unwrap();
        assert_eq!(replies.len(), 7);
        assert_eq!(state.applied, 8);
        assert_eq!(state.write_batches, 6);
        let Reply::Kv(outcome) = &replies[2].reply else {
            panic!("expected transaction reply");
        };
        let Some(Resp::ResponseTxn(txn)) = &outcome.result.as_ref().unwrap().response else {
            panic!("expected transaction");
        };
        assert!(txn.succeeded);
        let read = RequestOp {
            request: Some(Op::RequestRange(RangeRequest {
                key: b"a".to_vec(),
                range_end: b"z".to_vec(),
                ..Default::default()
            })),
        };
        let Some(Resp::ResponseRange(result)) = state
            .read_etcd(&read)
            .await
            .unwrap()
            .result
            .unwrap()
            .response
        else {
            panic!("expected range");
        };
        assert_eq!(
            result
                .kvs
                .iter()
                .map(|kv| kv.value.as_slice())
                .collect::<Vec<_>>(),
            vec![b"detached".as_slice(), b"observed", b"last"]
        );
        assert_eq!(result.header.unwrap().revision, 7);
        state.close().await.unwrap();
    }
}
