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
    /// The background read generation that the next compaction step needs.
    pub compaction_plan: u64,
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
            compaction_plan: state.compaction_plan.generation(),
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
    /// Evaluate committed Puts of one term before their state write.
    PreparePuts {
        commands: Vec<(u64, RaftCommand)>,
        term: u64,
    },
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
    Snapshot(Rebound),
    Query {
        response: Response,
        value: Value,
        require_leader: bool,
    },
    /// A test worker returns its checkpoint result inline to keep schedules deterministic.
    Checkpoint(Option<Built>),
}

/// The database handles after a snapshot install.
pub struct Rebound {
    pub db: std::sync::Arc<slatedb::Db>,
    pub admin: std::sync::Arc<slatedb::admin::Admin>,
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
        if (changes_state || matches!(work.job, Job::PreparePuts { .. })) && !work.delay.is_zero() {
            tokio::time::sleep(work.delay).await;
        }
        let output = match work.job {
            #[cfg(test)]
            Job::Panic => panic!("injected apply worker panic"),
            Job::Entries(entries) => {
                Output::Entries(apply_entries(&mut self.state, entries, work.header).await?)
            }
            Job::PreparePuts { commands, term } => {
                let (batch, replies) =
                    super::quorum_put::prepare(&self.state, &commands, term, work.header).await?;
                Output::PreparedPuts(batch, replies)
            }
            Job::PublishPuts(batch) => {
                // The clients already have their replies. Recovery must replay the same outcome.
                crate::failpoint::hit("apply-after-early-reply");
                self.state.publish_puts(batch).await?;
                Output::Entries(Vec::new())
            }
            Job::Snapshot(snapshot) => {
                self.state.install_snapshot(&snapshot).await?;
                // An object snapshot replaces the database. The node rebinds its handles.
                Output::Snapshot(Rebound {
                    db: self.state.db.clone(),
                    admin: self.state.admin.clone(),
                })
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
                let job = self.state.checkpoint_job(image).await?;
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
        let completion = self
            .completed
            .recv()
            .await
            .context("apply worker stopped")??;
        if let Output::Snapshot(rebound) = &completion.output {
            self.database = rebound.db.clone();
        }
        Ok(completion)
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
            Ok(work) => {
                let completion = executor.execute(work).await?;
                if let Output::Snapshot(rebound) = &completion.output {
                    self.database = rebound.db.clone();
                }
                Ok(Some(completion))
            }
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
        // Each state batch stores the term of its last entry beside `meta/applied`.
        state.entry_term = entry.term;
        if entry.data.is_empty() {
            state.apply(entry.index, None).await?;
            continue;
        }
        let change = match kind {
            EntryType::EntryNormal => None,
            EntryType::EntryConfChange => {
                let change = raft::eraftpb::ConfChange::decode(entry.data.as_slice())?;
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
        if change.is_none() && super::quorum_put::kv_command(&command).is_some() {
            let mut commands = vec![(entry.index, command)];
            let mut term = entry.term;
            // Later members read earlier group writes through a point-read overlay only.
            while let Some(next) = entries.peek() {
                if next.entry_type != EntryType::EntryNormal as i32 || next.data.is_empty() {
                    break;
                }
                let command = RaftCommand::decode(&next.data)?;
                match super::quorum_put::kv_command(&command) {
                    Some((request, credentials, _))
                        if !crate::mvcc::needs_scan(request, credentials.is_some()) => {}
                    _ => break,
                }
                let next = entries.next().unwrap();
                term = next.term;
                commands.push((next.index, command));
            }
            let (batch, outcomes) =
                super::quorum_put::prepare(state, &commands, term, header).await?;
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
    async fn kv_groups_stop_at_leases_and_empty_entries() {
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
                data: command.encode().unwrap(),
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
        assert_eq!(state.write_batches, 4);
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

    /// Return the stored applied term. The row must hold the applied index, and the in-memory
    /// term must match it.
    async fn stored_term(state: &StateMachine) -> u64 {
        let row = state
            .db
            .get(crate::state::APPLIED_TERM)
            .await
            .unwrap()
            .unwrap();
        let term = u64::from_be_bytes(row[8..].try_into().unwrap());
        assert_eq!(
            row.as_ref(),
            crate::state::applied_term_row(state.applied, term)
        );
        assert_eq!(term, state.applied_term);
        term
    }

    #[tokio::test]
    async fn every_state_batch_stores_the_term_of_its_last_entry() {
        use crate::proto::authpb::UserAddOptions;
        let directory = tempfile::tempdir().unwrap();
        let mut state = StateMachine::open(directory.path()).await.unwrap();
        assert_eq!(state.applied_term, 0);
        let credentials = crate::auth::Credentials::default;
        let etcd = |id: &str, etcd| {
            Some(RaftCommand::Etcd {
                id: id.into(),
                origin: 1,
                etcd,
            })
        };
        // Each group applies as one state batch. A group of two puts uses the term of the last.
        let groups = vec![
            ("empty entry", vec![None]),
            (
                "grouped puts",
                vec![etcd("a", put_op(b"a", b"1")), etcd("b", put_op(b"b", b"2"))],
            ),
            ("range", vec![etcd("range", range_op(b"a", b"z", 0))]),
            (
                "lease",
                vec![Some(RaftCommand::Lease {
                    id: "lease".into(),
                    origin: 1,
                    lease: LeaseCommand::Grant { id: 5, ttl: 60 },
                })],
            ),
            (
                "auth",
                vec![Some(RaftCommand::Auth {
                    id: "auth".into(),
                    auth: crate::auth::Command::UserAdd(AuthUserAddRequest {
                        name: "user".into(),
                        options: Some(UserAddOptions { no_password: true }),
                        ..Default::default()
                    }),
                    credentials: credentials(),
                })],
            ),
            (
                "cluster",
                vec![Some(RaftCommand::Cluster {
                    id: "cluster".into(),
                    request: crate::cluster::Command::List(MemberListRequest::default()),
                    credentials: credentials(),
                })],
            ),
            (
                "maintenance",
                vec![Some(RaftCommand::Maintenance {
                    id: "alarm".into(),
                    request: crate::maintenance::Command::Alarm(AlarmRequest::default()),
                    credentials: credentials(),
                })],
            ),
            (
                "diagnostic",
                vec![Some(RaftCommand::Diagnostic(crate::Command {
                    id: "diagnostic".into(),
                    request: crate::Request {
                        operations: vec![crate::Operation::Put {
                            key: "k".into(),
                            value: "v".into(),
                        }],
                    },
                }))],
            ),
            (
                "compaction",
                vec![Some(RaftCommand::Compact {
                    id: "compact".into(),
                    compact: CompactionRequest {
                        revision: 2,
                        physical: false,
                    },
                })],
            ),
            (
                "compaction step",
                vec![Some(RaftCommand::CompactStep { id: "step".into() })],
            ),
        ];
        let header = Header {
            cluster: 1,
            member: 1,
            term: 1,
        };
        let (mut index, mut term) = (0, 0);
        for (name, commands) in groups {
            let batches = state.write_batches;
            let entries = commands
                .iter()
                .map(|command| {
                    index += 1;
                    term += 3;
                    Entry {
                        index,
                        term,
                        data: command.as_ref().map_or(vec![], |c| c.encode().unwrap()),
                        ..Default::default()
                    }
                })
                .collect();
            apply_entries(&mut state, entries, header).await.unwrap();
            assert_eq!(state.write_batches, batches + 1, "{name}");
            assert_eq!(
                (state.applied, stored_term(&state).await),
                (index, term),
                "{name}"
            );
        }
        state.db.flush().await.unwrap();
        state.close().await.unwrap();
        drop(state);
        let state = StateMachine::open(directory.path()).await.unwrap();
        assert_eq!((state.applied, state.applied_term), (index, term));
        state.close().await.unwrap();
    }

    fn put_op(key: &[u8], value: &[u8]) -> RequestOp {
        RequestOp {
            request: Some(Op::RequestPut(PutRequest {
                key: key.to_vec(),
                value: value.to_vec(),
                prev_kv: true,
                ..Default::default()
            })),
        }
    }

    fn range_op(key: &[u8], end: &[u8], revision: i64) -> RequestOp {
        RequestOp {
            request: Some(Op::RequestRange(RangeRequest {
                key: key.to_vec(),
                range_end: end.to_vec(),
                revision,
                ..Default::default()
            })),
        }
    }

    fn delete_op(key: &[u8], end: &[u8]) -> RequestOp {
        RequestOp {
            request: Some(Op::RequestDeleteRange(DeleteRangeRequest {
                key: key.to_vec(),
                range_end: end.to_vec(),
                prev_kv: true,
            })),
        }
    }

    fn txn_op(
        compare: Vec<Compare>,
        success: Vec<RequestOp>,
        failure: Vec<RequestOp>,
    ) -> RequestOp {
        RequestOp {
            request: Some(Op::RequestTxn(TxnRequest {
                compare,
                success,
                failure,
            })),
        }
    }

    fn compare(key: &[u8], target: i32, value: compare::TargetUnion) -> Compare {
        Compare {
            key: key.to_vec(),
            target,
            target_union: Some(value),
            ..Default::default()
        }
    }

    async fn rows(state: &StateMachine) -> Vec<(bytes::Bytes, bytes::Bytes)> {
        let mut scan = state.db.scan_prefix(b"", ..).await.unwrap();
        let mut rows = vec![];
        while let Some(row) = scan.next().await.unwrap() {
            rows.push((row.key, row.value));
        }
        rows
    }

    #[tokio::test]
    async fn grouped_transactions_match_sequential_apply() {
        use compare::TargetUnion as T;
        let leased = RequestOp {
            request: Some(Op::RequestPut(PutRequest {
                key: b"l".to_vec(),
                value: b"leased".to_vec(),
                lease: 5,
                ..Default::default()
            })),
        };
        let keep_value = RequestOp {
            request: Some(Op::RequestPut(PutRequest {
                key: b"e".to_vec(),
                ignore_value: true,
                prev_kv: true,
                ..Default::default()
            })),
        };
        let missing_lease = RequestOp {
            request: Some(Op::RequestPut(PutRequest {
                key: b"m".to_vec(),
                value: b"m".to_vec(),
                lease: 99,
                ..Default::default()
            })),
        };
        let requests = vec![
            put_op(b"a", b"1"),
            // The compare sees the put from the same group.
            txn_op(
                vec![compare(b"a", 3, T::Value(b"1".to_vec()))],
                vec![
                    put_op(b"a", b"2"),
                    put_op(b"b", b"x"),
                    range_op(b"a", b"", 0),
                ],
                vec![put_op(b"c", b"no")],
            ),
            txn_op(
                vec![compare(b"a", 2, T::ModRevision(2))],
                vec![put_op(b"c", b"no")],
                vec![range_op(b"a", b"", 0), delete_op(b"b", b"")],
            ),
            txn_op(
                vec![compare(b"b", 0, T::Version(0))],
                vec![put_op(b"b", b"again"), range_op(b"b", b"", 0)],
                vec![],
            ),
            put_op(b"a", b"3"),
            txn_op(vec![], vec![put_op(b"a", b"4")], vec![]),
            // Range reads start a new group.
            txn_op(
                vec![compare(b"a", 1, T::CreateRevision(0))],
                vec![range_op(b"a", b"z", 0), put_op(b"d", b"1")],
                vec![],
            ),
            put_op(b"e", b"5"),
            txn_op(vec![], vec![delete_op(b"a", b"c")], vec![]),
            txn_op(vec![], vec![put_op(b"x", b"1"), put_op(b"x", b"2")], vec![]),
            txn_op(vec![], vec![missing_lease], vec![]),
            delete_op(b"d", b""),
            txn_op(vec![], vec![range_op(b"a", b"", 2)], vec![]),
            txn_op(
                vec![compare(b"e", 3, T::Value(b"5".to_vec()))],
                vec![txn_op(
                    vec![compare(b"d", 0, T::Version(0))],
                    vec![put_op(b"f", b"nested")],
                    vec![],
                )],
                vec![],
            ),
            keep_value,
            leased,
            txn_op(vec![], vec![range_op(b"a", b"", 0)], vec![]),
            txn_op(
                vec![compare(b"l", 4, T::Lease(5))],
                vec![put_op(b"l", b"kept")],
                vec![],
            ),
        ];
        let header = Header {
            cluster: 1,
            member: 1,
            term: 1,
        };
        let grant = LeaseCommand::Grant { id: 5, ttl: 60 };
        for quota in [0, 1, 900, 1500, 100000] {
            let left = tempfile::tempdir().unwrap();
            let right = tempfile::tempdir().unwrap();
            let mut grouped = StateMachine::open(left.path()).await.unwrap();
            let mut separate = StateMachine::open(right.path()).await.unwrap();
            // Grouped apply stores the entry term. Direct apply calls store `entry_term`.
            separate.entry_term = 1;
            grouped.quota = quota;
            separate.quota = quota;
            let mut commands = vec![RaftCommand::Lease {
                id: "grant".into(),
                lease: grant.clone(),
                origin: 1,
            }];
            commands.extend(
                requests
                    .iter()
                    .enumerate()
                    .map(|(i, etcd)| RaftCommand::Etcd {
                        id: format!("r{i}"),
                        etcd: etcd.clone(),
                        origin: 1,
                    }),
            );
            let entries: Vec<_> = commands
                .iter()
                .enumerate()
                .map(|(offset, command)| Entry {
                    index: offset as u64 + 1,
                    term: 1,
                    data: command.encode().unwrap(),
                    ..Default::default()
                })
                .collect();
            let replies = apply_entries(&mut grouped, entries, header).await.unwrap();
            assert_eq!(replies.len(), commands.len());
            separate.apply_lease_from(1, &grant, 1).await.unwrap();
            for (offset, (reply, request)) in replies[1..].iter().zip(&requests).enumerate() {
                let mut expected = separate
                    .apply_etcd_quota(offset as u64 + 2, request, 1)
                    .await
                    .unwrap();
                header.outcome(&mut expected);
                let Reply::Kv(outcome) = &reply.reply else {
                    panic!("expected a KV reply");
                };
                assert_eq!(
                    serde_json::to_value(outcome).unwrap(),
                    serde_json::to_value(&expected).unwrap(),
                    "quota {quota}, request {offset}"
                );
            }
            // Grant, then groups starting at requests 0, 6, 8, and 12.
            assert_eq!(grouped.write_batches, 5, "quota {quota}");
            assert_eq!(rows(&grouped).await, rows(&separate).await, "quota {quota}");
            assert_eq!(
                (grouped.applied, grouped.revision),
                (separate.applied, separate.revision)
            );
            assert_eq!(grouped.alarms, separate.alarms);
            assert_eq!(grouped.logical_size(), separate.logical_size());
            assert_eq!(
                grouped.logical_size(),
                crate::state::maintenance::count_logical_size(grouped.db.as_ref())
                    .await
                    .unwrap()
            );
            assert_eq!(
                separate.logical_size(),
                crate::state::maintenance::count_logical_size(separate.db.as_ref())
                    .await
                    .unwrap()
            );
            grouped.close().await.unwrap();
            separate.close().await.unwrap();
        }
    }

    /// A small deterministic generator, so a failing seed reproduces exactly.
    struct Rng(u64);

    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }

        fn below(&mut self, n: u64) -> u64 {
            self.next() % n
        }

        fn pick<'a, T>(&mut self, items: &'a [T]) -> &'a T {
            &items[self.below(items.len() as u64) as usize]
        }
    }

    const KEYS: [&[u8]; 4] = [b"a", b"b", b"c", b"d"];

    fn random_value(rng: &mut Rng) -> Vec<u8> {
        let len = rng.below(4) as usize * 40;
        vec![b'a' + rng.below(3) as u8; len]
    }

    fn random_end(rng: &mut Rng, key: &[u8]) -> Vec<u8> {
        match rng.below(3) {
            0 => vec![0],
            1 => [key, &[0]].concat(),
            _ => rng.pick(&KEYS).to_vec(),
        }
    }

    fn random_op(rng: &mut Rng, depth: u32) -> RequestOp {
        let key = rng.pick(&KEYS).to_vec();
        let kinds = if depth < 2 { 12 } else { 7 };
        let request = match rng.below(kinds) {
            0..=2 => Op::RequestPut(PutRequest {
                key,
                value: random_value(rng),
                lease: *rng.pick(&[0, 0, 0, 0, 5, 6, 99]),
                prev_kv: rng.below(2) == 0,
                ignore_value: rng.below(8) == 0,
                ignore_lease: rng.below(8) == 0,
            }),
            3 => Op::RequestDeleteRange(DeleteRangeRequest {
                range_end: if rng.below(4) == 0 {
                    random_end(rng, &key)
                } else {
                    vec![]
                },
                key,
                prev_kv: rng.below(2) == 0,
            }),
            4..=6 => Op::RequestRange(RangeRequest {
                range_end: if rng.below(4) == 0 {
                    random_end(rng, &key)
                } else {
                    vec![]
                },
                key,
                revision: if rng.below(6) == 0 {
                    rng.below(40) as i64
                } else {
                    0
                },
                count_only: rng.below(8) == 0,
                keys_only: rng.below(8) == 0,
                ..Default::default()
            }),
            10 | 11 => random_put_txn(rng),
            _ => {
                use compare::TargetUnion as T;
                let compares = (0..rng.below(3))
                    .map(|_| {
                        let target = rng.below(5) as i32;
                        let number = rng.below(12) as i64;
                        let key = rng.pick(&KEYS).to_vec();
                        Compare {
                            result: rng.below(4) as i32,
                            target,
                            range_end: if rng.below(6) == 0 {
                                random_end(rng, &key)
                            } else {
                                vec![]
                            },
                            key,
                            target_union: Some(match target {
                                0 => T::Version(number % 4),
                                1 => T::CreateRevision(number),
                                2 => T::ModRevision(number),
                                3 => T::Value(random_value(rng)),
                                _ => T::Lease(*rng.pick(&[0, 5, 6])),
                            }),
                        }
                    })
                    .collect();
                let branch = |rng: &mut Rng| {
                    (0..rng.below(3))
                        .map(|_| random_op(rng, depth + 1))
                        .collect::<Vec<_>>()
                };
                let success = branch(rng);
                let failure = branch(rng);
                Op::RequestTxn(TxnRequest {
                    compare: compares,
                    success,
                    failure,
                })
            }
        };
        RequestOp {
            request: Some(request),
        }
    }

    /// A transaction of ordinary puts with point compares. It takes the direct write path.
    fn random_put_txn(rng: &mut Rng) -> Op {
        use compare::TargetUnion as T;
        const PUT_KEYS: [&[u8]; 8] = [b"a", b"b", b"c", b"d", b"e", b"f", b"g", b"h"];
        let branch = |rng: &mut Rng| {
            let mut keys = PUT_KEYS.to_vec();
            let mut ops = vec![];
            for _ in 0..rng.below(9) {
                let key = keys.remove(rng.below(keys.len() as u64) as usize);
                ops.push(RequestOp {
                    request: Some(Op::RequestPut(PutRequest {
                        key: key.to_vec(),
                        value: random_value(rng),
                        prev_kv: rng.below(2) == 0,
                        ..Default::default()
                    })),
                });
            }
            // A repeated key fails validation.
            if !ops.is_empty() && rng.below(10) == 0 {
                ops.push(ops[0].clone());
            }
            ops
        };
        let compare = (0..rng.below(3))
            .map(|_| {
                let target = rng.below(5) as i32;
                let number = rng.below(12) as i64;
                Compare {
                    result: rng.below(4) as i32,
                    target,
                    key: rng.pick(&PUT_KEYS).to_vec(),
                    target_union: Some(match target {
                        0 => T::Version(number % 4),
                        1 => T::CreateRevision(number),
                        2 => T::ModRevision(number),
                        3 => T::Value(random_value(rng)),
                        _ => T::Lease(*rng.pick(&[0, 5, 6])),
                    }),
                    ..Default::default()
                }
            })
            .collect();
        let success = branch(rng);
        let failure = branch(rng);
        Op::RequestTxn(TxnRequest {
            compare,
            success,
            failure,
        })
    }

    fn reply_value(reply: &Reply) -> serde_json::Value {
        match reply {
            Reply::Kv(outcome) => serde_json::to_value(outcome).unwrap(),
            Reply::Lease(outcome) => serde_json::to_value(outcome).unwrap(),
            Reply::Encoded(value) => value.clone(),
            _ => panic!("unexpected reply kind"),
        }
    }

    /// The pre-grouping apply path: one storage write per entry.
    async fn apply_alone(
        state: &mut StateMachine,
        index: u64,
        command: Option<&RaftCommand>,
        header: Header,
    ) -> Option<serde_json::Value> {
        let Some(command) = command else {
            state.apply(index, None).await.unwrap();
            return None;
        };
        let command = match command {
            RaftCommand::Secured {
                credentials,
                command,
                ..
            } => {
                if let Err(error) = state.authorize(credentials, command).await {
                    state.apply(index, None).await.unwrap();
                    return Some(
                        serde_json::to_value(Outcome::<serde_json::Value> { result: Err(error) })
                            .unwrap(),
                    );
                }
                command.as_ref()
            }
            command => command,
        };
        Some(match command {
            RaftCommand::Etcd { etcd, origin, .. } => {
                let mut outcome = state.apply_etcd_quota(index, etcd, *origin).await.unwrap();
                header.outcome(&mut outcome);
                serde_json::to_value(outcome).unwrap()
            }
            RaftCommand::Lease { lease, origin, .. } => {
                serde_json::to_value(state.apply_lease_from(index, lease, *origin).await.unwrap())
                    .unwrap()
            }
            _ => panic!("unexpected command"),
        })
    }

    async fn enable_auth(state: &mut StateMachine) {
        use crate::auth::Command as A;
        use crate::proto::authpb::Permission;
        let user = |name: &str| {
            A::UserAdd(AuthUserAddRequest {
                name: name.into(),
                options: Some(crate::proto::authpb::UserAddOptions { no_password: true }),
                ..Default::default()
            })
        };
        let grant = |key: &[u8], end: &[u8], perm_type| {
            A::RoleGrantPermission(AuthRoleGrantPermissionRequest {
                name: "r".into(),
                perm: Some(Permission {
                    perm_type,
                    key: key.to_vec(),
                    range_end: end.to_vec(),
                }),
            })
        };
        let role = |user: &str, role: &str| {
            A::UserGrantRole(AuthUserGrantRoleRequest {
                user: user.into(),
                role: role.into(),
            })
        };
        for command in [
            user("root"),
            role("root", "root"),
            user("u"),
            A::RoleAdd(AuthRoleAddRequest { name: "r".into() }),
            // Read and write on a and b, read only on c, nothing on d.
            grant(b"a", b"c", 2),
            grant(b"c", b"d", 0),
            role("u", "r"),
            A::Enable(AuthEnableRequest {}),
        ] {
            state
                .apply_auth(state.applied + 1, &Default::default(), &command)
                .await
                .unwrap()
                .result
                .unwrap();
        }
    }

    fn journals(state: &mut StateMachine, previous: i64) -> Vec<(i64, Vec<u8>)> {
        let Some(batch) = state.take_watch_batch(previous) else {
            return vec![];
        };
        batch
            .journals()
            .expect("complete watch batch")
            .iter()
            .map(|(revision, journal)| (*revision, journal.encode_to_vec()))
            .collect()
    }

    fn index_rows(state: &StateMachine) -> Vec<Option<crate::state::key_index::KeyMeta>> {
        KEYS.iter()
            .map(|key| state.key_index.get(key).copied())
            .collect()
    }

    /// Random command streams applied in random apply jobs must match one-by-one apply exactly.
    #[tokio::test]
    async fn random_groups_match_one_by_one_apply_byte_for_byte() {
        let header = Header {
            cluster: 1,
            member: 1,
            term: 1,
        };
        let seeds: u64 = std::env::var("GROUP_EQUIVALENCE_SEEDS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(24);
        let direct = crate::mvcc::DIRECT_TXNS.with(|count| count.get());
        for seed in 1..=seeds {
            let mut rng = Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
            let secured = seed % 3 == 0;
            let quota = *rng.pick(&[0, 0, 2500, 6000]);
            let mut commands = vec![];
            for (offset, id) in [5, 6].into_iter().enumerate() {
                commands.push(Some(RaftCommand::Lease {
                    id: format!("grant{offset}"),
                    lease: LeaseCommand::Grant { id, ttl: 60 },
                    origin: 1,
                }));
            }
            for i in 0..160 {
                let command = match rng.below(40) {
                    0 => None,
                    1 => Some(RaftCommand::Lease {
                        id: format!("lease{i}"),
                        lease: match rng.below(2) {
                            0 => LeaseCommand::Revoke {
                                id: *rng.pick(&[5, 6]),
                            },
                            _ => LeaseCommand::Grant {
                                id: *rng.pick(&[5, 6]),
                                ttl: 60,
                            },
                        },
                        origin: 1,
                    }),
                    _ => {
                        let etcd = RaftCommand::Etcd {
                            id: format!("r{i}"),
                            etcd: random_op(&mut rng, 0),
                            origin: 1 + rng.below(3),
                        };
                        Some(if secured {
                            etcd.secure(Some(crate::auth::Credentials {
                                user: Some(rng.pick(&["u", "u", "u", "root"]).to_string()),
                                ..Default::default()
                            }))
                        } else {
                            etcd
                        })
                    }
                };
                commands.push(command);
            }

            let left = tempfile::tempdir().unwrap();
            let right = tempfile::tempdir().unwrap();
            let mut grouped = StateMachine::open(left.path()).await.unwrap();
            let mut separate = StateMachine::open(right.path()).await.unwrap();
            // Grouped apply stores the entry term. Direct apply calls store `entry_term`.
            separate.entry_term = 1;
            for state in [&mut grouped, &mut separate] {
                if secured {
                    enable_auth(state).await;
                }
                state.quota = quota;
                state.watch_capture = Some(Default::default());
            }
            let base = grouped.applied;
            let entries: Vec<_> = commands
                .iter()
                .enumerate()
                .map(|(offset, command)| Entry {
                    index: base + offset as u64 + 1,
                    term: 1,
                    data: command
                        .as_ref()
                        .map_or_else(Vec::new, |c| c.encode().unwrap()),
                    ..Default::default()
                })
                .collect();

            let mut grouped_replies = vec![];
            let mut grouped_events = vec![];
            let mut entries = entries.into_iter().peekable();
            while entries.peek().is_some() {
                let size = 1 + rng.below(40) as usize;
                let job: Vec<_> = entries.by_ref().take(size).collect();
                let previous = grouped.revision;
                let replies = apply_entries(&mut grouped, job, header).await.unwrap();
                grouped_replies.extend(replies.iter().map(|r| reply_value(&r.reply)));
                grouped_events.extend(journals(&mut grouped, previous));
            }

            let mut separate_replies = vec![];
            let mut separate_events = vec![];
            for (offset, command) in commands.iter().enumerate() {
                let previous = separate.revision;
                let index = base + offset as u64 + 1;
                if let Some(reply) =
                    apply_alone(&mut separate, index, command.as_ref(), header).await
                {
                    separate_replies.push(reply);
                }
                separate_events.extend(journals(&mut separate, previous));
            }

            let context = format!("seed {seed}, quota {quota}, auth {secured}");
            assert_eq!(grouped_replies.len(), separate_replies.len(), "{context}");
            for (offset, (left, right)) in grouped_replies.iter().zip(&separate_replies).enumerate()
            {
                assert_eq!(left, right, "{context}, reply {offset}");
            }
            assert_eq!(grouped_events, separate_events, "{context}");
            assert_eq!(rows(&grouped).await, rows(&separate).await, "{context}");
            assert_eq!(
                (grouped.applied, grouped.revision, grouped.logical_size()),
                (separate.applied, separate.revision, separate.logical_size()),
                "{context}"
            );
            assert_eq!(grouped.alarms, separate.alarms, "{context}");
            assert_eq!(index_rows(&grouped), index_rows(&separate), "{context}");
            assert_eq!(
                grouped.logical_size(),
                crate::state::maintenance::count_logical_size(grouped.db.as_ref())
                    .await
                    .unwrap(),
                "{context}"
            );
            assert!(grouped.write_batches < separate.write_batches, "{context}");
            let expected = (index_rows(&separate), rows(&separate).await);
            grouped.db.flush().await.unwrap();
            grouped.close().await.unwrap();
            separate.close().await.unwrap();
            // The applied marker and the rows reopen together.
            let reopened = StateMachine::open(left.path()).await.unwrap();
            assert_eq!(reopened.applied, separate.applied, "{context}");
            assert_eq!(reopened.revision, separate.revision, "{context}");
            assert_eq!(
                (index_rows(&reopened), rows(&reopened).await),
                expected,
                "{context}"
            );
            reopened.close().await.unwrap();
        }
        // Put-only transactions took the direct path, and the one-by-one path checked them.
        assert!(crate::mvcc::DIRECT_TXNS.with(|count| count.get()) > direct);
    }
}
