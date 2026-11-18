use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use anyhow::{ensure, Context, Result};
use protobuf::Message as ProtobufMessage;
use raft::eraftpb::{Entry, EntryType, Message, MessageType};
use raft::{Config, RawNode, SnapshotStatus, StateRole, Storage};
use serde_json::{json, Value};
use tokio::sync::{mpsc, oneshot};

use crate::lease::{LeaseCommand, LeaseReply};
use crate::proto::etcdserverpb::{CompactionRequest, RequestOp, ResponseHeader};
use crate::state::StateMachine;
use crate::storage::RaftStorage;
use crate::{ApiError, Command, RaftCommand, Request};

mod apply;
mod cache;
pub use cache::LogMemory;
mod cluster;
mod log;
mod maintenance;
mod read;
use apply::{AppliedState, Completion, Header, Job, Output, Query, Reply, Work, Worker};

#[cfg(test)]
mod dst;
#[cfg(test)]
mod tests;

pub type Response = oneshot::Sender<Result<Value, ApiError>>;

pub enum Event {
    SubscribeApply(oneshot::Sender<tokio::sync::watch::Receiver<u64>>),
    WaitRevision(i64, Response),
    Authenticated(crate::auth::Credentials, Box<Event>),
    Auth(crate::auth::Command, Response),
    AuthLookup(String, Response),
    Maintenance(crate::maintenance::Command, Response),
    Cluster(crate::cluster::Command, Response),
    PeerAddress(u64, Response),
    Propose(Request, Response),
    EtcdPropose(RequestOp, Response),
    EtcdRead(RequestOp, Response),
    LinearRead(RequestOp, Response),
    EtcdCompact(CompactionRequest, Response),
    Lease(LeaseCommand, Response),
    Watch(
        crate::proto::etcdserverpb::WatchCreateRequest,
        bool,
        Option<i64>,
        Response,
    ),
    Step(Message, Response),
    Status(Response),
    LogStats(Response),
    Checkpoint(Response),
    LocalValue(String, Response),
    BlockPeers(BTreeSet<u64>, Response),
    SnapshotBuilt {
        index: u64,
        conf: raft::eraftpb::ConfState,
        result: Result<Vec<u8>>,
    },
    SnapshotDelay(u64, Response),
    ApplyDelay(u64, Response),
    LogFault(u64, bool, Response),
    LogReadFault(u64, bool, Response),
    NetworkResult {
        peer: u64,
        snapshot: bool,
        success: bool,
    },
}

pub struct NodeOptions {
    pub id: u64,
    pub cluster_id: String,
    pub peers: BTreeMap<u64, String>,
    pub dir: PathBuf,
    pub snapshot_every: u64,
    pub log_memory: LogMemory,
    pub quota_bytes: u64,
    pub strict_reconfiguration: bool,
    pub join: bool,
    pub client_peers: BTreeMap<u64, String>,
    pub transport: crate::tls::Transport,
    pub object_store: crate::object::Config,
    pub require_peer_tls: bool,
}

struct Pending {
    response: Response,
    started: Instant,
    term: u64,
}

struct RevisionWaiter {
    revision: i64,
    response: Response,
    started: Instant,
}

struct LogTask {
    id: u64,
    work: Option<log::Job>,
    bytes: usize,
    snapshot_bytes: usize,
    ready: Option<u64>,
    checkpoint: Option<u64>,
    messages: Vec<Message>,
    reads: Vec<raft::ReadState>,
    responses: Vec<Response>,
    stats_response: Option<Response>,
}

pub struct Node {
    applied_notify: tokio::sync::watch::Sender<u64>,
    transfers: Vec<(u64, Instant, Response)>,
    published_at: Option<Instant>,
    events: mpsc::Sender<Event>,
    manual_transport: bool,
    peer_health: BTreeMap<u64, Instant>,
    membership_checkpoint: u64,
    raw: RawNode<log::Cache>,
    log_worker: log::Worker,
    log_queue: VecDeque<LogTask>,
    log_busy: bool,
    log_sequence: u64,
    log_bytes: usize,
    log_snapshot_bytes: usize,
    log_delay_ms: u64,
    log_fail: bool,
    log_read_delay_ms: u64,
    log_read_fail: bool,
    peer_waiters: Vec<Response>,
    built_checkpoint: Option<(u64, raft::eraftpb::ConfState, Vec<u8>)>,
    reads: BTreeMap<Vec<u8>, read::PendingRead>,
    revision_waiters: VecDeque<RevisionWaiter>,
    read_nonce: [u8; 16],
    read_sequence: u64,
    state: AppliedState,
    worker: Worker,
    apply_busy: bool,
    apply_target: u64,
    committed_ready: u64,
    queries: VecDeque<Query>,
    prefer_query: bool,
    apply_delay_ms: u64,
    options: NodeOptions,
    senders: BTreeMap<u64, mpsc::Sender<Message>>,
    transports: tokio::task::JoinSet<()>,
    pending: HashMap<String, Pending>,
    blocked: BTreeSet<u64>,
    installed_snapshots: u64,
    recovered_from_index: u64,
    send_failures: Vec<(u64, bool)>,
    checkpoint_running: bool,
    checkpoint_waiters: Vec<(u64, Response)>,
    snapshot_delay_ms: u64,
    lease_term: u64,
    lease_deadlines: BTreeMap<i64, (u64, Instant)>,
    lease_expiring: BTreeSet<(i64, u64)>,
}

impl Node {
    pub async fn open(options: NodeOptions, events: mpsc::Sender<Event>) -> Result<Self> {
        Self::open_with_transport(options, events, None).await
    }

    async fn open_with_transport(
        options: NodeOptions,
        events: mpsc::Sender<Event>,
        transport_override: Option<BTreeMap<u64, mpsc::Sender<Message>>>,
    ) -> Result<Self> {
        ensure!(!options.peers.is_empty(), "the initial voter set is empty");
        ensure!(
            options.id != 0 && options.peers.contains_key(&options.id),
            "invalid node id"
        );
        ensure!(
            !options.peers.contains_key(&0),
            "zero is not a valid voter id"
        );
        std::fs::create_dir_all(&options.dir)?;
        // Open Raft first. Its exclusive file lock also protects this node's SlateDB directory.
        let storage = RaftStorage::open(
            &options.dir.join("raft"),
            options.id,
            &options.cluster_id,
            if options.join {
                vec![]
            } else {
                options.peers.keys().copied().collect()
            },
        )?;
        let mut state = options
            .object_store
            .open(&options.dir, &options.cluster_id, options.id)
            .await?;
        let recovered = async {
            state.quota = options.quota_bytes;
            ensure!(
                state.applied <= storage.commit_index(),
                "state is ahead of durable commit"
            );
            let recovered_from_index = state.applied;
            state.install_snapshot(storage.recovery_snapshot()).await?;
            if state.members.members.is_empty() && !options.join {
                state.members = crate::cluster::Store::initial(&options.peers);
                for (&id, address) in &options.client_peers {
                    if let Some(member) = state.members.members.get_mut(&id) {
                        member.client_ur_ls = vec![crate::cluster::url(address)];
                    }
                }
            }
            if options.require_peer_tls {
                ensure!(
                    options.transport.tls.is_some(),
                    "remote peers require a TLS transport"
                );
                for member in state.members.members.values() {
                    for address in &member.peer_ur_ls {
                        crate::tls::validate_url(address, true, true)?;
                    }
                }
            }
            let mut cache = match log::Cache::load(&storage, state.applied, options.log_memory) {
                Ok(cache) => cache,
                Err(error) if error.is::<cache::PendingQuota>() => {
                    apply::recover(
                        &mut state,
                        &storage,
                        Header {
                            cluster: 0,
                            member: options.id,
                            term: storage.initial_state()?.hard_state.term,
                        },
                    )
                    .await?;
                    log::Cache::load(&storage, state.applied, options.log_memory)?
                }
                Err(error) => return Err(error),
            };
            cache.initial_conf = Some(state.members.conf());
            let config = Config {
                id: options.id,
                election_tick: 10,
                heartbeat_tick: 1,
                applied: state.applied,
                check_quorum: true,
                pre_vote: true,
                read_only_option: raft::ReadOnlyOption::Safe,
                max_size_per_msg: 1024 * 1024,
                max_inflight_msgs: 64,
                max_uncommitted_size: 16 * 1024 * 1024,
                max_committed_size_per_ready: apply::BATCH_BYTES,
                ..Config::default()
            };
            config.validate()?;
            let logger = slog::Logger::root(slog::Discard, slog::o!());
            let raw = RawNode::new(&config, cache, &logger)?;
            anyhow::Ok((raw, recovered_from_index))
        }
        .await;
        let (raw, recovered_from_index) = match recovered {
            Ok(recovered) => recovered,
            Err(error) => {
                let _ = state.close().await;
                return Err(error);
            }
        };
        let manual_apply = transport_override.is_some();
        let mut transports = tokio::task::JoinSet::new();
        let senders = if let Some(senders) = transport_override {
            senders
        } else {
            let mut senders = BTreeMap::new();
            for (&peer, address) in &options.peers {
                if peer == options.id {
                    continue;
                }
                let (tx, rx) = mpsc::channel(256);
                senders.insert(peer, tx);
                transports.spawn(transport(
                    peer,
                    address.clone(),
                    options.cluster_id.clone(),
                    rx,
                    events.clone(),
                    options.transport.http.clone(),
                ));
            }
            senders
        };
        let applied = AppliedState::from(&state);
        let worker = Worker::start(state, events.clone(), manual_apply);
        let log_worker = log::Worker::start(storage, manual_apply)?;
        let mut node = Self {
            applied_notify: tokio::sync::watch::channel(applied.applied).0,
            transfers: vec![],
            published_at: None,
            events,
            manual_transport: manual_apply,
            peer_health: BTreeMap::new(),
            membership_checkpoint: 0,
            raw,
            log_worker,
            log_queue: VecDeque::new(),
            log_busy: false,
            log_sequence: 0,
            log_bytes: 0,
            log_snapshot_bytes: 0,
            log_delay_ms: 0,
            log_fail: false,
            log_read_delay_ms: 0,
            log_read_fail: false,
            peer_waiters: vec![],
            built_checkpoint: None,
            reads: BTreeMap::new(),
            revision_waiters: VecDeque::new(),
            read_nonce: *uuid::Uuid::new_v4().as_bytes(),
            read_sequence: 0,
            apply_target: applied.applied,
            committed_ready: applied.applied,
            state: applied,
            worker,
            apply_busy: false,
            queries: VecDeque::new(),
            prefer_query: false,
            apply_delay_ms: 0,
            options,
            senders,
            transports,
            pending: HashMap::new(),
            blocked: BTreeSet::new(),
            installed_snapshots: 0,
            recovered_from_index,
            send_failures: vec![],
            checkpoint_running: false,
            checkpoint_waiters: vec![],
            snapshot_delay_ms: 0,
            lease_term: 0,
            lease_deadlines: BTreeMap::new(),
            lease_expiring: BTreeSet::new(),
        };
        node.sync_members();
        Ok(node)
    }

    pub async fn run(self, events: mpsc::Receiver<Event>) -> Result<()> {
        self.run_until(events, std::future::pending()).await
    }

    pub(crate) async fn run_until(
        mut self,
        events: mpsc::Receiver<Event>,
        stop: impl std::future::Future<Output = ()>,
    ) -> Result<()> {
        let result = tokio::select! {
            result = self.run_loop(events) => result,
            _ = stop => Ok(()),
        };
        self.transports.shutdown().await;
        // Keep the Raft directory locked until the state worker has stopped.
        let apply = self.worker.shutdown().await;
        let log = self.log_worker.shutdown().await;
        result.and(apply).and(log)
    }

    async fn run_loop(&mut self, mut events: mpsc::Receiver<Event>) -> Result<()> {
        // Schedule committed log recovery before accepting client work.
        self.drive_ready().await?;
        let mut ticks = tokio::time::interval(Duration::from_millis(100));
        ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut old_role = (
            self.raw.raft.state,
            self.raw.raft.term,
            self.raw.raft.leader_id,
        );
        loop {
            let mut step_responses = vec![];
            tokio::select! {
                _ = ticks.tick() => { self.raw.tick(); self.retry_reads(); self.expire_leases()?; self.publish_member()?; }
                persisted = self.log_worker.next() => { self.complete_log(persisted?)?; }
                completed = self.worker.next() => { self.complete_apply(completed?)?; }
                event = events.recv() => {
                    let Some(event) = event else { return Ok(()); };
                    self.handle(event, &mut step_responses).await?;
                    // Accumulate proposals so concurrent clients can share one durable log write.
                    for _ in 0..63 {
                        let Ok(event) = events.try_recv() else { break; };
                        self.handle(event, &mut step_responses).await?;
                    }
                }
            }
            self.peer_waiters.extend(step_responses);
            self.drive_ready().await?;
            self.sync_lease_deadlines();
            self.expire_pending();
            self.finish_revision_waiters();
            self.finish_transfers()?;
            let role = (
                self.raw.raft.state,
                self.raw.raft.term,
                self.raw.raft.leader_id,
            );
            if role != old_role {
                tracing::info!(node = self.options.id, role = ?role.0, term = role.1,
                    leader = role.2, "Raft state changed");
                old_role = role;
            }
            if self.options.snapshot_every > 0
                && self
                    .state
                    .applied
                    .saturating_sub(self.raw.store().snapshot_index())
                    >= self.options.snapshot_every
            {
                self.start_checkpoint()?;
            }
            self.finish_checkpoint_waiters()?;
        }
    }

    fn admits_peer(&self, message: &Message) -> bool {
        let kind = message.get_msg_type();
        if message.entries.is_empty()
            || !matches!(kind, MessageType::MsgAppend | MessageType::MsgPropose)
        {
            return true;
        }
        if kind == MessageType::MsgAppend
            && (message.index < self.raw.raft.raft_log.committed
                || !self
                    .raw
                    .raft
                    .raft_log
                    .match_term(message.index, message.log_term))
        {
            // Let Raft reject a prefix mismatch without charging entries it will discard.
            return true;
        }
        self.raw
            .store()
            .admits_message(self.raw.raft.raft_log.unstable_entries(), message)
    }

    async fn handle(&mut self, event: Event, step_responses: &mut Vec<Response>) -> Result<()> {
        let (credentials, event) = match event {
            Event::Authenticated(credentials, event) => (Some(credentials), *event),
            event => (None, event),
        };
        match event {
            Event::Authenticated(..) => anyhow::bail!("nested authentication context"),
            Event::Auth(auth, response) => self.propose(
                RaftCommand::Auth {
                    id: uuid::Uuid::new_v4().to_string(),
                    auth,
                    credentials: credentials.unwrap_or_default(),
                },
                response,
            )?,
            Event::Cluster(request, response) => {
                self.cluster_request(request, credentials.unwrap_or_default(), response)?
            }
            Event::PeerAddress(id, response) => {
                let address = self.options.peers.get(&id).cloned();
                let _ = response.send(Ok(json!(address)));
            }
            Event::Maintenance(request, response) => {
                use crate::maintenance::Command as M;
                match request {
                    M::MoveLeader(target) => {
                        self.move_leader(target, credentials.unwrap_or_default(), response)?
                    }
                    M::Alarm(_) | M::Downgrade(_) => self.propose(
                        RaftCommand::Maintenance {
                            id: uuid::Uuid::new_v4().to_string(),
                            request,
                            credentials: credentials.unwrap_or_default(),
                        },
                        response,
                    )?,
                    _ => {
                        let status = crate::proto::etcdserverpb::StatusResponse {
                            is_learner: self
                                .state
                                .members
                                .members
                                .get(&self.options.id)
                                .is_some_and(|member| member.is_learner),
                            leader: self.raw.raft.leader_id,
                            raft_index: self.raw.store().commit_index(),
                            raft_term: self.raw.raft.term,
                            version: "3.6.5".into(),
                            storage_version: "3.6.0".into(),
                            ..Default::default()
                        };
                        self.enqueue_query(Query::Maintenance(
                            request,
                            credentials.unwrap_or_default(),
                            Box::new(status),
                            response,
                        ));
                    }
                }
            }
            Event::AuthLookup(name, response) => {
                self.enqueue_query(Query::AuthLookup(name, response))
            }
            Event::Propose(request, response) => {
                self.propose(
                    RaftCommand::Legacy(Command {
                        id: uuid::Uuid::new_v4().to_string(),
                        request,
                    }),
                    response,
                )?;
            }
            Event::EtcdPropose(etcd, response) => {
                self.propose(
                    RaftCommand::Etcd {
                        origin: self.options.id,
                        id: uuid::Uuid::new_v4().to_string(),
                        etcd,
                    }
                    .secure(credentials),
                    response,
                )?;
            }
            Event::LinearRead(request, response) => {
                self.linear_read_auth(request, credentials, response)
            }
            Event::WaitRevision(revision, response) => self.wait_revision(revision, response),
            Event::LogStats(response) => {
                if self.log_queue.len() >= log::MAX_JOBS {
                    let _ = response.send(Err(self.error("too many pending log jobs")));
                } else {
                    self.queue_log(log::Job::Stats, None, None, vec![], vec![]);
                    self.log_queue.back_mut().unwrap().stats_response = Some(response);
                }
            }
            Event::EtcdRead(request, response) => {
                self.enqueue_query(Query::Read(request, credentials, response));
            }
            Event::EtcdCompact(compact, response) => {
                self.propose(
                    RaftCommand::Compact {
                        id: uuid::Uuid::new_v4().to_string(),
                        compact,
                    }
                    .secure(credentials),
                    response,
                )?;
            }
            Event::Lease(mut lease, response) => {
                self.expire_leases()?;
                if let LeaseCommand::Grant { id, .. } = &mut lease {
                    if *id == 0 {
                        *id = ((uuid::Uuid::new_v4().as_u128() as u64) & i64::MAX as u64).max(1)
                            as i64;
                    }
                }
                self.propose(
                    RaftCommand::Lease {
                        origin: self.options.id,
                        id: uuid::Uuid::new_v4().to_string(),
                        lease,
                    }
                    .secure(credentials),
                    response,
                )?;
            }
            Event::Watch(request, require_leader, through, response) => {
                if require_leader && self.raw.raft.leader_id == 0 {
                    let _ = response.send(Err(self.error("no leader")));
                } else {
                    self.enqueue_query(Query::Watch(
                        request,
                        require_leader,
                        through,
                        credentials,
                        response,
                    ));
                }
            }
            Event::Step(message, response) => {
                if self.options.join
                    && self.state.applied == 0
                    && self.raw.store().snapshot_index() == 0
                    && message.get_msg_type() == MessageType::MsgAppend
                {
                    self.send(vec![Message {
                        msg_type: MessageType::MsgAppendResponse,
                        to: message.from,
                        from: self.options.id,
                        term: message.term,
                        index: message.index,
                        reject: true,
                        reject_hint: 0,
                        ..Default::default()
                    }]);
                    let _ = response.send(Ok(json!({"accepted": false})));
                } else if message.to != self.options.id
                    || message.from == self.options.id
                    || !self.options.peers.contains_key(&message.from)
                    || self.blocked.contains(&message.from)
                    || !self.admits_peer(&message)
                    || self.peer_waiters.len() + step_responses.len() >= 256
                {
                    let _ = response.send(Err(self.error("peer message rejected")));
                } else {
                    let snapshot = message.get_msg_type() == MessageType::MsgSnapshot;
                    match self.raw.step(message) {
                        Ok(()) if snapshot => step_responses.push(response),
                        Ok(()) => {
                            // HTTP receipt is not a Raft vote or append acknowledgement.
                            let _ = response.send(Ok(json!({"accepted": true})));
                        }
                        Err(error) => {
                            let _ = response.send(Err(self.error(&error.to_string())));
                        }
                    }
                }
            }
            Event::SubscribeApply(response) => {
                let _ = response.send(self.applied_notify.subscribe());
            }
            Event::Status(response) => {
                let _ = response.send(Ok(self.status()?));
            }
            Event::Checkpoint(response) => {
                if self.checkpoint_waiters.len() >= apply::MAX_QUERIES {
                    let _ = response.send(Err(self.error("too many checkpoint requests")));
                } else {
                    self.checkpoint_waiters
                        .push((self.raw.raft.raft_log.committed, response));
                    self.schedule_apply()?;
                }
            }
            Event::SnapshotBuilt {
                index,
                conf,
                result,
            } => {
                ensure!(
                    self.built_checkpoint.is_none(),
                    "overlapping checkpoint results"
                );
                self.built_checkpoint = Some((index, conf, result?));
            }
            Event::SnapshotDelay(delay_ms, response) => {
                self.snapshot_delay_ms = delay_ms.min(5000);
                let _ = response.send(Ok(self.status()?));
            }
            Event::ApplyDelay(delay_ms, response) => {
                self.apply_delay_ms = delay_ms.min(5000);
                let _ = response.send(Ok(self.status()?));
            }
            Event::LogReadFault(delay_ms, fail, response) => {
                self.log_read_delay_ms = delay_ms.min(5000);
                self.log_read_fail = fail;
                let _ = response.send(Ok(self.status()?));
            }
            Event::LogFault(delay_ms, fail, response) => {
                self.log_delay_ms = delay_ms.min(5000);
                self.log_fail = fail;
                let _ = response.send(Ok(self.status()?));
            }
            Event::LocalValue(key, response) => {
                self.enqueue_query(Query::Local(key, response));
            }
            Event::BlockPeers(peers, response) => {
                self.blocked = peers;
                let _ = response.send(Ok(self.status()?));
            }
            Event::NetworkResult {
                peer,
                snapshot,
                success,
            } => {
                if success {
                    self.peer_health.entry(peer).or_insert_with(Instant::now);
                } else {
                    self.peer_health.remove(&peer);
                }
                if snapshot {
                    self.raw.report_snapshot(
                        peer,
                        if success {
                            SnapshotStatus::Finish
                        } else {
                            SnapshotStatus::Failure
                        },
                    );
                }
                if !success {
                    self.raw.report_unreachable(peer);
                }
            }
        }
        Ok(())
    }

    async fn drive_ready(&mut self) -> Result<()> {
        self.expire_reads();
        self.issue_reads();
        if self.log_queue.len() < log::MAX_JOBS
            && self.log_bytes < log::MAX_BYTES
            && self.log_snapshot_bytes < log::MAX_SNAPSHOT_BYTES
        {
            if let Some((index, conf, data)) = self.built_checkpoint.take() {
                self.queue_log(
                    log::Job::Checkpoint { index, conf, data },
                    None,
                    Some(index),
                    vec![],
                    vec![],
                );
            }
        }
        while self.raw.has_ready()
            && self.log_queue.len() < log::MAX_JOBS
            && self.log_bytes < log::MAX_BYTES
            && self.log_snapshot_bytes < log::MAX_SNAPSHOT_BYTES
        {
            let mut ready = self.raw.ready();
            let snapshot = (!ready.snapshot().is_empty()).then(|| ready.snapshot().clone());
            self.raw
                .mut_store()
                .stage(ready.entries(), ready.hs(), snapshot.as_ref())?;
            let job = log::Job::Ready {
                entries: ready.take_entries(),
                hs: ready.hs().cloned(),
                snapshot,
            };
            let early = ready.take_messages();
            let messages = ready.take_persisted_messages();
            let reads = ready.take_read_states();
            let number = ready.number();
            // Apply only entries that Raft has delivered, even if disk commit is ahead.
            self.committed_ready = self
                .committed_ready
                .max(ready.snapshot().get_metadata().index);
            if let Some(entry) = ready.take_committed_entries().last() {
                self.committed_ready = self.committed_ready.max(entry.index);
            }
            // Cached entries must be readable before Raft can advance without disk completion.
            self.raw.advance_append_async(ready);
            self.queue_log(job, Some(number), None, messages, reads);
            // raft-rs permits leader replication before local persistence.
            self.send(early);
            for (peer, snapshot) in std::mem::take(&mut self.send_failures) {
                self.raw.report_unreachable(peer);
                if snapshot {
                    self.raw.report_snapshot(peer, SnapshotStatus::Failure);
                }
            }
        }
        if self.log_queue.len() < log::MAX_JOBS {
            if let Some(fetch) = self.raw.mut_store().take_fetch() {
                self.queue_log(log::Job::Fetch(fetch), None, None, vec![], vec![]);
            }
        }
        if !self.raw.has_ready() {
            if let Some(task) = self.log_queue.back_mut() {
                task.responses.append(&mut self.peer_waiters);
            } else {
                for response in self.peer_waiters.drain(..) {
                    let _ = response.send(Ok(json!({"accepted": true})));
                }
            }
        }
        self.start_log()?;
        self.schedule_reads();
        self.schedule_apply()?;
        Ok(())
    }

    fn queue_log(
        &mut self,
        job: log::Job,
        ready: Option<u64>,
        checkpoint: Option<u64>,
        messages: Vec<Message>,
        reads: Vec<raft::ReadState>,
    ) {
        self.log_sequence += 1;
        let bytes = job.bytes();
        let snapshot_bytes = job.snapshot_bytes();
        self.log_bytes += bytes;
        self.log_snapshot_bytes += snapshot_bytes;
        self.log_queue.push_back(LogTask {
            id: self.log_sequence,
            work: Some(job),
            bytes,
            snapshot_bytes,
            ready,
            checkpoint,
            messages,
            reads,
            responses: vec![],
            stats_response: None,
        });
    }

    fn start_log(&mut self) -> Result<()> {
        if !self.log_busy {
            // Empty Ready batches only fence earlier writes. They need no storage work.
            while self
                .log_queue
                .front()
                .is_some_and(|task| task.work.as_ref().is_some_and(log::Job::is_barrier))
            {
                let task = self.log_queue.pop_front().unwrap();
                self.finish_log_task(task)?;
            }
            // Combine queued Ready work without adding a timer or crossing other job types.
            while self.log_queue.len() >= 2
                && self
                    .log_queue
                    .iter()
                    .take(2)
                    .all(|task| task.work.as_ref().is_some_and(log::Job::can_batch))
            {
                let mut task = self.log_queue.pop_front().unwrap();
                let next = self.log_queue.pop_front().unwrap();
                task.work.as_mut().unwrap().append(next.work.unwrap())?;
                task.bytes += next.bytes;
                task.ready = next.ready.or(task.ready);
                task.messages.extend(next.messages);
                task.reads.extend(next.reads);
                task.responses.extend(next.responses);
                self.log_queue.push_front(task);
            }
            if let Some(task) = self.log_queue.front_mut() {
                let job = task.work.take().context("log job already submitted")?;
                let reading = matches!(job, log::Job::Fetch(_));
                self.log_worker.submit(log::Work {
                    id: task.id,
                    job,
                    delay: Duration::from_millis(if reading {
                        self.log_read_delay_ms
                    } else {
                        self.log_delay_ms
                    }),
                    fail: if reading {
                        self.log_read_fail
                    } else {
                        self.log_fail
                    },
                })?;
                self.log_busy = true;
            }
        }
        Ok(())
    }

    fn complete_log(&mut self, completed: log::Completion) -> Result<()> {
        ensure!(self.log_busy, "unexpected log completion");
        let mut task = self.log_queue.pop_front().context("missing log job")?;
        ensure!(task.id == completed.id, "log completion is out of order");
        self.log_bytes -= task.bytes;
        self.log_snapshot_bytes -= task.snapshot_bytes;
        self.log_busy = false;
        self.raw.mut_store().complete(completed.durable)?;
        if let Some(stats) = completed.stats {
            if let Some(response) = task.stats_response.take() {
                let _ = response.send(Ok(serde_json::to_value(stats)?));
            }
        }
        if let Some((fetch, entries)) = completed.fetched {
            self.raw.mut_store().fetched(&fetch, entries)?;
            self.raw.on_entries_fetched(fetch.context);
        }
        self.finish_log_task(task)?;
        self.start_log()?;
        Ok(())
    }

    fn finish_log_task(&mut self, task: LogTask) -> Result<()> {
        if let Some(number) = task.ready {
            crate::failpoint::hit("raft-before-advance");
            self.raw.on_persist_ready(number);
        }
        if let Some(index) = task.checkpoint {
            self.checkpoint_running = false;
            self.state.durable_applied = self.state.durable_applied.max(index);
        }
        self.confirm_reads(task.reads);
        self.send(task.messages);
        for response in task.responses {
            let _ = response.send(Ok(json!({"accepted": true})));
        }
        Ok(())
    }

    #[cfg(test)]
    async fn drain_log(&mut self) -> Result<()> {
        while let Some(completed) = self.log_worker.step()? {
            self.complete_log(completed)?;
            self.drive_ready().await?;
        }
        Ok(())
    }

    fn enqueue_query(&mut self, query: Query) {
        if self.queries.len() >= apply::MAX_QUERIES {
            query.reject(self.error("too many queued state reads"));
        } else {
            self.queries.push_back(query);
        }
    }

    fn submit_apply(&mut self, job: Job, target: u64) -> Result<()> {
        ensure!(!self.apply_busy, "apply worker already has a job");
        self.worker.submit(Work {
            job,
            header: self.header(),
            delay: Duration::from_millis(self.apply_delay_ms),
        })?;
        self.apply_busy = true;
        self.apply_target = target;
        Ok(())
    }

    fn schedule_apply(&mut self) -> Result<()> {
        if self.apply_busy || self.raw.store().snapshot_pending() {
            return Ok(());
        }
        let snapshot_index = self.raw.store().snapshot_index();
        if self.state.applied < snapshot_index {
            return self.submit_apply(
                Job::Snapshot(self.raw.store().recovery_snapshot().clone()),
                snapshot_index,
            );
        }
        let checkpoint_due = (self.membership_checkpoint > snapshot_index
            && self.state.applied >= self.membership_checkpoint)
            || self
                .checkpoint_waiters
                .iter()
                .any(|(index, _)| *index <= self.state.applied)
            || (self.options.snapshot_every > 0
                && self.state.applied.saturating_sub(snapshot_index)
                    >= self.options.snapshot_every);
        if checkpoint_due && !self.checkpoint_running && self.state.applied > snapshot_index {
            return self.start_checkpoint();
        }
        if self.prefer_query {
            if let Some(query) = self.queries.pop_front() {
                self.prefer_query = false;
                return self.submit_apply(Job::Query(query), self.state.applied);
            }
        }
        let committed = self.raw.store().commit_index().min(self.committed_ready);
        if self.state.applied < committed {
            let low = self.state.applied + 1;
            let high = (low + apply::BATCH_ENTRIES).min(committed + 1);
            let entries = self.raw.store().entries(
                low,
                high,
                Some(apply::BATCH_BYTES),
                raft::GetEntriesContext::empty(false),
            )?;
            let target = entries.last().context("empty apply batch")?.index;
            ensure!(
                target <= self.raw.store().commit_index(),
                "apply exceeds durable commit"
            );
            self.prefer_query = true;
            return self.submit_apply(Job::Entries(entries), target);
        }
        if let Some(query) = self.queries.pop_front() {
            return self.submit_apply(Job::Query(query), self.state.applied);
        }
        Ok(())
    }

    fn complete_apply(&mut self, completed: Completion) -> Result<()> {
        ensure!(self.apply_busy, "unexpected apply completion");
        ensure!(
            completed.state.applied == self.apply_target,
            "apply completion has the wrong index"
        );
        ensure!(
            completed.state.applied >= self.state.applied,
            "apply position regressed"
        );
        ensure!(
            completed.state.applied <= self.raw.store().commit_index(),
            "apply completion exceeds durable commit"
        );
        let durable = self
            .state
            .durable_applied
            .max(completed.state.durable_applied);
        self.state = completed.state;
        if *self.applied_notify.borrow() != self.state.applied {
            self.applied_notify.send_replace(self.state.applied);
        }
        self.state.durable_applied = durable;
        self.apply_busy = false;
        self.finish_revision_waiters();

        self.sync_lease_deadlines();
        self.expire_pending();
        self.expire_reads();
        match completed.output {
            Output::Entries(replies) => {
                for command in replies {
                    if let Some(change) = &command.change {
                        self.raw.apply_conf_change(change)?;
                        self.membership_checkpoint = self.state.applied;
                    }
                    if let Some(LeaseCommand::Expire { id, generation }) = command.lease {
                        self.lease_expiring.remove(&(id, generation));
                    }
                    if let Some(pending) = self.pending.remove(&command.id) {
                        let reply = match command.reply {
                            Reply::Encoded(value) => value,
                            Reply::Lease(mut outcome) => {
                                if let Ok(response) = &mut outcome.result {
                                    if let LeaseReply::TimeToLive(response) = response {
                                        if response.ttl >= 0 {
                                            if let Some((_, deadline)) =
                                                self.lease_deadlines.get(&response.id)
                                            {
                                                response.ttl = deadline
                                                    .saturating_duration_since(Instant::now())
                                                    .as_secs()
                                                    as i64;
                                            }
                                        }
                                    }
                                    self.fill_header(response.header_mut());
                                }
                                serde_json::to_value(outcome)?
                            }
                        };
                        let _ = pending.response.send(Ok(reply));
                    }
                }
            }
            Output::Snapshot => {
                self.installed_snapshots += 1;
                self.options.join = false;
            }
            Output::Query {
                response,
                value,
                require_leader,
            } => {
                let result = if require_leader && self.raw.raft.leader_id == 0 {
                    Err(self.error("no leader"))
                } else {
                    Ok(value)
                };
                let _ = response.send(result);
            }
            Output::Linear { id, value } => self.finish_read(id, value),
            Output::Checkpoint => {}
        }
        self.raw.advance_apply_to(self.state.applied);
        self.raw.mut_store().applied_to(self.state.applied);
        self.sync_members();
        Ok(())
    }

    #[cfg(test)]
    async fn drain_apply(&mut self) -> Result<()> {
        while let Some(completed) = self.worker.step().await? {
            self.complete_apply(completed)?;
            self.drive_ready().await?;
        }
        Ok(())
    }

    fn sync_lease_deadlines(&mut self) {
        if self.raw.raft.state != StateRole::Leader || self.lease_term != self.raw.raft.term {
            self.lease_deadlines.clear();
            self.lease_expiring.clear();
            self.lease_term = self.raw.raft.term;
        }
        if self.raw.raft.state != StateRole::Leader {
            return;
        }
        self.lease_deadlines
            .retain(|id, _| self.state.leases.contains_key(id));
        for (&id, lease) in &self.state.leases {
            let entry = self
                .lease_deadlines
                .entry(id)
                .or_insert((0, Instant::now()));
            if entry.0 != lease.generation {
                *entry = (
                    lease.generation,
                    Instant::now() + Duration::from_secs(lease.ttl as u64),
                );
            }
        }
    }

    fn expire_leases(&mut self) -> Result<()> {
        self.sync_lease_deadlines();
        if self
            .raw
            .store()
            .commit_index()
            .max(self.raw.raft.raft_log.committed)
            .saturating_sub(self.state.applied)
            >= apply::MAX_APPLY_LAG
            || self.log_queue.len() >= log::MAX_JOBS
            || self.log_bytes >= log::MAX_BYTES
        {
            return Ok(());
        }
        let now = Instant::now();
        for (&id, &(generation, deadline)) in &self.lease_deadlines {
            if deadline > now || self.lease_expiring.contains(&(id, generation)) {
                continue;
            }
            let command = RaftCommand::Lease {
                origin: self.options.id,
                id: uuid::Uuid::new_v4().to_string(),
                lease: LeaseCommand::Expire { id, generation },
            };
            let data = serde_json::to_vec(&command)?;
            if !self.raw.store().admits(
                self.raw.raft.raft_log.unstable_entries(),
                &[],
                data.len() + cache::ENTRY_OVERHEAD + 64,
            ) {
                break;
            }
            if self.raw.propose(vec![], data).is_ok() {
                self.lease_expiring.insert((id, generation));
            }
        }
        Ok(())
    }

    fn propose(&mut self, command: RaftCommand, response: Response) -> Result<()> {
        if self.raw.raft.state != StateRole::Leader {
            let _ = response.send(Err(self.error("not leader")));
        } else if self
            .raw
            .store()
            .commit_index()
            .max(self.raw.raft.raft_log.committed)
            .saturating_sub(self.state.applied)
            >= apply::MAX_APPLY_LAG
            || self.log_queue.len() >= log::MAX_JOBS
            || self.log_bytes >= log::MAX_BYTES
        {
            let _ = response.send(Err(self.error("state apply backlog is full")));
        } else if self.pending.len() >= 1024 {
            let _ = response.send(Err(self.error("too many pending requests")));
        } else {
            let data = serde_json::to_vec(&command)?;
            if data.len() + 64 > cache::MAX_ENTRY_BYTES
                || !self.raw.store().admits(
                    self.raw.raft.raft_log.unstable_entries(),
                    &[],
                    data.len() + cache::ENTRY_OVERHEAD + 64,
                )
            {
                let _ = response.send(Err(self.error("pending log memory quota is full")));
                return Ok(());
            }
            let proposed = if let RaftCommand::Cluster { request, .. } = &command {
                if matches!(
                    request,
                    crate::cluster::Command::List(_)
                        | crate::cluster::Command::Update(_)
                        | crate::cluster::Command::Publish { .. }
                ) {
                    self.raw.propose(vec![], data)
                } else {
                    let mut change = request.change();
                    change.context = data.into();
                    self.raw.propose_conf_change(vec![], change)
                }
            } else {
                self.raw.propose(vec![], data)
            };
            match proposed {
                Ok(()) => {
                    self.pending.insert(
                        command.id().to_owned(),
                        Pending {
                            response,
                            started: Instant::now(),
                            term: self.raw.raft.term,
                        },
                    );
                }
                Err(error) => {
                    let _ = response.send(Err(self.error(&error.to_string())));
                }
            }
        }
        Ok(())
    }

    fn header(&self) -> Header {
        Header {
            cluster: self
                .options
                .cluster_id
                .bytes()
                .fold(0xcbf29ce484222325u64, |hash, byte| {
                    (hash ^ u64::from(byte)).wrapping_mul(0x100000001b3)
                }),
            member: self.options.id,
            term: self.raw.raft.term,
        }
    }

    fn fill_header(&self, header: Option<&mut ResponseHeader>) {
        self.header().fill(header);
    }

    fn send(&mut self, messages: Vec<Message>) {
        for message in messages {
            if self.raw.raft.state == StateRole::Leader {
                crate::failpoint::hit("raft-before-leader-send");
            } else {
                crate::failpoint::hit("raft-before-follower-send");
            }
            let peer = message.to;
            let snapshot = message.get_msg_type() == MessageType::MsgSnapshot;
            let sent = !self.blocked.contains(&peer)
                && self
                    .senders
                    .get(&peer)
                    .is_some_and(|sender| sender.try_send(message).is_ok());
            if !sent {
                self.send_failures.push((peer, snapshot));
            }
        }
    }

    fn start_checkpoint(&mut self) -> Result<()> {
        if self.checkpoint_running || self.apply_busy {
            return Ok(());
        }
        if self.state.applied <= self.raw.store().snapshot_index() {
            return Ok(());
        }
        self.submit_apply(
            Job::Checkpoint(Duration::from_millis(self.snapshot_delay_ms)),
            self.state.applied,
        )?;
        self.checkpoint_running = true;
        Ok(())
    }

    fn finish_checkpoint_waiters(&mut self) -> Result<()> {
        let mut waiting = vec![];
        for (index, response) in std::mem::take(&mut self.checkpoint_waiters) {
            if response.is_closed() {
                continue;
            }
            if index <= self.raw.store().snapshot_index() && index <= self.state.applied {
                let _ = response.send(Ok(self.status()?));
            } else {
                waiting.push((index, response));
            }
        }
        self.checkpoint_waiters = waiting;
        if !self.checkpoint_waiters.is_empty() {
            self.schedule_apply()?;
        }
        Ok(())
    }

    fn expire_pending(&mut self) {
        let is_leader = self.raw.raft.state == StateRole::Leader;
        let error = self.error("request outcome unknown: leadership changed or request timed out");
        let expired: Vec<_> = self
            .pending
            .iter()
            .filter(|(_, p)| {
                !is_leader
                    || p.term != self.raw.raft.term
                    || p.response.is_closed()
                    || p.started.elapsed() > Duration::from_secs(5)
            })
            .map(|(id, _)| id.clone())
            .collect();
        for id in expired {
            if let Some(pending) = self.pending.remove(&id) {
                let _ = pending.response.send(Err(error.clone()));
            }
        }
    }

    fn error(&self, message: &str) -> ApiError {
        ApiError {
            error: message.to_owned(),
            leader_id: self.raw.raft.leader_id,
        }
    }

    fn status(&self) -> Result<Value> {
        Ok(json!({
            "id": self.options.id, "cluster_id": self.options.cluster_id,
            "role": format!("{:?}", self.raw.raft.state), "leader_id": self.raw.raft.leader_id,
            "term": self.raw.raft.term, "commit_index": self.raw.store().commit_index(),
            "applied_index": self.state.applied, "durable_applied_index": self.state.durable_applied,
            "snapshot_index": self.raw.store().snapshot_index(),
            "first_log_index": self.raw.store().snapshot_index() + 1, "last_log_index": self.raw.store().durable.last_index,
            "sync_writes": self.raw.store().durable.sync_writes, "pending": self.pending.len(),
            "blocked_peers": self.blocked, "installed_snapshots": self.installed_snapshots,
            "recovered_from_index": self.recovered_from_index,
            "checkpoint_running": self.checkpoint_running,
            "snapshot_delay_ms": self.snapshot_delay_ms,
            "apply_busy": self.apply_busy, "apply_target": self.apply_target,
            "apply_lag": self.raw.store().commit_index().saturating_sub(self.state.applied),
            "queued_state_reads": self.queries.len(), "apply_delay_ms": self.apply_delay_ms,
            "log_busy": self.log_busy, "queued_log_jobs": self.log_queue.len(),
            "queued_log_bytes": self.log_bytes, "queued_snapshot_bytes": self.log_snapshot_bytes, "log_delay_ms": self.log_delay_ms,
            "log_fail": self.log_fail, "pending_read_indexes": self.reads.len(),
            "pending_revision_waiters": self.revision_waiters.len(),
            "raft_commit_index": self.raw.raft.raft_log.committed,
            "cached_last_log_index": self.raw.store().last_index()?,
            "log_memory": self.raw.store().stats(),
            "log_read_delay_ms": self.log_read_delay_ms, "log_read_fail": self.log_read_fail,
            "revision": self.state.revision,
            "compact_revision": self.state.compact_revision,
        }))
    }
}

async fn transport(
    peer: u64,
    address: String,
    cluster_id: String,
    mut messages: mpsc::Receiver<Message>,
    events: mpsc::Sender<Event>,
    client: reqwest::Client,
) {
    while let Some(message) = messages.recv().await {
        let snapshot = message.get_msg_type() == MessageType::MsgSnapshot;
        let result: Result<()> = async {
            let response = client
                .post(format!("{}/raft", crate::cluster::url(&address)))
                .header("x-cluster-id", &cluster_id)
                .timeout(if snapshot {
                    Duration::from_secs(30)
                } else {
                    Duration::from_secs(2)
                })
                .body(message.write_to_bytes()?)
                .send()
                .await?;
            ensure!(
                response.status().is_success(),
                "peer returned {}",
                response.status()
            );
            response.bytes().await.context("read peer response")?;
            Ok(())
        }
        .await;
        {
            let _ = events
                .send(Event::NetworkResult {
                    peer,
                    snapshot,
                    success: result.is_ok(),
                })
                .await;
        }
    }
}
