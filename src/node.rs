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
mod forwarding;
mod lease;
mod log;
mod maintenance;
mod proposal;
pub use forwarding::ForwardView;
mod quorum_put;
mod read;
mod ready;
mod response;
mod schedule;
use crate::peer::transport;
use apply::{AppliedState, Completion, Header, Job, Output, Query, Reply, Work, Worker};
use response::ClientResponse;

#[cfg(test)]
mod dst;
#[cfg(test)]
mod tests;

pub type Response = oneshot::Sender<Result<Value, ApiError>>;
pub type KvResponse = oneshot::Sender<Result<crate::mvcc::Outcome, ApiError>>;

pub enum Event {
    SubscribeApply(oneshot::Sender<tokio::sync::watch::Receiver<u64>>),
    WaitRevision(i64, Response),
    Authenticated(crate::auth::Credentials, Box<Event>),
    Auth(crate::auth::Command, Response),
    AuthLookup(String, Response),
    Maintenance(crate::maintenance::Command, Response),
    Cluster(crate::cluster::Command, Response),
    SubscribeForwarding(oneshot::Sender<ForwardView>),
    Propose(Request, Response),
    Kv(RequestOp, KvResponse),
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
    PeerFrame(Message, usize, Response),
    Status(Response),
    Metrics(oneshot::Sender<crate::metrics::Sample>),
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
    response: ClientResponse,
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
    pub(crate) metrics: std::sync::Arc<crate::metrics::Member>,
    last_observed_leader: u64,
    traffic: BTreeMap<u64, std::sync::Arc<crate::metrics::PeerTraffic>>,
    applied_notify: tokio::sync::watch::Sender<u64>,
    forwarding: forwarding::Publisher,
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
    read_executor: read::executor::Executor,
    read_queries: VecDeque<Query>,
    reads: BTreeMap<u64, read::PendingRead>,
    revision_waiters: VecDeque<RevisionWaiter>,
    read_nonce: [u8; 16],
    read_sequence: u64,
    state: AppliedState,
    worker: Worker,
    apply_busy: bool,
    apply_target: u64,
    committed_ready: u64,
    prepared_puts: Option<crate::state::put_batch::PreparedPuts>,
    quorum_reply_index: u64,
    queries: VecDeque<Query>,
    prefer_query: bool,
    apply_delay_ms: u64,
    options: NodeOptions,
    senders: BTreeMap<u64, mpsc::Sender<Message>>,
    transports: tokio::task::JoinSet<()>,
    pending: HashMap<String, Pending>,
    put_proposals: proposal::Group,
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
        let mut storage = RaftStorage::open(
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
            // Flushed state proves this prefix was committed, even if its commit marker was lost.
            if state.applied > storage.commit_index() {
                storage.persist_commit(state.applied)?;
            }
            let recovered_from_index = state.applied;
            state.install_snapshot(storage.recovery_snapshot()).await?;
            if state.members.members.is_empty() && !options.join {
                let mut members = crate::cluster::Store::initial(&options.peers);
                for (&id, address) in &options.client_peers {
                    if let Some(member) = members.members.get_mut(&id) {
                        member.client_ur_ls = vec![crate::cluster::url(address)];
                    }
                }
                state.members = std::sync::Arc::new(members);
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
        let mut traffic = BTreeMap::<u64, std::sync::Arc<crate::metrics::PeerTraffic>>::new();
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
                    options.transport.clone(),
                    traffic.entry(peer).or_default().clone(),
                ));
            }
            senders
        };
        let applied = AppliedState::new(&state)?;
        let metrics = std::sync::Arc::new(crate::metrics::Member::new(
            storage.wal_metrics.clone(),
            state.admin.clone(),
            state.metrics.registry.clone(),
        )?);
        let worker = Worker::start(state, events.clone(), manual_apply);
        let log_worker = log::Worker::start(storage, manual_apply)?;
        let mut node = Self {
            metrics,
            last_observed_leader: 0,
            traffic,
            applied_notify: tokio::sync::watch::channel(applied.applied).0,
            forwarding: forwarding::Publisher::new(&options.peers, applied.revision),
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
            read_executor: read::executor::Executor::new(manual_apply),
            read_queries: VecDeque::new(),
            reads: BTreeMap::new(),
            revision_waiters: VecDeque::new(),
            read_nonce: *uuid::Uuid::new_v4().as_bytes(),
            read_sequence: 0,
            apply_target: applied.applied,
            committed_ready: applied.applied,
            prepared_puts: None,
            quorum_reply_index: 0,
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
            put_proposals: proposal::Group::default(),
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
        self.read_executor.shutdown().await;
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
                read = self.read_executor.next(), if self.read_executor.len() > 0 => {
                    self.complete_read(read?)?;
                }
                event = events.recv() => {
                    let Some(event) = event else { return Ok(()); };
                    self.handle(event, &mut step_responses).await?;
                    // Let ready tasks add proposals before starting the next durable log write.
                    if !self.pending.is_empty() || !self.put_proposals.is_empty() {
                        tokio::task::yield_now().await;
                    }
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
        let event = if let Event::PeerFrame(message, bytes, tx) = event {
            if let Some(meter) = self.traffic.get(&message.from) {
                meter
                    .received
                    .fetch_add(bytes as u64, std::sync::atomic::Ordering::Relaxed);
            }
            Event::Step(message, tx)
        } else {
            event
        };
        let (credentials, event) = match event {
            Event::Authenticated(credentials, event) => (Some(credentials), *event),
            event => (None, event),
        };
        let event = match event {
            Event::Kv(request, tx) if crate::mvcc::batchable_put(&request) => {
                return self.queue_put(request, credentials, tx);
            }
            event => event,
        };
        self.flush_puts();
        match event {
            Event::PeerFrame(..) | Event::Authenticated(..) => {
                anyhow::bail!("nested authentication context")
            }
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
            Event::SubscribeForwarding(response) => {
                let _ = response.send(self.forwarding.subscribe());
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
                    RaftCommand::Diagnostic(Command {
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
            Event::Kv(request, response) => {
                let response = ClientResponse::Kv(response);
                if crate::mvcc::read_only(&request, true) {
                    self.enqueue_query(Query::Read(request, credentials, response));
                } else if crate::mvcc::read_only(&request, false) {
                    self.linear_read_auth(request, credentials, response);
                } else {
                    self.propose(
                        RaftCommand::Etcd {
                            origin: self.options.id,
                            id: uuid::Uuid::new_v4().to_string(),
                            etcd: request,
                        }
                        .secure(credentials),
                        response,
                    )?;
                }
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
                self.enqueue_query(Query::Read(request, credentials, response.into()));
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
            Event::Metrics(response) => {
                let id = self.options.id;
                let _ = response.send(crate::metrics::Sample {
                    id,
                    leader: self.raw.raft.leader_id,
                    learner: self
                        .state
                        .members
                        .members
                        .get(&id)
                        .is_some_and(|m| m.is_learner),
                    committed: self.raw.raft.raft_log.committed,
                    applied: self.state.applied,
                    pending: self.pending.len(),
                    quota: self.options.quota_bytes,
                    used: self.state.logical_size,
                    revision: self.state.revision,
                    compact: if self.state.compact_revision == 0 {
                        -1
                    } else {
                        self.state.compact_revision
                    },
                    auth_revision: self.state.auth.revision,
                    traffic: self
                        .traffic
                        .iter()
                        .map(|(&id, m)| {
                            (
                                id,
                                m.sent.load(std::sync::atomic::Ordering::Relaxed),
                                m.received.load(std::sync::atomic::Ordering::Relaxed),
                            )
                        })
                        .collect(),
                    peers: self
                        .state
                        .members
                        .members
                        .keys()
                        .map(|&id| (id, self.peer_health.contains_key(&id)))
                        .collect(),
                });
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

    fn backlogged(&self) -> bool {
        self.raw
            .store()
            .commit_index()
            .max(self.raw.raft.raft_log.committed)
            .saturating_sub(self.state.applied)
            >= apply::MAX_APPLY_LAG
            || self.log_queue.len() >= log::MAX_JOBS
            || self.log_bytes >= log::MAX_BYTES
    }

    fn propose(&mut self, command: RaftCommand, response: impl Into<ClientResponse>) -> Result<()> {
        let response = response.into();
        if self.raw.raft.state != StateRole::Leader {
            response.error(self.error("not leader"));
        } else if self.backlogged() {
            response.error(self.error("state apply backlog is full"));
        } else if self.pending.len() >= 1024 {
            response.error(self.error("too many pending requests"));
        } else {
            let data = command.encode()?;
            if data.len() + 64 > cache::MAX_ENTRY_BYTES
                || !self.raw.store().admits(
                    self.raw.raft.raft_log.unstable_entries(),
                    &[],
                    data.len() + cache::ENTRY_OVERHEAD + 64,
                )
            {
                response.error(self.error("pending log memory quota is full"));
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
                    self.metrics.failed_proposals.inc();
                    response.error(self.error(&error.to_string()));
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
        if self.checkpoint_running || self.apply_busy || self.prepared_puts.is_some() {
            return Ok(());
        }
        if self.state.applied <= self.raw.store().snapshot_index() {
            return Ok(());
        }
        self.submit_apply(
            Job::Checkpoint(
                Duration::from_millis(self.snapshot_delay_ms),
                self.raw.store().recovery_snapshot().clone(),
            ),
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
        let now = Instant::now();
        let expired: Vec<_> = self
            .pending
            .iter()
            .filter(|(_, p)| {
                !is_leader
                    || p.term != self.raw.raft.term
                    || p.response.is_closed()
                    || now.duration_since(p.started) > Duration::from_secs(5)
            })
            .map(|(id, _)| id.clone())
            .collect();
        for id in expired {
            if let Some(pending) = self.pending.remove(&id) {
                self.metrics.failed_proposals.inc();
                pending.response.error(
                    self.error("request outcome unknown: leadership changed or request timed out"),
                );
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
        let mut status = json!({
            "id": self.options.id, "cluster_id": self.options.cluster_id,
            "role": format!("{:?}", self.raw.raft.state), "leader_id": self.raw.raft.leader_id,
            "term": self.raw.raft.term, "commit_index": self.raw.store().commit_index(),
            "applied_index": self.state.applied,
            "state_write_batches": self.state.write_batches,
            "active_read_jobs": self.read_executor.len(), "queued_reads": self.read_queries.len(),
            "durable_applied_index": self.state.durable_applied,
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
        });
        status["quorum_reply_index"] = self.quorum_reply_index.into();
        Ok(status)
    }
}
