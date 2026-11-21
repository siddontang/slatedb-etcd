use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use anyhow::{ensure, Context, Result};
use prost::Message as ProtobufMessage;
use raft::eraftpb::{Entry, EntryType, Message, MessageType};
use raft::{Config, RawNode, SnapshotStatus, StateRole, Storage};
use serde_json::{json, Value};
use tokio::sync::{mpsc, oneshot};

use crate::lease::{LeaseCommand, LeaseReply};
use crate::proto::etcdserverpb::{CompactionRequest, RequestOp, ResponseHeader};
use crate::state::StateMachine;
use crate::storage::{PreparedCheckpoint, RaftStorage};
use crate::{ApiError, Command, RaftCommand, Request};

mod apply;
/// ReadIndex reads that may wait in the Raft owner's table. One quorum round confirms
/// every read in a batch, so a larger table lets one round cover more clients.
pub(crate) const MAX_PENDING_READS: usize = 256;
/// Apply barriers that may wait for the local state to reach a revision.
pub(crate) const MAX_APPLY_BARRIERS: usize = apply::MAX_QUERIES;
mod cache;
pub use cache::LogMemory;
mod cluster;
mod compaction;
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
mod watch;
use crate::peer::transport_to;
use apply::{AppliedState, Completion, Header, Job, Output, Query, Reply, Work, Worker};
use response::ClientResponse;
pub use response::KvReply;

#[cfg(test)]
mod dst;
#[cfg(test)]
mod tests;

pub type Response = oneshot::Sender<Result<Value, ApiError>>;
pub type KvResponse = oneshot::Sender<Result<KvReply, ApiError>>;
/// Typed lease reply. It skips the JSON round trip on the KeepAlive path.
pub type LeaseResponse = oneshot::Sender<Result<crate::mvcc::Outcome<LeaseReply>, ApiError>>;

pub enum Event {
    SubscribeWatch(oneshot::Sender<crate::watch::Subscription>),
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
    /// One KeepAlive from a gRPC stream, with the caller's credentials.
    KeepAlive {
        id: i64,
        credentials: crate::auth::Credentials,
        response: LeaseResponse,
    },
    Step(Message, Response),
    PeerFrame(Message, usize, Response),
    Status(Response),
    Metrics(oneshot::Sender<crate::metrics::Sample>),
    LogStats(Response),
    Checkpoint(Response),
    LocalValue(String, Response),
    BlockPeers(BTreeSet<u64>, Response),
    SnapshotBuilt(apply::Built),
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

/// Flush SlateDB for log truncation about once per L0 SST instead of once per entry interval.
pub const DEFAULT_SNAPSHOT_MIN_BYTES: u64 = 64 * 1024 * 1024;

pub struct NodeOptions {
    pub id: u64,
    pub cluster_id: String,
    pub peers: BTreeMap<u64, String>,
    pub dir: PathBuf,
    pub snapshot_every: u64,
    /// Automatic log truncation also waits for this many appended log bytes. Each truncation
    /// flushes SlateDB with synced writes, so small flushes cost more than the log they free.
    pub snapshot_min_bytes: u64,
    pub log_memory: LogMemory,
    pub state_cache: crate::state::StateCache,
    pub quota_bytes: i64,
    pub strict_reconfiguration: bool,
    pub join: bool,
    pub client_peers: BTreeMap<u64, String>,
    pub transport: crate::tls::Transport,
    pub object_store: crate::object::Config,
    pub require_peer_tls: bool,
}

impl NodeOptions {
    fn next_checkpoint(&self, saved: u64) -> Option<u64> {
        let interval = self.snapshot_every;
        if interval == 0 {
            return None;
        }
        // Each configured member gets one position in the same log interval.
        let position = self.peers.range(..self.id).count() as u128;
        let phase = (u128::from(interval) * position / self.peers.len() as u128) as u64;
        let remainder = saved % interval;
        let distance = if remainder < phase {
            phase - remainder
        } else {
            interval - (remainder - phase)
        };
        saved.checked_add(distance)
    }

    fn effective_quota(&self) -> i64 {
        match self.quota_bytes {
            0 => 2 * 1024 * 1024 * 1024,
            limit => limit,
        }
    }
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

/// Raft tick interval. Election timeouts count these ticks.
pub(crate) const TICK: Duration = Duration::from_millis(100);

pub struct Node {
    pub(crate) metrics: std::sync::Arc<crate::metrics::Member>,
    last_observed_leader: u64,
    traffic: BTreeMap<u64, std::sync::Arc<crate::metrics::PeerTraffic>>,
    watch_feed: watch::Publisher,
    forwarding: forwarding::Publisher,
    compaction: compaction::Flush,
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
    built_checkpoint: Option<(u64, raft::eraftpb::ConfState, Option<PreparedCheckpoint>)>,
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
    /// Submitted apply jobs in order: target index, and whether a later Entries job may follow.
    apply_jobs: VecDeque<(u64, bool)>,
    committed_ready: u64,
    prepared_puts: Option<crate::state::put_batch::PreparedPuts>,
    quorum_reply_index: u64,
    queries: VecDeque<Query>,
    prefer_query: bool,
    apply_delay_ms: u64,
    options: NodeOptions,
    senders: BTreeMap<u64, mpsc::Sender<Message>>,
    /// The URL that each peer sender uses on its next dial.
    peer_addresses: BTreeMap<u64, tokio::sync::watch::Sender<String>>,
    transports: tokio::task::JoinSet<()>,
    pending: HashMap<String, Pending>,
    put_proposals: proposal::Group,
    waiting_proposals: proposal::Waiting,
    blocked: BTreeSet<u64>,
    installed_snapshots: u64,
    recovered_from_index: u64,
    send_failures: Vec<(u64, bool)>,
    checkpoint_running: bool,
    // Full backlog checks by cause: apply lag, queued log jobs, queued log bytes.
    backlog_rejects: [u64; 3],
    // Log bytes appended since the last checkpoint started.
    appended_bytes: u64,
    // Count full images started for peers or manual checkpoints.
    snapshot_images: u64,
    next_checkpoint: Option<u64>,
    checkpoint_waiters: Vec<(u64, Response)>,
    snapshot_delay_ms: u64,
    lease_term: u64,
    lease_deadlines: BTreeMap<i64, (u64, Instant)>,
    lease_expiring: BTreeSet<(i64, u64)>,
    compact_step: Option<(u64, u64)>,
    compact_ready_at: Option<Instant>,
    lease_view: Option<std::sync::Arc<BTreeMap<i64, crate::lease::LeaseRecord>>>,
    /// Whether this leader term has built its lease deadlines.
    lease_promoted: bool,
    renewals: lease::Renewals,
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
            .open(
                &options.dir,
                &options.cluster_id,
                options.id,
                options.state_cache,
            )
            .await?;
        let recovered = async {
            state.quota = options.effective_quota().max(0) as u64;
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
        let mut peer_addresses = BTreeMap::new();
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
                let (address_tx, address_rx) = tokio::sync::watch::channel(address.clone());
                peer_addresses.insert(peer, address_tx);
                transports.spawn(transport_to(
                    peer,
                    address_rx,
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
        let compaction = compaction::Flush::new(&state);
        let worker = Worker::start(state, events.clone(), manual_apply);
        let log_worker = log::Worker::start(storage, manual_apply)?;
        let next_checkpoint = options.next_checkpoint(raw.store().snapshot_index());
        let mut node = Self {
            metrics,
            last_observed_leader: 0,
            traffic,
            watch_feed: watch::Publisher::new(crate::watch::FeedState {
                view: applied.read.clone(),
                header: ResponseHeader::default(),
                leader: 0,
            }),
            forwarding: forwarding::Publisher::new(
                &options.peers,
                applied.revision,
                compaction.durable.subscribe(),
            ),
            compaction,
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
            apply_jobs: VecDeque::new(),
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
            peer_addresses,
            transports,
            pending: HashMap::new(),
            put_proposals: proposal::Group::default(),
            waiting_proposals: proposal::Waiting::default(),
            blocked: BTreeSet::new(),
            installed_snapshots: 0,
            recovered_from_index,
            send_failures: vec![],
            checkpoint_running: false,
            backlog_rejects: [0; 3],
            appended_bytes: 0,
            snapshot_images: 0,
            next_checkpoint,
            checkpoint_waiters: vec![],
            snapshot_delay_ms: 0,
            lease_term: 0,
            lease_deadlines: BTreeMap::new(),
            lease_expiring: BTreeSet::new(),
            compact_step: None,
            compact_ready_at: None,
            lease_view: None,
            lease_promoted: false,
            renewals: lease::Renewals::default(),
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
        let owner = self.run_loop(events);
        #[cfg(feature = "write-profile")]
        let probe = crate::write_profile::Probe::new(crate::write_profile::Stage::Raft);
        #[cfg(feature = "write-profile")]
        let owner = probe.run(owner);
        let result = tokio::select! {
            result = owner => result,
            _ = stop => Ok(()),
        };
        self.transports.shutdown().await;
        // Keep the Raft directory locked until the state worker has stopped.
        self.compaction.cancel();
        self.read_executor.shutdown().await;
        let apply = self.worker.shutdown().await;
        let log = self.log_worker.shutdown().await;
        result.and(apply).and(log)
    }

    async fn run_loop(&mut self, mut events: mpsc::Receiver<Event>) -> Result<()> {
        self.publish_watch(None);
        // Schedule committed log recovery before accepting client work.
        self.drive_ready().await?;
        let mut ticks = tokio::time::interval(TICK);
        ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut old_role = (
            self.raw.raft.state,
            self.raw.raft.term,
            self.raw.raft.leader_id,
        );
        loop {
            let mut step_responses = vec![];
            tokio::select! {
                _ = tokio::time::sleep_until(
                    self.compact_ready_at.unwrap_or_else(Instant::now).into()
                ), if self.compact_ready_at.is_some() => { self.compact_ready_at = None; }
                _ = ticks.tick() => { self.raw.tick(); self.retry_reads(); self.expire_leases()?; self.publish_member()?; }
                persisted = self.log_worker.next() => { self.complete_log(persisted?)?; }
                target = self.compaction.next(), if self.compaction.active() => {
                    self.finish_compaction_flush(target?)?;
                }
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
                self.publish_watch(None);
                tracing::info!(node = self.options.id, role = ?role.0, term = role.1,
                    leader = role.2, "Raft state changed");
                old_role = role;
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
                            db_size_quota: self.options.quota_bytes,
                            leader: self.raw.raft.leader_id,
                            raft_index: self.raw.store().commit_index(),
                            raft_term: self.raw.raft.term,
                            version: crate::ETCD_API_VERSION.into(),
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
            Event::Lease(LeaseCommand::KeepAlive { id }, response) => {
                self.keep_alive(id, credentials, response)?;
            }
            Event::KeepAlive {
                id,
                credentials,
                response,
            } => {
                self.keep_alive(id, Some(credentials), ClientResponse::Lease(response))?;
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
            Event::Step(message, response) => {
                if self.options.join
                    && self.state.applied == 0
                    && self.raw.store().snapshot_index() == 0
                    && message.get_msg_type() == MessageType::MsgAppend
                {
                    self.send(vec![Message {
                        msg_type: MessageType::MsgAppendResponse as i32,
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
            Event::SubscribeWatch(response) => {
                let _ = response.send(self.watch_feed.subscribe());
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
                    quota: self.options.effective_quota(),
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
                        .map(|(&id, meter)| (id, meter.clone()))
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
            Event::SnapshotBuilt(built) => self.checkpoint_built(built)?,
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
                } else if self.peer_health.remove(&peer).is_some() {
                    if let Some(meter) = self.traffic.get(&peer) {
                        meter
                            .disconnected
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
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

    /// Return true when a new proposal must wait. Count each full check by cause for `/status`.
    fn backlogged(&mut self) -> bool {
        let causes = [
            self.raw
                .store()
                .commit_index()
                .max(self.raw.raft.raft_log.committed)
                .saturating_sub(self.state.applied)
                >= apply::MAX_APPLY_LAG,
            self.log_queue.len() >= log::MAX_JOBS,
            self.log_bytes >= log::MAX_BYTES,
        ];
        for (count, hit) in self.backlog_rejects.iter_mut().zip(causes) {
            *count += u64::from(hit);
        }
        causes.contains(&true)
    }

    /// Propose a client write. Ordinary writes wait in a bounded FIFO while apply or log
    /// I/O is behind. Membership changes still fail at once because their checks ran now.
    fn propose(&mut self, command: RaftCommand, response: impl Into<ClientResponse>) -> Result<()> {
        let response = response.into();
        if self.raw.raft.state != StateRole::Leader {
            response.error(self.error("not leader"));
            return Ok(());
        }
        if self.pending.len() >= 1024 {
            response.error(self.error("too many pending requests"));
            return Ok(());
        }
        let data = command.encode()?;
        let id = command.id().to_owned();
        let change = match &command {
            RaftCommand::Cluster { request, .. }
                if !matches!(
                    request,
                    crate::cluster::Command::List(_)
                        | crate::cluster::Command::Update(_)
                        | crate::cluster::Command::Publish { .. }
                ) =>
            {
                Some(request.change())
            }
            _ => None,
        };
        let Some(mut change) = change else {
            if self.must_wait() {
                self.defer_command(id, data, response);
            } else {
                self.propose_data(id, data, response);
            }
            return Ok(());
        };
        if self.backlogged() {
            response.error(self.error("state apply backlog is full"));
            return Ok(());
        }
        if !self.admits_proposal(&data) {
            response.error(self.error("pending log memory quota is full"));
            return Ok(());
        }
        change.context = data.into();
        let proposed = self.raw.propose_conf_change(vec![], change);
        self.track_proposal(id, proposed, response);
        Ok(())
    }

    fn admits_proposal(&self, data: &[u8]) -> bool {
        data.len() + 64 <= cache::MAX_ENTRY_BYTES
            && self.raw.store().admits(
                self.raw.raft.raft_log.unstable_entries(),
                &[],
                data.len() + cache::ENTRY_OVERHEAD + 64,
            )
    }

    /// Append one normal entry now. The caller has checked leadership and capacity.
    fn propose_data(&mut self, id: String, data: Vec<u8>, response: ClientResponse) {
        if !self.admits_proposal(&data) {
            response.error(self.error("pending log memory quota is full"));
            return;
        }
        let proposed = self.raw.propose(vec![], data);
        self.track_proposal(id, proposed, response);
    }

    fn track_proposal(&mut self, id: String, proposed: raft::Result<()>, response: ClientResponse) {
        match proposed {
            Ok(()) => {
                self.pending.insert(
                    id,
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

    /// Publish the current view, header, and leader, then the optional event batch.
    fn publish_watch(&self, batch: Option<std::sync::Arc<crate::watch::Batch>>) {
        let mut header = ResponseHeader::default();
        self.fill_header(Some(&mut header));
        self.watch_feed.publish(
            crate::watch::FeedState {
                view: self.state.read.clone(),
                header,
                leader: self.raw.raft.leader_id,
            },
            batch,
        );
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
                if let Some(meter) = self.traffic.get(&peer) {
                    meter.failed(1);
                }
                self.send_failures.push((peer, snapshot));
            }
        }
    }

    fn start_checkpoint(&mut self, image: bool) -> Result<()> {
        if self.checkpoint_running || self.apply_busy || self.prepared_puts.is_some() {
            return Ok(());
        }
        let store = self.raw.store();
        let advances = self.state.applied > store.snapshot_index();
        // An image can replace a record without an image at the same index.
        let attaches = image && store.snapshot_index() > 0 && !store.has_image();
        if !advances && !attaches {
            return Ok(());
        }
        self.submit_apply(
            Job::Checkpoint {
                delay: Duration::from_millis(self.snapshot_delay_ms),
                dir: self.options.dir.join("raft"),
                image,
            },
            self.state.applied,
        )?;
        self.checkpoint_running = true;
        self.appended_bytes = 0;
        if image {
            self.snapshot_images += 1;
        }
        Ok(())
    }

    fn checkpoint_built(&mut self, built: apply::Built) -> Result<()> {
        ensure!(
            self.built_checkpoint.is_none(),
            "overlapping checkpoint results"
        );
        self.built_checkpoint = Some((built.index, built.conf, built.result?));
        Ok(())
    }

    /// Return true while a peer receives an image. Truncation would discard that image.
    fn sending_snapshot(&self) -> bool {
        self.raw
            .raft
            .prs()
            .iter()
            .any(|(_, progress)| progress.state == raft::ProgressState::Snapshot)
    }

    fn finish_checkpoint_waiters(&mut self) -> Result<()> {
        let mut waiting = vec![];
        for (index, response) in std::mem::take(&mut self.checkpoint_waiters) {
            if response.is_closed() {
                continue;
            }
            // A manual checkpoint completes only with a complete image.
            let store = self.raw.store();
            if index <= store.snapshot_index()
                && index <= self.state.applied
                && (index == 0 || store.has_image())
            {
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
        self.compaction.replies.retain(|waiter| !waiter.closed());
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
        #[cfg(feature = "write-profile")]
        {
            status["write_profile"] = crate::write_profile::snapshot();
        }
        status["quorum_reply_index"] = self.quorum_reply_index.into();
        status["snapshot_has_image"] = self.raw.store().has_image().into();
        status["snapshot_image_wanted"] = self.raw.store().image_wanted().into();
        status["snapshot_images_built"] = self.snapshot_images.into();
        status["backlog_full_checks"] = json!({
            "apply_lag": self.backlog_rejects[0],
            "log_jobs": self.backlog_rejects[1],
            "log_bytes": self.backlog_rejects[2],
        });
        Ok(status)
    }
}
