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
mod auto_compaction;
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
mod pause;
mod proposal;
pub use forwarding::ForwardView;
pub(crate) use forwarding::LocalRead;
pub(crate) use shared::WriterReport;
mod quorum_put;
mod quota;
mod read;
mod ready;
mod response;
mod schedule;
mod shared;
mod watch;
use crate::peer::transport_to;
use apply::{AppliedState, Completion, Header, Job, Output, Query, Reply, Work, Worker};
use response::ClientResponse;
pub use response::KvReply;

#[cfg(test)]
mod dst;

/// Reject a data directory that holds an interrupted snapshot restore.
fn ensure_no_restore_marker(dir: &std::path::Path) -> Result<()> {
    ensure!(
        !dir.join(crate::state::backup::RESTORE_MARKER).exists(),
        "the data directory holds an interrupted snapshot restore; remove it and restore again. \
         With S3, delete the member namespace first only if its owner.json matches the \
         object-store.json of this directory; otherwise restore with a new cluster ID or \
         --s3-prefix"
    );
    Ok(())
}

#[cfg(test)]
mod tests;

pub type Response = oneshot::Sender<Result<Value, ApiError>>;
pub type KvResponse = oneshot::Sender<Result<KvReply, ApiError>>;
/// Typed lease reply. It skips the JSON round trip on the KeepAlive path.
pub type LeaseResponse = oneshot::Sender<Result<crate::mvcc::Outcome<LeaseReply>, ApiError>>;

pub enum Event {
    SubscribeWatch(oneshot::Sender<crate::watch::Subscription>),
    /// Answer once this member applied every Put it already answered. A Watch create waits for
    /// it, then drains the feed, so that its header and start cover such a Put.
    WatchBarrier(oneshot::Sender<()>),
    WaitRevision(i64, Response),
    Authenticated(crate::auth::Credentials, Box<Event>),
    Auth(crate::auth::Command, Response),
    AuthLookup(String, Response),
    Maintenance(crate::maintenance::Command, Response),
    /// Stream a native backup of the applied state. See `state::backup`.
    Snapshot(oneshot::Sender<Result<crate::state::backup::Stream, crate::mvcc::KvError>>),
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
    /// The shared-mode publisher created this checkpoint with this row `F`. The leader
    /// announces it. The reply tells whether Raft took the proposal.
    ViewCheckpoint(uuid::Uuid, u64, Option<tokio::sync::oneshot::Sender<bool>>),
    /// A member reports the checkpoints that it pins. The leader keeps them.
    ViewAck {
        member: u64,
        checkpoints: std::collections::HashSet<uuid::Uuid>,
        /// A manual checkpoint on the member asks for a publication through this index.
        publish: Option<u64>,
        /// The process of a member that installs a base above its checkpoint row.
        coverage: Option<u64>,
        /// The health of the member's writer. An earlier binary sends none.
        writer: Option<shared::WriterReport>,
    },
    /// The first peer frame of a stream or request came from this process of `member`. An
    /// earlier binary sends no process.
    PeerProcess {
        member: u64,
        process: Option<u64>,
    },
    /// `member` refused a snapshot above its row that named process `target`.
    SnapshotRefused {
        member: u64,
        target: u64,
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
    /// Size of one Raft log file. Truncation frees disk space one whole file at a time.
    pub log_file_bytes: u64,
    pub log_memory: LogMemory,
    pub state_cache: crate::state::StateCache,
    pub quota_bytes: i64,
    /// Request `quota_bytes` again at startup with a new time, so that it replaces a newer
    /// cluster quota. See `node::quota`.
    pub quota_reassert: bool,
    /// Quota usage fraction that raises the early warning. Zero disables the warning.
    pub quota_warning_fraction: f64,
    /// Automatic compaction retention. `None` disables automatic compaction.
    pub auto_compaction: Option<crate::compactor::Retention>,
    pub strict_reconfiguration: bool,
    pub join: bool,
    pub client_peers: BTreeMap<u64, String>,
    pub transport: crate::tls::Transport,
    pub object_store: crate::object::Config,
    pub require_peer_tls: bool,
    /// The state mode. Only a new cluster can select shared mode.
    pub state_mode: crate::state::StateMode,
    /// Shared mode: the age of the oldest unflushed write that starts a flush and a checkpoint.
    pub view_max_age: Duration,
    /// Shared mode: the longest time that a read view may pin its base checkpoint.
    pub view_pin_max: Duration,
    /// Shared mode: the leader rejects client proposals above this overlay size. Apply waits
    /// above twice this size.
    pub overlay_max_bytes: u64,
    /// Shared mode: a new writer does not start while the local clock is behind the manifest
    /// clock by more than this.
    pub max_clock_skew: Duration,
    /// Shared mode: whether the leader deletes old view checkpoints.
    pub view_retention: crate::state::ViewRetention,
    /// Shared mode: the rows of a native snapshot that seed a new cluster.
    pub restore: Option<crate::state::shared::Restore>,
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

/// The usage of `quota` that raises the early warning, or `None` when it is off. Zero
/// disables the quota.
///
/// This is the first whole byte count at or above `quota * fraction`, so a fractional
/// threshold does not warn early and a tiny one still warns at one byte.
fn warning_bytes(quota: u64, fraction: f64) -> Option<u64> {
    (quota > 0 && fraction > 0.0).then(|| ((quota as f64 * fraction).ceil() as u64).clamp(1, quota))
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
/// A client write fails if it waits longer than this before proposal, or after it.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
/// Raft's uncommitted byte limit. Writers wait at half of it; see `Node::backlog`.
const MAX_UNCOMMITTED_BYTES: usize = 2 * log::MAX_BYTES;

pub struct Node {
    pub(crate) metrics: std::sync::Arc<crate::metrics::Member>,
    /// The cluster and the shared store that this member names on its peer traffic.
    peer: crate::peer::Identity,
    /// The object namespace that serves snapshot objects to peers.
    pub(crate) location: std::sync::Arc<crate::state::transfer::Location>,
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
    /// Replies for `prepared_puts` evaluated before commit. See `quorum_put::HeldPuts`.
    held_puts: Option<quorum_put::HeldPuts>,
    /// The term of the entries in the last submitted PreparePuts job.
    prepare_term: u64,
    /// True while the log above a held entry is past its alarm bound. See `pause`.
    hold_alarm: bool,
    /// The last entry of a PreparePuts job that a storage error ended. No early reply runs
    /// until apply passes it, so the committed path applies those entries and retries.
    unprepared: u64,
    quorum_reply_index: u64,
    /// Watch creations that wait while a Put reply runs ahead of the local apply.
    watch_waiters: Vec<oneshot::Sender<()>>,
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
    // Full backlog checks by cause: apply lag, queued log jobs, queued log bytes,
    // uncommitted Raft bytes, unapplied log bytes.
    backlog_rejects: [u64; 5],
    // `REQUEST_TIMEOUT`. Tests that are slow by design raise it.
    request_timeout: Duration,
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
    compact_plan: compaction::PlanReady,
    lease_view: Option<std::sync::Arc<BTreeMap<i64, crate::lease::LeaseRecord>>>,
    /// Whether this leader term has built its lease deadlines.
    lease_promoted: bool,
    renewals: lease::Renewals,
    /// The shared-mode leader writer. `None` in replicated mode.
    writer: Option<shared::Writer>,
    /// The leader's automatic Compact schedule. `None` when automatic compaction is off.
    auto_compaction: Option<auto_compaction::AutoCompaction>,
    /// Quota usage has reached the warning threshold and the warning is logged.
    quota_warned: bool,
    /// Simulation: the leader does not propose compaction steps, as if their reads were slow.
    #[cfg(test)]
    hold_compact_steps: bool,
    /// The quota flag of this member and its `SetQuota` proposals.
    quota: quota::Quota,
    /// The term and index of the first `SeedMembers` proposal of this member in its last term.
    members_proposed: Option<(u64, u64)>,
    /// The last term in which this member proposed its digest again. See `propose_members`.
    members_rechecked: u64,
    /// Simulation: the member proposes no `SeedMembers`, like an earlier binary.
    #[cfg(test)]
    pub(super) hold_members: bool,
    /// The node compared the flags with the stored membership row.
    members_observed: bool,
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
        // A directly built retention gets the same zero and sign rules as the flags.
        let auto_compaction = options
            .auto_compaction
            .map(crate::compactor::Retention::validate)
            .transpose()?
            .flatten();
        if options.state_mode == crate::state::StateMode::Shared {
            ensure!(
                !options.view_max_age.is_zero() && !options.view_pin_max.is_zero(),
                "the view age limits must be positive"
            );
            ensure!(
                options.overlay_max_bytes > 0,
                "the overlay limit must be positive"
            );
        }
        // Check before any change to the directory, and again under the lock below.
        ensure_no_restore_marker(&options.dir)?;
        #[cfg(test)]
        if let Some(hook) = tests::BEFORE_RAFT_LOCK.with(|hook| hook.take()) {
            hook(&options.dir);
        }
        std::fs::create_dir_all(&options.dir)?;
        // Open Raft first. Its exclusive file lock also protects this node's SlateDB directory.
        let mut storage = RaftStorage::open_with_file_bytes(
            &options.dir.join("raft"),
            options.id,
            &options.cluster_id,
            if options.join {
                vec![]
            } else {
                options.peers.keys().copied().collect()
            },
            options.log_file_bytes,
        )?;
        // A restore creates its marker only under this lock, so a restore that started after
        // the first check is visible now.
        ensure_no_restore_marker(&options.dir)?;
        let empty_log = storage.last_index()? == 0;
        let empty_raft = empty_log && storage.snapshot_index() == 0;
        // A shared restore keeps its progress in the shared state. A member can start it again
        // until its Raft log has an entry.
        ensure!(
            options.restore.is_none() || empty_raft,
            "snapshot restore requires a new, empty data directory"
        );
        let shared = crate::state::shared::Options {
            quota: options.effective_quota().max(0) as u64,
            voters: options.peers.keys().copied().collect(),
            join: options.join,
            empty_log,
            pin_max: options.view_pin_max,
            overlay_max_bytes: options.overlay_max_bytes,
            restore: options.restore.clone(),
            record: crate::state::shared::Record::of(storage.recovery_snapshot())?,
            store_id: None,
        };
        let mut state = options
            .object_store
            .open(
                &options.dir,
                &options.cluster_id,
                options.id,
                options.state_cache,
                options.state_mode,
                &shared,
            )
            .await?;
        // Mark the directory before the first format 4 row, not on open. See `RowMarker`.
        state.row_marker = Some(storage.row_marker());
        state.location.bind(
            options.id,
            &options.cluster_id,
            options
                .peers
                .get(&options.id)
                .map(|address| crate::cluster::url(address)),
            Some(options.transport.http.clone()),
        );
        let location = state.location.clone();
        let quota = quota::Quota::open(
            &options.dir,
            quota::Startup {
                member: options.id,
                flag: options.effective_quota(),
                shared: state.is_shared(),
                empty: empty_raft && state.applied == 0,
                reassert: options.quota_reassert,
                now: quota::now_millis(),
            },
        );
        let mut quota = match quota {
            Ok(quota) => quota,
            Err(error) => {
                let _ = state.close().await;
                return Err(error);
            }
        };
        let recovered = async {
            // Replicated apply uses the flag only until a `SetQuota` entry applies. Shared mode
            // takes the quota from the cluster claim.
            state.set_quota_flag(options.effective_quota());
            // A shared base can be ahead of the log, or on another term at its index.
            Self::align_log(&mut storage, &state)?;
            // Flushed state proves this prefix was committed, even if its commit marker was lost.
            if state.applied > storage.commit_index() {
                storage.persist_commit(state.applied)?;
            }
            let recovered_from_index = state.applied;
            let recovery = storage.recovery_snapshot();
            if let Ok(Some(descriptor)) =
                crate::state::transfer::Descriptor::decode(recovery.get_data())
            {
                // The durable snapshot may still need its staging copy.
                state
                    .location
                    .protect(recovery.get_metadata().index, &descriptor.checkpoint);
            }
            state.install_snapshot(storage.recovery_snapshot()).await?;
            // The bootstrap membership stays in memory. The seed of the first leader stores the
            // row. See `state::cluster`.
            if state.members.members.is_empty() && !options.join {
                state.members = std::sync::Arc::new(crate::cluster::Store::bootstrap(
                    &options.peers,
                    &options.client_peers,
                ));
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
            let loaded = match log::Cache::load(&storage, state.applied, options.log_memory) {
                Err(error) if error.is::<cache::PendingQuota>() => {
                    apply::recover(
                        &mut state,
                        &storage,
                        Header {
                            cluster: 0,
                            member: options.id,
                            term: storage.initial_state()?.hard_state.term,
                        },
                        quota.origin(),
                    )
                    .await?;
                    log::Cache::load(&storage, state.applied, options.log_memory)
                }
                loaded => loaded,
            };
            let guards = quota::Guards::of(&state, quota.origin());
            if guards.any() {
                let (gate, scanned, open) = quota::scan_storage(&storage, state.applied, guards)?;
                quota.set_scan(gate, scanned, open);
            }
            // A member that waits for a snapshot before its next entry does not hold its
            // backlog. Raft treats the committed log as applied, and the cache keeps it only
            // as evictable history. The snapshot install replaces it.
            let mut raft_applied = state.applied;
            let mut conf = state.members.conf();
            let mut cache = match loaded {
                Err(error)
                    if error.is::<cache::PendingQuota>()
                        && quota.blocked() == Some(state.applied + 1) =>
                {
                    // Shared mode does not take this start. An in-process base adoption would
                    // make the member a voter again while Raft counts the skipped entries above
                    // the base as applied. A shared member truncates its log at each checkpoint,
                    // so such a backlog needs a log of an earlier binary that the leader never
                    // truncated.
                    ensure!(
                        !state.is_shared(),
                        "a waiting shared member cannot skip a log backlog above the pending log \
                         memory; raise the pending log memory for this start"
                    );
                    raft_applied = storage.commit_index();
                    tracing::warn!(
                        applied = state.applied,
                        commit = raft_applied,
                        "the log backlog waits for a snapshot"
                    );
                    // Raft would count the skipped configuration changes as applied without
                    // seeing them. So this member starts as a learner in memory: Raft does not
                    // let it campaign or take a transfer, but it still votes. The snapshot
                    // configuration makes it a voter again. Nothing stores this configuration.
                    conf = quota::without_vote(conf, options.id);
                    log::Cache::load(&storage, raft_applied, options.log_memory)?
                }
                loaded => loaded?,
            };
            cache.initial_conf = Some(conf);
            let config = Config {
                id: options.id,
                election_tick: 10,
                heartbeat_tick: 1,
                applied: raft_applied,
                check_quorum: true,
                pre_vote: true,
                read_only_option: raft::ReadOnlyOption::Safe,
                max_size_per_msg: 1024 * 1024,
                max_inflight_msgs: 64,
                // A safety net only: `backlogged` holds writers once Raft's uncommitted
                // bytes reach `log::MAX_BYTES`. The other half is headroom for one
                // admitted command or put group, so Raft does not drop a client write.
                max_uncommitted_size: MAX_UNCOMMITTED_BYTES as u64,
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
        let identity = crate::peer::Identity {
            cluster: options.cluster_id.clone(),
            store: state.store_id().map(|id| id.to_string()),
        };
        state.set_open_retries(!manual_apply);
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
                    identity.clone(),
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
        crate::metrics::reads::READS.register(&state.metrics.registry)?;
        crate::object::bounded::LIMITER.register(&state.metrics.registry)?;
        crate::mvcc::storage_fault::register(&state.metrics.registry)?;
        let handles = state.shared_handles();
        let writer = match &handles {
            Some(handles) => {
                let gauge = prometheus::IntGauge::new(
                    "slatedb_etcd_shared_clock_skew_milliseconds",
                    "Milliseconds that the local clock is behind the shared writer clock.",
                )?;
                state.metrics.registry.register(Box::new(gauge.clone()))?;
                let clock = shared::Clock::system(options.max_clock_skew, Some(gauge));
                let retained = prometheus::IntGauge::new(
                    "slatedb_etcd_shared_view_checkpoints",
                    "View checkpoints of the shared state. Each one pins its SSTs.",
                )?;
                state
                    .metrics
                    .registry
                    .register(Box::new(retained.clone()))?;
                let mut writer = shared::Writer::new(handles, options.view_max_age, clock);
                writer.retention = options.view_retention;
                writer.retained = Some(retained);
                writer.gauges = Some(shared::Gauges::register(&state.metrics.registry)?);
                Some(writer)
            }
            None => None,
        };
        let mut compaction = compaction::Flush::new(&state);
        if let Some(handles) = handles {
            compaction.share(handles.based);
        }
        let compact_plan = compaction::PlanReady::new(&state, manual_apply);
        let log_reader = state.is_shared().then(|| storage.reader());
        let worker = Worker::start(state, events.clone(), manual_apply, log_reader);
        let log_worker = log::Worker::start(storage, manual_apply)?;
        let next_checkpoint = options.next_checkpoint(raw.store().snapshot_index());
        let auto_compaction = auto_compaction.map(|retention| {
            auto_compaction::AutoCompaction::new(retention, Instant::now(), applied.revision)
        });
        let mut node = Self {
            auto_compaction,
            quota_warned: false,
            #[cfg(test)]
            hold_compact_steps: false,
            location,
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
            held_puts: None,
            prepare_term: 0,
            unprepared: 0,
            hold_alarm: false,
            quorum_reply_index: 0,
            watch_waiters: Vec::new(),
            state: applied,
            worker,
            apply_busy: false,
            queries: VecDeque::new(),
            prefer_query: false,
            apply_delay_ms: 0,
            options,
            peer: identity,
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
            backlog_rejects: [0; 5],
            request_timeout: REQUEST_TIMEOUT,
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
            compact_plan,
            lease_view: None,
            lease_promoted: false,
            renewals: lease::Renewals::default(),
            writer,
            quota,
            members_proposed: None,
            members_rechecked: 0,
            #[cfg(test)]
            hold_members: false,
            members_observed: false,
        };
        node.sync_members();
        node.observe_quota()?;
        node.observe_members();
        // A request can arrive before the first apply or tick. The log cache must already know
        // the base to judge the record. See `Cache::has_image`.
        node.note_base();
        Ok(node)
    }

    /// Propose the entries that a leader puts first in its term: `SeedMembers`, then `SetQuota`.
    /// A leader that waits for a snapshot proposes neither, and hands leadership over.
    ///
    /// The node calls this right after each Raft step and tick, before it takes other work. So
    /// these entries follow the empty entry of a new leader, before every client write, cluster
    /// command, and `Publish` of the term. See `state::cluster`.
    fn propose_term_start(&mut self) -> Result<()> {
        if !self.acts_as_leader() {
            return Ok(());
        }
        self.propose_members()?;
        self.propose_quota()
    }

    /// One logical tick. A leader that this tick elects proposes its term-start entries before
    /// the `Publish` of this member.
    fn tick(&mut self) -> Result<()> {
        self.tick_raft()?;
        self.retry_reads();
        self.tick_apply_pause();
        self.expire_leases()?;
        self.publish_member()?;
        self.tick_shared()?;
        self.drive_auto_compaction(Instant::now())?;
        self.check_quota_warning();
        Ok(())
    }

    /// Like etcd's election tick advance: a lone voter cannot lose an election, so it
    /// campaigns at once instead of after a randomized timeout. Raft safety does not depend
    /// on when a node campaigns. The new leader proposes its term-start entries before any
    /// client write of the first event. The run loop then sees the role change. A member with
    /// an accepted snapshot that apply did not install waits, as in `tick_raft`. So does a
    /// member whose committed backlog leaves no room for the term-start entries: they would
    /// exceed the pending log quota before replay frees it.
    fn campaign_alone(&mut self) -> Result<()> {
        if self.raw.raft.prs().is_singleton()
            && self.raw.raft.promotable()
            && !self.installing()
            && self.raw.store().has_control_room()
        {
            self.raw.campaign()?;
        }
        self.propose_term_start()
    }

    /// The identity that this member sends with its peer traffic. Peers must name the same one.
    pub(crate) fn identity(&self) -> crate::peer::Identity {
        self.peer.clone()
    }

    /// The shared-mode objects that the snapshot receive path opens.
    pub(crate) fn shared_source(&self) -> Option<std::sync::Arc<crate::state::shared::Source>> {
        self.writer.as_ref().map(|writer| writer.source.clone())
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
        // A stopped owner no longer applies entries. Its last view must not serve reads.
        self.forwarding.publish_local(None);
        self.transports.shutdown().await;
        // Keep the Raft directory locked until the state worker has stopped.
        self.compaction.cancel();
        if let Some(writer) = &mut self.writer {
            writer.stop().await;
        }
        self.read_executor.shutdown().await;
        let apply = self.worker.shutdown().await;
        let log = self.log_worker.shutdown().await;
        result.and(apply).and(log)
    }

    async fn run_loop(&mut self, mut events: mpsc::Receiver<Event>) -> Result<()> {
        self.publish_watch(None);
        self.publish_local_reads();
        // Schedule committed log recovery before accepting client work.
        self.drive_ready().await?;
        let mut ticks = tokio::time::interval(TICK);
        ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut old_role = (
            self.raw.raft.state,
            self.raw.raft.term,
            self.raw.raft.leader_id,
        );
        self.campaign_alone()?;
        loop {
            let mut step_responses = vec![];
            tokio::select! {
                _ = tokio::time::sleep_until(
                    self.compact_ready_at.unwrap_or_else(Instant::now).into()
                ), if self.compact_ready_at.is_some() => { self.compact_ready_at = None; }
                _ = ticks.tick() => self.tick()?,
                persisted = self.log_worker.next() => { self.complete_log(persisted?)?; }
                _ = self.compact_plan.changed() => {}
                target = self.compaction.next(), if self.compaction.active() => {
                    self.finish_compaction_flush(target?)?;
                }
                completed = self.worker.next() => { self.complete_apply(completed?)?; }
                event = shared::next(&mut self.writer), if self.writer.is_some() => {
                    if let shared::WriterEvent::Failed(error) = event {
                        return Err(error);
                    }
                }
                read = self.read_executor.next(), if self.read_executor.len() > 0 => {
                    self.complete_read(read?)?;
                }
                event = events.recv() => {
                    let Some(event) = event else { return Ok(()); };
                    let peer = matches!(
                        event,
                        Event::PeerFrame(..) | Event::Step(..) | Event::NetworkResult { .. }
                    );
                    self.handle(event, &mut step_responses).await?;
                    // Let ready tasks add proposals before starting the next durable log write.
                    // A peer reply only moves commit forward, so the yield would delay apply.
                    // A lone Put with no other request in flight has no proposal to wait for.
                    let lone = self.pending.is_empty() && self.put_proposals.len() == 1;
                    if !peer && !lone && (!self.pending.is_empty() || !self.put_proposals.is_empty())
                    {
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
                self.sync_writer();
                self.publish_watch(None);
                self.publish_local_reads();
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
                    M::Defragment if self.writer.is_some() => {
                        self.defragment_shared(credentials.unwrap_or_default(), response)?
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
                            db_size_quota: self.status_quota(),
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
            Event::Snapshot(response) => {
                self.enqueue_query(Query::Snapshot(credentials.unwrap_or_default(), response))
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
                } else if let Some(reserved) = self.snapshot_objects_ready(&message) {
                    let snapshot = message.get_msg_type() == MessageType::MsgSnapshot;
                    let stepped = self.step_peer(message);
                    if let Some(checkpoint) = reserved {
                        // Raft can skip a snapshot: an older commit, or a log that already
                        // matches. Then no install releases the staging copy.
                        let accepted = self
                            .raw
                            .raft
                            .raft_log
                            .unstable_snapshot()
                            .as_ref()
                            .and_then(|s| {
                                crate::state::transfer::Descriptor::decode(s.get_data()).ok()?
                            })
                            .is_some_and(|d| d.checkpoint == checkpoint);
                        if !accepted {
                            self.location.release(&checkpoint);
                        }
                    }
                    match stepped {
                        Ok(()) if snapshot => step_responses.push(response),
                        Ok(()) => {
                            // HTTP receipt is not a Raft vote or append acknowledgement.
                            let _ = response.send(Ok(json!({"accepted": true})));
                        }
                        Err(error) => {
                            let _ = response.send(Err(self.error(&error.to_string())));
                        }
                    }
                } else {
                    // Raft must not accept a snapshot whose objects this member lacks. The copy
                    // runs in the background, and the leader retries the snapshot later.
                    let _ = response.send(Err(self.error("snapshot objects are not copied")));
                }
            }
            Event::SubscribeWatch(response) => {
                let _ = response.send(self.watch_feed.subscribe());
            }
            Event::WatchBarrier(response) => {
                // etcd creates a Watch at the local revision. Wait like a read so that the
                // create covers a Put this member already answered.
                if self.reply_ahead() {
                    // A stream that closed while it waited drops its receiver.
                    self.watch_waiters.retain(|waiter| !waiter.is_closed());
                    self.watch_waiters.push(response);
                } else {
                    let _ = response.send(());
                }
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
                    quota: self.state.quota_bytes,
                    quota_mismatch: self.state.quota_mismatch,
                    usage: self.state.usage,
                    warning_bytes: self.warning_bytes(),
                    members_mismatch: self.state.members_mismatch.is_some(),
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
                    let committed = self.raw.raft.raft_log.committed;
                    // Shared mode: the leader publishes a checkpoint through the commit index. A
                    // follower asks the leader for that publication.
                    if let Some(writer) = &self.writer {
                        if self.raw.raft.state == StateRole::Leader {
                            writer.request_publication(committed);
                        } else {
                            self.send_view_ack(Some(committed));
                        }
                    }
                    self.checkpoint_waiters.push((committed, response));
                    self.schedule_apply()?;
                }
            }
            Event::SnapshotBuilt(built) => self.checkpoint_built(built)?,
            Event::ViewCheckpoint(checkpoint, row, reply) => {
                let accepted = self.announce_view(checkpoint, row)?;
                if let Some(reply) = reply {
                    let _ = reply.send(accepted);
                }
            }
            Event::ViewAck {
                member,
                checkpoints,
                publish,
                coverage,
                writer,
            } => self.view_ack(member, checkpoints, publish, coverage, writer),
            Event::PeerProcess { member, process } => {
                self.raw.store().note_process(member, process);
            }
            Event::SnapshotRefused { member, target } => {
                self.raw.store().note_refused(member, target);
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
        self.backlog(true)
    }

    /// Return true when a membership change must fail. The change skips the byte holds for
    /// ordinary writes: they keep room for it, so the remaining voters can remove a failed
    /// voter while the writes wait for a quorum. The caller checks room for the entry.
    fn membership_backlogged(&mut self) -> bool {
        self.backlog(false)
    }

    fn backlog(&mut self, byte_holds: bool) -> bool {
        let causes = [
            self.raw
                .store()
                .commit_index()
                .max(self.raw.raft.raft_log.committed)
                .saturating_sub(self.state.applied)
                >= apply::MAX_APPLY_LAG,
            self.log_queue.len() >= log::MAX_JOBS,
            self.log_bytes >= log::MAX_BYTES,
            // Raft drops proposals past `max_uncommitted_size`, and clients do not retry
            // a dropped Put. Raft counts each proposed entry until it leaves in a Ready
            // as committed, so this also covers entries that are on disk but need a
            // quorum. The open put group is not in Raft yet.
            byte_holds
                && self.raw.raft.uncommitted_size() + self.put_proposals.bytes() >= log::MAX_BYTES,
            // The pending log quota holds each entry until it is applied. Hold writers at
            // half of it, so a put fails on this quota only if it is very large.
            byte_holds
                && self.raw.store().pending_half_full(
                    self.raw.raft.raft_log.unstable_entries(),
                    self.put_proposals.bytes(),
                ),
        ];
        for (count, hit) in self.backlog_rejects.iter_mut().zip(causes) {
            *count += u64::from(hit);
        }
        causes.contains(&true)
    }

    /// Propose a client write. Ordinary writes wait in a bounded FIFO while apply or log
    /// I/O is behind. A membership change is proposed at once, so its checks stay current,
    /// or it fails.
    fn propose(&mut self, command: RaftCommand, response: impl Into<ClientResponse>) -> Result<()> {
        let response = response.into();
        if self.raw.raft.state != StateRole::Leader {
            response.error(self.error("not leader"));
            return Ok(());
        }
        if self.pending.len() >= 1024 || self.overlay_full() {
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
        if self.membership_backlogged() {
            response.error(self.error("state apply backlog is full"));
            return Ok(());
        }
        change.context = data;
        // Raft drops an entry that does not fit in its uncommitted limit.
        if !self.admits_proposal(&change.context)
            || self.raw.raft.uncommitted_size() + change.encoded_len() > MAX_UNCOMMITTED_BYTES
        {
            response.error(self.error("pending log memory quota is full"));
            return Ok(());
        }
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

    /// A prepared Put reply runs ahead of the local apply. Reads and Watch creation wait until
    /// the replied entries are applied here.
    fn reply_ahead(&self) -> bool {
        self.state.applied < self.quorum_reply_index
    }

    /// Publish the applied view for serializable point reads outside the owner.
    ///
    /// While a prepared Put reply runs ahead of the local apply (`quorum_reply_index`), reads
    /// wait in the owner so that a client that saw the reply also reads its effect here.
    fn publish_local_reads(&self) {
        let local =
            (!self.reply_ahead()).then(|| LocalRead::new(self.state.read.clone(), self.header()));
        self.forwarding.publish_local(local);
    }

    fn fill_header(&self, header: Option<&mut ResponseHeader>) {
        self.header().fill(header);
    }

    fn send(&mut self, messages: Vec<Message>) {
        for mut message in messages {
            // A view snapshot above its row names the receiver process that can install it, or
            // zero, which no process has. The transport sends it on its own route. See
            // `peer::post_snapshot`.
            if message.get_msg_type() == MessageType::MsgSnapshot
                && crate::state::transfer::above_row(message.get_snapshot())
            {
                let process = self.raw.store().capable_process(message.to).unwrap_or(0);
                message.context = process.to_le_bytes().to_vec();
            }
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

    /// Return `None` for an object snapshot that is not copied yet, and start the copy then.
    /// Otherwise return the checkpoint of a new staging reservation, if any.
    fn snapshot_objects_ready(&self, message: &Message) -> Option<Option<String>> {
        if message.get_msg_type() != MessageType::MsgSnapshot {
            return Some(None);
        }
        let snapshot = message.get_snapshot();
        let Ok(Some(descriptor)) = crate::state::transfer::Descriptor::decode(snapshot.get_data())
        else {
            return Some(None);
        };
        if snapshot.get_metadata().index <= self.state.applied {
            return Some(None);
        }
        // Raft may persist it now. Keep its staging copy until the install finishes.
        match self
            .location
            .reserve(snapshot.get_metadata().index, &descriptor.checkpoint)
        {
            Some(true) => return Some(Some(descriptor.checkpoint)),
            Some(false) => return Some(None),
            None => (),
        }
        let location = self.location.clone();
        tokio::spawn(async move {
            if let Err(error) = location.prepare(&descriptor).await {
                tracing::warn!(%error, "cannot copy snapshot objects");
            }
        });
        None
    }

    fn start_checkpoint(&mut self, image: bool) -> Result<()> {
        if self.checkpoint_running || self.apply_busy || self.prepared_puts.is_some() {
            return Ok(());
        }
        // A record built at a base below the record could not replace it. See `certify_base`.
        if self.base_below_record() {
            return Ok(());
        }
        let store = self.raw.store();
        let advances = self.checkpointable(self.state.applied) > store.snapshot_index();
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
        let leader = self.raw.raft.state == StateRole::Leader;
        let applied = self.state.applied;
        for (index, response) in std::mem::take(&mut self.checkpoint_waiters) {
            if response.is_closed() {
                continue;
            }
            // Shared mode: the first request can arrive before apply reaches its commit index.
            // The covering publication is due once apply reaches it.
            if let Some(writer) = &mut self.writer {
                if leader {
                    writer.request_checkpoint(index, applied);
                }
            }
            // A manual checkpoint completes only with a complete image. In shared mode the
            // image is a view checkpoint, so the member's base must hold the state at `index`.
            let store = self.raw.store();
            let done = match &self.writer {
                Some(writer) => {
                    index <= self.state.applied
                        && self
                            .state
                            .view
                            .is_some_and(|(base, _, _)| base >= writer.covering(index))
                }
                None => {
                    index <= store.snapshot_index()
                        && index <= self.state.applied
                        && (index == 0 || store.has_image())
                }
            };
            if done {
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
                    || now.duration_since(p.started) > self.request_timeout
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
        status["apply_paused"] = self.pause_status();
        status["quota"] = self.quota_status();
        status["members_row"] = self.members_status();
        if let (Some(writer), Some((base, applied, bytes))) = (&self.writer, self.state.view) {
            let record = crate::state::transfer::ViewDescriptor::decode(
                self.raw.store().recovery_snapshot().get_data(),
            )
            .ok()
            .flatten()
            .and_then(|view| view.checkpoint().ok());
            status["shared_state"] = json!({
                "store_id": self.peer.store,
                "base_applied_index": base,
                "base_checkpoint": self.state.base_checkpoint,
                "record_index": self.raw.store().snapshot_index(),
                "record_checkpoint": record,
                "applied_view_checkpoints": applied,
                "overlay_bytes": bytes,
                "writer": writer.status(),
            });
        }
        status["snapshot_has_image"] = self.raw.store().has_image().into();
        status["snapshot_image_wanted"] = self.raw.store().image_wanted().into();
        status["snapshot_images_built"] = self.snapshot_images.into();
        status["backlog_full_checks"] = json!({
            "apply_lag": self.backlog_rejects[0],
            "log_jobs": self.backlog_rejects[1],
            "log_bytes": self.backlog_rejects[2],
            "uncommitted_bytes": self.backlog_rejects[3],
            "pending_log_bytes": self.backlog_rejects[4],
        });
        Ok(status)
    }
}
