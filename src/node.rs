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
mod checkpoint;
/// Apply barriers that may wait for the local state to reach a revision.
pub(crate) const MAX_APPLY_BARRIERS: usize = apply::MAX_QUERIES;
/// The longest run of one wide read in the read executor. A full LIST of a large prefix can
/// take several seconds. A failed read returns Unavailable, and clients retry it from the
/// start, so a short limit makes a large LIST scan again and again. The Kubernetes API server
/// also gives a request 60 s.
pub(crate) const WIDE_READ_LIMIT: Duration = Duration::from_secs(60);
mod cache;
pub use cache::LogMemory;
mod cluster;
mod compaction;
mod forwarding;
mod lease;
mod links;
mod log;
mod maintenance;
mod proposal;
pub use forwarding::ForwardView;
pub(crate) use forwarding::LocalRead;
pub(crate) use shared::WriterReport;
pub(crate) use shared::{Millis, ProbeReply, ProbeRequest, PROBE_ROUTE};
pub(crate) use shared::{Prober, WriteProbeReply, WriteProbeRequest, WRITE_PROBE_ROUTE};
mod quorum_put;
mod read;
pub(crate) use read::table::MAX_PENDING_READS;
mod ready;
mod response;
mod schedule;
mod shared;
mod step;
mod watch;
use apply::{AppliedState, Completion, Header, Job, Output, Query, Reply, Worker};
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
        /// The health of the member's writer.
        writer: Option<shared::WriterReport>,
    },
    /// The first peer frame of a stream or request came from this process of `member`, if the
    /// frame names one.
    PeerProcess {
        member: u64,
        process: Option<u64>,
    },
    /// `member` refused a snapshot above its row that named process `target`.
    SnapshotRefused {
        member: u64,
        target: u64,
    },
    /// Shared mode: the reply of `member` to the write probe `nonce` of this leader, or the
    /// error of the request.
    WriteProbed {
        member: u64,
        nonce: u64,
        reply: std::result::Result<WriteProbeReply, String>,
    },
}

/// A metrics scrape asks the event loop for a sample.
impl From<oneshot::Sender<crate::metrics::Sample>> for Event {
    fn from(response: oneshot::Sender<crate::metrics::Sample>) -> Self {
        Event::Metrics(response)
    }
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
    last_observed_leader: u64,
    watch_feed: watch::Publisher,
    forwarding: forwarding::Publisher,
    compaction: compaction::Flush,
    transfers: Vec<(u64, Instant, Response)>,
    published_at: Option<Instant>,
    events: mpsc::Sender<Event>,
    manual_transport: bool,
    raw: RawNode<log::Cache>,
    log_worker: log::Worker,
    /// The queued log jobs and the step replies that wait for them. See `ready::pipeline`.
    log: ready::pipeline::LogPipeline,
    read_executor: read::executor::Executor,
    /// The ReadIndex reads, the queued reads, and the revision waits. See `read::table`.
    reads: read::table::ReadIndexTable,
    state: AppliedState,
    worker: Worker,
    /// The submitted apply jobs, the delivered commit index, and the queued state reads. See
    /// `schedule::scheduler`.
    apply: schedule::scheduler::ApplyScheduler,
    /// Put replies that leave before the local apply. See `quorum_put::EarlyPuts`.
    early_puts: quorum_put::EarlyPuts,
    /// True while the log above a held entry is past its alarm bound. See `tick_apply_pause`.
    hold_alarm: bool,
    /// The entry that apply held on, until apply passes it. See `WriterReport::apply_held_at`.
    apply_held_at: Option<u64>,
    options: NodeOptions,
    /// The queue, transport task, meter, and health of each peer link. See `links`.
    links: links::PeerLinks,
    pending: HashMap<String, Pending>,
    put_proposals: proposal::Group,
    waiting_proposals: proposal::Waiting,
    blocked: BTreeSet<u64>,
    installed_snapshots: u64,
    recovered_from_index: u64,
    /// The checkpoint schedule and the manual checkpoint requests. See `checkpoint`.
    checkpoints: checkpoint::Checkpointer,
    // Full backlog checks by cause: apply lag, queued log jobs, queued log bytes,
    // uncommitted Raft bytes, unapplied log bytes.
    backlog_rejects: [u64; 5],
    // `REQUEST_TIMEOUT`. Tests that are slow by design raise it.
    request_timeout: Duration,
    compact_step: Option<(u64, u64)>,
    compact_ready_at: Option<Instant>,
    compact_plan: compaction::PlanReady,
    /// The leader's lease deadlines and its Expire proposals. See `lease::clock`.
    lease_clock: lease::clock::LeaseClock,
    renewals: lease::Renewals,
    /// The leader writer of the shared state.
    writer: shared::Writer,
    /// The leader's automatic Compact schedule. `None` when automatic compaction is off.
    auto_compaction: Option<auto_compaction::AutoCompaction>,
    /// Quota usage has reached the warning threshold and the warning is logged.
    quota_warned: bool,
    /// Simulation: the leader does not propose compaction steps, as if their reads were slow.
    #[cfg(test)]
    hold_compact_steps: bool,
    /// Tests: run the clock check of the shared writer also with a manual transport. Probes
    /// still need the peer routes, so the test gives the samples itself.
    #[cfg(test)]
    clock_in_manual: bool,
    /// The term and index of the first `SeedMembers` proposal of this member in its last term.
    members_proposed: Option<(u64, u64)>,
    /// The last term in which this member proposed its digest again. See `propose_members`.
    members_rechecked: u64,
    /// Tests: promote a shared learner without its write proof.
    #[cfg(test)]
    pub(super) skip_write_proof: bool,
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
        ensure!(
            !options.view_max_age.is_zero() && !options.view_pin_max.is_zero(),
            "the view age limits must be positive"
        );
        ensure!(
            options.overlay_max_bytes > 0,
            "the overlay limit must be positive"
        );
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
        let mut state = StateMachine::open_bound(
            &options.object_store,
            &options.dir,
            &options.cluster_id,
            options.id,
            options.state_cache,
            &shared,
        )
        .await?;
        let recovered = async {
            // Apply takes the quota from the cluster claim. The flag only shows a difference.
            state.set_quota_flag(options.effective_quota());
            // A shared base can be ahead of the log, or on another term at its index.
            Self::align_log(&mut storage, &state)?;
            // Flushed state proves this prefix was committed, even if its commit marker was lost.
            if state.applied > storage.commit_index() {
                storage.persist_commit(state.applied)?;
            }
            let recovered_from_index = state.applied;
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
                    )
                    .await?;
                    log::Cache::load(&storage, state.applied, options.log_memory)
                }
                loaded => loaded,
            };
            let raft_applied = state.applied;
            let mut cache = loaded?;
            cache.initial_conf = Some(state.members.conf());
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
            store: Some(state.store_id().to_string()),
        };
        state.set_open_retries(!manual_apply);
        let links = match transport_override {
            Some(senders) => links::PeerLinks::manual(senders),
            None => links::PeerLinks::dial(
                options.id,
                &options.peers,
                &identity,
                &events,
                &options.transport,
            ),
        };
        let applied = AppliedState::new(&state)?;
        let metrics = std::sync::Arc::new(crate::metrics::Member::new(
            storage.wal_metrics.clone(),
            state.admin.clone(),
            state.metrics.registry.clone(),
        )?);
        crate::metrics::reads::READS.register(&state.metrics.registry)?;
        crate::storage_fault::register(&state.metrics.registry)?;
        let handles = state.shared_handles();
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
        let prober = shared::Prober::new(options.id, handles.source.clone());
        let writer = shared::Writer::new(
            &handles,
            options.view_max_age,
            clock,
            Some(std::sync::Arc::new(prober)),
            options.view_retention,
            Some(retained),
            Some(shared::Gauges::register(&state.metrics.registry)?),
        );
        let compaction = compaction::Flush::new(&state, handles.based);
        let compact_plan = compaction::PlanReady::new(&state, manual_apply);
        let log_reader = Some(storage.reader());
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
            #[cfg(test)]
            clock_in_manual: false,
            metrics,
            last_observed_leader: 0,
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
            raw,
            log_worker,
            log: ready::pipeline::LogPipeline::new(),
            read_executor: read::executor::Executor::new(manual_apply),
            reads: read::table::ReadIndexTable::new(),
            apply: schedule::scheduler::ApplyScheduler::new(applied.applied),
            early_puts: quorum_put::EarlyPuts::default(),
            hold_alarm: false,
            apply_held_at: None,
            state: applied,
            worker,
            options,
            peer: identity,
            links,
            pending: HashMap::new(),
            put_proposals: proposal::Group::default(),
            waiting_proposals: proposal::Waiting::default(),
            blocked: BTreeSet::new(),
            installed_snapshots: 0,
            recovered_from_index,
            checkpoints: checkpoint::Checkpointer::new(next_checkpoint),
            backlog_rejects: [0; 5],
            request_timeout: REQUEST_TIMEOUT,
            compact_step: None,
            compact_ready_at: None,
            compact_plan,
            lease_clock: lease::clock::LeaseClock::new(),
            renewals: lease::Renewals::default(),
            writer,
            members_proposed: None,
            members_rechecked: 0,
            #[cfg(test)]
            skip_write_proof: false,
            members_observed: false,
        };
        node.sync_members();
        node.observe_members();
        // A request can arrive before the first apply or tick. The log cache must already know
        // the base to judge the record. See `Cache::has_image`.
        node.note_base();
        Ok(node)
    }

    /// Propose the entry that a leader puts first in its term: `SeedMembers`. A leader that
    /// waits for a snapshot proposes none, and hands leadership over.
    ///
    /// The node calls this right after each Raft step and tick, before it takes other work. So
    /// the entry follows the empty entry of a new leader, before every client write, cluster
    /// command, and `Publish` of the term. See `state::cluster`.
    fn propose_term_start(&mut self) -> Result<()> {
        if !self.acts_as_leader() {
            return Ok(());
        }
        self.propose_members()
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
    pub(crate) fn shared_source(&self) -> std::sync::Arc<crate::state::shared::Source> {
        self.writer.source().clone()
    }

    /// Shared mode: the write probes that this member runs for the leader.
    pub(crate) fn write_prober(&self) -> Option<std::sync::Arc<Prober>> {
        self.writer.prober()
    }

    /// Shared mode: the wall clock that this member returns to a clock probe of the leader.
    pub(crate) fn probe_clock(&self) -> Millis {
        self.writer.clock().now_ms.clone()
    }

    /// Tests: skew the wall clock of this member's writer and of its probe replies.
    #[cfg(test)]
    pub(crate) fn set_clock(&mut self, now_ms: Millis) {
        self.writer.set_clock(now_ms);
    }

    /// Run with no peer queue. Peer frames can still come through `events`.
    pub async fn run(self, events: mpsc::Receiver<Event>) -> Result<()> {
        let (_, peers) = mpsc::channel(1);
        self.run_until(events, peers, std::future::pending()).await
    }

    pub(crate) async fn run_until(
        mut self,
        events: mpsc::Receiver<Event>,
        peers: mpsc::Receiver<Event>,
        stop: impl std::future::Future<Output = ()>,
    ) -> Result<()> {
        let owner = self.run_loop(events, peers);
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
        self.links.shutdown().await;
        // Keep the Raft directory locked until the state worker has stopped.
        self.compaction.cancel();
        self.writer.stop().await;
        self.read_executor.shutdown().await;
        let apply = self.worker.shutdown().await;
        let log = self.log_worker.shutdown().await;
        result.and(apply).and(log)
    }

    async fn run_loop(
        &mut self,
        mut events: mpsc::Receiver<Event>,
        mut peers: mpsc::Receiver<Event>,
    ) -> Result<()> {
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
                event = shared::next(&mut self.writer) => {
                    if let shared::WriterEvent::Failed(error) = event {
                        return Err(error);
                    }
                }
                read = self.read_executor.next(|query| self.reads.live(query)),
                    if self.read_executor.len() > 0 => {
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
                // A closed peer queue disables this branch. Batch frames like client events.
                Some(event) = peers.recv() => {
                    self.handle(event, &mut step_responses).await?;
                    for _ in 0..63 {
                        let Ok(event) = peers.try_recv() else { break; };
                        self.handle(event, &mut step_responses).await?;
                    }
                }
            }
            self.log.add_peer_waiters(step_responses);
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
            self.links.received(message.from, bytes);
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
                    M::Defragment => {
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
                if self.log.jobs_full() {
                    let _ = response.send(Err(self.error("too many pending log jobs")));
                } else {
                    self.log.queue_stats(response);
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
                    || self.log.peer_waiters() + step_responses.len() >= 256
                {
                    let _ = response.send(Err(self.error("peer message rejected")));
                } else {
                    let snapshot = message.get_msg_type() == MessageType::MsgSnapshot;
                    match self.step_peer(message) {
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
            Event::WatchBarrier(response) => {
                // etcd creates a Watch at the local revision. Wait like a read so that the
                // create covers a Put this member already answered.
                self.early_puts.watch_barrier(self.state.applied, response);
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
                    traffic: self.links.traffic(),
                    peers: self
                        .state
                        .members
                        .members
                        .keys()
                        .map(|&id| (id, self.links.connected_since(id).is_some()))
                        .collect(),
                });
            }
            Event::Status(response) => {
                let _ = response.send(Ok(self.status()?));
            }
            Event::Checkpoint(response) => {
                if self.checkpoints.waiting() >= apply::MAX_QUERIES {
                    let _ = response.send(Err(self.error("too many checkpoint requests")));
                } else {
                    let committed = self.raw.raft.raft_log.committed;
                    // Shared mode: the leader publishes a checkpoint through the commit index. A
                    // follower asks the leader for that publication.
                    if self.raw.raft.state == StateRole::Leader {
                        self.writer.request_publication(committed);
                    } else {
                        self.send_view_ack(Some(committed));
                    }
                    self.checkpoints.wait(committed, response);
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
                let changed = self.raw.store().process(member) != process;
                self.raw.store().note_process(member, process);
                // A new voter process drops its clock sample before the next batch admission. A
                // new learner process drops its write proof and its running probe at once. A late
                // frame of the earlier process then cannot bring that proof back.
                if changed {
                    self.tick_clock(false);
                    self.writer.forget_proof(member);
                }
            }
            Event::SnapshotRefused { member, target } => {
                self.raw.store().note_refused(member, target);
            }
            Event::WriteProbed {
                member,
                nonce,
                reply,
            } => self.write_probed(member, nonce, reply),
            Event::SnapshotDelay(delay_ms, response) => {
                self.checkpoints.set_delay(delay_ms.min(5000));
                let _ = response.send(Ok(self.status()?));
            }
            Event::ApplyDelay(delay_ms, response) => {
                self.apply.set_delay(delay_ms.min(5000));
                let _ = response.send(Ok(self.status()?));
            }
            Event::LogReadFault(delay_ms, fail, response) => {
                self.log.set_read_fault(delay_ms.min(5000), fail);
                let _ = response.send(Ok(self.status()?));
            }
            Event::LogFault(delay_ms, fail, response) => {
                self.log.set_fault(delay_ms.min(5000), fail);
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
                self.links.reported(peer, success);
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
            self.log.jobs_full(),
            self.log.bytes_full(),
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

    /// Publish the applied view for serializable point reads outside the owner.
    ///
    /// While a prepared Put reply runs ahead of the local apply (`EarlyPuts::reads_blocked`),
    /// reads wait in the owner so that a client that saw the reply also reads its effect here.
    fn publish_local_reads(&self) {
        let local = (!self.early_puts.reads_blocked(self.state.applied))
            .then(|| LocalRead::new(self.state.read.clone(), self.header()));
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
            self.links.send(message, &self.blocked);
        }
    }

    fn start_checkpoint(&mut self, image: bool) -> Result<()> {
        if self.checkpoints.running() || self.apply.busy() || self.early_puts.active() {
            return Ok(());
        }
        // A record built at a base below the record could not replace it. See `certify_base`.
        if self.base_below_record() {
            return Ok(());
        }
        let store = self.raw.store();
        let advances = self.checkpointable() > store.snapshot_index();
        // An image can replace a record without an image at the same index.
        let attaches = image && store.snapshot_index() > 0 && !store.has_image();
        if !advances && !attaches {
            return Ok(());
        }
        self.submit_apply(
            Job::Checkpoint {
                delay: self.checkpoints.delay(),
                dir: self.options.dir.join("raft"),
            },
            self.state.applied,
        )?;
        self.checkpoints.start(image);
        Ok(())
    }

    fn checkpoint_built(&mut self, built: apply::Built) -> Result<()> {
        let image = match built.result {
            Ok(image) => image,
            // A storage error fails only this checkpoint. The record stays, and the next
            // checkpoint starts after a wait. Apply runs other jobs meanwhile.
            Err(error) if crate::storage_fault::classify(&error).is_some() => {
                self.checkpoints.failed()?;
                tracing::warn!(
                    error = format!("{error:#}"),
                    "a checkpoint failed; retry later"
                );
                return Ok(());
            }
            Err(error) => return Err(error),
        };
        self.checkpoints.built(built.index, built.conf, image)
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
        for (index, response) in self.checkpoints.take_waiters() {
            if response.is_closed() {
                continue;
            }
            // Shared mode: the first request can arrive before apply reaches its commit index.
            // The covering publication is due once apply reaches it.
            if leader {
                self.writer.request_checkpoint(index, applied);
            }
            // A manual checkpoint completes only with a complete image. In shared mode the
            // image is a view checkpoint, so the member's base must hold the state at `index`.
            let (base, _, _) = self.state.view;
            let done = index <= self.state.applied && base >= self.writer.covering(index);
            if done {
                let _ = response.send(Ok(self.status()?));
            } else {
                waiting.push((index, response));
            }
        }
        self.checkpoints.requeue(waiting);
        if self.checkpoints.waiting() > 0 {
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
            "active_read_jobs": self.read_executor.len(), "queued_reads": self.reads.queued(),
            "durable_applied_index": self.state.durable_applied,
            "snapshot_index": self.raw.store().snapshot_index(),
            "first_log_index": self.raw.store().snapshot_index() + 1, "last_log_index": self.raw.store().durable.last_index,
            "sync_writes": self.raw.store().durable.sync_writes, "pending": self.pending.len(),
            "blocked_peers": self.blocked, "installed_snapshots": self.installed_snapshots,
            "recovered_from_index": self.recovered_from_index,
            "checkpoint_running": self.checkpoints.running(),
            "snapshot_delay_ms": self.checkpoints.delay().as_millis() as u64,
            "apply_lag": self.raw.store().commit_index().saturating_sub(self.state.applied),
            "pending_read_indexes": self.reads.pending(),
            "pending_revision_waiters": self.reads.revision_waiters(),
            "raft_commit_index": self.raw.raft.raft_log.committed,
            "cached_last_log_index": self.raw.store().last_index()?,
            "log_memory": self.raw.store().stats(),
            "revision": self.state.revision,
            "compact_revision": self.state.compact_revision,
        });
        #[cfg(feature = "write-profile")]
        {
            status["write_profile"] = crate::write_profile::snapshot();
        }
        self.log.status(&mut status);
        self.apply.status(&mut status);
        status["quorum_reply_index"] = self.early_puts.reply_index().into();
        status["apply_paused"] = self.pause_status();
        status["apply_held_at"] = self.apply_held_at.into();
        status["quota"] = self.quota_status();
        status["members_row"] = self.members_status();
        let (base, applied, bytes) = self.state.view;
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
            "writer": self.writer.status(),
        });
        status["snapshot_has_image"] = self.raw.store().has_image().into();
        status["snapshot_image_wanted"] = self.raw.store().image_wanted().into();
        status["snapshot_images_built"] = self.checkpoints.images().into();
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
