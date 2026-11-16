use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use anyhow::{ensure, Context, Result};
use protobuf::Message as ProtobufMessage;
use raft::eraftpb::{Entry, EntryType, Message, MessageType};
use raft::{Config, RawNode, SnapshotStatus, StateRole, Storage};
use serde_json::{json, Value};
use tokio::sync::{mpsc, oneshot};

use crate::state::StateMachine;
use crate::storage::RaftStorage;
use crate::{ApiError, Command, Request};

pub type Response = oneshot::Sender<Result<Value, ApiError>>;

pub enum Event {
    Propose(Request, Response),
    Step(Message, Response),
    Status(Response),
    Checkpoint(Response),
    LocalValue(String, Response),
    BlockPeers(BTreeSet<u64>, Response),
    SnapshotBuilt {
        index: u64,
        result: Result<Vec<u8>>,
    },
    SnapshotDelay(u64, Response),
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
}

struct Pending {
    response: Response,
    started: Instant,
}

pub struct Node {
    raw: RawNode<RaftStorage>,
    state: StateMachine,
    options: NodeOptions,
    senders: BTreeMap<u64, mpsc::Sender<Message>>,
    pending: HashMap<String, Pending>,
    blocked: BTreeSet<u64>,
    installed_snapshots: u64,
    recovered_from_index: u64,
    send_failures: Vec<(u64, bool)>,
    events: mpsc::Sender<Event>,
    checkpoint_running: bool,
    checkpoint_waiters: Vec<(u64, Response)>,
    snapshot_delay_ms: u64,
}

impl Node {
    pub async fn open(options: NodeOptions, events: mpsc::Sender<Event>) -> Result<Self> {
        ensure!(
            options.peers.len() == 3,
            "this PoC requires exactly three fixed voters"
        );
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
            options.peers.keys().copied().collect(),
        )?;
        let mut state = StateMachine::open(&options.dir.join("objects")).await?;
        ensure!(
            state.applied <= storage.commit_index(),
            "state is ahead of durable commit"
        );
        let recovered_from_index = state.applied;
        state.install_snapshot(storage.recovery_snapshot()).await?;
        let config = Config {
            id: options.id,
            election_tick: 10,
            heartbeat_tick: 1,
            applied: state.applied,
            check_quorum: true,
            pre_vote: true,
            max_size_per_msg: 1024 * 1024,
            max_inflight_msgs: 64,
            max_uncommitted_size: 16 * 1024 * 1024,
            ..Config::default()
        };
        config.validate()?;
        let logger = slog::Logger::root(slog::Discard, slog::o!());
        let raw = RawNode::new(&config, storage, &logger)?;
        let mut senders = BTreeMap::new();
        for (&peer, address) in &options.peers {
            if peer == options.id {
                continue;
            }
            let (tx, rx) = mpsc::channel(256);
            senders.insert(peer, tx);
            tokio::spawn(transport(
                peer,
                address.clone(),
                options.cluster_id.clone(),
                rx,
                events.clone(),
            ));
        }
        Ok(Self {
            raw,
            state,
            options,
            senders,
            pending: HashMap::new(),
            blocked: BTreeSet::new(),
            installed_snapshots: 0,
            recovered_from_index,
            send_failures: vec![],
            events,
            checkpoint_running: false,
            checkpoint_waiters: vec![],
            snapshot_delay_ms: 0,
        })
    }

    pub async fn run(mut self, mut events: mpsc::Receiver<Event>) -> Result<()> {
        // Replay committed durable entries before this process accepts client work.
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
                _ = ticks.tick() => { self.raw.tick(); }
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
            self.drive_ready().await?;
            for response in step_responses {
                let _ = response.send(Ok(json!({"accepted": true})));
            }
            self.expire_pending();
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

    async fn handle(&mut self, event: Event, step_responses: &mut Vec<Response>) -> Result<()> {
        match event {
            Event::Propose(request, response) => {
                if self.raw.raft.state != StateRole::Leader {
                    let _ = response.send(Err(self.error("not leader")));
                } else if self.pending.len() >= 1024 {
                    let _ = response.send(Err(self.error("too many pending requests")));
                } else {
                    let command = Command {
                        id: uuid::Uuid::new_v4().to_string(),
                        request,
                    };
                    match self.raw.propose(vec![], serde_json::to_vec(&command)?) {
                        Ok(()) => {
                            self.pending.insert(
                                command.id,
                                Pending {
                                    response,
                                    started: Instant::now(),
                                },
                            );
                        }
                        Err(error) => {
                            let _ = response.send(Err(self.error(&error.to_string())));
                        }
                    }
                }
            }
            Event::Step(message, response) => {
                if message.to != self.options.id
                    || message.from == self.options.id
                    || !self.options.peers.contains_key(&message.from)
                    || self.blocked.contains(&message.from)
                {
                    let _ = response.send(Err(self.error("peer message rejected")));
                } else {
                    match self.raw.step(message) {
                        Ok(()) => step_responses.push(response),
                        Err(error) => {
                            let _ = response.send(Err(self.error(&error.to_string())));
                        }
                    }
                }
            }
            Event::Status(response) => {
                let _ = response.send(Ok(self.status()?));
            }
            Event::Checkpoint(response) => {
                self.checkpoint_waiters.push((self.state.applied, response));
                self.start_checkpoint()?;
            }
            Event::SnapshotBuilt { index, result } => {
                self.checkpoint_running = false;
                let data = result?;
                if index > self.raw.store().snapshot_index() {
                    self.raw.mut_store().checkpoint(index, data)?;
                    self.state.durable_applied = self.state.durable_applied.max(index);
                    tracing::info!(
                        node = self.options.id,
                        index,
                        "Snapshot saved and log compacted"
                    );
                }
            }
            Event::SnapshotDelay(delay_ms, response) => {
                self.snapshot_delay_ms = delay_ms.min(5000);
                let _ = response.send(Ok(self.status()?));
            }
            Event::LocalValue(key, response) => {
                let value = self.state.local_value(&key).await?;
                let _ = response.send(Ok(
                    json!({"value": value, "applied_index": self.state.applied}),
                ));
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
        while self.raw.has_ready() {
            let mut ready = self.raw.ready();
            let snapshot = (!ready.snapshot().is_empty()).then(|| ready.snapshot().clone());
            self.raw
                .mut_store()
                .persist(ready.entries(), ready.hs(), snapshot.as_ref())?;
            if let Some(snapshot) = snapshot {
                self.state.install_snapshot(&snapshot).await?;
                self.installed_snapshots += 1;
            }
            // This baseline persists before sending even the messages that Raft can send early.
            self.send(ready.take_messages());
            self.send(ready.take_persisted_messages());
            self.apply(ready.take_committed_entries()).await?;
            let mut light = self.raw.advance_append(ready);
            if let Some(commit) = light.commit_index() {
                self.raw.mut_store().persist_commit(commit)?;
            }
            self.send(light.take_messages());
            self.apply(light.take_committed_entries()).await?;
            self.raw.advance_apply_to(self.state.applied);
            // Report local send failures only after the Ready has been advanced.
            for (peer, snapshot) in std::mem::take(&mut self.send_failures) {
                self.raw.report_unreachable(peer);
                if snapshot {
                    self.raw.report_snapshot(peer, SnapshotStatus::Failure);
                }
            }
        }
        Ok(())
    }

    async fn apply(&mut self, entries: Vec<Entry>) -> Result<()> {
        for entry in entries {
            if entry.index <= self.state.applied {
                continue;
            }
            ensure!(
                entry.index <= self.raw.store().commit_index(),
                "apply exceeds durable commit"
            );
            ensure!(
                entry.get_entry_type() == EntryType::EntryNormal,
                "membership changes are not supported"
            );
            let command: Option<Command> = if entry.data.is_empty() {
                None
            } else {
                Some(serde_json::from_slice(&entry.data)?)
            };
            let reply = self.state.apply(entry.index, command.as_ref()).await?;
            if let Some(command) = command {
                if let Some(pending) = self.pending.remove(&command.id) {
                    let _ = pending.response.send(Ok(serde_json::to_value(reply)?));
                }
            }
        }
        Ok(())
    }

    fn send(&mut self, messages: Vec<Message>) {
        for message in messages {
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
        if self.checkpoint_running {
            return Ok(());
        }
        if self.state.applied <= self.raw.store().snapshot_index() {
            self.raw.store().purge()?;
            return Ok(());
        }
        let index = self.state.applied;
        let job = self.state.snapshot_job()?;
        let events = self.events.clone();
        let delay = Duration::from_millis(self.snapshot_delay_ms);
        self.checkpoint_running = true;
        tokio::spawn(async move {
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
            let result = job.await;
            let _ = events.send(Event::SnapshotBuilt { index, result }).await;
        });
        Ok(())
    }

    fn finish_checkpoint_waiters(&mut self) -> Result<()> {
        let mut waiting = vec![];
        for (index, response) in std::mem::take(&mut self.checkpoint_waiters) {
            if response.is_closed() {
                continue;
            }
            if index <= self.raw.store().snapshot_index() {
                let _ = response.send(Ok(self.status()?));
            } else {
                waiting.push((index, response));
            }
        }
        self.checkpoint_waiters = waiting;
        if !self.checkpoint_waiters.is_empty() {
            self.start_checkpoint()?;
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
                !is_leader || p.response.is_closed() || p.started.elapsed() > Duration::from_secs(5)
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
            "first_log_index": self.raw.store().first_index()?, "last_log_index": self.raw.store().last_index()?,
            "sync_writes": self.raw.store().sync_writes, "pending": self.pending.len(),
            "blocked_peers": self.blocked, "installed_snapshots": self.installed_snapshots,
            "recovered_from_index": self.recovered_from_index, "log": self.raw.store().log_stats()?,
            "checkpoint_running": self.checkpoint_running,
            "snapshot_delay_ms": self.snapshot_delay_ms,
        }))
    }
}

async fn transport(
    peer: u64,
    address: String,
    cluster_id: String,
    mut messages: mpsc::Receiver<Message>,
    events: mpsc::Sender<Event>,
) {
    let client = reqwest::Client::new();
    while let Some(message) = messages.recv().await {
        let snapshot = message.get_msg_type() == MessageType::MsgSnapshot;
        let result: Result<()> = async {
            let response = client
                .post(format!("http://{address}/raft"))
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
        if snapshot || result.is_err() {
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
