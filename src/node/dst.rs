//! Drive the real node with seeded message delivery and a logical clock.

use super::*;
use crate::lease::{LeaseCommand, LeaseReply};
use crate::mvcc::Outcome;
use crate::proto::etcdserverpb::request_op::Request as Op;
use crate::proto::etcdserverpb::response_op::Response as Resp;
use crate::proto::etcdserverpb::{PutRequest, RangeRequest};
use crate::state::shared::Record;
use crate::state::{StateRead, StateScan};

/// A digest of every `data/` and `etcd/` row of a state. The length of each key and value comes
/// before it. Derived meta rows differ between storage modes, so the digest leaves them out.
async fn rows_digest(state: &impl StateRead) -> std::result::Result<u32, slatedb::Error> {
    let mut digest = 0;
    for prefix in [&b"data/"[..], b"etcd/"] {
        let mut scan = state.scan_under(prefix, &Default::default()).await?;
        while let Some(row) = scan.next().await? {
            for part in [&row.key, &row.value] {
                digest = crc32c::crc32c_append(digest, &(part.len() as u64).to_be_bytes());
                digest = crc32c::crc32c_append(digest, part);
            }
        }
    }
    Ok(digest)
}

#[path = "shared/dst_tests.rs"]
mod coverage;
#[path = "shared/dst_reference.rs"]
mod reference;

struct ClientRequest {
    key: String,
    value: String,
    reply: oneshot::Receiver<Result<KvReply, ApiError>>,
}

struct ReadRequest {
    node: u64,
    known: BTreeMap<String, String>,
    reply: oneshot::Receiver<Result<KvReply, ApiError>>,
}

struct RevisionRequest {
    node: u64,
    revision: i64,
    reply: oneshot::Receiver<Result<Value, ApiError>>,
}

struct Simulation {
    seed: u64,
    root: tempfile::TempDir,
    nodes: BTreeMap<u64, Node>,
    links: BTreeMap<(u64, u64), mpsc::Receiver<Message>>,
    messages: Vec<Message>,
    requests: Vec<ClientRequest>,
    reads: Vec<ReadRequest>,
    read_count: usize,
    revisions: Vec<RevisionRequest>,
    revision_count: usize,
    acknowledged: BTreeMap<String, String>,
    committed_entries: BTreeMap<u64, Vec<u8>>,
    terms: BTreeMap<u64, u64>,
    isolated: Option<u64>,
    /// A member that starts as a joiner, as after MemberAdd. With no log it has an empty base.
    joining: Option<u64>,
    paused_apply: Option<u64>,
    paused_flush: Option<u64>,
    paused_reads: Option<u64>,
    paused_log: Option<u64>,
    expected_log_failure: Option<u64>,
    log_failures: usize,
    random: u64,
    next_request: u64,
    trace: Vec<String>,
    installed_snapshots: u64,
    /// Every value that a client proposed. A read must not return another value.
    proposed: BTreeSet<(String, String)>,
    /// Shared mode: the one object store of all members.
    shared: std::sync::Arc<crate::object::faults::FaultStore>,
    /// Shared mode and quota checks: the state that applies the committed entries in order.
    reference: Option<reference::Reference>,
    /// The reference digest of the `data/` and `etcd/` rows after each index.
    digests: BTreeMap<u64, u32>,
    /// The last applied index, base, and process that the view check covered on each member.
    checked: BTreeMap<u64, reference::ViewPoint>,
    /// Member views that matched the reference at a new applied index.
    view_checks: u64,
    /// Member views that matched the reference again at the same index, after a base change or a
    /// restart.
    base_checks: u64,
    /// The `--quota-backend-bytes` value of each member. Zero selects 2 GiB.
    quotas: BTreeMap<u64, i64>,
    /// The number of voters.
    members: u64,
    /// The process ID of each started member, new at each start.
    processes: BTreeMap<u64, u64>,
    /// View snapshots above their row that the receiver refused before Raft saw them.
    refused_snapshots: u64,
    /// The checkpoints of the view snapshots that Raft received. IDs are random, so they stay
    /// out of the trace.
    delivered_views: Vec<uuid::Uuid>,
    /// Opens and installs of stopped members that raised a base above its row.
    extended: u64,
}

impl Drop for Simulation {
    fn drop(&mut self) {
        if let Some(reference) = self.reference.as_ref().filter(|r| r.compared > 0) {
            eprintln!(
                "DST reference kinds of seed {}: {}",
                self.seed,
                reference.kinds()
            );
        }
        // Keep the partial schedule when an invariant fails.
        let dir = std::env::var("SLATEDB_DST_TRACE_DIR").unwrap_or_else(|_| "target/dst".into());
        if std::fs::create_dir_all(&dir).is_ok() {
            if let Ok(data) = serde_json::to_vec_pretty(&self.trace) {
                let _ = std::fs::write(format!("{dir}/seed-{}-last.json", self.seed), data);
            }
        }
    }
}

/// The `--client-peer` flags of member `id`. Each member writes other URL text for the same
/// peers, so a member-local membership row would differ from the reference.
fn client_urls(id: u64, members: u64) -> BTreeMap<u64, String> {
    let host = match id {
        1 => "127.0.0.1".to_owned(),
        2 => "localhost".to_owned(),
        _ => format!("{}example", "a.".repeat(100)),
    };
    (1..=members)
        .map(|n| (n, format!("http://{host}:{}", 2379 + n)))
        .collect()
}

impl Simulation {
    /// Start three members on one fault-injectable in-memory store, with a reference state that
    /// checks each member state after every step.
    async fn new(seed: u64) -> Self {
        Self::with_options(seed, BTreeMap::new(), true).await
    }

    /// Start three members. `reference` checks each member state against a reference state
    /// after every step.
    async fn with_options(seed: u64, quotas: BTreeMap<u64, i64>, reference: bool) -> Self {
        Self::build(seed, quotas, reference, 3).await
    }

    /// Start a simulation with `members` voters and a reference state.
    async fn with_size(seed: u64, members: u64) -> Self {
        Self::build(seed, BTreeMap::new(), true, members).await
    }

    async fn build(seed: u64, quotas: BTreeMap<u64, i64>, reference: bool, members: u64) -> Self {
        let root = tempfile::tempdir().unwrap();
        let reference = if reference {
            Some(reference::Reference::open(&root.path().join("reference")).await)
        } else {
            None
        };
        let mut sim = Self {
            seed,
            root,
            nodes: BTreeMap::new(),
            links: BTreeMap::new(),
            messages: vec![],
            requests: vec![],
            reads: vec![],
            read_count: 0,
            revisions: vec![],
            revision_count: 0,
            acknowledged: BTreeMap::new(),
            committed_entries: BTreeMap::new(),
            terms: BTreeMap::new(),
            isolated: None,
            joining: None,
            paused_apply: None,
            paused_flush: None,
            paused_reads: None,
            paused_log: None,
            expected_log_failure: None,
            log_failures: 0,
            random: seed.max(1),
            next_request: 0,
            trace: vec![],
            installed_snapshots: 0,
            proposed: BTreeSet::new(),
            shared: std::sync::Arc::new(crate::object::faults::FaultStore::new(
                std::sync::Arc::new(slatedb::object_store::memory::InMemory::new()),
            )),
            reference,
            digests: BTreeMap::new(),
            checked: BTreeMap::new(),
            view_checks: 0,
            base_checks: 0,
            quotas,
            members,
            processes: BTreeMap::new(),
            refused_snapshots: 0,
            delivered_views: vec![],
            extended: 0,
        };
        for id in 1..=sim.members {
            sim.start(id).await;
        }
        sim.nodes.get_mut(&1).unwrap().raw.campaign().unwrap();
        sim.drive().await;
        sim
    }

    fn choose(&mut self, count: usize) -> usize {
        self.random ^= self.random << 13;
        self.random ^= self.random >> 7;
        self.random ^= self.random << 17;
        (self.random % count as u64) as usize
    }

    async fn start(&mut self, id: u64) {
        let (events, _) = mpsc::channel(256);
        let mut senders = BTreeMap::new();
        for peer in 1..=self.members {
            if peer == id {
                continue;
            }
            let (tx, rx) = mpsc::channel(256);
            senders.insert(peer, tx);
            self.links.insert((id, peer), rx);
        }
        let store = self.shared.clone();
        let flag = self.quotas.get(&id).copied().unwrap_or(0);
        // Box the large open future, so the schedule future fits the test thread stack.
        let node = Box::pin(Node::open_with_transport(
            NodeOptions {
                client_peers: client_urls(id, self.members),
                transport: crate::tls::Transport::default(),
                require_peer_tls: false,
                view_max_age: std::time::Duration::from_secs(10),
                view_pin_max: std::time::Duration::from_secs(300),
                overlay_max_bytes: 1 << 30,
                max_clock_skew: std::time::Duration::from_secs(5),
                view_retention: Default::default(),
                restore: None,
                object_store: crate::object::Config::Test(store, "root".into()),
                strict_reconfiguration: false,
                join: self.joining == Some(id),
                quota_bytes: flag,
                quota_warning_fraction: 0.0,
                auto_compaction: None,
                id,
                cluster_id: "dst".into(),
                dir: self.root.path().join(id.to_string()),
                peers: (1..=self.members)
                    .map(|n| (n, format!("127.0.0.1:{n}")))
                    .collect(),
                // Members truncate often, so a lagging member needs a format 6 snapshot.
                snapshot_every: 8,
                snapshot_min_bytes: 0,
                log_file_bytes: 256 * 1024,
                log_memory: LogMemory {
                    cache_bytes: 1024,
                    ..LogMemory::default()
                },
                state_cache: Default::default(),
            },
            events,
            Some(senders),
        ))
        .await
        .unwrap();
        // The compactor runs only on request, so GC removes the same SSTs on every replay.
        let compaction = &node.writer.source().manual_compaction;
        compaction.store(true, std::sync::atomic::Ordering::Relaxed);
        self.nodes.insert(id, node);
        let process = self.processes.values().max().map_or(1, |last| last + 1);
        self.processes.insert(id, process);
        self.trace.push(format!("start {id}"));
    }

    fn stop(&mut self, id: u64) {
        if let Some(node) = self.nodes.remove(&id) {
            self.installed_snapshots += node.installed_snapshots;
            self.extended += node.worker.state().extensions();
            drop(node);
        }
        self.trace.push(format!("stop {id}"));
    }

    fn leader(&self) -> Option<u64> {
        self.nodes.iter().find_map(|(&id, n)| {
            (n.raw.raft.state == StateRole::Leader && Some(id) != self.isolated).then_some(id)
        })
    }

    async fn drive(&mut self) {
        let mut failed = vec![];
        for (&id, node) in &mut self.nodes {
            node.drive_ready().await.unwrap();
            if self.paused_log != Some(id) {
                if let Err(error) = node.drain_log().await {
                    assert_eq!(
                        self.expected_log_failure,
                        Some(id),
                        "unexpected disk error: {error}"
                    );
                    self.expected_log_failure = None;
                    self.log_failures += 1;
                    failed.push(id);
                    continue;
                }
            }
            if self.paused_apply != Some(id) {
                if self.paused_reads == Some(id) {
                    node.drain_state().await.unwrap();
                } else {
                    node.drain_apply().await.unwrap();
                }
            } else if self.paused_reads != Some(id) {
                node.drain_reads().await.unwrap();
            }
            if self.paused_flush != Some(id) && node.compaction.active() {
                // A flush only waits for a covering base, which a later writer step publishes.
                // So the simulation polls it once and goes on.
                let completed = futures::FutureExt::now_or_never(node.compaction.next());
                if let Some(completed) = completed {
                    node.finish_compaction_flush(completed.unwrap()).unwrap();
                }
            }
        }
        for id in failed {
            self.trace.push(format!("log failure {id}"));
            self.stop(id);
        }
        for (&(from, _), link) in &mut self.links {
            while let Ok(message) = link.try_recv() {
                if let Some(node) = self.nodes.get(&from) {
                    if !message.reject
                        && matches!(
                            message.get_msg_type(),
                            MessageType::MsgAppendResponse | MessageType::MsgRequestVoteResponse
                        )
                    {
                        let durable = &node.raw.store().durable;
                        assert!(
                            durable.hard_state.term >= message.term,
                            "reply preceded durable term"
                        );
                        if durable.hard_state.term == message.term {
                            if message.get_msg_type() == MessageType::MsgRequestVoteResponse {
                                assert_eq!(
                                    durable.hard_state.vote, message.to,
                                    "vote preceded persistence"
                                );
                            } else {
                                assert!(
                                    message.index <= durable.last_index,
                                    "append reply preceded persistence"
                                );
                            }
                        }
                    }
                }
                self.messages.push(message);
            }
        }
        let mut pending = vec![];
        for mut request in self.requests.drain(..) {
            match request.reply.try_recv() {
                Ok(Ok(result)) => {
                    let mut result = match result.into_outcome().result {
                        Ok(result) => result,
                        // Only a quota check rejects a client Put here.
                        Err(error) => {
                            assert_eq!(error.code, tonic::Code::ResourceExhausted as i32);
                            self.trace.push(format!("reject {}", request.key));
                            continue;
                        }
                    };
                    let revision = crate::mvcc::response_header(&mut result).unwrap().revision;
                    for (&id, node) in &mut self.nodes {
                        let (tx, rx) = oneshot::channel();
                        if node.forwarding.subscribe().applied(revision) {
                            assert!(node.state.revision >= revision);
                            tx.send(Ok(Value::Null)).unwrap();
                        } else {
                            node.wait_revision(revision, tx);
                        }
                        self.revisions.push(RevisionRequest {
                            node: id,
                            revision,
                            reply: rx,
                        });
                    }
                    self.trace.push(format!("ack {}", request.key));
                    self.acknowledged.insert(request.key, request.value);
                }
                Ok(Err(_)) | Err(oneshot::error::TryRecvError::Closed) => {}
                Err(oneshot::error::TryRecvError::Empty) => pending.push(request),
            }
        }
        self.requests = pending;
        let mut pending_reads = vec![];
        for mut read in self.reads.drain(..) {
            match read.reply.try_recv() {
                Ok(Ok(result)) => {
                    let shared = matches!(result, KvReply::Shared(_));
                    let Some(Resp::ResponseRange(response)) =
                        result.into_outcome().result.unwrap().response
                    else {
                        panic!("expected range");
                    };
                    for kv in &response.kvs {
                        let key = String::from_utf8_lossy(&kv.key).into_owned();
                        let value = String::from_utf8_lossy(&kv.value).into_owned();
                        assert!(
                            !key.starts_with("key-") || self.proposed.contains(&(key, value)),
                            "a read returned a value that no client wrote"
                        );
                    }
                    for (key, value) in read.known {
                        assert!(
                            response
                                .kvs
                                .iter()
                                .any(|kv| kv.key == key.as_bytes() && kv.value == value.as_bytes()),
                            "read lost a prior acknowledged write"
                        );
                    }
                    self.trace.push(format!(
                        "read ack {} {} shared={shared}",
                        read.node,
                        response.kvs.len()
                    ));
                    self.read_count += 1;
                }
                Ok(Err(_)) | Err(oneshot::error::TryRecvError::Closed) => {}
                Err(oneshot::error::TryRecvError::Empty) => pending_reads.push(read),
            }
        }
        self.reads = pending_reads;
        self.revisions
            .retain_mut(|request| match request.reply.try_recv() {
                Ok(Ok(_)) => {
                    if let Some(node) = self.nodes.get(&request.node) {
                        assert!(
                            node.state.revision >= request.revision,
                            "revision barrier preceded local apply"
                        );
                    }
                    self.trace.push(format!(
                        "revision ack {} {}",
                        request.node, request.revision
                    ));
                    self.revision_count += 1;
                    false
                }
                Ok(Err(_)) | Err(oneshot::error::TryRecvError::Closed) => false,
                Err(oneshot::error::TryRecvError::Empty) => true,
            });
        let mut leaders = BTreeMap::new();
        for (&id, node) in &self.nodes {
            let term = node.raw.raft.term;
            assert!(term >= *self.terms.get(&id).unwrap_or(&0));
            self.terms.insert(id, term);
            if node.raw.raft.state == StateRole::Leader {
                assert!(
                    leaders.insert(term, id).is_none(),
                    "two leaders in term {term}"
                );
            }
            let store = node.raw.store();
            // Apply may pass the volatile commit marker, but never Raft commit or local persistence.
            assert!(node.state.applied <= node.raw.raft.raft_log.committed);
            assert!(
                node.state.applied <= node.raw.raft.raft_log.persisted.max(store.snapshot_index())
            );
            assert!(store.commit_index() <= store.last_index().unwrap());
            let stats = store.stats();
            for (used, limit) in [
                ("cache_bytes", "cache_limit"),
                ("pending_bytes", "pending_limit"),
                ("term_bytes", "term_limit"),
                ("read_bytes", "read_limit"),
            ] {
                assert!(stats[used].as_u64().unwrap() <= stats[limit].as_u64().unwrap());
            }
            let disk = node.log_worker.storage();
            let entries = disk
                .entries(
                    disk.first_index().unwrap(),
                    disk.commit_index() + 1,
                    None,
                    raft::GetEntriesContext::empty(false),
                )
                .unwrap();
            for entry in entries {
                let bytes = entry.encode_to_vec();
                if let Some(previous) = self.committed_entries.insert(entry.index, bytes.clone()) {
                    assert_eq!(previous, bytes, "committed index {} changed", entry.index);
                }
            }
        }
        self.settle_opens().await;
        if self.reference.is_some() {
            self.check_reference().await;
        }
    }

    /// Background checkpoint opens of shared mode finish before the next step, so a schedule
    /// stays exact.
    async fn settle_opens(&mut self) {
        for node in self.nodes.values_mut() {
            node.worker.state_mut().settle_opens().await;
            // An announcement of the current base offers it at once, with no open.
            node.writer.note_offers();
        }
    }

    async fn deliver(&mut self, faults: bool) {
        if self.messages.is_empty() {
            return;
        }
        let index = self.choose(self.messages.len());
        let mut message = self.messages.remove(index);
        self.trace.push(format!(
            "message {} {} {:?} {} {}",
            message.from,
            message.to,
            message.get_msg_type(),
            message.term,
            message.index
        ));
        let partitioned = self
            .isolated
            .is_some_and(|id| message.from == id || message.to == id);
        if partitioned || (faults && self.choose(30) == 0) {
            return;
        }
        if faults && self.choose(40) == 0 {
            self.messages.push(message.clone());
        }
        if message.get_msg_type() == MessageType::MsgSnapshot {
            let Some(accepted) = self.receive_snapshot(message.clone()).await else {
                return;
            };
            message = accepted;
        }
        let process = self.process(message.from);
        let mut saved = None;
        if let Some(node) = self.nodes.get_mut(&message.to) {
            node.raw.store().note_process(message.from, process);
            let snapshot = message.get_msg_type() == MessageType::MsgSnapshot;
            let from = message.from;
            let to = message.to;
            let index = message.get_snapshot().get_metadata().index;
            node.step_peer(message).unwrap();
            node.drive_ready().await.unwrap();
            if snapshot
                && node.raw.store().snapshot_index() > 0
                && !node.raw.store().snapshot_pending()
            {
                if let Some(sender) = self.nodes.get_mut(&from) {
                    // The normal HTTP transport reports this after the receiver saves Ready.
                    sender.raw.report_snapshot(to, SnapshotStatus::Finish);
                }
            } else if snapshot {
                saved = Some((from, to, index));
            }
        }
        self.drive().await;
        // The receiver saved the snapshot during the drive. A snapshot that the receiver asked
        // for keeps the leader in snapshot state until this report.
        if let Some((from, to, index)) = saved {
            let stored = self
                .nodes
                .get(&to)
                .is_some_and(|node| node.raw.store().snapshot_index() >= index);
            if let (true, Some(sender)) = (stored, self.nodes.get_mut(&from)) {
                sender.raw.report_snapshot(to, SnapshotStatus::Finish);
            }
        }
    }

    async fn tick(&mut self, id: u64) {
        if let Some(node) = self.nodes.get_mut(&id) {
            // Fix the timeout before every logical tick, including after a Raft role change.
            // Five members need timeouts below twice the election tick.
            let step = if self.members > 3 { 1 } else { 2 };
            node.raw
                .raft
                .set_randomized_election_timeout(10 + id as usize * step);
            node.tick_raft().unwrap();
            // The shared leader serves a demand for a base at each tick, as `tick_shared` does.
            node.certify_base().unwrap();
            node.retry_reads();
            self.trace.push(format!("tick {id}"));
        }
        self.report_view(id);
        self.drive().await;
    }

    async fn propose(&mut self) {
        let Some(leader) = self.leader() else {
            return;
        };
        self.next_request += 1;
        let key = format!("key-{}", self.next_request);
        let value = format!("value-{}", self.next_request);
        let (tx, rx) = oneshot::channel();
        self.nodes
            .get_mut(&leader)
            .unwrap()
            .propose(
                RaftCommand::Etcd {
                    origin: 1,
                    id: key.clone(),
                    etcd: RequestOp {
                        request: Some(Op::RequestPut(PutRequest {
                            key: key.as_bytes().to_vec(),
                            value: value.as_bytes().to_vec(),
                            lease: 77,
                            ..Default::default()
                        })),
                    },
                },
                ClientResponse::Kv(tx),
            )
            .unwrap();
        self.trace.push(format!("propose {leader} {key}"));
        self.proposed.insert((key.clone(), value.clone()));
        self.requests.push(ClientRequest {
            key,
            value,
            reply: rx,
        });
        self.drive().await;
    }

    async fn assert_acknowledged(&self, id: u64) {
        let state = self.nodes[&id].worker.state();
        for (key, value) in &self.acknowledged {
            let outcome = state
                .read_etcd(&RequestOp {
                    request: Some(Op::RequestRange(RangeRequest {
                        key: key.as_bytes().to_vec(),
                        ..Default::default()
                    })),
                })
                .await
                .unwrap();
            let Some(Resp::ResponseRange(response)) = outcome.result.unwrap().response else {
                panic!("expected range");
            };
            assert_eq!(response.kvs.len(), 1, "member {id} lost {key}");
            assert_eq!(response.kvs[0].value, value.as_bytes());
        }
    }

    /// Tick every member `rounds` times and deliver the resulting messages.
    /// 25 rounds pass an election timeout.
    async fn settle(&mut self, rounds: usize) {
        let deliveries = if self.members > 3 { 60 } else { 30 };
        for _ in 0..rounds {
            for id in 1..=self.members {
                self.tick(id).await;
            }
            for _ in 0..deliveries {
                self.deliver(false).await;
            }
        }
    }

    async fn lease(&mut self, lease: LeaseCommand) {
        let leader = self.leader().unwrap();
        let (tx, mut rx) = oneshot::channel();
        let id = format!("lease-{lease:?}");
        self.trace.push(id.clone());
        self.nodes
            .get_mut(&leader)
            .unwrap()
            .propose(
                RaftCommand::Lease {
                    id,
                    lease,
                    origin: leader,
                },
                tx,
            )
            .unwrap();
        self.drive().await;
        for _ in 0..300 {
            self.deliver(false).await;
            if let Ok(result) = rx.try_recv() {
                let outcome: Outcome<LeaseReply> = serde_json::from_value(result.unwrap()).unwrap();
                outcome.result.unwrap();
                return;
            }
        }
        panic!("lease command did not commit");
    }
}

impl Simulation {
    /// Make `id` the leader with a leadership transfer.
    async fn lead(&mut self, id: u64) {
        for _ in 0..40 {
            match self.leader() {
                Some(leader) if leader == id => return,
                Some(leader) => self.nodes.get_mut(&leader).unwrap().raw.transfer_leader(id),
                None => {}
            }
            for _ in 0..30 {
                self.deliver(false).await;
            }
            for member in 1..=3 {
                self.tick(member).await;
            }
        }
        panic!("member {id} did not become the leader");
    }

    /// Propose `count` Puts on the leader and deliver their messages.
    async fn write(&mut self, count: usize) {
        for _ in 0..count {
            self.propose().await;
            for _ in 0..30 {
                self.deliver(false).await;
            }
        }
        self.settle(1).await;
    }

    /// Replace member `id` with an empty data directory and the quota flag `flag`.
    async fn replace(&mut self, id: u64, flag: i64) {
        self.stop(id);
        // The new member has no log. Drop the old messages, and let the leader probe it again
        // as it would probe a new member.
        self.messages
            .retain(|message| message.to != id && message.from != id);
        if let Some(leader) = self.leader() {
            let progress = self.nodes.get_mut(&leader).unwrap().raw.raft.mut_prs();
            let progress = progress.get_mut(id).unwrap();
            progress.matched = 0;
            progress.become_probe();
        }
        std::fs::remove_dir_all(self.root.path().join(id.to_string())).unwrap();
        self.checked.remove(&id);
        self.terms.remove(&id);
        self.quotas.insert(id, flag);
        self.start(id).await;
    }

    /// Run out the election timer of every member, as after a long time with no leader. The next
    /// tick of a member starts an election, and no member refuses it for its leader lease.
    fn expire_elections(&mut self) {
        for node in self.nodes.values_mut() {
            let raft = &mut node.raw.raft;
            raft.election_elapsed = raft.election_elapsed.max(2 * raft.election_timeout());
        }
    }

    /// A follower, not `except`.
    fn follower(&self, except: u64) -> u64 {
        let leader = self.leader().unwrap();
        (1..=self.members)
            .find(|&id| id != leader && id != except)
            .unwrap()
    }
}

/// Compact every L0 SST and sorted run into one run when the L0 has `limit` SSTs or more, and
/// wait until the compaction ends. DST turns off the compactor of the writer. See
/// `Source::manual_compaction`.
pub(super) async fn dst_compact(admin: &slatedb::admin::Admin, limit: usize) -> Result<()> {
    use slatedb::compactor::{CompactionSpec, CompactionStatus, SourceId};
    let manifest = admin
        .read_manifest(None)
        .await?
        .context("missing manifest")?;
    if manifest.l0().len() < limit {
        return Ok(());
    }
    let mut sources: Vec<_> = manifest
        .l0()
        .iter()
        .map(|view| SourceId::SstView(view.id))
        .collect();
    sources.extend(
        manifest
            .compacted()
            .iter()
            .map(|run| SourceId::SortedRun(run.id)),
    );
    let destination = manifest
        .compacted()
        .iter()
        .map(|run| run.id)
        .min()
        .unwrap_or(0);
    let job = admin
        .submit_compaction(CompactionSpec::new(sources, destination))
        .await?;
    for _ in 0..400 {
        tokio::time::sleep(Duration::from_millis(10)).await;
        match admin.read_compaction(job.id(), None).await? {
            Some(job) if matches!(job.status(), CompactionStatus::Completed) => {
                // The compactor commits the manifest after the job. Wait for the new L0.
                let manifest = admin
                    .read_manifest(None)
                    .await?
                    .context("missing manifest")?;
                if manifest.l0().len() < limit {
                    return Ok(());
                }
            }
            Some(job) if matches!(job.status(), CompactionStatus::Failed) => {
                anyhow::bail!("a DST compaction failed")
            }
            _ => {}
        }
    }
    anyhow::bail!("a DST compaction timed out")
}

/// Flush the open writer of a checkpoint step. Then compact the L0 SSTs on request before SlateDB
/// would stop writes for them.
async fn flush(node: &Node, writer: &shared::DstWriter) -> Result<()> {
    let Some(db) = writer.db() else {
        return Ok(());
    };
    db.flush().await?;
    dst_compact(node.writer.admin(), 4).await
}

/// The writer steps of a shared-mode schedule.
#[derive(Clone, Copy, Debug)]
enum WriterStep {
    Open,
    Materialize,
    Checkpoint,
    Announce,
    Retain,
}

impl Simulation {
    /// Run one writer step on member `id`. An error closes the writer, and the next open step
    /// opens it again. An invariant error also stops the member, as `fault::recover` does.
    async fn writer_step(
        &mut self,
        writers: &mut BTreeMap<u64, shared::DstWriter>,
        id: u64,
        step: WriterStep,
    ) {
        // Members that the leader can reach report their pins. An isolated member does not, so
        // its base can go away, as for a member that stopped reporting.
        let mut pins = std::collections::HashSet::new();
        for (&member, other) in &self.nodes {
            if Some(member) != self.isolated {
                pins.extend(other.dst_pins());
            }
        }
        let Some(node) = self.nodes.get_mut(&id) else {
            return;
        };
        let writer = writers.entry(id).or_default();
        // SlateDB finds a fence at its next manifest write or poll, at a time that the schedule
        // does not fix. The simulated writer checks the epoch first, so a newer writer always
        // fences it at its next step.
        if writer.db().is_some()
            && !matches!(step, WriterStep::Open)
            && node.dst_fenced(writer).await
        {
            writer.close().await;
            self.trace.push(format!("writer {id} {step:?} fenced"));
            self.drive().await;
            return;
        }
        let result = match step {
            WriterStep::Open => node.dst_open(writer, false).await.map(drop),
            WriterStep::Materialize => node.dst_materialize(writer).await,
            WriterStep::Checkpoint => match flush(node, writer).await {
                Ok(()) => node.dst_checkpoint(writer).await,
                error => error,
            },
            WriterStep::Announce => node.dst_announce(writer).await,
            WriterStep::Retain => node.dst_retain(writer, &pins).await,
        };
        match result {
            Ok(()) => self.trace.push(format!("writer {id} {step:?}")),
            Err(error) if shared::fenced(&error) => {
                writer.close().await;
                self.trace.push(format!("writer {id} {step:?} fenced"));
            }
            // `fault::recover`: the task abandons the writer and opens it again after a backoff,
            // and the member stays. Only an invariant error stops the member.
            Err(error) => {
                writer.close().await;
                let fault = shared::classify(&error);
                self.trace
                    .push(format!("writer {id} {step:?} failed {fault:?}"));
                tracing::debug!(%error, id, "writer step failed");
                if fault == shared::Fault::Invariant {
                    self.stop(id);
                }
            }
        }
        self.drive().await;
    }

    /// Read the keys on a member's published view, as a serializable Range does. A ReadIndex read
    /// waits on wall-clock limits, so shared schedules use this read. It must return only
    /// values that clients wrote.
    async fn view_read(&mut self) {
        let pick = self.choose(3);
        let Some((&id, node)) = self.nodes.iter().nth(pick) else {
            return;
        };
        let view = node.state.read.clone();
        let request = RequestOp {
            request: Some(Op::RequestRange(RangeRequest {
                key: b"key-".to_vec(),
                range_end: b"key.".to_vec(),
                ..Default::default()
            })),
        };
        if let Ok(Some(Resp::ResponseRange(response))) = view
            .read(&request, None)
            .await
            .map(|outcome| outcome.result.ok().and_then(|reply| reply.response))
        {
            for kv in &response.kvs {
                let key = String::from_utf8_lossy(&kv.key).into_owned();
                let value = String::from_utf8_lossy(&kv.value).into_owned();
                assert!(
                    self.proposed.contains(&(key, value)),
                    "seed {}: a view read returned a value that no client wrote",
                    self.seed
                );
            }
        }
        self.trace.push(format!("view read {id}"));
    }

    /// Close every outstanding client request. The members see closed responses at their next
    /// checks, which the schedule fixes.
    async fn drop_clients(&mut self) {
        self.drive().await;
        self.requests.clear();
        self.reads.clear();
        self.revisions.clear();
        self.trace.push("drop clients".into());
        self.drive().await;
    }

    /// Run GC once on the shared database, after its minimum age.
    async fn collect_garbage(&mut self) {
        let Some(node) = self.nodes.values().next() else {
            return;
        };
        let writer = &node.writer;
        let directory = slatedb::config::GarbageCollectorDirectoryOptions {
            interval: None,
            min_age: std::time::Duration::from_millis(200),
            dry_run: false,
        };
        let options = slatedb::config::GarbageCollectorOptions {
            manifest_options: Some(directory),
            compacted_options: Some(directory),
            ..Default::default()
        };
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        let admin = writer.admin().clone();
        let _ = admin.run_gc_once(options).await;
        self.trace.push("gc".into());
        self.drive().await;
    }

    /// A shared-mode schedule: one store, writer steps at random points, leader changes,
    /// crashes between writer steps, late opens of deposed writers, partitions, object store
    /// failures, retention with GC, and reads on old views.
    async fn run_shared(seed: u64) -> Vec<String> {
        let mut sim = Self::new(seed).await;
        let mut writers = BTreeMap::new();
        for _ in 0..100 {
            sim.deliver(false).await;
        }
        sim.lease(LeaseCommand::Grant { id: 77, ttl: 3600 }).await;
        let mut stopped = None;
        let mut held = vec![];
        let mut collections = 0;
        for step in 0..800 {
            // Requests expire after 5 s of wall time, and shared steps do real object I/O. The
            // clients give up at fixed steps, so no request lives long enough to expire on time.
            if step % 40 == 39 {
                sim.drop_clients().await;
            }
            let leader = sim.leader();
            match sim.choose(34) {
                0..=10 => {
                    for _ in 0..4 {
                        sim.deliver(true).await;
                    }
                }
                11..=12 => {
                    let id = sim.choose(3) as u64 + 1;
                    // Ticks are rare here, so without this no member campaigns after a crash.
                    if leader.is_none() {
                        sim.expire_elections();
                    }
                    sim.tick(id).await;
                    // A follower asks for a snapshot at its last index. Announcements and
                    // unmaterialized batches can follow the leader's base row.
                    if sim.choose(2) == 0 {
                        if let Some(node) = sim.nodes.get_mut(&id) {
                            if node.raw.request_snapshot().is_ok() {
                                sim.trace.push(format!("request snapshot {id}"));
                            }
                        }
                    }
                }
                // A failed update changes no state, so an announcement can cover it.
                13..=14 => sim.propose_noop().await,
                15..=17 => sim.propose().await,
                18 => sim.view_read().await,
                31..=33 => {
                    if let Some(leader) = leader {
                        sim.writer_step(&mut writers, leader, WriterStep::Retain)
                            .await;
                    }
                }
                19..=20 | 28..=29 => {
                    if let Some(leader) = leader {
                        let step = match sim.choose(5) {
                            0 => WriterStep::Open,
                            1 => WriterStep::Materialize,
                            2 => WriterStep::Checkpoint,
                            3 => WriterStep::Announce,
                            _ => WriterStep::Retain,
                        };
                        let open = writers
                            .get(&leader)
                            .is_some_and(|writer: &shared::DstWriter| writer.db().is_some());
                        let step = if open { step } else { WriterStep::Open };
                        sim.writer_step(&mut writers, leader, step).await;
                    }
                }
                21 => {
                    // A crash between two writer steps, often on the leader: before or after a
                    // checkpoint, its marker, or its announcement.
                    if let Some(id) = stopped.take() {
                        sim.start(id).await;
                        sim.drive().await;
                    } else {
                        let id = leader.unwrap_or(sim.choose(3) as u64 + 1);
                        if let Some(mut writer) = writers.remove(&id) {
                            writer.close().await;
                        }
                        sim.stop(id);
                        stopped = Some(id);
                    }
                }
                22 => {
                    // A deposed member's slow open fences the current writer.
                    let pick = sim.choose(2);
                    let deposed = (1..=3)
                        .filter(|id| Some(*id) != leader && sim.nodes.contains_key(id))
                        .nth(pick);
                    if let Some(id) = deposed {
                        let writer = writers.entry(id).or_default();
                        let node = sim.nodes.get_mut(&id).unwrap();
                        if node.dst_open(writer, true).await.unwrap_or(false) {
                            sim.trace.push(format!("writer {id} late open"));
                        }
                    }
                }
                23 => {
                    sim.isolated = match sim.isolated {
                        Some(_) => None,
                        None => Some(sim.choose(3) as u64 + 1),
                    };
                    sim.trace.push(format!("isolate {:?}", sim.isolated));
                }
                24..=26 => {
                    // Hold a view across later switches, retention, and GC, then read it.
                    if held.len() < 3 {
                        let pick = sim.choose(3);
                        if let Some(node) = sim.nodes.values().nth(pick) {
                            held.push((node.state.applied, node.state.read.clone()));
                        }
                    } else {
                        let (applied, view) = held.remove(0);
                        if let (Ok(actual), Some(expected)) =
                            (rows_digest(view.state()).await, sim.digests.get(&applied))
                        {
                            assert_eq!(actual, *expected, "seed {seed}: a held view changed");
                        }
                        sim.trace.push("held read".into());
                    }
                }
                27 if sim.choose(2) == 0 => {
                    // The object store rejects SST writes for a while. The writer of the leader
                    // fails, and the leader opens it again.
                    let store = sim.shared.clone();
                    store.set(crate::object::faults::FAIL_PUT);
                    for _ in 0..5 {
                        sim.deliver(true).await;
                        if let Some(leader) = sim.leader() {
                            sim.writer_step(&mut writers, leader, WriterStep::Checkpoint)
                                .await;
                        }
                    }
                    store.set(0);
                    sim.trace.push("store recovered".into());
                }
                27 => {
                    // The next writer open of the leader reads an object that does not decode.
                    // The member stays, and a later open step opens the writer.
                    if let Some(leader) = leader {
                        let source = sim.nodes[&leader].writer.source();
                        let error = crate::mvcc::storage_fault::stored("an injected data error");
                        *source.fail_writer.lock().unwrap() = Some(error);
                        sim.writer_step(&mut writers, leader, WriterStep::Open)
                            .await;
                    }
                }
                _ => {
                    if collections < 3 {
                        collections += 1;
                        sim.collect_garbage().await;
                    }
                }
            }
        }
        // Recover: every member runs, the network heals, and the leader publishes.
        if let Some(id) = stopped.take() {
            sim.start(id).await;
        }
        sim.isolated = None;
        for round in 0..60 {
            if round % 10 == 9 {
                sim.drop_clients().await;
            }
            for id in 1..=3 {
                sim.tick(id).await;
            }
            if round < 40 {
                sim.propose().await;
                // A failed update after the Put lets the next announcement cover it.
                if round % 2 == 0 {
                    sim.propose_noop().await;
                }
                sim.view_read().await;
            }
            for _ in 0..30 {
                sim.deliver(false).await;
            }
            if let Some(leader) = sim.leader() {
                for step in [
                    WriterStep::Open,
                    WriterStep::Materialize,
                    WriterStep::Checkpoint,
                    WriterStep::Announce,
                ] {
                    let open = writers
                        .get(&leader)
                        .is_some_and(|writer: &shared::DstWriter| writer.db().is_some());
                    if matches!(step, WriterStep::Open) == open {
                        continue;
                    }
                    sim.writer_step(&mut writers, leader, step).await;
                }
            }
        }
        sim.restart_above_rows().await;
        sim.assert_records_open().await;
        // Every member reaches the last commit with the reference state.
        let commit = sim
            .nodes
            .values()
            .map(|n| n.raw.raft.raft_log.committed)
            .max();
        for (&id, node) in &sim.nodes {
            assert_eq!(
                Some(node.state.applied),
                commit,
                "seed {seed}: member {id} did not catch up"
            );
            let actual = rows_digest(node.state.read.state()).await.unwrap();
            assert_eq!(Some(&actual), sim.digests.get(&node.state.applied));
        }
        for (key, value) in sim.acknowledged.clone() {
            let node = sim.nodes.values().next().unwrap();
            let row = node
                .state
                .read
                .state()
                .get(format!("etcd/current/{key}").as_bytes())
                .await
                .unwrap();
            assert!(
                row.is_some(),
                "seed {seed}: lost acknowledged {key}={value}"
            );
        }
        for (_, mut writer) in writers {
            writer.close().await;
        }
        let extended = sim.extended
            + sim
                .nodes
                .values()
                .map(|node| node.worker.state().extensions())
                .sum::<u64>();
        sim.trace.push(format!(
            "acknowledged {} views checked {} extended {extended}",
            sim.acknowledged.len(),
            sim.view_checks
        ));
        let compared = sim.reference.as_ref().unwrap().compared;
        eprintln!(
            "shared DST seed {seed}: {compared} reference indexes equal, {} views checked again \
             at an index",
            sim.base_checks
        );
        sim.trace.clone()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shared_store_schedules_replay_identically() {
    let seeds = std::env::var("SLATEDB_DST_SHARED_SEEDS").unwrap_or_else(|_| "1,2,3".into());
    // Opens and installs above a checkpoint row, over all seeds. The loaded-field check covers
    // that path only when it runs.
    let mut extended = 0;
    for seed in seeds.split(',').map(|s| s.parse::<u64>().unwrap()) {
        eprintln!("shared DST seed {seed}: starting");
        let first = Simulation::run_shared(seed).await;
        let second = Simulation::run_shared(seed).await;
        let trace_dir =
            std::env::var("SLATEDB_DST_TRACE_DIR").unwrap_or_else(|_| "target/dst".into());
        std::fs::create_dir_all(&trace_dir).unwrap();
        std::fs::write(
            format!("{trace_dir}/shared-seed-{seed}.json"),
            serde_json::to_vec_pretty(&first).unwrap(),
        )
        .unwrap();
        assert_eq!(
            first, second,
            "shared seed {seed}: simulation schedule changed"
        );
        let count = first
            .last()
            .and_then(|line| line.rsplit(' ').next())
            .and_then(|count| count.parse::<u64>().ok())
            .unwrap();
        extended += count;
        eprintln!(
            "shared DST seed {seed}: {} events replayed, {count} bases above their row",
            first.len()
        );
    }
    assert!(
        extended > 0,
        "no member opened or installed a base above its row"
    );
}

impl Simulation {
    /// Give every member the same automatic compaction schedule, starting at `start`.
    fn enable_auto_compaction(&mut self, period: Duration, start: Instant) {
        for node in self.nodes.values_mut() {
            node.auto_compaction = Some(auto_compaction::AutoCompaction::new(
                crate::compactor::Retention::Periodic(period),
                start,
                node.state.revision,
            ));
        }
    }

    /// Run one automatic compaction turn on every member with the logical time `now`.
    async fn auto_compaction_turn(&mut self, now: Instant) {
        for node in self.nodes.values_mut() {
            // The run loop fails a pending proposal after a leadership change.
            node.expire_pending();
            node.drive_auto_compaction(now).unwrap();
        }
        self.trace.push("auto compaction turn".into());
        self.drive().await;
    }

    fn auto_running(&self, id: u64) -> bool {
        self.nodes[&id]
            .auto_compaction
            .as_ref()
            .is_some_and(|auto| auto.running())
    }

    async fn rows(&self, id: u64) -> Vec<(bytes::Bytes, bytes::Bytes)> {
        let state = self.nodes[&id].worker.state();
        let mut rows = state
            .applied_state()
            .scan_under(b"etcd/", &Default::default())
            .await
            .unwrap();
        let mut all = vec![];
        while let Some(row) = rows.next().await.unwrap() {
            all.push((row.key, row.value));
        }
        all
    }
}

/// Automatic compaction stays correct when leadership moves twice: once between the first
/// leader's Compact proposal and its commit, and once while replicas remove compacted rows.
#[tokio::test]
async fn auto_compaction_survives_leader_changes_mid_compaction() {
    let mut sim = Simulation::new(23).await;
    for _ in 0..100 {
        sim.deliver(false).await;
    }
    sim.lease(LeaseCommand::Grant { id: 77, ttl: 3600 }).await;
    let start = Instant::now();
    let period = Duration::from_secs(10);
    let step = period / 10;
    sim.enable_auto_compaction(period, start);
    let mut clock = 0;
    let mut now;
    // Write until the leader proposes its first automatic Compact.
    let first = loop {
        clock += 1;
        assert!(clock < 100, "no automatic Compact was proposed");
        now = start + step * clock;
        for _ in 0..3 {
            sim.propose().await;
            for _ in 0..30 {
                sim.deliver(false).await;
            }
        }
        let leader = sim.leader().unwrap();
        // Cut the leader off before its Compact entry reaches any follower.
        sim.isolated = Some(leader);
        sim.auto_compaction_turn(now).await;
        if sim.auto_running(leader) {
            break leader;
        }
        sim.isolated = None;
    };
    assert!(clock >= 10, "the first Compact must wait one period");
    let proposed = sim.nodes[&first].raw.raft.raft_log.last_index();
    sim.trace.push(format!("isolate compacting leader {first}"));
    // The majority elects a new leader. Its own samples let it compact without waiting.
    sim.settle(25).await;
    let second = sim.leader().expect("the majority must elect a leader");
    assert_ne!(second, first);
    sim.auto_compaction_turn(now).await;
    assert!(
        !sim.auto_running(first),
        "the old leader must fail its Compact: role {:?}, pending {}, waiting {}",
        sim.nodes[&first].raw.raft.state,
        sim.nodes[&first].pending.len(),
        sim.nodes[&first].waiting_proposals.is_empty(),
    );
    assert!(
        sim.nodes[&first].state.compact_revision <= 0,
        "first {first} second {second} compact {} applied {} proposed {proposed} others {:?}",
        sim.nodes[&first].state.compact_revision,
        sim.nodes[&first].state.applied,
        sim.nodes
            .values()
            .map(|n| (n.state.compact_revision, n.state.applied))
            .collect::<Vec<_>>(),
    );
    // The new leader may need one turn to see the Compact result of a failed attempt.
    for _ in 0..3 {
        if sim.auto_running(second) {
            break;
        }
        clock += 1;
        now = start + step * clock;
        sim.auto_compaction_turn(now).await;
    }
    assert!(sim.auto_running(second), "the new leader must compact");
    // Apply the Compact on the majority, then move leadership again while rows remain.
    for _ in 0..200 {
        sim.deliver(false).await;
        if sim.nodes[&second].state.compaction_pending
            && sim.nodes[&second].state.compact_revision > 0
        {
            break;
        }
    }
    let boundary = sim.nodes[&second].state.compact_revision;
    assert!(boundary > 0, "the new leader must apply its Compact");
    assert!(
        sim.nodes[&second].state.compaction_pending,
        "physical removal must still run"
    );
    sim.isolated = Some(second);
    sim.trace
        .push(format!("isolate leader {second} during physical removal"));
    // Heal the first leader so the remaining pair forms a quorum.
    sim.settle(30).await;
    let third = sim
        .leader()
        .expect("the remaining pair must elect a leader");
    assert_ne!(third, second);
    sim.isolated = None;
    sim.trace.push("heal".into());
    for _ in 0..40 {
        clock += 1;
        now = start + step * clock;
        sim.auto_compaction_turn(now).await;
        sim.settle(1).await;
    }
    sim.settle(25).await;
    let applied: BTreeSet<u64> = sim.nodes.values().map(|n| n.state.applied).collect();
    assert_eq!(
        applied.len(),
        1,
        "members must apply the same log: {applied:?}"
    );
    let compacted: BTreeSet<i64> = sim
        .nodes
        .values()
        .map(|n| n.state.compact_revision)
        .collect();
    assert_eq!(
        compacted.len(),
        1,
        "members disagree on the boundary: {compacted:?}"
    );
    let boundary = *compacted.first().unwrap();
    assert!(boundary > 0);
    let reference = sim.rows(1).await;
    for id in 1..=3 {
        let node = &sim.nodes[&id];
        assert!(
            !node.state.compaction_pending,
            "member {id} did not finish removal"
        );
        assert!(boundary <= node.state.revision);
        let state = node.worker.state();
        assert_eq!(
            state.logical_usage(),
            crate::state::maintenance::count_logical_usage(state.applied_state())
                .await
                .unwrap()
        );
        // Watch journal rows below the boundary are gone.
        let mut journal = state
            .applied_state()
            .scan_under(crate::watch::EVENTS, &Default::default())
            .await
            .unwrap();
        while let Some(row) = journal.next().await.unwrap() {
            assert!(
                row.key.as_ref() >= crate::watch::key(boundary).as_slice(),
                "member {id} kept a compacted journal row"
            );
        }
        assert_eq!(sim.rows(id).await, reference, "member {id} rows differ");
        sim.assert_acknowledged(id).await;
    }
}

impl Simulation {
    /// Write a few entries, then run one automatic compaction turn at `now`.
    async fn write_and_auto_compact(&mut self, now: Instant) {
        for _ in 0..3 {
            self.propose().await;
            for _ in 0..30 {
                self.deliver(false).await;
            }
        }
        self.auto_compaction_turn(now).await;
        self.settle(1).await;
    }
}

fn next(clock: &mut u32) -> u32 {
    *clock += 1;
    *clock
}

/// A step read that stays unfinished across several sampling intervals must not be restarted by
/// new automatic targets. The leader keeps the boundary until removal finishes, then compacts
/// again.
#[tokio::test]
async fn auto_compaction_waits_for_pending_removal() {
    let mut sim = Simulation::new(31).await;
    for _ in 0..100 {
        sim.deliver(false).await;
    }
    sim.lease(LeaseCommand::Grant { id: 77, ttl: 3600 }).await;
    let start = Instant::now();
    let period = Duration::from_secs(10);
    let step = period / 10;
    sim.enable_auto_compaction(period, start);
    // Every leader holds its steps, as if each background read were slow.
    for node in sim.nodes.values_mut() {
        node.hold_compact_steps = true;
    }
    let mut clock = 0;
    let leader = loop {
        sim.write_and_auto_compact(start + step * next(&mut clock))
            .await;
        assert!(clock < 100, "no automatic Compact was applied");
        let leader = sim.leader().unwrap();
        if sim.nodes[&leader].state.compact_revision > 0 {
            break leader;
        }
    };
    let first = sim.nodes[&leader].state.compact_revision;
    assert!(sim.nodes[&leader].state.compaction_pending);
    // Four more periods while the revision grows. The removal stays pending, so no new target
    // replaces its step.
    for _ in 0..40 {
        sim.write_and_auto_compact(start + step * next(&mut clock))
            .await;
        assert_eq!(sim.leader(), Some(leader), "leadership must not move");
        let node = &sim.nodes[&leader];
        assert!(node.state.compaction_pending);
        assert_eq!(
            node.state.compact_revision, first,
            "a new automatic target was proposed during pending removal"
        );
        assert!(!sim.auto_running(leader));
    }
    assert!(sim.nodes[&leader].state.revision > first + 100);
    // Release the steps. Removal finishes, and the schedule moves the boundary again.
    for node in sim.nodes.values_mut() {
        node.hold_compact_steps = false;
    }
    let mut finished = false;
    for _ in 0..40 {
        sim.write_and_auto_compact(start + step * next(&mut clock))
            .await;
        finished |= !sim.nodes[&leader].state.compaction_pending;
        if finished && sim.nodes[&leader].state.compact_revision > first {
            break;
        }
    }
    assert!(finished, "removal did not finish after the release");
    let boundary = sim.nodes[&leader].state.compact_revision;
    assert!(boundary > first, "automatic compaction did not resume");
    sim.settle(25).await;
    for id in 1..=3 {
        let node = &sim.nodes[&id];
        assert_eq!(node.state.compact_revision, boundary);
        assert!(
            !node.state.compaction_pending,
            "member {id} did not finish removal"
        );
        let state = node.worker.state();
        let mut journal = state
            .applied_state()
            .scan_under(crate::watch::EVENTS, &Default::default())
            .await
            .unwrap();
        while let Some(row) = journal.next().await.unwrap() {
            assert!(
                row.key.as_ref() >= crate::watch::key(boundary).as_slice(),
                "member {id} kept a compacted journal row"
            );
        }
        sim.assert_acknowledged(id).await;
    }
}
