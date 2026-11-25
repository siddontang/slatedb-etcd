//! Drive the real node with seeded message delivery and a logical clock.

use super::*;
use crate::lease::{LeaseCommand, LeaseReply};
use crate::mvcc::Outcome;
use crate::proto::etcdserverpb::request_op::Request as Op;
use crate::proto::etcdserverpb::response_op::Response as Resp;
use crate::proto::etcdserverpb::{PutRequest, RangeRequest, WatchCreateRequest};
use crate::state::{StateRead, StateScan};

/// A digest of every `etcd/` row of a state.
async fn rows_digest(state: &impl StateRead) -> std::result::Result<u32, slatedb::Error> {
    let mut scan = state.scan_under(b"etcd/", &Default::default()).await?;
    let mut digest = 0;
    while let Some(row) = scan.next().await? {
        digest = crc32c::crc32c_append(digest, &row.key);
        digest = crc32c::crc32c_append(digest, &row.value);
    }
    Ok(digest)
}

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
    stores: BTreeMap<u64, std::sync::Arc<crate::object::faults::FaultStore>>,
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
    paused_apply: Option<u64>,
    paused_flush: Option<u64>,
    paused_reads: Option<u64>,
    paused_log: Option<u64>,
    held_log: Option<(u64, log::Completion)>,
    expected_log_failure: Option<u64>,
    log_failures: usize,
    random: u64,
    next_request: u64,
    trace: Vec<String>,
    installed_snapshots: u64,
    /// Every value that a client proposed. A read must not return another value.
    proposed: BTreeSet<(String, String)>,
    /// Shared mode: the one object store of all members.
    shared: Option<std::sync::Arc<crate::object::faults::FaultStore>>,
    /// Shared mode and quota checks: a replicated state that applies the committed entries in
    /// order. Its quota flag disables the check, unlike the flag of any member.
    reference: Option<StateMachine>,
    /// The reference digest of the `etcd/` rows after each index.
    digests: BTreeMap<u64, u32>,
    /// The last applied index that the view check covered on each member.
    checked: BTreeMap<u64, u64>,
    /// Member views that matched the reference.
    view_checks: u64,
    /// The `--quota-backend-bytes` value of each member. Zero selects 2 GiB.
    quotas: BTreeMap<u64, i64>,
    /// Started members are directories of an earlier binary that propose no `SetQuota`.
    upgraded: bool,
}

impl Drop for Simulation {
    fn drop(&mut self) {
        // Keep the partial schedule when an invariant fails.
        let dir = std::env::var("SLATEDB_DST_TRACE_DIR").unwrap_or_else(|_| "target/dst".into());
        if std::fs::create_dir_all(&dir).is_ok() {
            if let Ok(data) = serde_json::to_vec_pretty(&self.trace) {
                let _ = std::fs::write(format!("{dir}/seed-{}-last.json", self.seed), data);
            }
        }
    }
}

impl Simulation {
    /// Start three replicated members. Their quota flags differ, but none rejects a write.
    async fn new(seed: u64) -> Self {
        let quotas = BTreeMap::from([(1, 0), (2, 1 << 30), (3, -1)]);
        Self::with_options(seed, false, quotas, false, false).await
    }

    async fn with_mode(seed: u64, shared: bool) -> Self {
        Self::with_options(seed, shared, BTreeMap::new(), shared, false).await
    }

    /// Start three members. Shared mode gives them one fault-injectable in-memory store.
    /// `reference` checks each member state against a reference state after every step.
    /// `upgraded` starts the members as directories of an earlier binary that propose no
    /// `SetQuota` until `release_quota`.
    async fn with_options(
        seed: u64,
        shared: bool,
        quotas: BTreeMap<u64, i64>,
        reference: bool,
        upgraded: bool,
    ) -> Self {
        let root = tempfile::tempdir().unwrap();
        let reference = if reference {
            let mut reference = StateMachine::open(&root.path().join("reference"))
                .await
                .unwrap();
            let peers = (1..=3).map(|n| (n, format!("127.0.0.1:{n}"))).collect();
            reference.members = std::sync::Arc::new(crate::cluster::Store::initial(&peers));
            reference.members_unsaved = true;
            Some(reference)
        } else {
            None
        };
        let mut sim = Self {
            seed,
            root,
            stores: BTreeMap::new(),
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
            paused_apply: None,
            paused_flush: None,
            paused_reads: None,
            paused_log: None,
            held_log: None,
            expected_log_failure: None,
            log_failures: 0,
            random: seed.max(1),
            next_request: 0,
            trace: vec![],
            installed_snapshots: 0,
            proposed: BTreeSet::new(),
            shared: shared.then(|| {
                std::sync::Arc::new(crate::object::faults::FaultStore::new(std::sync::Arc::new(
                    slatedb::object_store::memory::InMemory::new(),
                )))
            }),
            reference,
            digests: BTreeMap::new(),
            checked: BTreeMap::new(),
            view_checks: 0,
            quotas,
            upgraded,
        };
        for id in 1..=3 {
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
        for peer in 1..=3 {
            if peer == id {
                continue;
            }
            let (tx, rx) = mpsc::channel(256);
            senders.insert(peer, tx);
            self.links.insert((id, peer), rx);
        }
        let store = match &self.shared {
            Some(store) => store.clone(),
            None => self
                .stores
                .entry(id)
                .or_insert_with(|| {
                    let path = self.root.path().join(id.to_string()).join("objects");
                    std::fs::create_dir_all(&path).unwrap();
                    std::sync::Arc::new(crate::object::faults::FaultStore::new(
                        std::sync::Arc::new(
                            slatedb::object_store::local::LocalFileSystem::new_with_prefix(path)
                                .unwrap(),
                        ),
                    ))
                })
                .clone(),
        };
        let shared = self.shared.is_some();
        let flag = self.quotas.get(&id).copied().unwrap_or(0);
        if self.upgraded {
            // An earlier binary created the directory, so this binary did not create it empty.
            let dir = self.root.path().join(id.to_string());
            std::fs::create_dir_all(&dir).unwrap();
            let startup = quota::Startup {
                member: id,
                flag,
                shared,
                empty: false,
                reassert: false,
                now: 0,
            };
            quota::Quota::open(&dir, startup).unwrap();
        }
        // Box the large open future, so the schedule future fits the test thread stack.
        let mut node = Box::pin(Node::open_with_transport(
            NodeOptions {
                client_peers: BTreeMap::new(),
                transport: crate::tls::Transport::default(),
                require_peer_tls: false,
                state_mode: if shared {
                    crate::state::StateMode::Shared
                } else {
                    crate::state::StateMode::Replicated
                },
                view_max_age: std::time::Duration::from_secs(10),
                view_pin_max: std::time::Duration::from_secs(300),
                overlay_max_bytes: 1 << 30,
                max_clock_skew: std::time::Duration::from_secs(5),
                view_retention: Default::default(),
                restore: None,
                object_store: crate::object::Config::Test(
                    store,
                    if shared { "root" } else { "state" }.into(),
                ),
                strict_reconfiguration: false,
                join: false,
                quota_bytes: flag,
                quota_reassert: false,
                quota_warning_fraction: 0.0,
                auto_compaction: None,
                id,
                cluster_id: "dst".into(),
                dir: self.root.path().join(id.to_string()),
                peers: (1..=3).map(|n| (n, format!("127.0.0.1:{n}"))).collect(),
                // Shared members truncate often, so a lagging member needs a format 6 snapshot.
                snapshot_every: if shared { 8 } else { 0 },
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
        node.quota.hold = self.upgraded;
        self.nodes.insert(id, node);
        self.trace.push(format!("start {id}"));
    }

    fn stop(&mut self, id: u64) {
        if let Some(node) = self.nodes.remove(&id) {
            self.installed_snapshots += node.installed_snapshots;
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
                let completed = node.compaction.next().await.unwrap();
                node.finish_compaction_flush(completed).unwrap();
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
        if self.shared.is_some() {
            self.settle_opens().await;
        }
        if self.reference.is_some() {
            self.check_reference().await;
        }
    }

    /// Background checkpoint opens of shared mode finish before the next step, so a schedule
    /// stays exact.
    async fn settle_opens(&mut self) {
        for node in self.nodes.values_mut() {
            if node.worker.state_mut().settle_opens().await {
                if let Some(writer) = &mut node.writer {
                    writer.switch_due = true;
                }
            }
        }
    }

    /// The reference applies the committed entries in order. Each member's view at its applied
    /// index must equal the reference at that index. A view can fail on a deleted object, but it
    /// never returns other data.
    async fn check_reference(&mut self) {
        let reference = self.reference.as_mut().unwrap();
        while let Some(bytes) = self.committed_entries.get(&(reference.applied + 1)) {
            let entry = Entry::decode(bytes.as_slice()).unwrap();
            let header = Header {
                cluster: 0,
                member: 0,
                term: entry.term,
            };
            apply::apply_entries(reference, vec![entry], header)
                .await
                .unwrap();
            let digest = rows_digest(reference.applied_state()).await.unwrap();
            self.digests.insert(reference.applied, digest);
        }
        for (&id, node) in &self.nodes {
            let applied = node.state.applied;
            if self.checked.get(&id) == Some(&applied) {
                continue;
            }
            let Some(&expected) = self.digests.get(&applied) else {
                continue;
            };
            let Ok(actual) = rows_digest(node.state.read.state()).await else {
                continue;
            };
            assert_eq!(
                actual, expected,
                "seed {}: member {id} view at {applied} differs from the reference",
                self.seed
            );
            self.checked.insert(id, applied);
            self.view_checks += 1;
        }
    }

    async fn deliver(&mut self, faults: bool) {
        if self.messages.is_empty() {
            return;
        }
        let index = self.choose(self.messages.len());
        let message = self.messages.remove(index);
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
        let mut saved = None;
        if let Some(node) = self.nodes.get_mut(&message.to) {
            let snapshot = message.get_msg_type() == MessageType::MsgSnapshot;
            let from = message.from;
            let to = message.to;
            let index = message.get_snapshot().get_metadata().index;
            node.raw.step(message).unwrap();
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
            node.raw
                .raft
                .set_randomized_election_timeout(10 + id as usize * 2);
            node.raw.tick();
            node.retry_reads();
            self.trace.push(format!("tick {id}"));
        }
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

    async fn read(&mut self) {
        let candidates: Vec<_> = self
            .nodes
            .iter()
            .filter_map(|(&id, node)| (node.raw.raft.leader_id != 0).then_some(id))
            .collect();
        if candidates.is_empty() {
            return;
        }
        let id = candidates[self.choose(candidates.len())];
        let count = self.choose(6) + 3;
        let point = if self.choose(2) == 0 {
            self.acknowledged
                .first_key_value()
                .map(|(key, value)| (key.clone(), value.clone()))
        } else {
            None
        };
        for offset in 0..count {
            let (tx, rx) = oneshot::channel();
            let node = self.nodes.get_mut(&id).unwrap();
            node.linear_read_auth(
                RequestOp {
                    request: Some(Op::RequestRange(RangeRequest {
                        key: point
                            .as_ref()
                            .map_or_else(|| b"key-".to_vec(), |(key, _)| key.as_bytes().to_vec()),
                        range_end: if point.is_some() {
                            vec![]
                        } else {
                            b"key.".to_vec()
                        },
                        ..Default::default()
                    })),
                },
                None,
                ClientResponse::Kv(tx),
            );
            // Issue one batch, cancel its first client, then queue later reads.
            if offset == 1 {
                node.drive_ready().await.unwrap();
                self.reads.pop();
                self.trace.push(format!("cancel read {id}"));
            }
            self.trace.push(format!("read {id}"));
            self.reads.push(ReadRequest {
                node: id,
                known: point.as_ref().map_or_else(
                    || self.acknowledged.clone(),
                    |(key, value)| [(key.clone(), value.clone())].into_iter().collect(),
                ),
                reply: rx,
            });
        }
        self.drive().await;
    }

    async fn checkpoint(&mut self) {
        for node in self.nodes.values_mut() {
            let index = node.state.applied;
            if index <= node.raw.store().snapshot_index() {
                continue;
            }
            // Build the same checkpoint descriptor that a peer snapshot request builds.
            let data = node
                .worker
                .state()
                .checkpoint_job(true)
                .await
                .unwrap()
                .await
                .unwrap()
                .unwrap();
            node.handle(
                Event::SnapshotBuilt(apply::Built {
                    index,
                    conf: node.worker.state().members.conf(),
                    result: PreparedCheckpoint::write(&node.options.dir.join("raft"), data.into())
                        .map(Some),
                }),
                &mut vec![],
            )
            .await
            .unwrap();
            self.trace
                .push(format!("snapshot {} {index}", node.options.id));
        }
        self.drive().await;
    }

    /// Flush state and truncate each log without an image, as automatic checkpoints do.
    async fn truncate(&mut self) {
        for (&id, node) in &mut self.nodes {
            let index = node.state.applied;
            if index <= node.raw.store().snapshot_index() {
                continue;
            }
            let conf = node.worker.state().members.conf();
            let job = node.worker.state().checkpoint_job(false).await.unwrap();
            assert!(job.await.unwrap().is_none());
            node.handle(
                Event::SnapshotBuilt(apply::Built {
                    index,
                    conf,
                    result: Ok(None),
                }),
                &mut vec![],
            )
            .await
            .unwrap();
            self.trace.push(format!("truncate {id} {index}"));
        }
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
        for _ in 0..rounds {
            for id in 1..=3 {
                self.tick(id).await;
            }
            for _ in 0..30 {
                self.deliver(false).await;
            }
        }
    }

    async fn restart_between_checkpoint_file_and_reference(&mut self) {
        self.checkpoint().await;
        let id = self.choose(3) as u64 + 1;
        let storage = self.nodes[&id].log_worker.storage();
        let saved = storage.recovery_snapshot().clone();
        let index = saved.get_metadata().index;
        assert!(index > 0);
        // Keep a synced local image pending while Raft processes more messages.
        let dir = self.root.path().join(id.to_string()).join("raft");
        let file = dir.join(format!("checkpoint-{index:016x}.data"));
        // The image is a small checkpoint descriptor. Recovery tests cover compressed images.
        assert!(std::fs::metadata(file).unwrap().len() > 0);
        let staged = PreparedCheckpoint::write(&dir, saved.data.clone().into()).unwrap();
        self.read().await;
        self.drive().await;
        assert_eq!(self.nodes[&id].raw.store().snapshot_index(), index);
        drop(staged);
        self.stop(id);
        let orphan = dir.join(format!("checkpoint-{:016x}.data", index + 1));
        std::fs::write(&orphan, b"unpublished snapshot").unwrap();
        self.start(id).await;
        assert_eq!(
            self.nodes[&id].log_worker.storage().recovery_snapshot(),
            &saved
        );
        assert!(!orphan.exists());
        self.trace
            .push(format!("reject unpublished checkpoint {id} {index}"));
    }

    async fn object_faults(&mut self) {
        use crate::object::faults::{FAIL_GET, FAIL_PUT, LOSE_PUT_REPLY, PAUSE_PUT};
        let id = self.choose(3) as u64 + 1;
        let store = self.stores[&id].clone();
        let node = &self.nodes[&id];
        let before = node.raw.store().snapshot_index();
        let first = node.raw.store().first_index().unwrap();
        store.set(PAUSE_PUT);
        let index = node.state.applied;
        let conf = node.worker.state().members.conf();
        let job = tokio::spawn(node.worker.state().snapshot_job().unwrap());
        tokio::time::timeout(Duration::from_secs(10), store.entered.acquire())
            .await
            .unwrap()
            .unwrap()
            .forget();
        self.trace.push(format!("pause object put {id}"));
        // Raft can commit and ReadIndex can complete while the checkpoint waits for S3.
        self.propose().await;
        for _ in 0..100 {
            self.deliver(false).await;
        }
        self.read().await;
        assert!(!job.is_finished());
        assert_eq!(self.nodes[&id].raw.store().snapshot_index(), before);
        assert_eq!(self.nodes[&id].raw.store().first_index().unwrap(), first);
        store.release.add_permits(1);
        let data = job.await.unwrap().unwrap();
        let staged =
            PreparedCheckpoint::write(&self.nodes[&id].options.dir.join("raft"), data.into())
                .unwrap();
        if self.choose(2) == 0 {
            self.nodes
                .get_mut(&id)
                .unwrap()
                .handle(
                    Event::SnapshotBuilt(apply::Built {
                        index,
                        conf,
                        result: Ok(Some(staged)),
                    }),
                    &mut vec![],
                )
                .await
                .unwrap();
            self.drive().await;
            self.trace.push(format!("publish object checkpoint {id}"));
        } else {
            self.trace
                .push(format!("drop object checkpoint completion {id}"));
        }
        self.stop(id);
        store.set(FAIL_GET);
        assert!(StateMachine::open_store(store.clone(), "state")
            .await
            .is_err());
        self.trace.push(format!("fail object recovery read {id}"));
        store.set(0);
        self.start(id).await;
        for _ in 0..25 {
            for member in 1..=3 {
                self.tick(member).await;
            }
            for _ in 0..30 {
                self.deliver(false).await;
            }
        }
        // A successful PUT with a lost reply must keep the same immutable object.
        self.propose_before_object_fault(id).await;
        let before = self.nodes[&id].raw.store().snapshot_index();
        while let Ok(permit) = store.entered.try_acquire() {
            permit.forget();
        }
        store.set(LOSE_PUT_REPLY);
        self.nodes[&id]
            .worker
            .state()
            .snapshot_job()
            .unwrap()
            .await
            .unwrap();
        assert_eq!(self.nodes[&id].raw.store().snapshot_index(), before);
        store.entered.try_acquire().unwrap().forget();
        self.trace
            .push(format!("recover lost object put reply {id}"));
        self.propose_before_object_fault(id).await;
        let before = self.nodes[&id].raw.store().snapshot_index();
        let first = self.nodes[&id].raw.store().first_index().unwrap();
        store.set(FAIL_PUT);
        let result = tokio::time::timeout(
            Duration::from_secs(10),
            self.nodes[&id].worker.state().snapshot_job().unwrap(),
        )
        .await
        .unwrap();
        assert!(result.is_err());
        store.entered.try_acquire().unwrap().forget();
        assert_eq!(self.nodes[&id].raw.store().snapshot_index(), before);
        assert_eq!(self.nodes[&id].raw.store().first_index().unwrap(), first);
        self.trace
            .push(format!("fail object checkpoint without log GC {id}"));
        self.stop(id);
        store.set(0);
        self.start(id).await;
        for _ in 0..25 {
            for member in 1..=3 {
                self.tick(member).await;
            }
            for _ in 0..30 {
                self.deliver(false).await;
            }
        }
        for (key, value) in &self.acknowledged {
            let outcome = self.nodes[&id]
                .worker
                .state()
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
            assert_eq!(response.kvs.len(), 1);
            assert_eq!(response.kvs[0].value, value.as_bytes());
        }
    }

    async fn propose_before_object_fault(&mut self, id: u64) {
        let leader = self.leader().unwrap();
        self.propose().await;
        let index = self.nodes[&leader].raw.raft.raft_log.last_index();
        let key = format!("key-{}", self.next_request);
        for _ in 0..100 {
            self.deliver(false).await;
        }
        // Reordered appends can leave the follower waiting for a heartbeat with the commit index.
        // A clean state flush does not PUT an object and cannot exercise a PUT fault.
        for _ in 0..100 {
            if self.nodes[&id].state.applied >= index && self.acknowledged.contains_key(&key) {
                self.trace
                    .push(format!("object fault apply barrier {id} {index}"));
                return;
            }
            for member in 1..=3 {
                self.tick(member).await;
            }
            for _ in 0..30 {
                self.deliver(false).await;
            }
        }
        panic!("object fault setup did not apply {key} at index {index} on node {id}");
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

    async fn admin_command(&mut self, command: RaftCommand) -> Outcome<Value> {
        let leader = self.leader().unwrap();
        self.trace.push(format!("admin {}", command.id()));
        let (tx, mut rx) = oneshot::channel();
        self.nodes
            .get_mut(&leader)
            .unwrap()
            .propose(command, tx)
            .unwrap();
        self.drive().await;
        for _ in 0..500 {
            self.deliver(false).await;
            if let Ok(result) = rx.try_recv() {
                return serde_json::from_value(result.unwrap()).unwrap();
            }
        }
        panic!("admin command did not commit");
    }

    async fn admin_recovery(&mut self) {
        use crate::auth::{Command as A, Credentials};
        use crate::proto::etcdserverpb::*;
        let root = Credentials {
            user: Some("root".into()),
            time: 100,
            ..Default::default()
        };
        let writer = Credentials {
            user: Some("writer".into()),
            time: 100,
            ..Default::default()
        };
        let commands = [
            A::UserAdd(AuthUserAddRequest {
                name: "root".into(),
                ..Default::default()
            }),
            A::UserGrantRole(AuthUserGrantRoleRequest {
                user: "root".into(),
                role: "root".into(),
            }),
            A::UserAdd(AuthUserAddRequest {
                name: "writer".into(),
                hashed_password: "hash".into(),
                ..Default::default()
            }),
            A::RoleAdd(AuthRoleAddRequest {
                name: "writer".into(),
            }),
            A::RoleGrantPermission(AuthRoleGrantPermissionRequest {
                name: "writer".into(),
                perm: Some(crate::proto::authpb::Permission {
                    perm_type: 2,
                    key: b"a".to_vec(),
                    range_end: b"z".to_vec(),
                }),
            }),
            A::UserGrantRole(AuthUserGrantRoleRequest {
                user: "writer".into(),
                role: "writer".into(),
            }),
            A::Enable(AuthEnableRequest {}),
            A::IssueToken {
                name: "writer".into(),
                password: "hash".into(),
                token: "dst-token".into(),
                expires: 150,
            },
        ];
        for (i, auth) in commands.into_iter().enumerate() {
            self.admin_command(RaftCommand::Auth {
                id: format!("auth-{i}"),
                auth,
                credentials: root.clone(),
            })
            .await
            .result
            .unwrap();
        }
        let token = Credentials {
            token: "dst-token".into(),
            time: 100,
            ..Default::default()
        };
        let leader = self.leader().unwrap();
        assert!(self.nodes[&leader]
            .state
            .auth
            .range(&token, b"admin", b"", true)
            .is_ok());
        let expired = Credentials {
            time: 150,
            ..token.clone()
        };
        assert!(self.nodes[&leader]
            .state
            .auth
            .range(&expired, b"admin", b"", true)
            .is_err());
        let lagging = leader % 3 + 1;
        self.isolated = Some(lagging);
        self.paused_apply = Some(leader);
        let (revoke_tx, mut revoke_rx) = oneshot::channel();
        let (put_tx, mut put_rx) = oneshot::channel();
        let node = self.nodes.get_mut(&leader).unwrap();
        node.propose(
            RaftCommand::Auth {
                id: "revoke".into(),
                credentials: root.clone(),
                auth: A::RoleRevokePermission(AuthRoleRevokePermissionRequest {
                    role: "writer".into(),
                    key: b"a".to_vec(),
                    range_end: b"z".to_vec(),
                }),
            },
            revoke_tx,
        )
        .unwrap();
        node.propose(
            RaftCommand::Etcd {
                id: "denied-put".into(),
                origin: leader,
                etcd: RequestOp {
                    request: Some(Op::RequestPut(PutRequest {
                        key: b"admin".to_vec(),
                        value: b"forbidden".to_vec(),
                        ..Default::default()
                    })),
                },
            }
            .secure(Some(writer.clone())),
            put_tx,
        )
        .unwrap();
        self.drive().await;
        for _ in 0..200 {
            self.deliver(false).await;
        }
        assert!(revoke_rx.try_recv().is_err());
        assert!(put_rx.try_recv().is_err());
        self.paused_apply = None;
        self.drive().await;
        let revoked: Outcome<Value> =
            serde_json::from_value(revoke_rx.await.unwrap().unwrap()).unwrap();
        revoked.result.unwrap();
        let denied: Outcome<Value> =
            serde_json::from_value(put_rx.await.unwrap().unwrap()).unwrap();
        assert_eq!(
            denied.result.unwrap_err().code,
            tonic::Code::PermissionDenied as i32
        );
        self.trace.push("revocation fences queued write".into());
        let first = self.nodes[&leader].state.revision;
        let physical = |id: &str, revision| {
            RaftCommand::Compact {
                id: id.into(),
                compact: CompactionRequest {
                    revision,
                    physical: true,
                },
            }
            .secure(Some(root.clone()))
        };
        self.paused_flush = Some(leader);
        let (tx, mut first_reply) = oneshot::channel();
        self.nodes
            .get_mut(&leader)
            .unwrap()
            .propose(physical("physical", first), tx)
            .unwrap();
        self.drive().await;
        for _ in 0..500 {
            if self.nodes[&leader].compaction.active() {
                break;
            }
            self.deliver(false).await;
        }
        let store = self.stores[&leader].clone();
        while let Ok(permit) = store.entered.try_acquire() {
            permit.forget();
        }
        store.set(crate::object::faults::PAUSE_PUT);
        let node = self.nodes.get_mut(&leader).unwrap();
        assert_eq!(node.state.compact_revision, first);
        let first_index = node.state.applied;
        tokio::time::timeout(Duration::from_secs(10), async {
            tokio::select! {
                result = node.compaction.next() => panic!("unexpected flush completion: {result:?}"),
                entered = store.entered.acquire() => entered.unwrap().forget(),
            }
        })
        .await
        .expect("the compaction flush must reach the object store");
        let (retry, mut retry_reply) = oneshot::channel();
        self.nodes
            .get_mut(&leader)
            .unwrap()
            .propose(physical("physical-retry", first), retry)
            .unwrap();
        self.admin_command(
            RaftCommand::Etcd {
                id: "put-during-flush".into(),
                origin: leader,
                etcd: RequestOp {
                    request: Some(Op::RequestPut(PutRequest {
                        key: b"flush-progress".to_vec(),
                        value: b"applied".to_vec(),
                        ..Default::default()
                    })),
                },
            }
            .secure(Some(root.clone())),
        )
        .await
        .result
        .unwrap();
        let revision = self.nodes[&leader].state.revision;
        assert!(revision > first);
        let (tx, mut second_reply) = oneshot::channel();
        self.nodes
            .get_mut(&leader)
            .unwrap()
            .propose(physical("second-physical", revision), tx)
            .unwrap();
        self.drive().await;
        // Replicated steps remove the rows. Their final flush queues behind the held flush.
        for _ in 0..500 {
            let state = &self.nodes[&leader].state;
            if state.compact_revision == revision && !state.compaction_pending {
                break;
            }
            self.deliver(false).await;
        }
        let node = self.nodes.get_mut(&leader).unwrap();
        assert_eq!(node.state.compact_revision, revision);
        assert_eq!(node.state.physical_revision, revision);
        assert!(matches!(
            first_reply.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        assert!(*node.forwarding.subscribe().compact.borrow() < first);
        store.release.add_permits(1);
        let flushed = node.compaction.next().await.unwrap();
        assert_eq!(flushed, (first_index, first));
        node.finish_compaction_flush(flushed).unwrap();
        let reply: Outcome<Value> =
            serde_json::from_value(first_reply.await.unwrap().unwrap()).unwrap();
        reply.result.unwrap();
        assert!(matches!(
            second_reply.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        assert!(matches!(
            retry_reply.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        assert_eq!(*node.forwarding.subscribe().compact.borrow(), first);
        self.paused_flush = None;
        self.drive().await;
        let retried: Outcome<Value> =
            serde_json::from_value(retry_reply.await.unwrap().unwrap()).unwrap();
        assert_eq!(
            retried.result.unwrap_err().code,
            tonic::Code::OutOfRange as i32
        );
        let reply: Outcome<Value> =
            serde_json::from_value(second_reply.await.unwrap().unwrap()).unwrap();
        reply.result.unwrap();
        assert_eq!(
            *self.nodes[&leader].forwarding.subscribe().compact.borrow(),
            revision
        );
        self.trace
            .push("a later write applied during the held compaction flush".into());
        self.checkpoint().await;
        self.stop(lagging);
        self.start(lagging).await;
        self.isolated = None;
        for _ in 0..50 {
            for id in 1..=3 {
                self.tick(id).await;
            }
            for _ in 0..30 {
                self.deliver(false).await;
            }
        }
        for node in self.nodes.values() {
            assert_eq!(node.state.compact_revision, revision);
            let state = node.worker.state();
            assert_eq!(
                state.logical_usage(),
                crate::state::maintenance::count_logical_usage(state.db().as_ref())
                    .await
                    .unwrap()
            );
            assert!(state.key_index.matches(state.db()).await.unwrap());
            assert!(node.state.auth.range(&writer, b"admin", b"", true).is_err());
            assert!(node.state.auth.range(&token, b"admin", b"", true).is_err());
            let value = node
                .worker
                .state()
                .read_etcd(&RequestOp {
                    request: Some(Op::RequestRange(RangeRequest {
                        key: b"admin".to_vec(),
                        ..Default::default()
                    })),
                })
                .await
                .unwrap();
            let Some(Resp::ResponseRange(value)) = value.result.unwrap().response else {
                panic!()
            };
            assert!(value.kvs.is_empty());
        }
        self.trace
            .push("auth and physical compact recovered".into());
    }

    /// Renew a lease in leader memory, as the gRPC KeepAlive path does.
    async fn renew(&mut self, member: u64, lease: i64) -> std::result::Result<i64, ApiError> {
        let (tx, mut rx) = oneshot::channel();
        self.trace.push(format!("renew {member} {lease}"));
        self.nodes
            .get_mut(&member)
            .unwrap()
            .keep_alive(lease, None, tx)
            .unwrap();
        self.drive().await;
        for _ in 0..300 {
            if let Ok(result) = rx.try_recv() {
                let outcome: Outcome<LeaseReply> = serde_json::from_value(result?).unwrap();
                let Ok(LeaseReply::KeepAlive(response)) = outcome.result else {
                    panic!("expected a KeepAlive reply");
                };
                return Ok(response.ttl);
            }
            self.deliver(false).await;
        }
        panic!("renewal on {member} did not finish");
    }

    /// Retry a renewal that a slow host cancelled for a stale proof.
    async fn renew_on_leader(&mut self, lease: i64) -> i64 {
        for _ in 0..3 {
            let leader = self.leader().unwrap();
            match self.renew(leader, lease).await {
                Ok(ttl) => return ttl,
                Err(error) => assert!(error.error.contains("renewal cancelled"), "{error:?}"),
            }
            self.settle(25).await;
        }
        panic!("leader did not renew lease {lease}");
    }

    /// An isolated leader cannot renew in memory. The next leader extends inherited leases.
    async fn renew_under_partition(&mut self) {
        self.settle(25).await;
        let ttl = self.nodes[&self.leader().unwrap()].state.leases[&77].ttl;
        assert_eq!(self.renew_on_leader(77).await, ttl);
        let old = self.leader().unwrap();
        let last = self.nodes[&old].raw.raft.raft_log.last_index();
        assert_eq!(self.renew(old, 78).await.unwrap(), 0, "unknown lease");
        assert_eq!(
            self.nodes[&old].raw.raft.raft_log.last_index(),
            last,
            "an in-memory renewal must not append a log entry"
        );

        // Cut the leader off. Its proof cannot reach a quorum, and check_quorum demotes it.
        self.isolated = Some(old);
        self.trace.push(format!("isolate renewing leader {old}"));
        let (tx, mut rx) = oneshot::channel();
        self.nodes
            .get_mut(&old)
            .unwrap()
            .keep_alive(77, None, tx)
            .unwrap();
        self.settle(25).await;
        let error = rx
            .try_recv()
            .expect("the isolated renewal must finish")
            .expect_err("an isolated leader must not renew");
        assert!(
            error.error.contains("renewal cancelled") || error.error == "not leader",
            "{error:?}"
        );
        let node = self.nodes.get_mut(&old).unwrap();
        assert_ne!(node.raw.raft.state, StateRole::Leader);
        // The run loop syncs deadlines after each event. A follower keeps none.
        node.sync_lease_deadlines();
        assert!(node.lease_deadlines.is_empty() && node.renewals.is_empty());
        // A member that is no longer leader rejects new renewals, so the gRPC layer forwards them.
        assert_eq!(self.renew(old, 77).await.unwrap_err().error, "not leader");

        // The majority side elects a leader that gives the inherited lease its TTL plus the
        // longest election timeout, so no renewal the old leader replied to can outlive it.
        let new = self.leader().expect("the majority must elect a leader");
        assert_ne!(new, old);
        let node = self.nodes.get_mut(&new).unwrap();
        node.sync_lease_deadlines();
        let remaining = node.lease_deadlines[&77]
            .1
            .saturating_duration_since(Instant::now());
        assert!(
            remaining > Duration::from_secs(ttl as u64),
            "seed {}: {remaining:?}",
            self.seed
        );
        assert_eq!(self.renew_on_leader(77).await, ttl);

        self.isolated = None;
        self.trace.push("heal renewing leader".into());
        self.settle(25).await;
        assert_eq!(self.renew_on_leader(77).await, ttl);
    }

    async fn batch_and_pinned_reads(&mut self) {
        let leader = self.leader().unwrap();
        self.paused_reads = Some(leader);
        let old_revision = self.nodes[&leader].state.revision;
        let (tx, read) = oneshot::channel();
        self.nodes
            .get_mut(&leader)
            .unwrap()
            .enqueue_query(Query::Read(
                RequestOp {
                    request: Some(Op::RequestRange(RangeRequest {
                        key: b"batch-key".to_vec(),
                        serializable: true,
                        ..Default::default()
                    })),
                },
                None,
                ClientResponse::Kv(tx),
            ));
        self.drive().await;
        assert!(self.nodes[&leader].read_executor.len() > 0);
        self.paused_apply = Some(leader);
        let before = self.nodes[&leader].state.write_batches;
        let mut replies = vec![];
        // Keep one large value through Watch replay, compaction, and checkpoint recovery.
        let batch_value = |index: usize| match index {
            3 => vec![],
            7 => vec![0, 255, 0, 127],
            15 => vec![b'v'; 64 * 1024],
            _ => format!("v{index}").into_bytes(),
        };
        for index in 0..16 {
            let (tx, rx) = oneshot::channel();
            self.nodes
                .get_mut(&leader)
                .unwrap()
                .propose(
                    RaftCommand::Etcd {
                        id: format!("batch-{index}"),
                        origin: leader,
                        etcd: RequestOp {
                            request: Some(Op::RequestPut(PutRequest {
                                key: b"batch-key".to_vec(),
                                value: batch_value(index),
                                prev_kv: index % 2 == 0,
                                ..Default::default()
                            })),
                        },
                    },
                    ClientResponse::Kv(tx),
                )
                .unwrap();
            replies.push(rx);
        }
        self.drive().await;
        for _ in 0..150 {
            self.deliver(false).await;
        }
        self.paused_apply = None;
        self.drive().await;
        for _ in 0..150 {
            self.deliver(false).await;
        }
        let writes = self.nodes[&leader].state.write_batches - before;
        assert!(writes < 16, "put burst did not share storage writes");
        for (offset, mut reply) in replies.into_iter().enumerate() {
            let Some(Resp::ResponsePut(value)) = reply
                .try_recv()
                .unwrap()
                .unwrap()
                .into_outcome()
                .result
                .unwrap()
                .response
            else {
                panic!("expected put");
            };
            assert_eq!(
                value.header.unwrap().revision,
                old_revision + offset as i64 + 1
            );
            assert_eq!(
                value.prev_kv.map(|kv| kv.value),
                offset
                    .checked_sub(1)
                    .filter(|_| offset % 2 == 0)
                    .map(batch_value)
            );
        }
        self.paused_reads = None;
        self.drive().await;
        let Some(Resp::ResponseRange(value)) = read
            .await
            .unwrap()
            .unwrap()
            .into_outcome()
            .result
            .unwrap()
            .response
        else {
            panic!("expected range");
        };
        assert!(value.kvs.is_empty());
        assert_eq!(value.header.unwrap().revision, old_revision);
        self.trace.push(format!(
            "pinned read across batch {leader} {old_revision} {writes}"
        ));
        self.stop(leader);
        self.start(leader).await;
        for _ in 0..25 {
            for id in 1..=3 {
                self.tick(id).await;
            }
            for _ in 0..30 {
                self.deliver(false).await;
            }
        }
        for node in self.nodes.values() {
            let state = node.worker.state();
            let page = state
                .watch(
                    &WatchCreateRequest {
                        key: b"batch-key".to_vec(),
                        start_revision: old_revision + 1,
                        prev_kv: true,
                        ..Default::default()
                    },
                    None,
                )
                .await
                .unwrap();
            let events: Vec<_> = page.responses.into_iter().flat_map(|r| r.events).collect();
            assert_eq!(events.len(), 16);
            for (offset, event) in events.iter().enumerate() {
                let kv = event.kv.as_ref().unwrap();
                assert_eq!(kv.mod_revision, old_revision + offset as i64 + 1);
                assert_eq!(kv.value, batch_value(offset));
                assert_eq!(kv.create_revision, old_revision + 1);
                assert_eq!(kv.version, offset as i64 + 1);
                let previous = offset
                    .checked_sub(1)
                    .map(|i| events[i].kv.as_ref().unwrap());
                assert_eq!(event.prev_kv.as_ref(), previous);
            }
            assert_eq!(
                state.logical_usage(),
                crate::state::maintenance::count_logical_usage(state.db().as_ref())
                    .await
                    .unwrap()
            );
            assert!(state.key_index.matches(state.db()).await.unwrap());
        }
        self.trace.push("batched put history recovered".into());
    }

    async fn crash_after_quorum_reply(&mut self) {
        let leader = self.leader().unwrap();
        let revision = self.nodes[&leader].state.revision + 1;
        let (tx, mut rx) = oneshot::channel();
        self.paused_log = Some(leader);
        let node = self.nodes.get_mut(&leader).unwrap();
        node.propose(
            RaftCommand::Etcd {
                id: "quorum-crash".into(),
                origin: leader,
                etcd: RequestOp {
                    request: Some(Op::RequestPut(PutRequest {
                        key: b"quorum-crash".to_vec(),
                        value: b"committed".to_vec(),
                        ..Default::default()
                    })),
                },
            },
            ClientResponse::Kv(tx),
        )
        .unwrap();
        self.drive().await;
        for _ in 0..100 {
            self.deliver(false).await;
        }
        let Some(Resp::ResponsePut(put)) = rx
            .try_recv()
            .unwrap()
            .unwrap()
            .into_outcome()
            .result
            .unwrap()
            .response
        else {
            panic!("expected a quorum Put reply");
        };
        assert_eq!(put.header.unwrap().revision, revision);
        let node = &self.nodes[&leader];
        let index = node.quorum_reply_index;
        assert!(index > node.log_worker.storage().last_index().unwrap());
        assert!(index > node.state.applied);
        assert!(!node.forwarding.subscribe().applied(revision));
        for (&id, node) in &self.nodes {
            if id != leader {
                assert!(node.log_worker.storage().last_index().unwrap() >= index);
            }
        }
        self.trace.push(format!(
            "crash after quorum reply {leader} {index} {revision}"
        ));
        for id in 1..=3 {
            self.stop(id);
        }
        self.paused_log = None;
        for id in 1..=3 {
            self.start(id).await;
        }
        for _ in 0..35 {
            for id in 1..=3 {
                self.tick(id).await;
            }
            for _ in 0..30 {
                self.deliver(false).await;
            }
        }
        for node in self.nodes.values() {
            let read = RequestOp {
                request: Some(Op::RequestRange(RangeRequest {
                    key: b"quorum-crash".to_vec(),
                    ..Default::default()
                })),
            };
            let result = node
                .worker
                .state()
                .read_etcd(&read)
                .await
                .unwrap()
                .result
                .unwrap();
            let Some(Resp::ResponseRange(range)) = result.response else {
                panic!("expected range");
            };
            assert_eq!(range.kvs[0].mod_revision, revision);
            assert_eq!(range.kvs[0].value, b"committed");
        }
    }

    async fn run(seed: u64) -> Vec<String> {
        let mut sim = Self::new(seed).await;
        for _ in 0..100 {
            sim.deliver(false).await;
        }
        sim.lease(LeaseCommand::Grant { id: 77, ttl: 3600 }).await;
        let generation = sim.nodes[&sim.leader().unwrap()].state.leases[&77].generation;
        sim.lease(LeaseCommand::KeepAlive { id: 77 }).await;
        sim.lease(LeaseCommand::Expire { id: 77, generation }).await;
        sim.object_faults().await;
        let failed = sim.leader().unwrap();
        sim.expected_log_failure = Some(failed);
        sim.nodes.get_mut(&failed).unwrap().log_fail = true;
        sim.propose().await;
        assert_eq!(sim.log_failures, 1);
        sim.start(failed).await;
        for _ in 0..25 {
            for id in 1..=3 {
                sim.tick(id).await;
            }
            for _ in 0..30 {
                sim.deliver(false).await;
            }
        }
        let leader = sim.leader().unwrap();
        for node in sim.nodes.values() {
            assert_eq!(node.forwarding.subscribe().leader(), leader);
        }
        // Fail a cache-miss read through the real disk executor, then restart the node.
        for _ in 0..8 {
            sim.propose().await;
            for _ in 0..60 {
                sim.deliver(false).await;
            }
        }
        let reader = sim.leader().unwrap();
        let node = sim.nodes.get_mut(&reader).unwrap();
        let low = node.raw.store().first_index().unwrap();
        assert!(matches!(
            node.raw
                .store()
                .entries(low, low + 1, None, raft::GetEntriesContext::empty(true)),
            Err(raft::Error::Store(
                raft::StorageError::LogTemporarilyUnavailable
            ))
        ));
        node.log_read_fail = true;
        sim.expected_log_failure = Some(reader);
        sim.drive().await;
        assert_eq!(sim.log_failures, 2);
        sim.start(reader).await;
        for _ in 0..25 {
            for id in 1..=3 {
                sim.tick(id).await;
            }
            for _ in 0..30 {
                sim.deliver(false).await;
            }
        }
        // Hold a real SendAppend fetch while a higher term cancels its Raft context.
        let reader = sim.leader().unwrap();
        sim.paused_log = Some(reader);
        let node = sim.nodes.get_mut(&reader).unwrap();
        let low = node.raw.store().first_index().unwrap();
        let peer = reader % 3 + 1;
        let progress = node.raw.raft.mut_prs().get_mut(peer).unwrap();
        progress.matched = 0;
        progress.become_probe();
        progress.next_idx = low;
        node.raw.raft.send_append(peer);
        assert!(node.raw.store().stats()["read_reserved"].as_u64().unwrap() > 0);
        node.drive_ready().await.unwrap();
        let completed = node.log_worker.step().unwrap().unwrap();
        assert!(completed.fetched.is_some());
        node.raw.raft.become_follower(node.raw.raft.term + 1, 0);
        node.complete_log(completed).unwrap();
        assert!(node
            .raw
            .raft
            .msgs
            .iter()
            .all(|m| m.get_msg_type() != MessageType::MsgAppend));
        sim.trace.push(format!("late log read {reader}"));
        sim.paused_log = None;
        for _ in 0..25 {
            for id in 1..=3 {
                sim.tick(id).await;
            }
            for _ in 0..30 {
                sim.deliver(false).await;
            }
        }
        let mut offline = 0;
        for step in 0..400 {
            match step {
                30 => {
                    sim.paused_apply = sim.leader();
                    sim.trace
                        .push(format!("pause apply {:?}", sim.paused_apply));
                }
                60 => {
                    sim.paused_apply = None;
                    sim.trace.push("resume apply".into());
                }
                65 => {
                    sim.paused_log = sim.leader();
                    sim.trace.push(format!("pause log {:?}", sim.paused_log));
                }
                67 => sim.propose().await,
                68 => {
                    if let Some(id) = sim.paused_log {
                        if let Some(node) = sim.nodes.get_mut(&id) {
                            while node.log_busy {
                                let bytes = node.log_queue.front().unwrap().bytes;
                                let completion = node.log_worker.step().unwrap().unwrap();
                                if bytes > 0 {
                                    sim.trace.push(format!("hold durable log completion {id}"));
                                    sim.held_log = Some((id, completion));
                                    break;
                                }
                                node.complete_log(completion).unwrap();
                                node.drive_ready().await.unwrap();
                            }
                        }
                    }
                }
                90 => {
                    if let Some((id, completion)) = sim.held_log.take() {
                        if let Some(node) = sim.nodes.get_mut(&id) {
                            node.complete_log(completion).unwrap();
                            sim.trace.push(format!("release log completion {id}"));
                        } else {
                            sim.trace.push(format!("crash before log completion {id}"));
                        }
                    }
                    sim.paused_log = None;
                    sim.trace.push("resume log".into());
                }
                70 => sim.stop(3),
                95 => sim.start(3).await,
                140 => {
                    sim.isolated = sim.leader();
                    sim.trace.push(format!("isolate {:?}", sim.isolated));
                }
                185 => {
                    sim.isolated = None;
                    sim.trace.push("heal".into());
                }
                220 => {
                    // A fault schedule can reach this step during an election.
                    offline = sim.leader().unwrap_or(1) % 3 + 1;
                    sim.stop(offline);
                }
                260 => {
                    // Establish a new committed prefix while the selected follower stays offline.
                    for _ in 0..25 {
                        for id in 1..=3 {
                            sim.tick(id).await;
                        }
                        for _ in 0..30 {
                            sim.deliver(false).await;
                        }
                    }
                    for _ in 0..5 {
                        sim.propose().await;
                        for _ in 0..100 {
                            sim.deliver(false).await;
                        }
                    }
                    // The offline follower must now catch up from an image built on demand.
                    sim.truncate().await;
                }
                300 => {
                    sim.paused_log = Some(offline);
                    sim.trace.push(format!("pause snapshot log {offline}"));
                    sim.start(offline).await;
                }
                320 => {
                    sim.paused_log = None;
                    sim.trace.push("resume snapshot log".into());
                }
                350 => {
                    for id in 1..=3 {
                        sim.stop(id);
                    }
                    for id in 1..=3 {
                        sim.start(id).await;
                    }
                }
                _ => {}
            }
            let id = sim.choose(3) as u64 + 1;
            sim.tick(id).await;
            if step % 17 == 0 {
                sim.read().await;
            }
            if step % 10 == 0 {
                sim.propose().await;
            }
            for _ in 0..12 {
                sim.deliver(true).await;
            }
        }
        sim.isolated = None;
        for _ in 0..100 {
            for id in 1..=3 {
                sim.tick(id).await;
            }
            for _ in 0..30 {
                sim.deliver(false).await;
            }
        }
        assert!(
            sim.acknowledged.len() >= 10,
            "seed {seed}: insufficient writes"
        );
        assert!(
            sim.read_count >= 5,
            "seed {seed}: insufficient read index checks"
        );
        assert!(
            sim.revision_count >= 10,
            "seed {seed}: insufficient revision barriers"
        );
        let snapshots = sim.installed_snapshots
            + sim
                .nodes
                .values()
                .map(|n| n.installed_snapshots)
                .sum::<u64>();
        assert!(snapshots > 0, "seed {seed}: no snapshot transfer");
        let mut expected_events = None;
        for node in sim.nodes.values() {
            let state = node.worker.state();
            let mut rows = state.db().scan_prefix(b"etcd/", ..).await.unwrap();
            let mut size = 0;
            while let Some(row) = rows.next().await.unwrap() {
                size += (row.key.len() + row.value.len()) as u64;
            }
            assert_eq!(state.logical_size(), size, "seed {seed}");
            assert!(
                state.key_index.matches(state.db()).await.unwrap(),
                "seed {seed}"
            );
            assert!(node.state.leases[&77].generation > generation);
            let mut request = WatchCreateRequest {
                key: vec![0],
                range_end: vec![0],
                start_revision: 1,
                prev_kv: true,
                ..Default::default()
            };
            let mut events = vec![];
            loop {
                let page = node.worker.state().watch(&request, None).await.unwrap();
                assert_eq!(page.compact_revision, 0);
                for response in page.responses {
                    events.extend(response.events);
                }
                if page.next_revision > node.state.revision {
                    break;
                }
                request.start_revision = page.next_revision;
            }
            assert_eq!(events.len() as i64, node.state.revision - 1);
            for (i, event) in events.iter().enumerate() {
                assert_eq!(event.r#type, 0);
                assert_eq!(event.kv.as_ref().unwrap().mod_revision, i as i64 + 2);
                assert_eq!(event.kv.as_ref().unwrap().lease, 77);
            }
            if let Some(expected) = &expected_events {
                assert_eq!(&events, expected);
            } else {
                expected_events = Some(events.clone());
            }
            for (key, value) in &sim.acknowledged {
                let outcome = node
                    .worker
                    .state()
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
                assert_eq!(
                    response.kvs.len(),
                    1,
                    "seed {seed}: lost acknowledged key {key}"
                );
                assert_eq!(response.kvs[0].value, value.as_bytes());
                assert_eq!(
                    events
                        .iter()
                        .filter(|event| event.kv.as_ref().unwrap().key == key.as_bytes())
                        .count(),
                    1
                );
            }
        }
        // The members have other flags, but they apply the quota of the first leader.
        let quotas: BTreeSet<_> = sim
            .nodes
            .values()
            .map(|node| {
                (
                    node.state.cluster_quota.map(|q| q.bytes),
                    node.state.quota_bytes,
                )
            })
            .collect();
        assert_eq!(quotas.len(), 1, "seed {seed}: {quotas:?}");
        assert!(quotas.iter().all(|(cluster, _)| cluster.is_some()));
        sim.renew_under_partition().await;
        sim.batch_and_pinned_reads().await;
        sim.crash_after_quorum_reply().await;
        sim.admin_recovery().await;
        sim.restart_between_checkpoint_file_and_reference().await;
        sim.trace.clone()
    }
}

#[tokio::test]
async fn seeded_fault_schedules_replay_identically() {
    let seeds = std::env::var("SLATEDB_DST_SEEDS").unwrap_or_else(|_| "1,2,42,58,70".into());
    for seed in seeds.split(',').map(|s| s.parse::<u64>().unwrap()) {
        eprintln!("DST seed {seed}: starting");
        let first = Simulation::run(seed).await;
        let second = Simulation::run(seed).await;
        let trace_dir =
            std::env::var("SLATEDB_DST_TRACE_DIR").unwrap_or_else(|_| "target/dst".into());
        std::fs::create_dir_all(&trace_dir).unwrap();
        std::fs::write(
            format!("{trace_dir}/seed-{seed}.json"),
            serde_json::to_vec_pretty(&first).unwrap(),
        )
        .unwrap();
        if first != second {
            std::fs::write(
                format!("{trace_dir}/seed-{seed}-replay.json"),
                serde_json::to_vec_pretty(&second).unwrap(),
            )
            .unwrap();
        }
        assert_eq!(first, second, "seed {seed}: simulation schedule changed");
        eprintln!("DST seed {seed}: {} events replayed", first.len());
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

    /// Restart `id` with the quota flag `flag`.
    async fn restart_with(&mut self, id: u64, flag: i64) {
        self.stop(id);
        self.quotas.insert(id, flag);
        self.start(id).await;
        self.settle(2).await;
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

    /// Return the cluster quota that every member applied. Members must agree on the state.
    fn cluster_quota(&self) -> Option<i64> {
        let first = &self.nodes[&1].state;
        for (&id, node) in &self.nodes {
            let state = &node.state;
            assert_eq!(state.applied, first.applied, "member {id}");
            assert_eq!(state.revision, first.revision, "member {id}");
            assert_eq!(state.usage.total(), first.usage.total(), "member {id}");
            assert_eq!(state.cluster_quota, first.cluster_quota, "member {id}");
            assert_eq!(
                node.worker.state().alarms.len(),
                self.nodes[&1].worker.state().alarms.len(),
                "member {id}"
            );
        }
        first.cluster_quota.map(|quota| quota.bytes)
    }

    /// Let the members propose `SetQuota` entries, as after an upgrade.
    fn release_quota(&mut self) {
        self.upgraded = false;
        for node in self.nodes.values_mut() {
            node.quota.hold = false;
        }
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
        self.stores.remove(&id);
        self.checked.remove(&id);
        self.terms.remove(&id);
        let upgraded = std::mem::replace(&mut self.upgraded, false);
        self.quotas.insert(id, flag);
        self.start(id).await;
        self.upgraded = upgraded;
        if let Some(node) = self.nodes.get_mut(&id) {
            node.quota.hold = upgraded;
        }
        self.settle(4).await;
    }

    /// A follower, not `except`.
    fn follower(&self, except: u64) -> u64 {
        let leader = self.leader().unwrap();
        (1..=3).find(|&id| id != leader && id != except).unwrap()
    }

    /// The indexes of the committed `SetQuota` entries that set `bytes`.
    fn set_quota_indexes(&self, bytes: i64) -> Vec<u64> {
        self.committed_entries
            .iter()
            .filter_map(|(&index, data)| {
                let entry = Entry::decode(data.as_slice()).unwrap();
                match RaftCommand::decode(&entry.data) {
                    Ok(RaftCommand::SetQuota { bytes: b, .. }) if b == bytes => Some(index),
                    _ => None,
                }
            })
            .collect()
    }
}

/// Members with different quota flags apply one cluster quota. Each member state at each applied
/// index equals a reference state that applies the committed entries in order.
#[tokio::test]
async fn members_with_different_quota_flags_switch_quota_at_one_index() {
    let quotas = BTreeMap::from([(1, 1 << 20), (2, 3000), (3, 1 << 20)]);
    let mut sim = Simulation::with_options(7, false, quotas, true, false).await;
    sim.settle(3).await;
    assert_eq!(sim.leader(), Some(1));
    // Client Puts use this lease.
    sim.lease(LeaseCommand::Grant { id: 77, ttl: 3600 }).await;
    // The first leader seeds the cluster quota. Member 2 does not apply its small flag.
    sim.write(40).await;
    assert_eq!(sim.cluster_quota(), Some(1 << 20));
    assert_eq!(sim.acknowledged.len(), 40);
    assert!(sim.nodes[&2].state.usage.total() > 3000);
    assert!(sim.nodes[&2].state.quota_mismatch);
    assert!(!sim.nodes[&1].state.quota_mismatch);
    // Member 2 leads with its unchanged flag. The cluster quota stays.
    sim.lead(2).await;
    sim.write(3).await;
    assert_eq!(sim.cluster_quota(), Some(1 << 20));
    // An operator changes the flag of member 3 and restarts it twice. It follows while other
    // entries commit, and proposes the latest flag when it leads.
    sim.restart_with(3, 9000).await;
    sim.restart_with(3, 5000).await;
    sim.write(3).await;
    assert_eq!(sim.cluster_quota(), Some(1 << 20));
    sim.lead(3).await;
    sim.settle(2).await;
    assert_eq!(sim.cluster_quota(), Some(5000));
    assert_eq!(sim.set_quota_indexes(5000).len(), 1);
    assert!(sim.set_quota_indexes(9000).is_empty());
    // The size exceeds the new quota, so every member rejects the next Put alike.
    let acknowledged = sim.acknowledged.len();
    sim.write(3).await;
    assert_eq!(sim.acknowledged.len(), acknowledged);
    assert_eq!(sim.cluster_quota(), Some(5000));
    assert_eq!(sim.nodes[&1].worker.state().alarms.len(), 1);
    // Member 2 restarts with its old flag and leads. It does not revert the quota.
    sim.restart_with(2, 3000).await;
    sim.lead(2).await;
    sim.write(1).await;
    assert_eq!(sim.cluster_quota(), Some(5000));
    // Member 1 replays its whole log after a restart, under the quota in force at each index.
    let applied = sim.nodes[&1].state.applied;
    sim.restart_with(1, 1 << 20).await;
    sim.settle(2).await;
    assert!(sim.nodes[&1].state.applied >= applied);
    assert_eq!(sim.cluster_quota(), Some(5000));
    assert!(sim.view_checks > 100, "{} view checks", sim.view_checks);
}

/// Members of an earlier binary applied Puts before the first cluster quota. A member that this
/// binary creates empty does not apply them under its own small flag. Before the seed it waits
/// for a snapshot. After the seed, every member truncates its log through the seed, so a new
/// member installs a snapshot. The reference check compares every member at every index.
#[tokio::test]
async fn a_new_member_does_not_replay_the_entries_of_an_earlier_binary() {
    let quotas = BTreeMap::from([(1, 1 << 20), (2, 1 << 20), (3, 1 << 20)]);
    let mut sim = Simulation::with_options(11, false, quotas, true, true).await;
    sim.settle(3).await;
    sim.lease(LeaseCommand::Grant { id: 77, ttl: 3600 }).await;
    sim.write(40).await;
    sim.settle(2).await;
    assert_eq!(sim.cluster_quota(), None);
    assert_eq!(sim.acknowledged.len(), 40);
    assert!(sim.nodes[&1].state.usage.total() > 3000);
    // Before the seed: the new member waits for a snapshot of the leader.
    let first = sim.follower(0);
    let installed = sim.nodes[&first].installed_snapshots;
    sim.replace(first, 3000).await;
    sim.settle(4).await;
    let leader = sim.leader().unwrap();
    assert_eq!(
        sim.nodes[&first].state.applied,
        sim.nodes[&leader].state.applied
    );
    assert!(sim.nodes[&first].installed_snapshots > installed || sim.installed_snapshots > 0);
    assert!(sim.nodes[&first].worker.state().alarms.is_empty());
    // The upgraded leader seeds the cluster quota. Every member truncates through it.
    sim.release_quota();
    sim.settle(2).await;
    sim.write(3).await;
    assert_eq!(sim.cluster_quota(), Some(1 << 20));
    let seed = sim.set_quota_indexes(1 << 20)[0];
    sim.settle(2).await;
    for (&id, node) in &sim.nodes {
        assert!(node.raw.store().snapshot_index() >= seed, "member {id}");
    }
    // After the seed: a new member installs a snapshot.
    let second = sim.follower(first);
    sim.replace(second, 3000).await;
    sim.settle(4).await;
    sim.write(3).await;
    assert_eq!(sim.acknowledged.len(), 46);
    assert_eq!(sim.cluster_quota(), Some(1 << 20));
    assert!(sim.nodes[&second].raw.store().snapshot_index() >= seed);
    assert!(sim.view_checks > 100, "{} view checks", sim.view_checks);
}

#[tokio::test]
async fn truncation_without_an_image_recovers_and_serves_an_on_demand_snapshot() {
    let mut sim = Simulation::new(7).await;
    for _ in 0..100 {
        sim.deliver(false).await;
    }
    sim.lease(LeaseCommand::Grant { id: 77, ttl: 3600 }).await;
    let offline = sim.leader().unwrap() % 3 + 1;
    sim.stop(offline);
    for _ in 0..6 {
        sim.propose().await;
        for _ in 0..100 {
            sim.deliver(false).await;
        }
    }
    assert!(sim.acknowledged.len() >= 3);
    sim.truncate().await;
    for (&id, node) in &sim.nodes {
        let store = node.raw.store();
        assert!(store.snapshot_index() > 0);
        assert!(!store.has_image());
        assert_eq!(node.log_worker.storage().recovery_snapshot().data.len(), 0);
        let dir = sim.root.path().join(id.to_string()).join("raft");
        assert!(std::fs::read_dir(dir).unwrap().all(|entry| {
            !entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with("checkpoint-")
        }));
        assert_eq!(node.snapshot_images, 0);
    }
    // Restart a truncated member. Flushed state must cover the removed entries.
    let restarted = sim.leader().unwrap() % 3 + 1;
    let restarted = if restarted == offline {
        restarted % 3 + 1
    } else {
        restarted
    };
    let truncated = sim.nodes[&restarted].raw.store().snapshot_index();
    sim.stop(restarted);
    sim.start(restarted).await;
    let node = &sim.nodes[&restarted];
    assert_eq!(node.raw.store().snapshot_index(), truncated);
    assert!(node.state.applied >= truncated);
    assert_eq!(node.installed_snapshots, 0);
    sim.assert_acknowledged(restarted).await;
    sim.settle(25).await;
    // The returning member needs entries that no live log retains.
    sim.start(offline).await;
    sim.settle(60).await;
    assert!(sim.nodes[&offline].installed_snapshots > 0);
    assert!(sim.nodes.values().map(|n| n.snapshot_images).sum::<u64>() > 0);
    sim.propose().await;
    sim.settle(10).await;
    for id in 1..=3 {
        sim.assert_acknowledged(id).await;
    }
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
    /// Run one writer step on member `id`. A fence closes the writer; the next open step opens
    /// it again. Any other writer error stops the member, as architecture rule 8 requires.
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
        if writer.db.is_some() && !matches!(step, WriterStep::Open) && node.dst_fenced(writer).await
        {
            writer.close().await;
            self.trace.push(format!("writer {id} {step:?} fenced"));
            self.drive().await;
            return;
        }
        let result = match step {
            WriterStep::Open => node.dst_open(writer, false).await.map(drop),
            WriterStep::Materialize => node.dst_materialize(writer).await.map(drop),
            WriterStep::Checkpoint => node.dst_checkpoint(writer).await.map(drop),
            WriterStep::Announce => node.dst_announce(writer),
            WriterStep::Retain => node.dst_retain(writer, &pins).await,
        };
        match result {
            Ok(()) => self.trace.push(format!("writer {id} {step:?}")),
            Err(error) if shared::fenced(&error) => {
                writer.close().await;
                self.trace.push(format!("writer {id} {step:?} fenced"));
            }
            Err(error) => {
                writer.close().await;
                self.trace.push(format!("writer {id} {step:?} failed"));
                tracing::debug!(%error, id, "writer step failed");
                self.stop(id);
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
        let Some(writer) = &node.writer else {
            return;
        };
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
        let admin = writer.admin.clone();
        let _ = admin.run_gc_once(options).await;
        self.trace.push("gc".into());
        self.drive().await;
    }

    /// A shared-mode schedule: one store, writer steps at random points, leader changes,
    /// crashes between writer steps, late opens of deposed writers, partitions, object store
    /// failures, retention with GC, and reads on old views.
    async fn run_shared(seed: u64) -> Vec<String> {
        let mut sim = Self::with_mode(seed, true).await;
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
                    sim.tick(id).await;
                }
                13..=17 => sim.propose().await,
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
                            .is_some_and(|writer: &shared::DstWriter| writer.db.is_some());
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
                            // The deposed writer's own next commit fails with a fence.
                            writer.close().await;
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
                27 => {
                    // The object store rejects SST writes for a while.
                    let store = sim.shared.clone().unwrap();
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
                        .is_some_and(|writer: &shared::DstWriter| writer.db.is_some());
                    if matches!(step, WriterStep::Open) == open {
                        continue;
                    }
                    sim.writer_step(&mut writers, leader, step).await;
                }
            }
        }
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
        sim.trace.push(format!(
            "acknowledged {} views checked {}",
            sim.acknowledged.len(),
            sim.view_checks
        ));
        sim.trace.clone()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shared_store_schedules_replay_identically() {
    let seeds = std::env::var("SLATEDB_DST_SHARED_SEEDS").unwrap_or_else(|_| "1,2,3".into());
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
        eprintln!("shared DST seed {seed}: {} events replayed", first.len());
    }
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
        let mut rows = state.db().scan_prefix(b"etcd/", ..).await.unwrap();
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
            crate::state::maintenance::count_logical_usage(state.db().as_ref())
                .await
                .unwrap()
        );
        // Watch journal rows below the boundary are gone.
        let mut journal = state
            .db()
            .scan_prefix(crate::watch::EVENTS, ..)
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
            .db()
            .scan_prefix(crate::watch::EVENTS, ..)
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
