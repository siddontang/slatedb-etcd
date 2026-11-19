//! Drive the real node with seeded message delivery and a logical clock.

use super::*;
use crate::lease::{LeaseCommand, LeaseReply};
use crate::mvcc::Outcome;
use crate::proto::etcdserverpb::request_op::Request as Op;
use crate::proto::etcdserverpb::response_op::Response as Resp;
use crate::proto::etcdserverpb::{PutRequest, RangeRequest, WatchCreateRequest};

struct ClientRequest {
    key: String,
    value: String,
    reply: oneshot::Receiver<Result<Outcome, ApiError>>,
}

struct ReadRequest {
    node: u64,
    known: BTreeMap<String, String>,
    reply: oneshot::Receiver<Result<Outcome, ApiError>>,
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
    async fn new(seed: u64) -> Self {
        let mut sim = Self {
            seed,
            root: tempfile::tempdir().unwrap(),
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
        let store = self
            .stores
            .entry(id)
            .or_insert_with(|| {
                let path = self.root.path().join(id.to_string()).join("objects");
                std::fs::create_dir_all(&path).unwrap();
                std::sync::Arc::new(crate::object::faults::FaultStore::new(std::sync::Arc::new(
                    slatedb::object_store::local::LocalFileSystem::new_with_prefix(path).unwrap(),
                )))
            })
            .clone();
        let node = Node::open_with_transport(
            NodeOptions {
                client_peers: BTreeMap::new(),
                transport: crate::tls::Transport::default(),
                require_peer_tls: false,
                object_store: crate::object::Config::Test(store, "state".into()),
                strict_reconfiguration: false,
                join: false,
                quota_bytes: 0,
                id,
                cluster_id: "dst".into(),
                dir: self.root.path().join(id.to_string()),
                peers: (1..=3).map(|n| (n, format!("127.0.0.1:{n}"))).collect(),
                snapshot_every: 0,
                log_memory: LogMemory {
                    cache_bytes: 1024,
                    ..LogMemory::default()
                },
            },
            events,
            Some(senders),
        )
        .await
        .unwrap();
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
                    let mut result = result.result.unwrap();
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
                    let Some(Resp::ResponseRange(response)) = result.result.unwrap().response
                    else {
                        panic!("expected range");
                    };
                    for (key, value) in read.known {
                        assert!(
                            response
                                .kvs
                                .iter()
                                .any(|kv| kv.key == key.as_bytes() && kv.value == value.as_bytes()),
                            "read lost a prior acknowledged write"
                        );
                    }
                    self.trace
                        .push(format!("read ack {} {}", read.node, response.kvs.len()));
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
            assert!(node.state.applied <= store.commit_index());
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
                let bytes = entry.write_to_bytes().unwrap();
                if let Some(previous) = self.committed_entries.insert(entry.index, bytes.clone()) {
                    assert_eq!(previous, bytes, "committed index {} changed", entry.index);
                }
            }
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
        if let Some(node) = self.nodes.get_mut(&message.to) {
            let snapshot = message.get_msg_type() == MessageType::MsgSnapshot;
            let from = message.from;
            let to = message.to;
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
            }
        }
        self.drive().await;
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
            let data = node.worker.state().snapshot_job().unwrap().await.unwrap();
            node.handle(
                Event::SnapshotBuilt {
                    index,
                    conf: node.worker.state().members.conf(),
                    result: PreparedCheckpoint::write(&node.options.dir.join("raft"), data.into()),
                },
                &mut vec![],
            )
            .await
            .unwrap();
            self.trace
                .push(format!("snapshot {} {index}", node.options.id));
        }
        self.drive().await;
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
        assert!(std::fs::metadata(file).unwrap().len() < saved.data.len() as u64);
        let staged = PreparedCheckpoint::write(&dir, saved.data.clone()).unwrap();
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
                    Event::SnapshotBuilt {
                        index,
                        conf,
                        result: Ok(staged),
                    },
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
        for _ in 0..500 {
            if self.nodes[&leader].state.compact_revision == revision {
                break;
            }
            self.deliver(false).await;
        }
        let node = self.nodes.get_mut(&leader).unwrap();
        assert_eq!(node.state.compact_revision, revision);
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
        assert_eq!(*node.forwarding.subscribe().compact.borrow(), first);
        self.paused_flush = None;
        self.drive().await;
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
                state.logical_size(),
                crate::state::maintenance::count_logical_size(&state.db)
                    .await
                    .unwrap()
            );
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
        let batch_value = |index: usize| {
            if index == 15 {
                vec![b'v'; 64 * 1024]
            } else {
                format!("v{index}").into_bytes()
            }
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
                                prev_kv: true,
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
            let Some(Resp::ResponsePut(value)) =
                reply.try_recv().unwrap().unwrap().result.unwrap().response
            else {
                panic!("expected put");
            };
            assert_eq!(
                value.header.unwrap().revision,
                old_revision + offset as i64 + 1
            );
            assert_eq!(
                value.prev_kv.map(|kv| kv.value),
                offset.checked_sub(1).map(batch_value)
            );
        }
        self.paused_reads = None;
        self.drive().await;
        let Some(Resp::ResponseRange(value)) =
            read.await.unwrap().unwrap().result.unwrap().response
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
            }
            assert_eq!(
                state.logical_size(),
                crate::state::maintenance::count_logical_size(&state.db)
                    .await
                    .unwrap()
            );
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
        let Some(Resp::ResponsePut(put)) = rx.try_recv().unwrap().unwrap().result.unwrap().response
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
                    sim.checkpoint().await;
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
            let mut rows = state.db.scan_prefix(b"etcd/", ..).await.unwrap();
            let mut size = 0;
            while let Some(row) = rows.next().await.unwrap() {
                size += (row.key.len() + row.value.len()) as u64;
            }
            assert_eq!(state.logical_size(), size, "seed {seed}");
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
