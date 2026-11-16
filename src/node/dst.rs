//! Drive the real node with seeded message delivery and a logical clock.

use super::*;
use crate::mvcc::Outcome;
use crate::proto::etcdserverpb::request_op::Request as Op;
use crate::proto::etcdserverpb::response_op::Response as Resp;
use crate::proto::etcdserverpb::{PutRequest, RangeRequest};

struct ClientRequest {
    key: String,
    value: String,
    reply: oneshot::Receiver<Result<Value, ApiError>>,
}

struct Simulation {
    seed: u64,
    root: tempfile::TempDir,
    nodes: BTreeMap<u64, Node>,
    links: BTreeMap<(u64, u64), mpsc::Receiver<Message>>,
    messages: Vec<Message>,
    requests: Vec<ClientRequest>,
    acknowledged: BTreeMap<String, String>,
    committed_entries: BTreeMap<u64, Vec<u8>>,
    terms: BTreeMap<u64, u64>,
    isolated: Option<u64>,
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
            nodes: BTreeMap::new(),
            links: BTreeMap::new(),
            messages: vec![],
            requests: vec![],
            acknowledged: BTreeMap::new(),
            committed_entries: BTreeMap::new(),
            terms: BTreeMap::new(),
            isolated: None,
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
        let node = Node::open_with_transport(
            NodeOptions {
                id,
                cluster_id: "dst".into(),
                dir: self.root.path().join(id.to_string()),
                peers: (1..=3).map(|n| (n, format!("127.0.0.1:{n}"))).collect(),
                snapshot_every: 0,
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
        for node in self.nodes.values_mut() {
            node.drive_ready().await.unwrap();
        }
        for link in self.links.values_mut() {
            while let Ok(message) = link.try_recv() {
                self.messages.push(message);
            }
        }
        let mut pending = vec![];
        for mut request in self.requests.drain(..) {
            match request.reply.try_recv() {
                Ok(Ok(reply)) => {
                    let result: Outcome = serde_json::from_value(reply).unwrap();
                    result.result.unwrap();
                    self.trace.push(format!("ack {}", request.key));
                    self.acknowledged.insert(request.key, request.value);
                }
                Ok(Err(_)) | Err(oneshot::error::TryRecvError::Closed) => {}
                Err(oneshot::error::TryRecvError::Empty) => pending.push(request),
            }
        }
        self.requests = pending;
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
            let entries = store
                .entries(
                    store.first_index().unwrap(),
                    store.commit_index() + 1,
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
            if snapshot {
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
                    id: key.clone(),
                    etcd: RequestOp {
                        request: Some(Op::RequestPut(PutRequest {
                            key: key.as_bytes().to_vec(),
                            value: value.as_bytes().to_vec(),
                            ..Default::default()
                        })),
                    },
                },
                tx,
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

    async fn checkpoint(&mut self) {
        for node in self.nodes.values_mut() {
            let index = node.state.applied;
            if index <= node.raw.store().snapshot_index() {
                continue;
            }
            let data = node.state.snapshot_data().await.unwrap();
            node.handle(
                Event::SnapshotBuilt {
                    index,
                    result: Ok(data),
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

    async fn run(seed: u64) -> Vec<String> {
        let mut sim = Self::new(seed).await;
        let mut offline = 0;
        for step in 0..400 {
            match step {
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
                    offline = sim.leader().unwrap() % 3 + 1;
                    sim.stop(offline);
                }
                260 => sim.checkpoint().await,
                300 => sim.start(offline).await,
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
        let snapshots = sim.installed_snapshots
            + sim
                .nodes
                .values()
                .map(|n| n.installed_snapshots)
                .sum::<u64>();
        assert!(snapshots > 0, "seed {seed}: no snapshot transfer");
        for node in sim.nodes.values() {
            for (key, value) in &sim.acknowledged {
                let outcome = node
                    .state
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
            }
        }
        sim.trace.clone()
    }
}

#[tokio::test]
async fn seeded_fault_schedules_replay_identically() {
    let seeds = std::env::var("SLATEDB_DST_SEEDS").unwrap_or_else(|_| "1,2,42".into());
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
