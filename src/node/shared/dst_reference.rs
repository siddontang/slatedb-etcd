//! The reference state of the simulation and the check of each member view against it.

use super::*;

/// The state that each member view must equal at its applied index.
///
/// The reference applies each committed entry in order with `apply::apply_entries`. It applies
/// them to an in-memory state from `StateMachine::open_memory`. That state is a whole
/// `StateMachine`, as in the members. It shares the apply rules with them: MVCC prepare, the
/// lease and membership rules, the key index, and the size counters. It leaves out the storage
/// side: the overlay, the base and its switch, the materializer, and the checkpoints. So the view
/// check finds an error in the storage side of the members. It does not check the apply rules,
/// because the members and the reference use the same rules.
///
/// A state with an overlay and no writer also applies each entry. Its claim quota disables the
/// check, as the quota of the in-memory state does. The two states must have equal rows and
/// equal loaded fields after each index, so the comparison finds an error in the in-memory
/// state.
pub(super) struct Reference {
    memory: StateMachine,
    overlay: StateMachine,
    /// The indexes at which the in-memory state and the overlay state were equal.
    pub(super) compared: u64,
    /// The compared entries of each kind. See `entry_kind`. A kind ends in ` (rejected)` when
    /// the in-memory state applied the entry with an error reply.
    kinds: BTreeMap<String, u64>,
    /// The loaded fields of the in-memory state after each index.
    loaded: BTreeMap<u64, String>,
}

impl Reference {
    /// Open the reference states in `dir`. The seed entry of the first leader gives them the
    /// membership row.
    pub(super) async fn open(dir: &std::path::Path) -> Self {
        let memory = StateMachine::open_memory().await.unwrap();
        let loaded = BTreeMap::from([(memory.applied, memory.loaded_fields())]);
        Self {
            memory,
            overlay: StateMachine::open_test(dir).await.unwrap(),
            compared: 0,
            kinds: BTreeMap::new(),
            loaded,
        }
    }

    fn applied(&self) -> u64 {
        self.overlay.applied
    }

    /// The loaded fields of the in-memory state after `index`.
    pub(super) fn loaded(&self, index: u64) -> Option<&String> {
        self.loaded.get(&index)
    }

    /// Apply `entry`, the next committed entry. Return the digest of the rows after it.
    async fn apply(&mut self, entry: Entry) -> u32 {
        let header = Header {
            cluster: 0,
            member: 0,
            term: entry.term,
        };
        let overlay = &mut self.overlay;
        apply::apply_entries(overlay, vec![entry.clone()], header)
            .await
            .unwrap();
        let expected = rows_digest(overlay.applied_state()).await.unwrap();
        let memory = &mut self.memory;
        let mut kind = entry_kind(&entry);
        let replies = apply::apply_entries(memory, vec![entry], header)
            .await
            .unwrap();
        if replies.iter().any(|applied| rejected(&applied.reply)) {
            kind.push_str(" (rejected)");
        }
        let digest = rows_digest(memory.applied_state()).await.unwrap();
        let index = memory.applied;
        assert_eq!(digest, expected, "the reference rows differ at {index}");
        let fields = memory.loaded_fields();
        assert_eq!(
            fields,
            overlay.loaded_fields(),
            "the reference fields differ at {index}"
        );
        self.loaded.insert(index, fields);
        self.compared += 1;
        *self.kinds.entry(kind).or_default() += 1;
        digest
    }

    /// The compared entries of each kind, as `kind=count` items.
    pub(super) fn kinds(&self) -> String {
        let kinds = self
            .kinds
            .iter()
            .map(|(kind, count)| format!("{kind}={count}"));
        kinds.collect::<Vec<_>>().join(", ")
    }
}

/// Return true for an error reply. Apply stores the state of a rejected entry too, such as the
/// apply marker, so the comparison still covers it.
fn rejected(reply: &apply::Reply) -> bool {
    let encoded = |value: &Value| value.get("result").is_some_and(|r| r.get("Err").is_some());
    match reply {
        apply::Reply::Encoded(value) | apply::Reply::PhysicalCompact { value, .. } => {
            encoded(value)
        }
        apply::Reply::CompactFlush { .. } => false,
        apply::Reply::Kv(outcome) => outcome.result.is_err(),
        apply::Reply::Lease(outcome) => outcome.result.is_err(),
    }
}

/// The kind of a committed entry: its command and the operation in it.
fn entry_kind(entry: &Entry) -> String {
    if entry.data.is_empty() {
        return "empty".into();
    }
    if entry.entry_type == EntryType::EntryConfChange as i32 {
        let change = raft::eraftpb::ConfChange::decode(entry.data.as_slice()).unwrap();
        let command = RaftCommand::decode(&change.context).unwrap();
        return format!(
            "conf change {:?} {}",
            change.change_type(),
            command_kind(&command)
        );
    }
    command_kind(&RaftCommand::decode(&entry.data).unwrap())
}

fn command_kind(command: &RaftCommand) -> String {
    // The name of an enum variant is the start of its debug text.
    let variant = |value: &dyn std::fmt::Debug| {
        let text = format!("{value:?}");
        text.split([' ', '(', '{'])
            .next()
            .unwrap_or_default()
            .to_owned()
    };
    match command {
        RaftCommand::Etcd { etcd, .. } => match &etcd.request {
            Some(Op::RequestPut(_)) => "put".into(),
            Some(Op::RequestTxn(_)) => "txn".into(),
            Some(Op::RequestDeleteRange(_)) => "delete range".into(),
            Some(Op::RequestRange(_)) => "range".into(),
            None => "etcd without request".into(),
        },
        RaftCommand::Lease { lease, .. } => format!("lease {}", variant(lease)),
        RaftCommand::Compact { compact, .. } if compact.physical => "physical compact".into(),
        RaftCommand::Compact { .. } => "compact".into(),
        RaftCommand::CompactStep { .. } => "compact step".into(),
        RaftCommand::ViewCheckpoint { .. } => "view checkpoint".into(),
        RaftCommand::SeedMembers(_) => "seed members".into(),
        RaftCommand::Cluster { request, .. } => format!("member {}", variant(request)),
        RaftCommand::Maintenance { request, .. } => format!("maintenance {}", variant(request)),
        RaftCommand::Auth { auth, .. } => format!("auth {}", variant(auth)),
        RaftCommand::Secured { command, .. } => format!("secured {}", command_kind(command)),
        RaftCommand::Diagnostic(_) => "diagnostic".into(),
    }
}

/// The member state that one view check covered: the applied index, the base of the published
/// view, and the process of the member. A base switch changes the view but keeps the applied
/// index. A restart builds a new view, also at the same index and base.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct ViewPoint {
    applied: u64,
    checkpoint: Option<uuid::Uuid>,
    base_index: u64,
    process: Option<u64>,
}

impl ViewPoint {
    fn of(state: &apply::AppliedState, process: Option<u64>) -> Self {
        Self {
            applied: state.applied,
            checkpoint: state.base_checkpoint,
            base_index: state.view.0,
            process,
        }
    }
}

impl Simulation {
    /// Shared mode: compare the loaded fields of each member with the in-memory reference at
    /// its index. A member that opens or installs a base above its row must also equal it.
    fn check_loaded(&self) -> Result<(), String> {
        let Some(reference) = &self.reference else {
            return Ok(());
        };
        for (&id, node) in &self.nodes {
            let state = node.worker.state();
            let Some(expected) = reference.loaded(state.applied) else {
                continue;
            };
            if state.loaded_fields() != *expected {
                return Err(format!(
                    "member {id} loaded other fields at {}",
                    state.applied
                ));
            }
        }
        Ok(())
    }

    /// Open, materialize, checkpoint, and announce on the leader.
    pub(super) async fn publish_view(&mut self, writers: &mut BTreeMap<u64, shared::DstWriter>) {
        let leader = self.leader().unwrap();
        for step in [
            WriterStep::Open,
            WriterStep::Materialize,
            WriterStep::Checkpoint,
            WriterStep::Announce,
        ] {
            let open = writers
                .get(&leader)
                .is_some_and(|writer| writer.db.is_some());
            if matches!(step, WriterStep::Open) && open {
                continue;
            }
            self.writer_step(writers, leader, step).await;
        }
    }

    /// The base index, the checkpoint, and the overlay bytes of member `id`.
    pub(super) fn base(&self, id: u64) -> (u64, Option<uuid::Uuid>, u64) {
        let state = self.nodes[&id].worker.state();
        let (index, _, bytes) = state.view_status();
        (index, state.base_checkpoint(), bytes)
    }

    /// Run `check_views`, and fail the simulation on a difference.
    pub(super) async fn check_reference(&mut self) {
        if let Err(error) = self.check_views().await {
            panic!("seed {}: {error}", self.seed);
        }
    }

    /// Apply the new committed entries to the reference. Then compare each member view with the
    /// reference at the applied index of the member. The check runs again on a member when its
    /// applied index, its base, or its process changes. A view can fail on a deleted object, but
    /// it never returns other data.
    pub(super) async fn check_views(&mut self) -> Result<(), String> {
        let reference = self.reference.as_mut().unwrap();
        while let Some(bytes) = self.committed_entries.get(&(reference.applied() + 1)) {
            let entry = Entry::decode(bytes.as_slice()).unwrap();
            let digest = reference.apply(entry).await;
            self.digests.insert(reference.applied(), digest);
        }
        self.check_loaded()?;
        for (&id, node) in &self.nodes {
            let point = ViewPoint::of(&node.state, self.processes.get(&id).copied());
            let last = self.checked.get(&id).copied();
            if last == Some(point) {
                continue;
            }
            let Some(&expected) = self.digests.get(&point.applied) else {
                continue;
            };
            let Ok(actual) = rows_digest(node.state.read.state()).await else {
                continue;
            };
            if actual != expected {
                return Err(format!(
                    "member {id} view at {} differs from the reference",
                    point.applied
                ));
            }
            match last {
                Some(last) if last.applied == point.applied => self.base_checks += 1,
                _ => self.view_checks += 1,
            }
            self.checked.insert(id, point);
        }
        Ok(())
    }
}

/// The power test of the view check. A follower in an idle cluster switches to an older
/// checkpoint at its applied index, as if an announcement certified that checkpoint there. No
/// entry applies after the switch, so only the check on a base change can find the view that
/// lost the later writes.
#[tokio::test]
async fn the_view_check_finds_a_wrong_base_switch_in_an_idle_cluster() {
    let mut sim = Simulation::new(97).await;
    sim.settle(4).await;
    // Some Puts attach this lease.
    sim.lease(LeaseCommand::Grant { id: 77, ttl: 3600 }).await;
    let mut writers = BTreeMap::new();
    sim.write(2).await;
    sim.publish_view(&mut writers).await;
    sim.settle(10).await;
    let follower = sim.follower(0);
    let (_, old, _) = sim.base(follower);
    sim.write(2).await;
    sim.publish_view(&mut writers).await;
    sim.settle(10).await;
    let (_, new, _) = sim.base(follower);
    assert!(old.is_some() && new.is_some() && new != old);
    let applied = sim.nodes[&follower].state.applied;
    assert_eq!(sim.checked[&follower].applied, applied);
    let checks = sim.base_checks;
    // From here the test runs the check, so it sees the result.
    let reference = sim.reference.take();
    let node = &sim.nodes[&follower];
    let base = node.shared_source();
    let base = base.open_base(old.unwrap()).await.unwrap();
    node.worker.state().offer_base(base, applied);
    for _ in 0..3 {
        sim.drive().await;
    }
    let state = &sim.nodes[&follower].state;
    assert_eq!((state.applied, state.base_checkpoint), (applied, old));
    sim.reference = reference;
    let error = sim.check_views().await.unwrap_err();
    assert_eq!(
        error,
        format!("member {follower} view at {applied} differs from the reference")
    );
    assert_eq!(sim.base_checks, checks);
}

/// A restart builds a new view. A follower in an idle cluster restarts at a recorded base, so the
/// new process opens the same checkpoint at the same applied index and base index. Its view is
/// wrong: it is a view of an earlier index. The check must run again for the new process.
#[tokio::test]
async fn the_view_check_runs_again_after_a_restart_at_the_same_base() {
    let mut sim = Simulation::new(101).await;
    sim.settle(4).await;
    sim.lease(LeaseCommand::Grant { id: 77, ttl: 3600 }).await;
    let mut writers = BTreeMap::new();
    sim.write(2).await;
    let follower = sim.follower(0);
    let earlier = sim.nodes[&follower].state.read.clone();
    sim.write(2).await;
    sim.publish_view(&mut writers).await;
    sim.settle(10).await;
    // The leader certifies its base at the last index and records it there for the follower.
    let node = sim.nodes.get_mut(&follower).unwrap();
    node.raw.request_snapshot().unwrap();
    sim.settle(30).await;
    let before = sim.checked[&follower];
    // From here the test runs the check, so it sees the result.
    let reference = sim.reference.take();
    sim.stop(follower);
    sim.start(follower).await;
    sim.settle(4).await;
    let process = sim.processes.get(&follower).copied();
    let after = ViewPoint::of(&sim.nodes[&follower].state, process);
    assert_eq!(after, ViewPoint { process, ..before });
    assert_ne!(process, before.process);
    sim.nodes.get_mut(&follower).unwrap().state.read = earlier;
    sim.reference = reference;
    let error = sim.check_views().await.unwrap_err();
    assert_eq!(
        error,
        format!(
            "member {follower} view at {} differs from the reference",
            before.applied
        )
    );
}

/// Every member loads the same wrong revision at a new index. The members agree, so only the
/// comparison with the fields of the reference finds it.
#[tokio::test]
async fn the_loaded_check_compares_each_member_with_the_reference() {
    let mut sim = Simulation::new(103).await;
    sim.settle(4).await;
    sim.lease(LeaseCommand::Grant { id: 77, ttl: 3600 }).await;
    // From here the test runs the check, so it sees the result.
    let reference = sim.reference.take();
    sim.write(1).await;
    let applied = sim.nodes[&1].state.applied;
    for node in sim.nodes.values_mut() {
        assert_eq!(node.state.applied, applied);
        node.worker.state_mut().revision += 7;
    }
    sim.reference = reference;
    let error = sim.check_views().await.unwrap_err();
    assert_eq!(error, format!("member 1 loaded other fields at {applied}"));
}

/// Only the in-memory reference stores a wrong diagnostic value of the same length. The digest
/// covers the `data/` rows with their lengths, so the comparison of the references finds it.
#[tokio::test]
#[should_panic(expected = "the reference rows differ at 2")]
async fn the_reference_comparison_covers_diagnostic_rows() {
    let dir = tempfile::tempdir().unwrap();
    let mut reference = Reference::open(dir.path()).await;
    let put = RaftCommand::Diagnostic(crate::Command {
        id: "put".into(),
        request: crate::Request {
            operations: vec![crate::Operation::Put {
                key: "k".into(),
                value: "right".into(),
            }],
        },
    });
    let entry = |index, data: Vec<u8>| Entry {
        index,
        term: 1,
        data,
        ..Default::default()
    };
    reference.apply(entry(1, put.encode().unwrap())).await;
    let mut wrong = crate::WriteBatch::new();
    wrong.put(b"data/k", b"wrong");
    reference.memory.write_memory(&wrong);
    reference.apply(entry(2, vec![])).await;
}

/// Two states whose rows join to the same bytes have other digests, because the length of each
/// key and value comes before it.
#[tokio::test]
async fn the_row_digest_frames_each_key_and_value() {
    let mut digests = vec![];
    for (key, value) in [("data/ab", "c"), ("data/a", "bc")] {
        let mut state = StateMachine::open_memory().await.unwrap();
        let mut batch = crate::WriteBatch::new();
        batch.put(key, value);
        state.write_memory(&batch);
        digests.push(rows_digest(state.applied_state()).await.unwrap());
    }
    assert_ne!(digests[0], digests[1]);
}

impl Simulation {
    /// Return true when the applied state of member `id` has a row at `key`.
    async fn has_row(&self, id: u64, key: &[u8]) -> bool {
        let state = self.nodes[&id].worker.state();
        state.applied_state().get(key).await.unwrap().is_some()
    }

    /// Propose `command` on the leader and deliver messages until it replies. The reply can be an
    /// error, but the entry must commit.
    async fn commit(&mut self, command: RaftCommand) {
        let leader = self.leader().unwrap();
        self.trace.push(format!("commit {}", command.id()));
        let (tx, mut rx) = oneshot::channel();
        self.nodes
            .get_mut(&leader)
            .unwrap()
            .propose(command, tx)
            .unwrap();
        self.drive().await;
        for _ in 0..500 {
            self.deliver(false).await;
            if let Ok(reply) = rx.try_recv() {
                reply.unwrap();
                return;
            }
        }
        panic!("the command did not commit");
    }
}

/// A shared cluster commits each entry kind that shared mode applies. The random shared schedules
/// write only puts, lease grants, and view checkpoints. Here the view check and the comparison of
/// the two references also cover transactions, deletes, the lease rules, compaction, alarms,
/// membership changes, auth, and diagnostic writes. Each of these must apply without an error,
/// and the test checks its effect. The maintenance commands that run only on a local member are
/// committed too: apply rejects them, but still stores the apply marker.
#[tokio::test]
async fn every_entry_kind_matches_in_a_shared_cluster() {
    use crate::auth::{Command as A, Credentials};
    use crate::proto::etcdserverpb::*;
    let mut sim = Simulation::new(107).await;
    sim.settle(4).await;
    let mut writers = BTreeMap::new();
    let leader = sim.leader().unwrap();
    let follower = sim.follower(0);
    let etcd = |id: &str, request| RaftCommand::Etcd {
        id: id.into(),
        origin: leader,
        etcd: RequestOp {
            request: Some(request),
        },
    };
    let put = |key: &str, lease| {
        Op::RequestPut(PutRequest {
            key: key.into(),
            value: b"value".to_vec(),
            lease,
            ..Default::default()
        })
    };
    let op = |request| RequestOp {
        request: Some(request),
    };
    // Leases: a renewal, a revoke that deletes a key, and an expiry.
    sim.lease(LeaseCommand::Grant { id: 77, ttl: 3600 }).await;
    sim.lease(LeaseCommand::KeepAlive { id: 77 }).await;
    sim.lease(LeaseCommand::Grant { id: 78, ttl: 3600 }).await;
    sim.commit(etcd("put-78", put("lease/78", 78))).await;
    assert!(sim.has_row(leader, b"etcd/current/lease/78").await);
    sim.lease(LeaseCommand::Revoke { id: 78 }).await;
    assert!(!sim.has_row(leader, b"etcd/current/lease/78").await);
    sim.lease(LeaseCommand::Grant { id: 79, ttl: 3600 }).await;
    sim.commit(etcd("put-79", put("lease/79", 79))).await;
    let generation = sim.nodes[&leader].state.leases[&79].generation;
    sim.lease(LeaseCommand::Expire { id: 79, generation }).await;
    assert!(!sim.has_row(leader, b"etcd/current/lease/79").await);
    assert_eq!(
        sim.nodes[&leader].state.leases.keys().collect::<Vec<_>>(),
        [&77]
    );
    // Puts, a transaction, and a delete range.
    for key in ["a/1", "a/2", "a/3", "b/1"] {
        sim.commit(etcd(key, put(key, 0))).await;
    }
    let txn = TxnRequest {
        success: vec![
            op(put("a/4", 0)),
            op(Op::RequestDeleteRange(DeleteRangeRequest {
                key: b"a/1".to_vec(),
                ..Default::default()
            })),
            op(Op::RequestRange(RangeRequest {
                key: b"a/".to_vec(),
                range_end: b"a0".to_vec(),
                ..Default::default()
            })),
        ],
        ..Default::default()
    };
    sim.commit(etcd("txn", Op::RequestTxn(txn))).await;
    assert!(sim.has_row(leader, b"etcd/current/a/4").await);
    assert!(!sim.has_row(leader, b"etcd/current/a/1").await);
    let delete = DeleteRangeRequest {
        key: b"a/".to_vec(),
        range_end: b"a0".to_vec(),
        ..Default::default()
    };
    sim.commit(etcd("delete", Op::RequestDeleteRange(delete)))
        .await;
    for key in ["a/2", "a/3", "a/4"] {
        let key = format!("etcd/current/{key}");
        assert!(!sim.has_row(leader, key.as_bytes()).await);
    }
    sim.publish_view(&mut writers).await;
    sim.settle(4).await;
    // Diagnostic writes.
    let diagnostic = |id: &str, operation| {
        RaftCommand::Diagnostic(crate::Command {
            id: id.into(),
            request: crate::Request {
                operations: vec![operation],
            },
        })
    };
    let value = "v".to_owned();
    let write = crate::Operation::Put {
        key: "k".into(),
        value,
    };
    sim.commit(diagnostic("diagnostic-put", write)).await;
    assert!(sim.has_row(leader, b"data/k").await);
    let delete = crate::Operation::Delete { key: "k".into() };
    sim.commit(diagnostic("diagnostic-delete", delete)).await;
    assert!(!sim.has_row(leader, b"data/k").await);
    // A logical compaction, then a physical one with its steps.
    for _ in 0..4 {
        sim.commit(etcd("b/1", put("b/1", 0))).await;
    }
    let revision = sim.nodes[&leader].state.revision;
    let compact = |id: &str, revision, physical| RaftCommand::Compact {
        id: id.into(),
        compact: CompactionRequest { revision, physical },
    };
    sim.commit(compact("logical", revision - 2, false)).await;
    assert_eq!(sim.nodes[&leader].state.compact_revision, revision - 2);
    let (tx, _reply) = oneshot::channel();
    let node = sim.nodes.get_mut(&leader).unwrap();
    node.propose(compact("physical", revision, true), tx)
        .unwrap();
    for _ in 0..6 {
        sim.settle(2).await;
        sim.publish_view(&mut writers).await;
    }
    let state = &sim.nodes[&leader].state;
    assert_eq!(
        (state.compact_revision, state.physical_revision),
        (revision, revision)
    );
    // An alarm that turns on and off.
    let maintenance = |id: &str, request| RaftCommand::Maintenance {
        id: id.into(),
        request,
        credentials: Default::default(),
    };
    use crate::maintenance::Command as M;
    let alarm = |action| {
        M::Alarm(AlarmRequest {
            action,
            member_id: leader,
            alarm: 1,
        })
    };
    sim.commit(maintenance("alarm-on", alarm(1))).await;
    assert_eq!(sim.nodes[&leader].worker.state().alarms.len(), 1);
    sim.commit(maintenance("alarm-off", alarm(2))).await;
    assert!(sim.nodes[&leader].worker.state().alarms.is_empty());
    // Commands that run only on a local member. Apply rejects them.
    let downgrade = DowngradeRequest::default();
    for (id, request) in [
        ("status", M::Status),
        ("hash", M::Hash(0)),
        ("hash-all", M::HashAll),
        ("defragment", M::Defragment),
        ("downgrade", M::Downgrade(downgrade)),
    ] {
        sim.commit(maintenance(id, request)).await;
    }
    // Membership: an update, a publish, and a learner that joins and leaves.
    let cluster = |id: &str, request| RaftCommand::Cluster {
        id: id.into(),
        request,
        credentials: Default::default(),
    };
    use crate::cluster::Command as C;
    let url = format!("http://127.0.0.1:{}", 2479 + follower);
    let update = MemberUpdateRequest {
        id: follower,
        peer_ur_ls: vec![url.clone()],
    };
    sim.commit(cluster("update", C::Update(update))).await;
    let publish = C::Publish {
        id: follower,
        client_urls: vec![url.clone()],
    };
    sim.commit(cluster("publish", publish)).await;
    let member = &sim.nodes[&leader].state.members.members[&follower];
    assert_eq!(
        (&member.peer_ur_ls, &member.client_ur_ls),
        (&vec![url.clone()], &vec![url])
    );
    let learner = Member {
        id: 4,
        peer_ur_ls: vec!["http://127.0.0.1:2383".into()],
        is_learner: true,
        ..Default::default()
    };
    sim.commit(cluster("add", C::Add(learner))).await;
    sim.settle(2).await;
    let members = &sim.nodes[&leader].state.members.members;
    assert!(members.get(&4).is_some_and(|member| member.is_learner));
    sim.commit(cluster("remove", C::Remove(4))).await;
    sim.settle(2).await;
    assert!(!sim.nodes[&leader].state.members.members.contains_key(&4));
    // Auth: enable it, write with credentials, and disable it.
    let root = Credentials {
        user: Some("root".into()),
        time: 100,
        ..Default::default()
    };
    let auth = |id: &str, auth| RaftCommand::Auth {
        id: id.into(),
        auth,
        credentials: root.clone(),
    };
    for (id, command) in [
        (
            "user",
            A::UserAdd(AuthUserAddRequest {
                name: "root".into(),
                ..Default::default()
            }),
        ),
        (
            "grant",
            A::UserGrantRole(AuthUserGrantRoleRequest {
                user: "root".into(),
                role: "root".into(),
            }),
        ),
        ("enable", A::Enable(AuthEnableRequest {})),
    ] {
        sim.commit(auth(id, command)).await;
    }
    assert!(sim.nodes[&leader].state.auth.enabled);
    let secured = etcd("secured", put("b/2", 0)).secure(Some(root.clone()));
    sim.commit(secured).await;
    assert!(sim.has_row(leader, b"etcd/current/b/2").await);
    sim.commit(auth("disable", A::Disable(AuthDisableRequest {})))
        .await;
    assert!(!sim.nodes[&leader].state.auth.enabled);
    sim.publish_view(&mut writers).await;
    sim.settle(10).await;
    let kinds = &sim.reference.as_ref().unwrap().kinds;
    let rejected = |kind: &str| format!("{kind} (rejected)");
    for kind in [
        "put",
        "txn",
        "delete range",
        "lease Grant",
        "lease KeepAlive",
        "lease Revoke",
        "lease Expire",
        "compact",
        "physical compact",
        "compact step",
        "diagnostic",
        "maintenance Alarm",
        "member Update",
        "member Publish",
        "conf change AddLearnerNode member Add",
        "conf change RemoveNode member Remove",
        "auth UserAdd",
        "auth UserGrantRole",
        "auth Enable",
        "secured put",
        "auth Disable",
        "view checkpoint",
        "seed members",
    ] {
        assert!(kinds.contains_key(kind), "no {kind} entry in {kinds:?}");
        assert!(
            !kinds.contains_key(&rejected(kind)),
            "a {kind} entry failed"
        );
    }
    for kind in [
        "maintenance Status",
        "maintenance Hash",
        "maintenance HashAll",
        "maintenance Defragment",
        "maintenance Downgrade",
    ] {
        assert!(
            kinds.contains_key(&rejected(kind)),
            "no rejected {kind} entry in {kinds:?}"
        );
        assert!(!kinds.contains_key(kind), "a {kind} entry applied");
    }
}
