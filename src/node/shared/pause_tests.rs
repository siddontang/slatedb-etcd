//! A held apply job, a covering snapshot, and the ConfState of Raft.

use super::*;
use crate::proto::etcdserverpb::Member;
use raft::eraftpb::{HardState, Snapshot};

/// A member at `dir` whose stored state holds the diagnostic value `bad`, which is not UTF-8,
/// through index 2. Its log holds entries 1 to 20 of term 1: entry 5 adds learner 4, and entry
/// 8 reads `bad`.
async fn held_member(dir: &std::path::Path) -> Node {
    let mut state = StateMachine::open_test(&dir.join("objects")).await.unwrap();
    // The direct write takes sequence number 1, so the apply marker goes to index 2.
    state.write_raw(&[(b"data/bad", b"\xff\xfe")]);
    state.apply(2, None).await.unwrap();
    state.close().await.unwrap();
    let request = crate::cluster::Command::Add(Member {
        id: 4,
        peer_ur_ls: vec!["http://127.0.0.1:4".into()],
        is_learner: true,
        ..Default::default()
    });
    let mut change = request.change();
    change.context = RaftCommand::Cluster {
        id: "5".into(),
        request,
        credentials: Default::default(),
    }
    .encode()
    .unwrap();
    let get = RaftCommand::Diagnostic(Command {
        id: "8".into(),
        request: Request {
            operations: vec![crate::Operation::Get { key: "bad".into() }],
        },
    });
    let entries: Vec<_> = (1..=20)
        .map(|index| {
            let mut entry = Entry {
                index,
                term: 1,
                ..Default::default()
            };
            if index == 5 {
                entry.entry_type = EntryType::EntryConfChange as i32;
                entry.data = change.encode_to_vec();
            } else if index == 8 {
                entry.data = get.encode().unwrap();
            }
            entry
        })
        .collect();
    let mut store = RaftStorage::open(&dir.join("raft"), 1, "worker-test", vec![1, 2, 3]).unwrap();
    let hard = HardState {
        term: 1,
        vote: 1,
        commit: 20,
    };
    store.persist(&entries, Some(&hard), None).unwrap();
    drop(store);
    let (events, _) = mpsc::channel(256);
    let options = super::super::tests::options(dir);
    Node::open_with_transport(options, events, Some(BTreeMap::new()))
        .await
        .unwrap()
}

/// A snapshot of index 200 and term 2 from leader 2. Its members row and its ConfState hold
/// voters 1 to 3 and no learner.
async fn covering_snapshot(source: &std::path::Path) -> Message {
    let mut state = StateMachine::open_test(source).await.unwrap();
    let peers = (1..=3).map(|id| (id, format!("127.0.0.1:{id}"))).collect();
    state.members = std::sync::Arc::new(crate::cluster::Store::initial(&peers));
    state.seed_for_test(1).await.unwrap();
    for index in 2..=200 {
        state.apply(index, None).await.unwrap();
    }
    let mut snapshot = Snapshot::default();
    snapshot.mut_metadata().index = 200;
    snapshot.mut_metadata().term = 2;
    snapshot.mut_metadata().mut_conf_state().voters = vec![1, 2, 3];
    snapshot.data = state.snapshot_data().await.unwrap();
    state.close().await.unwrap();
    let mut message = Message::default();
    message.set_msg_type(MessageType::MsgSnapshot);
    message.from = 2;
    message.to = 1;
    message.term = 2;
    message.set_snapshot(snapshot);
    message
}

/// Apply holds on corrupt data in entry 8, after entry 5 added learner 4. Raft then accepts a
/// snapshot without learner 4 and takes its ConfState. The snapshot ends the held job with
/// its prefix, and the prefix must not add learner 4 to Raft again. After the install, Raft's
/// ConfState is the one of the snapshot, and apply goes on above it. A stale adoption that
/// completes after the snapshot does not change it either.
#[tokio::test]
#[ignore = "see #149"]
async fn a_covering_snapshot_ends_a_held_job_without_its_stale_conf_changes() {
    let dir = tempfile::tempdir().unwrap();
    let source = tempfile::tempdir().unwrap();
    let mut node = held_member(dir.path()).await;
    node.drive_ready().await.unwrap();
    assert_eq!(node.state.applied, 2);
    let snapshot = covering_snapshot(source.path()).await;
    let conf = snapshot
        .get_snapshot()
        .get_metadata()
        .get_conf_state()
        .clone();
    node.raw.step(snapshot).unwrap();
    node.drive_ready().await.unwrap();
    node.drain_log().await.unwrap();
    assert_eq!(node.raw.raft.prs().conf().to_conf_state(), conf);
    // An adoption of an older base that completes now must not bring learner 4 back either.
    let mut stale = conf.clone();
    stale.learners = vec![4];
    node.finish_output(Output::Adopted(stale)).unwrap();
    assert_eq!(node.raw.raft.prs().conf().to_conf_state(), conf);
    let held = tokio::time::timeout(Duration::from_secs(5), node.worker.step())
        .await
        .expect("the snapshot did not end the held job")
        .unwrap()
        .unwrap();
    assert!(held.early);
    assert_eq!(held.applied, 7);
    node.complete_apply(held).unwrap();
    assert_eq!(node.raw.raft.prs().conf().to_conf_state(), conf);
    node.drive_ready().await.unwrap();
    node.drain_log().await.unwrap();
    let install = node.worker.step().await.unwrap().unwrap();
    node.complete_apply(install).unwrap();
    assert_eq!(node.state.applied, 200);
    assert_eq!(node.raw.raft.prs().conf().to_conf_state(), conf);
    assert_eq!(node.state.members.conf(), conf);
    assert_eq!(node.worker.control.paused(), None);
    let mut append = Message::default();
    append.set_msg_type(MessageType::MsgAppend);
    (append.from, append.to, append.term) = (2, 1, 2);
    (append.index, append.log_term, append.commit) = (200, 2, 201);
    append.entries.push(Entry {
        index: 201,
        term: 2,
        ..Default::default()
    });
    node.raw.step(append).unwrap();
    node.drive_ready().await.unwrap();
    node.drain_log().await.unwrap();
    node.drain_apply().await.unwrap();
    assert_eq!(node.state.applied, 201);
}

fn range(serializable: bool) -> RequestOp {
    RequestOp {
        request: Some(
            crate::proto::etcdserverpb::request_op::Request::RequestRange(
                crate::proto::etcdserverpb::RangeRequest {
                    key: b"key".to_vec(),
                    serializable,
                    ..Default::default()
                },
            ),
        ),
    }
}

/// Send the heartbeat reply of peer 2 that confirms the read batch of `id`.
async fn confirm_read(node: &mut Node, id: u64) {
    let mut message = Message::default();
    message.set_msg_type(MessageType::MsgHeartbeatResponse);
    (message.from, message.to, message.term) = (2, 1, node.raw.raft.term);
    message.context = node.reads.context(id);
    node.raw.step(message).unwrap();
    node.drive_ready().await.unwrap();
    node.drain_log().await.unwrap();
}

/// Issue a linearizable read and confirm its read index.
async fn linear_read(node: &mut Node) -> oneshot::Receiver<std::result::Result<Value, ApiError>> {
    let (tx, reply) = oneshot::channel();
    node.linear_read(range(false), tx);
    let id = node.reads.ids().last().unwrap();
    node.drive_ready().await.unwrap();
    confirm_read(node, id).await;
    reply
}

/// A held leader fails a linearizable read above its applied index at once, with the reason
/// of the hold. A serializable read serves the applied state during the hold. After the hold
/// ends, a linearizable read gets the new value and its revision.
#[tokio::test]
async fn a_held_member_fences_linearizable_reads() {
    let dir = tempfile::tempdir().unwrap();
    let mut node = super::super::tests::leader_node(dir.path()).await;
    let (tx, _) = oneshot::channel();
    let put = RequestOp {
        request: Some(crate::proto::etcdserverpb::request_op::Request::RequestPut(
            crate::proto::etcdserverpb::PutRequest {
                key: b"key".to_vec(),
                value: b"new".to_vec(),
                ..Default::default()
            },
        )),
    };
    let command = RaftCommand::Etcd {
        id: "put".into(),
        origin: 1,
        etcd: put,
    };
    node.propose(command, tx).unwrap();
    node.drive_ready().await.unwrap();
    node.drain_log().await.unwrap();
    let ack = Message {
        msg_type: MessageType::MsgAppendResponse as i32,
        from: 2,
        to: 1,
        term: node.raw.raft.term,
        index: node.raw.store().last_index().unwrap(),
        ..Default::default()
    };
    node.raw.step(ack).unwrap();
    node.drive_ready().await.unwrap();
    node.drain_log().await.unwrap();
    // The Put committed, but apply holds before it.
    let applied = node.state.applied;
    let revision = node.state.revision;
    assert!(node.raw.raft.raft_log.committed > applied);
    node.worker.control.set_paused(Some(apply::Paused {
        index: applied + 1,
        held: true,
        reason: "corrupt stored data: test".into(),
        since: Instant::now(),
    }));
    let (tx, serializable) = oneshot::channel();
    node.enqueue_query(Query::Read(range(true), None, tx.into()));
    node.schedule_reads();
    node.drain_reads().await.unwrap();
    let value = tokio::time::timeout(Duration::from_secs(5), serializable)
        .await
        .expect("the serializable read waited")
        .unwrap()
        .unwrap();
    let reply = &value["result"]["Ok"]["response"]["ResponseRange"];
    assert_eq!(reply["kvs"], json!([]), "{value}");
    assert_eq!(reply["header"]["revision"], revision, "{value}");
    let mut fenced = linear_read(&mut node).await;
    let error = fenced.try_recv().expect("the read waited").unwrap_err();
    assert!(
        error.error.contains("apply holds at entry"),
        "{}",
        error.error
    );
    node.worker.control.set_paused(None);
    node.drain_apply().await.unwrap();
    let reply = linear_read(&mut node).await;
    node.drain_apply().await.unwrap();
    let value = tokio::time::timeout(Duration::from_secs(5), reply)
        .await
        .expect("the read after the hold waited")
        .unwrap()
        .unwrap();
    let reply = &value["result"]["Ok"]["response"]["ResponseRange"];
    assert_eq!(reply["kvs"][0]["value"], json!(b"new"), "{value}");
    assert_eq!(reply["header"]["revision"], revision + 1, "{value}");
}

/// Raft accepts a snapshot, and a full log queue keeps it unstable. Apply then runs no other
/// work, such as a queued state read. After the log cache stages the record and stores it, the
/// install runs first, and the read sees the installed state.
#[tokio::test]
#[ignore = "see #149"]
async fn apply_waits_for_the_record_of_an_accepted_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let source = tempfile::tempdir().unwrap();
    let (events, _) = mpsc::channel(256);
    let options = super::super::tests::options(dir.path());
    let mut node = Node::open_with_transport(options, events, Some(BTreeMap::new()))
        .await
        .unwrap();
    node.raw
        .step(covering_snapshot(source.path()).await)
        .unwrap();
    assert_eq!(node.accepted_snapshot(), 200);
    assert_eq!(node.raw.store().staged_snapshot_index(), 0);
    let (tx, reply) = oneshot::channel();
    node.enqueue_query(Query::Local("absent".into(), tx));
    node.schedule_apply().unwrap();
    assert!(!node.apply.busy(), "apply ran work before the snapshot");
    node.drive_ready().await.unwrap();
    node.drain_log().await.unwrap();
    node.schedule_apply().unwrap();
    let install = node.worker.step().await.unwrap().unwrap();
    assert!(matches!(install.output, Output::Snapshot(_)));
    node.complete_apply(install).unwrap();
    assert_eq!(node.state.applied, 200);
    node.drain_apply().await.unwrap();
    node.schedule_apply().unwrap();
    node.drain_apply().await.unwrap();
    let value = reply.await.unwrap().unwrap();
    assert_eq!(value["applied_index"], 200, "{value}");
}
