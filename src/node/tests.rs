use super::*;
use raft::eraftpb::{HardState, Snapshot};

fn options(dir: &std::path::Path) -> NodeOptions {
    NodeOptions {
        id: 1,
        cluster_id: "worker-test".into(),
        dir: dir.to_owned(),
        peers: (1..=3).map(|id| (id, format!("127.0.0.1:{id}"))).collect(),
        snapshot_every: 0,
    }
}

fn seed_log(dir: &std::path::Path, count: u64) {
    let mut store = RaftStorage::open(&dir.join("raft"), 1, "worker-test", vec![1, 2, 3]).unwrap();
    let entries: Vec<_> = (1..=count)
        .map(|index| Entry {
            index,
            term: 1,
            ..Default::default()
        })
        .collect();
    store
        .persist(
            &entries,
            Some(&HardState {
                term: 1,
                vote: 1,
                commit: count,
                ..Default::default()
            }),
            None,
        )
        .unwrap();
    drop(store);
}

#[tokio::test]
async fn queued_state_reads_are_bounded_and_complete_without_raft_writes() {
    let dir = tempfile::tempdir().unwrap();
    let (events, _) = mpsc::channel(256);
    let mut node = Node::open_with_transport(options(dir.path()), events, Some(BTreeMap::new()))
        .await
        .unwrap();
    let mut replies = vec![];
    for _ in 0..apply::MAX_QUERIES {
        let (tx, rx) = oneshot::channel();
        node.enqueue_query(Query::Local("absent".into(), tx));
        replies.push(rx);
    }
    let (tx, rx) = oneshot::channel();
    node.enqueue_query(Query::Local("overflow".into(), tx));
    assert!(rx.await.unwrap().unwrap_err().error.contains("too many"));
    assert_eq!(node.queries.len(), apply::MAX_QUERIES);
    node.drive_ready().await.unwrap();
    assert!(node.apply_busy);
    node.drain_apply().await.unwrap();
    for reply in replies {
        let value = reply.await.unwrap().unwrap();
        assert!(value["value"].is_null());
        assert_eq!(value["applied_index"], 0);
    }
    assert_eq!(node.raw.store().last_index().unwrap(), 0);
}

#[tokio::test]
async fn committed_backlog_rejects_new_proposals_before_log_growth() {
    let dir = tempfile::tempdir().unwrap();
    seed_log(dir.path(), apply::MAX_APPLY_LAG);
    let (events, _) = mpsc::channel(256);
    let mut node = Node::open_with_transport(options(dir.path()), events, Some(BTreeMap::new()))
        .await
        .unwrap();
    node.raw.campaign().unwrap();
    let mut vote = Message::default();
    vote.set_msg_type(MessageType::MsgRequestVoteResponse);
    vote.from = 2;
    vote.to = 1;
    vote.term = node.raw.raft.term;
    // PreVote needs its own response before a real vote can elect the node.
    if node.raw.raft.state == StateRole::PreCandidate {
        vote.set_msg_type(MessageType::MsgRequestPreVoteResponse);
        vote.term += 1;
        node.raw.step(vote.clone()).unwrap();
        vote.set_msg_type(MessageType::MsgRequestVoteResponse);
        vote.term = node.raw.raft.term;
    }
    node.raw.step(vote).unwrap();
    assert_eq!(node.raw.raft.state, StateRole::Leader);
    node.drive_ready().await.unwrap();
    let last = node.raw.store().last_index().unwrap();
    let (tx, rx) = oneshot::channel();
    node.propose(
        RaftCommand::Lease {
            id: "overflow".into(),
            lease: LeaseCommand::List,
        },
        tx,
    )
    .unwrap();
    assert!(rx.await.unwrap().unwrap_err().error.contains("backlog"));
    node.drive_ready().await.unwrap();
    assert_eq!(node.raw.store().last_index().unwrap(), last);
    assert_eq!(node.state.applied, 0);
    assert!(node.apply_busy);
    let (tx, read) = oneshot::channel();
    node.enqueue_query(Query::Local("absent".into(), tx));
    let first_batch = node.worker.step().await.unwrap().unwrap();
    node.complete_apply(first_batch).unwrap();
    node.drive_ready().await.unwrap();
    let query = node.worker.step().await.unwrap().unwrap();
    node.complete_apply(query).unwrap();
    assert_eq!(
        read.await.unwrap().unwrap()["applied_index"],
        apply::BATCH_ENTRIES
    );
    node.drive_ready().await.unwrap();
    node.drain_apply().await.unwrap();
    assert_eq!(node.state.applied, apply::MAX_APPLY_LAG);
}

#[tokio::test]
async fn worker_panic_stops_the_node() {
    let dir = tempfile::tempdir().unwrap();
    let (events, receiver) = mpsc::channel(256);
    let mut node = Node::open(options(dir.path()), events).await.unwrap();
    node.submit_apply(Job::Panic, 0).unwrap();
    let result = tokio::time::timeout(Duration::from_secs(2), node.run(receiver))
        .await
        .unwrap();
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("apply worker stopped"));
}

#[tokio::test]
async fn invalid_state_snapshot_stops_the_node() {
    let dir = tempfile::tempdir().unwrap();
    let (events, receiver) = mpsc::channel(256);
    let mut node = Node::open(options(dir.path()), events).await.unwrap();
    let mut snapshot = Snapshot::default();
    snapshot.mut_metadata().index = 1;
    snapshot.data = b"invalid snapshot".to_vec().into();
    node.submit_apply(Job::Snapshot(snapshot), 1).unwrap();
    let result = tokio::time::timeout(Duration::from_secs(2), node.run(receiver))
        .await
        .unwrap();
    assert!(result.is_err());
}

#[tokio::test]
async fn future_checkpoint_and_automatic_checkpoints_do_not_stop_apply() {
    let dir = tempfile::tempdir().unwrap();
    seed_log(dir.path(), 100);
    let (events, mut receiver) = mpsc::channel(256);
    let mut config = options(dir.path());
    config.snapshot_every = 32;
    let mut node = Node::open_with_transport(config, events, Some(BTreeMap::new()))
        .await
        .unwrap();
    let (tx, mut reply) = oneshot::channel();
    node.handle(Event::Checkpoint(tx), &mut vec![])
        .await
        .unwrap();
    for _ in 0..1000 {
        node.drive_ready().await.unwrap();
        if let Some(completed) = node.worker.step().await.unwrap() {
            node.complete_apply(completed).unwrap();
        }
        while let Ok(event) = receiver.try_recv() {
            node.handle(event, &mut vec![]).await.unwrap();
        }
        node.finish_checkpoint_waiters().unwrap();
        if let Ok(result) = reply.try_recv() {
            assert_eq!(result.unwrap()["snapshot_index"], 100);
            assert_eq!(node.state.applied, 100);
            return;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    panic!("checkpoint blocked state apply");
}

#[tokio::test]
async fn received_snapshot_fences_an_older_batch_and_later_entries() {
    let dir = tempfile::tempdir().unwrap();
    let source = tempfile::tempdir().unwrap();
    seed_log(dir.path(), 100);
    let (events, _) = mpsc::channel(256);
    let mut node = Node::open_with_transport(options(dir.path()), events, Some(BTreeMap::new()))
        .await
        .unwrap();
    node.drive_ready().await.unwrap();
    assert_eq!(node.apply_target, 32);
    let mut state = StateMachine::open(source.path()).await.unwrap();
    for index in 1..200 {
        state.apply(index, None).await.unwrap();
    }
    let command = |value: &str| Command {
        id: value.into(),
        request: Request {
            operations: vec![crate::Operation::Put {
                key: "key".into(),
                value: value.into(),
            }],
        },
    };
    state.apply(200, Some(&command("snapshot"))).await.unwrap();
    let mut snapshot = Snapshot::default();
    snapshot.mut_metadata().index = 200;
    snapshot.mut_metadata().term = 2;
    snapshot.mut_metadata().mut_conf_state().voters = vec![1, 2, 3];
    snapshot.data = state.snapshot_data().await.unwrap().into();
    let mut message = Message::default();
    message.set_msg_type(MessageType::MsgSnapshot);
    message.from = 2;
    message.to = 1;
    message.term = 2;
    message.set_snapshot(snapshot);
    node.raw.step(message).unwrap();
    node.drive_ready().await.unwrap();
    assert_eq!(node.raw.store().snapshot_index(), 200);
    assert_eq!(node.state.applied, 0);
    let old_batch = node.worker.step().await.unwrap().unwrap();
    node.complete_apply(old_batch).unwrap();
    node.drive_ready().await.unwrap();
    assert_eq!(node.apply_target, 200);
    let mut append = Message::default();
    append.set_msg_type(MessageType::MsgAppend);
    append.from = 2;
    append.to = 1;
    append.term = 2;
    append.index = 200;
    append.log_term = 2;
    append.commit = 201;
    append.entries.push(Entry {
        index: 201,
        term: 2,
        data: serde_json::to_vec(&RaftCommand::Legacy(command("after")))
            .unwrap()
            .into(),
        ..Default::default()
    });
    node.raw.step(append).unwrap();
    node.drive_ready().await.unwrap();
    assert_eq!(node.raw.store().commit_index(), 201);
    assert_eq!(node.state.applied, 32);
    let install = node.worker.step().await.unwrap().unwrap();
    node.complete_apply(install).unwrap();
    assert_eq!(node.state.applied, 200);
    assert_eq!(
        node.worker
            .state()
            .local_value("key")
            .await
            .unwrap()
            .as_deref(),
        Some("snapshot")
    );
    node.drive_ready().await.unwrap();
    node.drain_apply().await.unwrap();
    assert_eq!(node.state.applied, 201);
    assert_eq!(node.installed_snapshots, 1);
    assert_eq!(
        node.worker
            .state()
            .local_value("key")
            .await
            .unwrap()
            .as_deref(),
        Some("after")
    );
}

#[tokio::test]
async fn dropping_the_node_cancels_an_active_worker() {
    let dir = tempfile::tempdir().unwrap();
    let (events, _) = mpsc::channel(256);
    let mut node = Node::open(options(dir.path()), events).await.unwrap();
    node.apply_delay_ms = 5000;
    node.submit_apply(Job::Snapshot(Snapshot::default()), 0)
        .unwrap();
    let task = node.worker.abort_handle();
    drop(node);
    tokio::time::timeout(Duration::from_secs(1), async {
        while !task.is_finished() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}
