use super::*;
use raft::eraftpb::{HardState, Snapshot};

fn options(dir: &std::path::Path) -> NodeOptions {
    NodeOptions {
        id: 1,
        cluster_id: "worker-test".into(),
        dir: dir.to_owned(),
        peers: (1..=3).map(|id| (id, format!("127.0.0.1:{id}"))).collect(),
        snapshot_every: 0,
        log_memory: LogMemory::default(),
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
        node.drain_log().await.unwrap();
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
    node.drain_log().await.unwrap();
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
    node.drain_log().await.unwrap();
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

async fn leader_node(dir: &std::path::Path) -> Node {
    seed_log(dir, 1);
    let (events, _) = mpsc::channel(256);
    let mut node = Node::open_with_transport(options(dir), events, Some(BTreeMap::new()))
        .await
        .unwrap();
    node.raw.campaign().unwrap();
    for kind in [
        MessageType::MsgRequestPreVoteResponse,
        MessageType::MsgRequestVoteResponse,
    ] {
        let mut vote = Message::default();
        vote.set_msg_type(kind);
        vote.from = 2;
        vote.to = 1;
        vote.term = node.raw.raft.term + u64::from(kind == MessageType::MsgRequestPreVoteResponse);
        node.raw.step(vote).unwrap();
    }
    node.drive_ready().await.unwrap();
    node.drain_log().await.unwrap();
    let mut ack = Message::default();
    ack.set_msg_type(MessageType::MsgAppendResponse);
    ack.from = 2;
    ack.to = 1;
    ack.term = node.raw.raft.term;
    ack.index = node.raw.store().last_index().unwrap();
    node.raw.step(ack).unwrap();
    node.drive_ready().await.unwrap();
    node.drain_log().await.unwrap();
    node.drain_apply().await.unwrap();
    assert_eq!(node.raw.raft.state, StateRole::Leader);
    node
}

fn range_request() -> RequestOp {
    RequestOp {
        request: Some(
            crate::proto::etcdserverpb::request_op::Request::RequestRange(
                crate::proto::etcdserverpb::RangeRequest {
                    key: b"key".to_vec(),
                    ..Default::default()
                },
            ),
        ),
    }
}

async fn confirm_read(node: &mut Node, id: &[u8]) {
    let mut message = Message::default();
    message.set_msg_type(MessageType::MsgHeartbeatResponse);
    message.from = 2;
    message.to = 1;
    message.term = node.raw.raft.term;
    message.context = id.to_vec().into();
    node.raw.step(message).unwrap();
    node.drive_ready().await.unwrap();
    node.drain_log().await.unwrap();
}

#[tokio::test]
async fn read_index_requires_quorum_and_does_not_write_the_log() {
    let dir = tempfile::tempdir().unwrap();
    let mut node = leader_node(dir.path()).await;
    let last = node.raw.store().durable.last_index;
    let syncs = node.raw.store().durable.sync_writes;
    let (tx, mut reply) = oneshot::channel();
    node.linear_read(range_request(), tx);
    let id = node.reads.keys().next().unwrap().clone();
    node.drive_ready().await.unwrap();
    node.drain_log().await.unwrap();
    node.drain_apply().await.unwrap();
    assert!(
        reply.try_recv().is_err(),
        "local commit must not replace quorum confirmation"
    );
    confirm_read(&mut node, &id).await;
    node.drain_apply().await.unwrap();
    assert!(reply.await.unwrap().is_ok());
    assert_eq!(node.raw.store().durable.last_index, last);
    assert_eq!(node.raw.store().durable.sync_writes, syncs);
}

#[tokio::test]
async fn read_index_waits_for_apply_and_rejects_late_results_after_term_change() {
    let dir = tempfile::tempdir().unwrap();
    let mut node = leader_node(dir.path()).await;
    let (tx, _) = oneshot::channel();
    node.propose(
        RaftCommand::Lease {
            id: "advance".into(),
            lease: LeaseCommand::List,
        },
        tx,
    )
    .unwrap();
    node.drive_ready().await.unwrap();
    node.drain_log().await.unwrap();
    let mut ack = Message::default();
    ack.set_msg_type(MessageType::MsgAppendResponse);
    ack.from = 2;
    ack.to = 1;
    ack.term = node.raw.raft.term;
    ack.index = node.raw.store().last_index().unwrap();
    node.raw.step(ack).unwrap();
    node.drive_ready().await.unwrap();
    node.drain_log().await.unwrap();
    assert!(node.state.applied < node.raw.store().commit_index());
    let (tx, mut reply) = oneshot::channel();
    node.linear_read(range_request(), tx);
    let id = node.reads.keys().next().unwrap().clone();
    node.drive_ready().await.unwrap();
    node.drain_log().await.unwrap();
    confirm_read(&mut node, &id).await;
    assert!(reply.try_recv().is_err());
    assert!(node.queries.is_empty(), "read passed the apply barrier");
    let completed = node.worker.step().await.unwrap().unwrap();
    node.complete_apply(completed).unwrap();
    node.drive_ready().await.unwrap();
    let read_result = node.worker.step().await.unwrap().unwrap();
    node.raw.raft.become_follower(node.raw.raft.term + 1, 2);
    node.complete_apply(read_result).unwrap();
    assert!(reply.await.unwrap().is_err());
    assert!(node.reads.is_empty());
}

#[tokio::test]
async fn log_persistence_fences_votes_and_survives_restart_before_completion() {
    let dir = tempfile::tempdir().unwrap();
    let (events, _) = mpsc::channel(256);
    let (peer, mut messages) = mpsc::channel(256);
    let mut node = Node::open_with_transport(
        options(dir.path()),
        events,
        Some(BTreeMap::from([(2, peer)])),
    )
    .await
    .unwrap();
    let mut vote = Message::default();
    vote.set_msg_type(MessageType::MsgRequestVote);
    vote.from = 2;
    vote.to = 1;
    vote.term = 1;
    node.raw.step(vote).unwrap();
    node.drive_ready().await.unwrap();
    assert!(messages.try_recv().is_err(), "vote escaped before fsync");
    assert_eq!(node.raw.store().durable.hard_state.vote, 0);
    // Save the write, then crash before the owner receives its completion.
    let completion = node.log_worker.step().unwrap().unwrap();
    assert_eq!(completion.durable.hard_state.vote, 2);
    drop(node);
    let (events, _) = mpsc::channel(256);
    let node = Node::open_with_transport(options(dir.path()), events, Some(BTreeMap::new()))
        .await
        .unwrap();
    assert_eq!(node.raw.store().durable.hard_state.vote, 2);
    assert_eq!(node.raw.raft.term, 1);
}

#[tokio::test]
async fn log_failure_and_panic_stop_the_node() {
    for panic in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let (events, receiver) = mpsc::channel(256);
        let mut node = Node::open(options(dir.path()), events).await.unwrap();
        node.log_fail = !panic;
        let job = if panic {
            log::Job::Panic
        } else {
            log::Job::Ready {
                entries: vec![],
                hs: Some(HardState {
                    term: 1,
                    vote: 1,
                    ..Default::default()
                }),
                snapshot: None,
            }
        };
        node.queue_log(job, None, None, vec![], vec![]);
        node.start_log().unwrap();
        let result = tokio::time::timeout(Duration::from_secs(2), node.run(receiver))
            .await
            .unwrap();
        assert!(result.is_err());
    }
}

#[tokio::test]
async fn pending_read_indexes_are_bounded_and_cancelled_on_leader_loss() {
    let dir = tempfile::tempdir().unwrap();
    let mut node = leader_node(dir.path()).await;
    let mut replies = vec![];
    for _ in 0..apply::MAX_QUERIES {
        let (tx, rx) = oneshot::channel();
        node.linear_read(range_request(), tx);
        replies.push(rx);
    }
    let (tx, rx) = oneshot::channel();
    node.linear_read(range_request(), tx);
    assert!(rx.await.unwrap().unwrap_err().error.contains("too many"));
    node.raw.raft.become_follower(node.raw.raft.term + 1, 2);
    node.drive_ready().await.unwrap();
    assert!(node.reads.is_empty());
    for reply in replies {
        assert!(reply.await.unwrap().is_err());
    }
}

#[tokio::test]
async fn shutdown_interrupts_the_log_fault_delay() {
    let dir = tempfile::tempdir().unwrap();
    let (events, _) = mpsc::channel(256);
    let mut node = Node::open(options(dir.path()), events).await.unwrap();
    node.log_delay_ms = 5000;
    node.queue_log(
        log::Job::Ready {
            entries: vec![],
            hs: Some(HardState {
                term: 1,
                vote: 1,
                ..Default::default()
            }),
            snapshot: None,
        },
        None,
        None,
        vec![],
        vec![],
    );
    node.start_log().unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;
    drop(node);
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if let Ok(store) =
                RaftStorage::open(&dir.path().join("raft"), 1, "worker-test", vec![1, 2, 3])
            {
                assert_eq!(store.initial_state().unwrap().hard_state.vote, 0);
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn recovery_replays_a_backlog_larger_than_the_pending_quota() {
    let dir = tempfile::tempdir().unwrap();
    let mut storage =
        RaftStorage::open(&dir.path().join("raft"), 1, "worker-test", vec![1, 2, 3]).unwrap();
    let entries: Vec<_> = (1..=12)
        .map(|index| Entry {
            index,
            term: 1,
            context: vec![0; 900 * 1024].into(),
            ..Default::default()
        })
        .collect();
    storage
        .persist(
            &entries,
            Some(&HardState {
                term: 1,
                commit: 12,
                ..Default::default()
            }),
            None,
        )
        .unwrap();
    drop(storage);
    let mut config = options(dir.path());
    config.log_memory.cache_bytes = 1024;
    config.log_memory.pending_bytes = 9 * 1024 * 1024;
    let (events, _) = mpsc::channel(256);
    let node = Node::open_with_transport(config, events, Some(BTreeMap::new()))
        .await
        .unwrap();
    assert_eq!(node.recovered_from_index, 0);
    assert_eq!(node.state.applied, 12);
    assert_eq!(node.raw.store().stats()["pending_bytes"], 0);
    assert_eq!(node.raw.store().stats()["cache_bytes"], 0);
}

#[tokio::test]
async fn read_index_retries_after_the_first_commit_in_a_new_term() {
    let dir = tempfile::tempdir().unwrap();
    let (events, _) = mpsc::channel(256);
    let mut node = Node::open_with_transport(options(dir.path()), events, Some(BTreeMap::new()))
        .await
        .unwrap();
    node.raw.raft.become_candidate();
    node.raw.raft.become_leader();
    node.drive_ready().await.unwrap();
    node.drain_log().await.unwrap();
    assert!(!node.raw.raft.commit_to_current_term());
    let (tx, mut reply) = oneshot::channel();
    node.linear_read(range_request(), tx);
    let id = node.reads.keys().next().unwrap().clone();
    // The initial request is discarded before the leader commits its no-op entry.
    confirm_read(&mut node, &id).await;
    node.drain_apply().await.unwrap();
    assert!(reply.try_recv().is_err());
    let ack = Message {
        msg_type: MessageType::MsgAppendResponse,
        from: 2,
        to: 1,
        term: node.raw.raft.term,
        index: 1,
        ..Default::default()
    };
    node.raw.step(ack).unwrap();
    node.drive_ready().await.unwrap();
    node.drain_log().await.unwrap();
    node.drain_apply().await.unwrap();
    assert!(node.raw.raft.commit_to_current_term());
    let syncs = node.raw.store().durable.sync_writes;
    node.retry_reads();
    node.retry_reads();
    confirm_read(&mut node, &id).await;
    node.drain_apply().await.unwrap();
    assert!(reply.await.unwrap().is_ok());
    assert_eq!(node.raw.store().durable.last_index, 1);
    assert_eq!(node.raw.store().durable.sync_writes, syncs);
    assert!(node.reads.is_empty());
}
