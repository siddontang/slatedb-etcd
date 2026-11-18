use super::*;
use raft::eraftpb::{HardState, Snapshot};

fn options(dir: &std::path::Path) -> NodeOptions {
    NodeOptions {
        client_peers: BTreeMap::new(),
        transport: crate::tls::Transport::default(),
        require_peer_tls: false,
        object_store: crate::object::Config::Local,
        strict_reconfiguration: false,
        join: false,
        quota_bytes: 0,
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
async fn flushed_state_recovers_a_lost_commit_marker_but_not_missing_entries() {
    for has_entry in [true, false] {
        let dir = tempfile::tempdir().unwrap();
        let command = Command {
            id: "committed".into(),
            request: Request {
                operations: vec![crate::Operation::Put {
                    key: "key".into(),
                    value: "committed".into(),
                }],
            },
        };
        let mut storage =
            RaftStorage::open(&dir.path().join("raft"), 1, "worker-test", vec![1, 2, 3]).unwrap();
        if has_entry {
            storage
                .persist(
                    &[Entry {
                        index: 1,
                        term: 1,
                        data: RaftCommand::Diagnostic(command.clone())
                            .encode()
                            .unwrap()
                            .into(),
                        ..Default::default()
                    }],
                    Some(&HardState {
                        term: 1,
                        vote: 1,
                        commit: 0,
                        ..Default::default()
                    }),
                    None,
                )
                .unwrap();
        }
        // Model a lost commit-only WAL update after committed state reached object storage.
        let mut state = StateMachine::open(&dir.path().join("objects"))
            .await
            .unwrap();
        state.apply(1, Some(&command)).await.unwrap();
        state.snapshot_data().await.unwrap();
        state.close().await.unwrap();
        drop(state);
        drop(storage);
        let (events, _) = mpsc::channel(256);
        let result =
            Node::open_with_transport(options(dir.path()), events, Some(BTreeMap::new())).await;
        if has_entry {
            let node = result.unwrap();
            assert_eq!(node.state.applied, 1);
            assert_eq!(node.raw.store().commit_index(), 1);
            assert_eq!(node.log_worker.storage().commit_index(), 1);
            assert_eq!(
                node.worker
                    .state()
                    .local_value("key")
                    .await
                    .unwrap()
                    .as_deref(),
                Some("committed")
            );
        } else {
            assert!(result
                .err()
                .unwrap()
                .to_string()
                .contains("commit exceeds durable log"));
        }
    }
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
async fn point_read_batches_stop_before_scans_and_historical_reads() {
    use crate::proto::etcdserverpb::request_op::Request as Op;
    use crate::proto::etcdserverpb::RangeRequest;

    let dir = tempfile::tempdir().unwrap();
    let (events, _) = mpsc::channel(256);
    let mut node = Node::open_with_transport(options(dir.path()), events, Some(BTreeMap::new()))
        .await
        .unwrap();
    let mut replies = Vec::new();
    for index in 0..apply::READ_BATCH + 7 {
        let (tx, rx) = oneshot::channel();
        node.enqueue_query(Query::Read(
            RequestOp {
                request: Some(Op::RequestRange(RangeRequest {
                    key: b"a".to_vec(),
                    range_end: if index == apply::READ_BATCH + 4 {
                        b"z".to_vec()
                    } else {
                        vec![]
                    },
                    serializable: true,
                    revision: i64::from(index == apply::READ_BATCH + 6),
                    ..Default::default()
                })),
            },
            None,
            ClientResponse::Kv(tx),
        ));
        replies.push(rx);
    }
    for count in [apply::READ_BATCH, 4, 1, 1, 1] {
        node.drive_ready().await.unwrap();
        let completion = node.read_executor.step().await.unwrap().unwrap();
        match &completion {
            Output::Reads(reads) => assert_eq!(reads.len(), count),
            Output::Read { .. } => assert_eq!(count, 1),
            _ => panic!("expected point reads or the range boundary"),
        }
        node.complete_read(completion).unwrap();
    }
    assert!(node.read_queries.is_empty());
    for reply in replies {
        assert!(reply.await.unwrap().unwrap().result.is_ok());
    }
    assert_eq!(node.raw.store().last_index().unwrap(), 0);
}

#[tokio::test]
async fn waiting_scans_do_not_block_current_point_reads() {
    use crate::proto::etcdserverpb::request_op::Request as Op;
    use crate::proto::etcdserverpb::RangeRequest;

    let dir = tempfile::tempdir().unwrap();
    let (events, _) = mpsc::channel(256);
    let mut node = Node::open_with_transport(options(dir.path()), events, Some(BTreeMap::new()))
        .await
        .unwrap();
    let mut replies = vec![];
    for revision in [1, 1, 0] {
        let (tx, rx) = oneshot::channel();
        node.enqueue_query(Query::Read(
            RequestOp {
                request: Some(Op::RequestRange(RangeRequest {
                    key: b"key".to_vec(),
                    revision,
                    serializable: true,
                    ..Default::default()
                })),
            },
            None,
            ClientResponse::Kv(tx),
        ));
        replies.push(rx);
    }
    node.drive_ready().await.unwrap();
    assert_eq!(node.read_executor.len(), 2);
    assert_eq!(node.read_queries.len(), 1);
    assert!(!node.read_queries[0].is_point_read());
    node.drain_reads().await.unwrap();
    assert!(node.read_queries.is_empty());
    for reply in replies {
        assert!(reply.await.unwrap().unwrap().result.is_ok());
    }
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
            origin: 1,
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
    let database = node.worker.database();
    node.submit_apply(Job::Panic, 0).unwrap();
    let result = tokio::time::timeout(Duration::from_secs(2), node.run(receiver))
        .await
        .unwrap();
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("apply worker stopped"));
    assert!(
        database.get(b"unused").await.is_err(),
        "state storage stayed open"
    );
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
        data: RaftCommand::Diagnostic(command("after"))
            .encode()
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

async fn confirm_read(node: &mut Node, id: &u64) {
    let mut message = Message::default();
    message.set_msg_type(MessageType::MsgHeartbeatResponse);
    message.from = 2;
    message.to = 1;
    message.term = node.raw.raft.term;
    message.context = node.read_context(*id).into();
    node.raw.step(message).unwrap();
    node.drive_ready().await.unwrap();
    node.drain_log().await.unwrap();
}

#[tokio::test]
async fn read_batches_freeze_before_quorum_and_survive_a_cancelled_first_request() {
    let dir = tempfile::tempdir().unwrap();
    let mut node = leader_node(dir.path()).await;
    let (sender, mut messages) = mpsc::channel(256);
    node.senders.insert(2, sender);
    let syncs = node.raw.store().durable.sync_writes;
    let (first, cancelled) = oneshot::channel();
    let (second, mut reply) = oneshot::channel();
    node.linear_read(range_request(), first);
    node.linear_read(range_request(), second);
    let context = *node.reads.keys().next().unwrap();
    node.drive_ready().await.unwrap();
    assert!(node.log_worker.step().unwrap().is_none());
    let mut contexts = vec![];
    while let Ok(message) = messages.try_recv() {
        if message.get_msg_type() == MessageType::MsgHeartbeat && !message.context.is_empty() {
            contexts.push(message.context.to_vec());
        }
    }
    assert_eq!(contexts, vec![node.read_context(context)]);
    drop(cancelled);
    let (later, mut late_reply) = oneshot::channel();
    node.linear_read(range_request(), later);
    let late_context = *node.reads.keys().last().unwrap();
    node.drive_ready().await.unwrap();
    assert!(reply.try_recv().is_err());
    confirm_read(&mut node, &context).await;
    node.drain_apply().await.unwrap();
    assert!(reply.await.unwrap().is_ok());
    assert!(late_reply.try_recv().is_err());
    confirm_read(&mut node, &context).await;
    node.drain_apply().await.unwrap();
    assert!(
        late_reply.try_recv().is_err(),
        "an old proof released a later read"
    );
    confirm_read(&mut node, &late_context).await;
    node.drain_apply().await.unwrap();
    assert!(late_reply.await.unwrap().is_ok());
    assert_eq!(node.raw.store().durable.sync_writes, syncs);
    assert!(node.log_worker.step().unwrap().is_none());
}

#[tokio::test]
async fn empty_log_barriers_wait_for_prior_persistence_and_fail_closed() {
    for fail in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let mut node = leader_node(dir.path()).await;
        node.queue_log(
            log::Job::Ready {
                entries: vec![],
                hs: Some(node.raw.store().durable.hard_state.clone()),
                snapshot: None,
            },
            None,
            None,
            vec![],
            vec![],
        );
        node.log_fail = fail;
        let (tx, mut rx) = oneshot::channel();
        node.queue_log(
            log::Job::Ready {
                entries: vec![],
                hs: None,
                snapshot: None,
            },
            None,
            None,
            vec![],
            vec![],
        );
        node.log_queue.back_mut().unwrap().responses.push(tx);
        node.start_log().unwrap();
        assert!(rx.try_recv().is_err());
        let completed = node.log_worker.step();
        if fail {
            assert!(completed.is_err());
            assert!(rx.try_recv().is_err());
            drop(node);
            assert!(rx.await.is_err());
        } else {
            node.complete_log(completed.unwrap().unwrap()).unwrap();
            assert!(rx.await.unwrap().is_ok());
            assert!(node.log_queue.is_empty());
            assert!(node.log_worker.step().unwrap().is_none());
        }
    }
}

#[tokio::test]
async fn forwarded_revision_waits_for_apply_without_a_new_quorum_round() {
    let dir = tempfile::tempdir().unwrap();
    let mut node = leader_node(dir.path()).await;
    let revision = node.state.revision + 1;
    let request = RequestOp {
        request: Some(crate::proto::etcdserverpb::request_op::Request::RequestPut(
            crate::proto::etcdserverpb::PutRequest {
                key: b"key".to_vec(),
                value: b"value".to_vec(),
                ..Default::default()
            },
        )),
    };
    let (tx, _) = oneshot::channel();
    node.propose(
        RaftCommand::Etcd {
            id: "forwarded".into(),
            origin: 2,
            etcd: request,
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
    let (tx, mut reply) = oneshot::channel();
    node.wait_revision(revision, tx);
    assert!(reply.try_recv().is_err());
    assert!(node.reads.is_empty());
    // A committed result remains valid after leader loss. Local apply is still required.
    node.raw.raft.become_follower(node.raw.raft.term + 1, 0);
    node.drain_apply().await.unwrap();
    assert!(reply.await.unwrap().is_ok());
    assert_eq!(node.state.revision, revision);

    let mut pending = vec![];
    for _ in 0..apply::MAX_QUERIES {
        let (tx, rx) = oneshot::channel();
        node.wait_revision(revision + 1, tx);
        pending.push(rx);
    }
    let (tx, rx) = oneshot::channel();
    node.wait_revision(revision + 1, tx);
    assert!(rx.await.unwrap().unwrap_err().error.contains("too many"));
    pending.pop();
    node.finish_revision_waiters();
    assert_eq!(node.revision_waiters.len(), apply::MAX_QUERIES - 1);
    for waiter in &mut node.revision_waiters {
        waiter.started = Instant::now() - Duration::from_secs(6);
    }
    node.finish_revision_waiters();
    assert!(node.revision_waiters.is_empty());
    for reply in pending {
        assert!(reply
            .await
            .unwrap()
            .unwrap_err()
            .error
            .contains("timed out"));
    }
}

#[tokio::test]
async fn read_index_requires_quorum_and_does_not_write_the_log() {
    let dir = tempfile::tempdir().unwrap();
    let mut node = leader_node(dir.path()).await;
    let last = node.raw.store().durable.last_index;
    let syncs = node.raw.store().durable.sync_writes;
    let (tx, mut reply) = oneshot::channel();
    node.linear_read(range_request(), tx);
    let id = *node.reads.keys().next().unwrap();
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
            origin: 1,
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
    let id = *node.reads.keys().next().unwrap();
    node.drive_ready().await.unwrap();
    node.drain_log().await.unwrap();
    confirm_read(&mut node, &id).await;
    assert!(reply.try_recv().is_err());
    assert!(node.queries.is_empty(), "read passed the apply barrier");
    let completed = node.worker.step().await.unwrap().unwrap();
    node.complete_apply(completed).unwrap();
    node.drive_ready().await.unwrap();
    let read_result = node.read_executor.step().await.unwrap().unwrap();
    node.raw.raft.become_follower(node.raw.raft.term + 1, 2);
    node.complete_read(read_result).unwrap();
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
    let id = *node.reads.keys().next().unwrap();
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

#[tokio::test]
async fn apply_waits_for_raft_delivery_when_durable_commit_is_ahead() {
    let dir = tempfile::tempdir().unwrap();
    seed_log(dir.path(), 8);
    let (events, _) = mpsc::channel(256);
    let mut node = Node::open_with_transport(options(dir.path()), events, Some(BTreeMap::new()))
        .await
        .unwrap();
    assert_eq!(node.raw.store().commit_index(), 8);
    node.schedule_apply().unwrap();
    assert!(
        !node.apply_busy,
        "disk commit alone must not schedule apply"
    );
    node.drive_ready().await.unwrap();
    node.drain_log().await.unwrap();
    node.drain_apply().await.unwrap();
    assert_eq!(node.state.applied, 8);
    assert_eq!(node.raw.raft.raft_log.applied, 8);
}

#[tokio::test]
async fn log_statistics_are_explicit_bounded_and_leave_status_responsive() {
    let dir = tempfile::tempdir().unwrap();
    let mut node = leader_node(dir.path()).await;
    let expected = serde_json::to_value(node.log_worker.storage().log_stats().unwrap()).unwrap();
    let mut replies = vec![];
    for _ in 0..log::MAX_JOBS {
        let (tx, rx) = oneshot::channel();
        node.handle(Event::LogStats(tx), &mut vec![]).await.unwrap();
        replies.push(rx);
    }
    let (tx, rx) = oneshot::channel();
    node.handle(Event::LogStats(tx), &mut vec![]).await.unwrap();
    assert!(rx.await.unwrap().unwrap_err().error.contains("too many"));
    node.drive_ready().await.unwrap();
    let (tx, rx) = oneshot::channel();
    node.handle(Event::Status(tx), &mut vec![]).await.unwrap();
    assert!(rx.await.unwrap().is_ok());
    assert!(replies[0].try_recv().is_err());
    let writes = node.raw.store().durable.sync_writes;
    node.drain_log().await.unwrap();
    for reply in replies {
        assert_eq!(reply.await.unwrap().unwrap(), expected);
    }
    assert_eq!(node.raw.store().durable.sync_writes, writes);
}

#[tokio::test]
async fn checkpoint_memory_does_not_use_the_entry_admission_budget() {
    let dir = tempfile::tempdir().unwrap();
    let mut node = leader_node(dir.path()).await;
    let size = log::MAX_BYTES + 1;
    node.queue_log(
        log::Job::Checkpoint {
            conf: node.state.members.conf(),
            index: node.state.applied,
            data: vec![0; size],
        },
        None,
        None,
        vec![],
        vec![],
    );
    assert_eq!(node.log_bytes, 0);
    assert_eq!(node.log_snapshot_bytes, size);
    let (tx, mut rx) = oneshot::channel();
    node.propose(
        RaftCommand::Lease {
            origin: 1,
            id: "during-checkpoint".into(),
            lease: LeaseCommand::List,
        },
        tx,
    )
    .unwrap();
    assert!(matches!(
        rx.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    assert_eq!(node.pending.len(), 1);
    node.log_bytes = log::MAX_BYTES;
    let (tx, rx) = oneshot::channel();
    node.propose(
        RaftCommand::Lease {
            origin: 1,
            id: "full-entry-pool".into(),
            lease: LeaseCommand::List,
        },
        tx,
    )
    .unwrap();
    assert!(rx.await.unwrap().unwrap_err().error.contains("backlog"));
}

#[tokio::test]
async fn a_full_log_queue_cannot_let_apply_pass_the_ready_cursor() {
    let dir = tempfile::tempdir().unwrap();
    seed_log(dir.path(), 128);
    let (events, _) = mpsc::channel(256);
    let mut node = Node::open_with_transport(options(dir.path()), events, Some(BTreeMap::new()))
        .await
        .unwrap();
    node.raw.raft.set_max_committed_size_per_ready(1);
    for _ in 0..log::MAX_JOBS - 1 {
        node.queue_log(log::Job::Stats, None, None, vec![], vec![]);
    }
    node.drive_ready().await.unwrap();
    assert_eq!(node.committed_ready, 1);
    assert_eq!(
        node.apply_target, 1,
        "apply passed the bounded Ready delivery"
    );
    let completed = node.worker.step().await.unwrap().unwrap();
    node.complete_apply(completed).unwrap();
    node.drain_log().await.unwrap();
    node.drain_apply().await.unwrap();
    assert_eq!(node.state.applied, 128);
}

#[tokio::test]
async fn queued_ready_jobs_share_sync_and_keep_the_final_log_suffix() {
    let dir = tempfile::tempdir().unwrap();
    let mut node = leader_node(dir.path()).await;
    let first = node.raw.store().last_index().unwrap() + 1;
    let entry = |index, term| Entry {
        index,
        term,
        ..Default::default()
    };
    let mut replies = vec![];
    for entries in [
        vec![entry(first, 1), entry(first + 1, 1)],
        vec![entry(first + 1, 2), entry(first + 2, 2)],
    ] {
        let term = entries.last().unwrap().term;
        node.queue_log(
            log::Job::Ready {
                entries,
                hs: Some(HardState {
                    term,
                    vote: term,
                    commit: first - 1,
                    ..Default::default()
                }),
                snapshot: None,
            },
            None,
            None,
            vec![],
            vec![],
        );
        let (tx, rx) = oneshot::channel();
        node.log_queue.back_mut().unwrap().responses.push(tx);
        replies.push(rx);
    }
    // A diagnostic job is a batch boundary.
    node.queue_log(log::Job::Stats, None, None, vec![], vec![]);
    let before = node.log_worker.storage().sync_writes;
    let charged = node.log_bytes;
    node.start_log().unwrap();
    assert_eq!(node.log_queue.len(), 2);
    assert_eq!(node.log_bytes, charged);
    assert!(replies.iter_mut().all(|reply| reply.try_recv().is_err()));
    let completed = node.log_worker.step().unwrap().unwrap();
    let storage = node.log_worker.storage();
    assert_eq!(storage.sync_writes, before + 1);
    assert_eq!(storage.initial_state().unwrap().hard_state.term, 2);
    assert_eq!(storage.initial_state().unwrap().hard_state.vote, 2);
    assert_eq!(
        storage
            .entries(
                first,
                first + 3,
                None,
                raft::GetEntriesContext::empty(false)
            )
            .unwrap(),
        vec![entry(first, 1), entry(first + 1, 2), entry(first + 2, 2)]
    );
    node.complete_log(completed).unwrap();
    for reply in replies {
        assert!(reply.await.unwrap().is_ok());
    }
    assert_eq!(node.log_bytes, 0);
    node.drain_log().await.unwrap();
    assert_eq!(node.log_worker.storage().sync_writes, before + 1);
    drop(node);
    let reopened =
        RaftStorage::open(&dir.path().join("raft"), 1, "worker-test", vec![1, 2, 3]).unwrap();
    assert_eq!(reopened.initial_state().unwrap().hard_state.term, 2);
    assert_eq!(reopened.initial_state().unwrap().hard_state.vote, 2);
    assert_eq!(reopened.term(first + 2).unwrap(), 2);
}

#[tokio::test]
async fn maintenance_flushes_update_durable_apply_metadata() {
    for command in [
        crate::maintenance::Command::Snapshot,
        crate::maintenance::Command::Defragment,
    ] {
        let dir = tempfile::tempdir().unwrap();
        seed_log(dir.path(), 1);
        let (events, _) = mpsc::channel(256);
        let mut node =
            Node::open_with_transport(options(dir.path()), events, Some(BTreeMap::new()))
                .await
                .unwrap();
        node.drive_ready().await.unwrap();
        node.drain_apply().await.unwrap();
        assert_eq!(node.state.applied, 1);
        assert_eq!(node.state.durable_applied, 0);
        let (response, result) = oneshot::channel();
        node.enqueue_query(Query::Maintenance(
            command,
            crate::auth::Credentials::default(),
            Box::default(),
            response,
        ));
        node.drive_ready().await.unwrap();
        let completed = node.worker.step().await.unwrap().unwrap();
        assert_eq!(completed.state.as_ref().unwrap().durable_applied, 1);
        node.complete_apply(completed).unwrap();
        result.await.unwrap().unwrap();
        assert_eq!(node.state.durable_applied, 1);
    }
}

#[tokio::test]
async fn read_contexts_require_the_current_process_nonce_and_exact_length() {
    let dir = tempfile::tempdir().unwrap();
    let mut node = leader_node(dir.path()).await;
    let (response, mut result) = oneshot::channel();
    node.linear_read(range_request(), response);
    let id = *node.reads.keys().next().unwrap();
    node.drive_ready().await.unwrap();
    let valid = node.read_context(id);
    let mut stale = valid.clone();
    stale[0] ^= 1;
    for context in [
        stale,
        valid[..23].to_vec(),
        [valid.as_slice(), &[0]].concat(),
    ] {
        node.confirm_reads(vec![raft::ReadState {
            index: node.state.applied,
            request_ctx: context,
        }]);
        node.schedule_reads();
        assert!(node.read_queries.is_empty());
        assert_eq!(node.read_executor.len(), 0);
        assert!(result.try_recv().is_err());
    }
    node.confirm_reads(vec![raft::ReadState {
        index: node.state.applied,
        request_ctx: valid,
    }]);
    node.schedule_reads();
    assert_eq!(node.read_executor.len(), 1);
}

#[tokio::test]
async fn independent_reads_pin_their_view_and_do_not_wait_for_apply() {
    let dir = tempfile::tempdir().unwrap();
    seed_log(dir.path(), 64);
    let (events, _) = mpsc::channel(256);
    let mut node = Node::open_with_transport(options(dir.path()), events, Some(BTreeMap::new()))
        .await
        .unwrap();
    node.drive_ready().await.unwrap();
    assert!(node.apply_busy);
    let (tx, reply) = oneshot::channel();
    node.enqueue_query(Query::Read(range_request(), None, ClientResponse::Kv(tx)));
    node.drive_ready().await.unwrap();
    let read = node.read_executor.step().await.unwrap().unwrap();
    node.complete_read(read).unwrap();
    assert!(reply.await.unwrap().unwrap().result.is_ok());
    assert_eq!(node.state.applied, 0);
    assert!(node.apply_busy);

    let (tx, reply) = oneshot::channel();
    node.enqueue_query(Query::Read(range_request(), None, ClientResponse::Kv(tx)));
    node.drive_ready().await.unwrap();
    let applied = node.worker.step().await.unwrap().unwrap();
    node.complete_apply(applied).unwrap();
    assert_eq!(node.state.applied, apply::BATCH_ENTRIES);
    let read = node.read_executor.step().await.unwrap().unwrap();
    node.complete_read(read).unwrap();
    let mut result = reply.await.unwrap().unwrap().result.unwrap();
    assert_eq!(
        crate::mvcc::response_header(&mut result).unwrap().revision,
        1
    );
}

#[tokio::test]
async fn independent_read_jobs_and_queue_have_separate_bounds() {
    let dir = tempfile::tempdir().unwrap();
    let (events, _) = mpsc::channel(256);
    let mut node = Node::open_with_transport(options(dir.path()), events, Some(BTreeMap::new()))
        .await
        .unwrap();
    let mut replies = vec![];
    for _ in 0..2 {
        for _ in 0..apply::MAX_QUERIES {
            let (tx, rx) = oneshot::channel();
            node.enqueue_query(Query::Read(range_request(), None, ClientResponse::Kv(tx)));
            replies.push(rx);
        }
        node.drive_ready().await.unwrap();
    }
    assert_eq!(node.read_executor.len(), 4);
    assert_eq!(node.read_queries.len(), apply::MAX_QUERIES);
    let (tx, rx) = oneshot::channel();
    node.enqueue_query(Query::Read(range_request(), None, ClientResponse::Kv(tx)));
    assert!(rx.await.unwrap().is_err());
    assert!(!node.apply_busy);
    node.drain_reads().await.unwrap();
    for rx in replies {
        assert!(rx.await.unwrap().unwrap().result.is_ok());
    }
}
