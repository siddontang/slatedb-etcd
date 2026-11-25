use super::*;
use raft::eraftpb::{HardState, Snapshot};

fn options(dir: &std::path::Path) -> NodeOptions {
    NodeOptions {
        client_peers: BTreeMap::new(),
        transport: crate::tls::Transport::default(),
        require_peer_tls: false,
        state_mode: Default::default(),
        view_max_age: std::time::Duration::from_secs(10),
        view_pin_max: std::time::Duration::from_secs(300),
        overlay_max_bytes: 1 << 30,
        max_clock_skew: std::time::Duration::from_secs(5),
        view_retention: Default::default(),
        restore: None,
        object_store: crate::object::Config::Local,
        strict_reconfiguration: false,
        join: false,
        quota_bytes: 0,
        quota_warning_fraction: 0.0,
        auto_compaction: None,
        id: 1,
        cluster_id: "worker-test".into(),
        dir: dir.to_owned(),
        peers: (1..=3).map(|id| (id, format!("127.0.0.1:{id}"))).collect(),
        snapshot_every: 0,
        snapshot_min_bytes: 0,
        log_file_bytes: 256 * 1024,
        log_memory: LogMemory::default(),
        state_cache: Default::default(),
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
                        data: RaftCommand::Diagnostic(command.clone()).encode().unwrap(),
                        ..Default::default()
                    }],
                    Some(&HardState {
                        term: 1,
                        vote: 1,
                        commit: 0,
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

/// A node that only opens a data directory leaves it readable by older binaries. The first
/// hash or Watch journal row adds the row marker first.
#[tokio::test]
async fn opening_a_node_does_not_mark_the_directory() {
    let dir = tempfile::tempdir().unwrap();
    seed_log(dir.path(), 2);
    for _ in 0..2 {
        let (events, _) = mpsc::channel(256);
        let node = Node::open_with_transport(options(dir.path()), events, Some(BTreeMap::new()))
            .await
            .unwrap();
        assert!(!node.log_worker.storage().row_marker().is_marked());
        drop(node);
        let storage =
            RaftStorage::open(&dir.path().join("raft"), 1, "worker-test", vec![1, 2, 3]).unwrap();
        assert!(!storage.row_marker().is_marked());
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
        assert!(reply.await.unwrap().unwrap().into_outcome().result.is_ok());
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
        assert!(reply.await.unwrap().unwrap().into_outcome().result.is_ok());
    }
}

#[tokio::test]
async fn committed_backlog_defers_new_proposals_before_log_growth() {
    use crate::proto::etcdserverpb::{request_op::Request as Op, PutRequest, TxnRequest};
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
    let (tx, mut rx) = oneshot::channel();
    node.propose(
        RaftCommand::Lease {
            origin: 1,
            id: "overflow".into(),
            lease: LeaseCommand::List,
        },
        tx,
    )
    .unwrap();
    // A slow apply holds the write before proposal instead of rejecting it.
    let (tx, mut txn) = oneshot::channel();
    node.handle(
        Event::Kv(
            RequestOp {
                request: Some(Op::RequestTxn(TxnRequest {
                    success: vec![RequestOp {
                        request: Some(Op::RequestPut(PutRequest {
                            key: b"after-backlog".to_vec(),
                            value: b"v".to_vec(),
                            ..Default::default()
                        })),
                    }],
                    ..Default::default()
                })),
            },
            tx,
        ),
        &mut vec![],
    )
    .await
    .unwrap();
    node.drive_ready().await.unwrap();
    assert!(matches!(
        rx.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    assert!(matches!(
        txn.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    assert_eq!(node.raw.store().last_index().unwrap(), last);
    assert_eq!(node.state.applied, 0);
    assert!(node.apply_busy);
    let (tx, mut read) = oneshot::channel();
    node.enqueue_query(Query::Local("absent".into(), tx));
    // The query does not jump the batches already queued, and no new batch passes it.
    let queued = node.apply_jobs.back().unwrap().0;
    let value = loop {
        let completed = node
            .worker
            .step()
            .await
            .unwrap()
            .expect("query not scheduled");
        node.complete_apply(completed).unwrap();
        if let Ok(value) = read.try_recv() {
            break value.unwrap();
        }
        node.drive_ready().await.unwrap();
    };
    assert_eq!(value["applied_index"], queued);
    node.drive_ready().await.unwrap();
    node.drain_apply().await.unwrap();
    assert_eq!(node.state.applied, apply::MAX_APPLY_LAG);
    // Once apply catches up, both waiting writes reach the log in arrival order.
    node.drive_ready().await.unwrap();
    assert!(node.waiting_proposals.is_empty());
    assert_eq!(node.raw.raft.raft_log.last_index(), last + 2);
    assert!(node.pending.contains_key("overflow"));
}

#[tokio::test]
async fn worker_panic_stops_the_node() {
    let dir = tempfile::tempdir().unwrap();
    let (events, receiver) = mpsc::channel(256);
    let mut node = Node::open(options(dir.path()), events).await.unwrap();
    let database = node.worker.database();
    let routes = node.forwarding.subscribe();
    node.submit_apply(Job::Panic, 0).unwrap();
    let result = tokio::time::timeout(Duration::from_secs(2), node.run(receiver))
        .await
        .unwrap();
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("apply worker stopped"));
    // A stopped owner applies nothing more, so gRPC must not keep serving its last view.
    assert!(routes.local_read().is_none());
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
    snapshot.data = b"invalid snapshot".to_vec();
    node.submit_apply(Job::Snapshot(snapshot), 1).unwrap();
    let result = tokio::time::timeout(Duration::from_secs(2), node.run(receiver))
        .await
        .unwrap();
    assert!(result.is_err());
}

#[test]
fn automatic_checkpoints_rotate_across_members_without_wrapping() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = options(dir.path());
    config.peers = [
        (11, String::new()),
        (55, String::new()),
        (99, String::new()),
    ]
    .into();
    config.snapshot_every = 10;
    for (id, first) in [(11, 10), (55, 3), (99, 6)] {
        config.id = id;
        assert_eq!(config.next_checkpoint(0), Some(first));
        assert_eq!(config.next_checkpoint(first - 1), Some(first));
        assert_eq!(config.next_checkpoint(first + 1), Some(first + 10));
        assert_eq!(config.next_checkpoint(u64::MAX), None);
    }
    config.snapshot_every = 1;
    assert_eq!(config.next_checkpoint(3), Some(4));
    config.snapshot_every = 0;
    assert_eq!(config.next_checkpoint(0), None);
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
    // Two older batches wait in the apply queue when the snapshot arrives.
    let seeded = apply::APPLY_ENTRIES + 32;
    seed_log(dir.path(), seeded);
    let (events, _) = mpsc::channel(256);
    let mut node = Node::open_with_transport(options(dir.path()), events, Some(BTreeMap::new()))
        .await
        .unwrap();
    node.drive_ready().await.unwrap();
    assert_eq!(
        node.apply_jobs.iter().map(|job| job.0).collect::<Vec<_>>(),
        [apply::APPLY_ENTRIES, seeded]
    );
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
    snapshot.data = state.snapshot_data().await.unwrap();
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
    // The queued batches finish in order before the snapshot replaces the state.
    for target in [apply::APPLY_ENTRIES, seeded] {
        let old_batch = node.worker.step().await.unwrap().unwrap();
        node.complete_apply(old_batch).unwrap();
        assert_eq!(node.state.applied, target);
        node.drive_ready().await.unwrap();
    }
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
        data: RaftCommand::Diagnostic(command("after")).encode().unwrap(),
        ..Default::default()
    });
    node.raw.step(append).unwrap();
    node.drive_ready().await.unwrap();
    node.drain_log().await.unwrap();
    assert_eq!(node.raw.store().commit_index(), 201);
    assert_eq!(node.state.applied, seeded);
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

pub(super) async fn leader_node(dir: &std::path::Path) -> Node {
    leader_node_with(dir, |_| {}).await
}

/// Like `leader_node`, but `configure` can change the node options first.
pub(super) async fn leader_node_with(
    dir: &std::path::Path,
    configure: impl FnOnce(&mut NodeOptions),
) -> Node {
    seed_log(dir, 1);
    let mut config = options(dir);
    configure(&mut config);
    let (events, _) = mpsc::channel(256);
    let mut node = Node::open_with_transport(config, events, Some(BTreeMap::new()))
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

pub(super) fn range_request() -> RequestOp {
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
    message.context = node.read_context(*id);
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
                hs: Some(node.raw.store().durable.hard_state),
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
    let view = node.forwarding.subscribe();
    assert!(!view.applied(revision));
    node.wait_revision(revision, tx);
    assert!(reply.try_recv().is_err());
    assert!(node.reads.is_empty());
    // A committed result remains valid after leader loss. Local apply is still required.
    node.raw.raft.become_follower(node.raw.raft.term + 1, 0);
    node.drain_apply().await.unwrap();
    assert!(reply.await.unwrap().is_ok());
    assert_eq!(node.state.revision, revision);
    assert!(view.applied(revision));

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
async fn scans_run_together_and_rerun_alone_over_their_row_budget() {
    use crate::proto::etcdserverpb::{
        request_op::Request as Op, response_op::Response as Resp, PutRequest, RangeRequest,
    };
    let dir = tempfile::tempdir().unwrap();
    let mut node = leader_node(dir.path()).await;
    for key in 0..8u8 {
        let (tx, _) = oneshot::channel();
        node.propose(
            RaftCommand::Etcd {
                id: format!("put-{key}"),
                origin: 1,
                etcd: RequestOp {
                    request: Some(Op::RequestPut(PutRequest {
                        key: vec![b'a' + key],
                        value: vec![key; 100],
                        ..Default::default()
                    })),
                },
            },
            tx,
        )
        .unwrap();
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
    let revision = node.state.revision;
    assert_eq!(revision, 9);
    // Each row counts more than 200 bytes. Two rows fit the shared budget; eight rows do not.
    node.read_executor.scan_row_budget = 500;

    let range = |limit, sort_order| RangeRequest {
        key: b"a".to_vec(),
        range_end: b"z".to_vec(),
        limit,
        sort_order,
        serializable: true,
        ..Default::default()
    };
    // Two small scans, one large scan, a sorted (exclusive) read, a small scan, then a point read.
    let requests = [
        range(1, 0),
        range(2, 0),
        range(0, 0),
        range(0, 2),
        range(1, 0),
        RangeRequest {
            key: b"a".to_vec(),
            serializable: true,
            ..Default::default()
        },
    ];
    let mut replies = vec![];
    for request in &requests {
        let (tx, rx) = oneshot::channel();
        node.enqueue_query(Query::Read(
            RequestOp {
                request: Some(Op::RequestRange(request.clone())),
            },
            None,
            ClientResponse::Kv(tx),
        ));
        replies.push(rx);
    }
    node.drive_ready().await.unwrap();
    // Three scans share the wide slots. The sorted read and later scans wait in queue order.
    // The point read passes them and uses the remaining task.
    assert_eq!(node.read_executor.len(), read::executor::MAX_TASKS);
    assert_eq!(node.read_queries.len(), 2);
    assert!(!node.read_executor.can_submit(&node.read_queries[0]));
    node.drain_reads().await.unwrap();
    assert!(node.read_queries.is_empty());
    assert_eq!(node.read_executor.reruns, 1);
    for (request, reply) in requests.iter().zip(replies) {
        let mut result = reply.await.unwrap().unwrap().into_outcome().result.unwrap();
        assert_eq!(
            crate::mvcc::response_header(&mut result).unwrap().revision,
            revision
        );
        let Some(Resp::ResponseRange(range)) = result.response else {
            panic!("expected a range response");
        };
        let keys: Vec<_> = range.kvs.iter().map(|kv| kv.key[0]).collect();
        let (count, mut expected): (i64, Vec<u8>) = if request.range_end.is_empty() {
            (1, vec![b'a'])
        } else {
            (8, (b'a'..b'i').collect())
        };
        if request.sort_order == 2 {
            expected.reverse();
        }
        if request.limit > 0 {
            expected.truncate(request.limit as usize);
        }
        assert_eq!(range.count, count);
        assert_eq!(range.more, keys.len() < count as usize);
        assert_eq!(keys, expected);
    }
}

#[tokio::test]
async fn confirmed_scans_share_only_matching_requests_and_credentials() {
    use crate::proto::etcdserverpb::{request_op::Request as Op, RangeRequest};
    let dir = tempfile::tempdir().unwrap();
    let mut node = leader_node(dir.path()).await;
    let mut replies = Vec::new();
    for (limit, token) in [(1, "a"), (1, "a"), (1, "b"), (2, "b"), (2, "b")] {
        let request = RequestOp {
            request: Some(Op::RequestRange(RangeRequest {
                key: vec![0],
                range_end: vec![0],
                limit,
                ..Default::default()
            })),
        };
        let credentials = crate::auth::Credentials {
            token: token.into(),
            ..Default::default()
        };
        let (tx, rx) = oneshot::channel();
        node.linear_read_auth(request, Some(credentials), ClientResponse::Kv(tx));
        replies.push(rx);
    }
    let first = *node.reads.keys().next().unwrap();
    node.drive_ready().await.unwrap();
    assert_eq!(node.read_executor.len(), 0);
    confirm_read(&mut node, &first).await;
    let shared = node.read_executor.step().await.unwrap().unwrap();
    assert!(matches!(&shared, Output::SharedLinear(ids, _) if ids.len() == 2));
    node.complete_read(shared).unwrap();
    let second = replies[1].try_recv().unwrap().unwrap();
    let first = replies[0].try_recv().unwrap().unwrap();
    let (KvReply::Shared(a), KvReply::Shared(b)) = (first, second) else {
        panic!("expected one pinned scan");
    };
    assert!(std::sync::Arc::ptr_eq(&a, &b));
    node.drive_ready().await.unwrap();
    let single = node.read_executor.step().await.unwrap().unwrap();
    assert!(matches!(&single, Output::Reads(outputs) if outputs.len() == 1));
    node.complete_read(single).unwrap();
    assert!(replies[2].try_recv().unwrap().is_ok());
    node.drive_ready().await.unwrap();
    let delayed = node.read_executor.step().await.unwrap().unwrap();
    assert!(matches!(&delayed, Output::SharedLinear(ids, _) if ids.len() == 2));
    node.raw.raft.become_follower(node.raw.raft.term + 1, 2);
    node.complete_read(delayed).unwrap();
    for rx in &mut replies[3..] {
        assert!(rx.try_recv().unwrap().is_err());
    }
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
    for _ in 0..MAX_PENDING_READS {
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
            context: vec![0; 900 * 1024],
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
        msg_type: MessageType::MsgAppendResponse as i32,
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
            image: Some(
                PreparedCheckpoint::write(&node.options.dir.join("raft"), vec![0; size].into())
                    .unwrap(),
            ),
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
    let (tx, mut rx) = oneshot::channel();
    node.propose(
        RaftCommand::Lease {
            origin: 1,
            id: "full-entry-pool".into(),
            lease: LeaseCommand::List,
        },
        tx,
    )
    .unwrap();
    // A full log byte budget holds the write before proposal.
    assert!(matches!(
        rx.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    assert_eq!(node.pending.len(), 1);
    assert_eq!(node.waiting_proposals.len(), 1);
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
    seed_log(dir.path(), 2 * apply::APPLY_ENTRIES);
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
    assert!(reply.await.unwrap().unwrap().into_outcome().result.is_ok());
    assert_eq!(node.state.applied, 0);
    assert!(node.apply_busy);

    let (tx, reply) = oneshot::channel();
    node.enqueue_query(Query::Read(range_request(), None, ClientResponse::Kv(tx)));
    node.drive_ready().await.unwrap();
    let applied = node.worker.step().await.unwrap().unwrap();
    node.complete_apply(applied).unwrap();
    assert_eq!(node.state.applied, apply::APPLY_ENTRIES);
    let read = node.read_executor.step().await.unwrap().unwrap();
    node.complete_read(read).unwrap();
    let mut result = reply.await.unwrap().unwrap().into_outcome().result.unwrap();
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
        assert!(rx.await.unwrap().unwrap().into_outcome().result.is_ok());
    }
}

#[tokio::test]
async fn forwarding_uses_live_membership_and_stops_with_its_owner() {
    let dir = tempfile::tempdir().unwrap();
    let config = options(dir.path());
    let (events, mut receiver) = mpsc::channel(4);
    let service = crate::grpc::Service::new(
        1,
        events.clone(),
        &config.peers,
        crate::auth::TokenIssuer::parse("simple").unwrap(),
        config.transport.clone(),
        crate::grpc::DEFAULT_WATCH_PROGRESS_INTERVAL,
    )
    .unwrap();
    let mut node = Node::open_with_transport(config, events, Some(BTreeMap::new()))
        .await
        .unwrap();
    let (first, ()) = tokio::join!(service.peer_channel(2), async {
        let event = receiver.recv().await.unwrap();
        node.handle(event, &mut vec![]).await.unwrap();
    });
    first.unwrap();
    assert!(service.wait_revision(node.state.revision).await.is_ok());
    assert!(receiver.try_recv().is_err());
    std::sync::Arc::make_mut(&mut node.state.members)
        .members
        .remove(&2);
    node.sync_members();
    assert_eq!(
        service.peer_channel(2).await.unwrap_err().code(),
        tonic::Code::Unavailable
    );
    let mut member = node.state.members.members[&3].clone();
    member.id = 2;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    member.peer_ur_ls = vec![format!("http://{}", listener.local_addr().unwrap())];
    std::sync::Arc::make_mut(&mut node.state.members)
        .members
        .insert(2, member);
    node.sync_members();
    let mut client =
        tonic_health::pb::health_client::HealthClient::new(service.peer_channel(2).await.unwrap());
    let request = tokio::spawn(async move {
        client
            .check(tonic_health::pb::HealthCheckRequest::default())
            .await
    });
    let (connection, _) = tokio::time::timeout(Duration::from_secs(5), listener.accept())
        .await
        .unwrap()
        .unwrap();
    drop(connection);
    request.abort();
    assert!(receiver.try_recv().is_err());
    drop(receiver);
    assert_eq!(
        service
            .wait_revision(node.state.revision)
            .await
            .unwrap_err()
            .code(),
        tonic::Code::Unavailable
    );
}

#[tokio::test]
async fn applied_state_enforces_the_effective_etcd_quota() {
    for (configured, effective) in [(0, 2_u64 << 30), (-3, 0), (65536, 65536)] {
        let dir = tempfile::tempdir().unwrap();
        let (events, _) = mpsc::channel(4);
        let mut config = options(dir.path());
        config.quota_bytes = configured;
        let node = Node::open_with_transport(config, events, Some(BTreeMap::new()))
            .await
            .unwrap();
        assert_eq!(node.worker.state().quota, effective);
    }
}

async fn commit_with_peer(node: &mut Node) {
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
    node.drain_apply().await.unwrap();
    node.drive_ready().await.unwrap();
}

async fn grant_lease(node: &mut Node, id: i64, ttl: i64) {
    let (tx, reply) = oneshot::channel();
    node.propose(
        RaftCommand::Lease {
            origin: 1,
            id: format!("grant-{id}"),
            lease: LeaseCommand::Grant { id, ttl },
        },
        tx,
    )
    .unwrap();
    commit_with_peer(node).await;
    reply.await.unwrap().unwrap();
    node.sync_lease_deadlines();
}

/// Send a KeepAlive and confirm its ReadIndex proof with the peer heartbeat reply.
async fn keep_alive(node: &mut Node, id: i64) -> std::result::Result<i64, ApiError> {
    let (tx, mut reply) = oneshot::channel();
    node.keep_alive(id, None, tx).unwrap();
    node.drive_ready().await.unwrap();
    node.drain_log().await.unwrap();
    if let Ok(result) = reply.try_recv() {
        result?;
        unreachable!("a leader renewal must wait for its proof");
    }
    let context = node.renewals.batches.back().unwrap().context;
    confirm_read(node, &context).await;
    node.drain_apply().await.unwrap();
    node.drive_ready().await.unwrap();
    let value = reply.try_recv().expect("renewal did not finish")?;
    let outcome: crate::mvcc::Outcome<LeaseReply> = serde_json::from_value(value).unwrap();
    match outcome.result.unwrap() {
        LeaseReply::KeepAlive(response) => {
            assert_eq!(response.id, id);
            Ok(response.ttl)
        }
        _ => panic!("expected a KeepAlive reply"),
    }
}

#[tokio::test]
async fn leader_renews_a_lease_without_a_log_append() {
    let dir = tempfile::tempdir().unwrap();
    let mut node = leader_node(dir.path()).await;
    grant_lease(&mut node, 7, 60).await;
    let short = Instant::now() + Duration::from_secs(5);
    node.lease_deadlines.get_mut(&7).unwrap().1 = short;
    let last = node.raw.store().durable.last_index;
    let syncs = node.raw.store().durable.sync_writes;
    let generation = node.state.leases[&7].generation;

    assert_eq!(keep_alive(&mut node, 7).await.unwrap(), 60);
    assert!(node.lease_deadlines[&7].1 > short + Duration::from_secs(50));
    assert_eq!(keep_alive(&mut node, 99).await.unwrap(), 0, "unknown lease");
    assert_eq!(node.raw.store().durable.last_index, last);
    assert_eq!(node.raw.store().durable.sync_writes, syncs);
    assert_eq!(node.state.leases[&7].generation, generation);
    assert!(node.renewals.is_empty());

    // A follower lets the gRPC layer forward the request to the leader.
    node.raw.raft.become_follower(node.raw.raft.term + 1, 2);
    let (tx, reply) = oneshot::channel();
    node.keep_alive(7, None, tx).unwrap();
    assert_eq!(reply.await.unwrap().unwrap_err().error, "not leader");
}

#[tokio::test]
async fn an_expired_lease_is_not_renewed_before_its_expire_entry_applies() {
    let dir = tempfile::tempdir().unwrap();
    let mut node = leader_node(dir.path()).await;
    grant_lease(&mut node, 7, 60).await;
    grant_lease(&mut node, 8, 60).await;
    let last = node.raw.raft.raft_log.last_index();

    // A renewal before the deadline moves it, so the owner proposes no expiry.
    node.lease_deadlines.get_mut(&8).unwrap().1 = Instant::now() + Duration::from_millis(200);
    assert_eq!(keep_alive(&mut node, 8).await.unwrap(), 60);
    tokio::time::sleep(Duration::from_millis(250)).await;
    node.expire_leases().unwrap();
    assert_eq!(node.raw.raft.raft_log.last_index(), last);

    // Past the deadline, a renewal fails and keeps the pending expiry valid.
    let past = Instant::now() - Duration::from_millis(1);
    node.lease_deadlines.get_mut(&7).unwrap().1 = past;
    node.expire_leases().unwrap();
    assert_eq!(node.raw.raft.raft_log.last_index(), last + 1);
    assert_eq!(keep_alive(&mut node, 7).await.unwrap(), 0);
    assert_eq!(node.lease_deadlines[&7].1, past);
    commit_with_peer(&mut node).await;
    assert!(!node.state.leases.contains_key(&7));
    assert!(node.state.leases.contains_key(&8));
    assert!(node.lease_expiring.is_empty());
    assert_eq!(keep_alive(&mut node, 7).await.unwrap(), 0);
}

#[tokio::test]
async fn leader_change_cancels_renewals_and_restores_the_full_ttl() {
    let dir = tempfile::tempdir().unwrap();
    let mut node = leader_node(dir.path()).await;
    grant_lease(&mut node, 7, 60).await;
    node.lease_deadlines.get_mut(&7).unwrap().1 = Instant::now() + Duration::from_secs(1);

    // A proof from the old term cannot renew the lease.
    let (tx, mut reply) = oneshot::channel();
    node.keep_alive(7, None, tx).unwrap();
    node.drive_ready().await.unwrap();
    let context = node.renewals.batches.back().unwrap().context;
    let term = node.raw.raft.term;
    node.raw.raft.become_follower(term + 1, 2);
    node.raw.raft.become_candidate();
    node.raw.raft.become_leader();
    confirm_read(&mut node, &context).await;
    let error = reply.try_recv().unwrap().unwrap_err();
    assert!(error.error.contains("renewal cancelled"), "{}", error.error);
    assert!(node.renewals.is_empty());

    // The new leader does not know the old deadline. Like etcd, it grants the full TTL plus the
    // longest election timeout, so an in-memory renewal by the old leader cannot outlive it.
    node.sync_lease_deadlines();
    let extend = node.promote_extend();
    assert_eq!(extend, Duration::from_secs(2));
    let remaining = node.lease_deadlines[&7]
        .1
        .saturating_duration_since(Instant::now());
    assert!(
        remaining > Duration::from_secs(60) + extend - Duration::from_secs(1),
        "{remaining:?}"
    );

    // A lease granted later in the same term gets only its TTL.
    grant_lease(&mut node, 8, 60).await;
    let remaining = node.lease_deadlines[&8]
        .1
        .saturating_duration_since(Instant::now());
    assert!(remaining <= Duration::from_secs(60), "{remaining:?}");
}

#[tokio::test]
async fn a_renewal_with_a_stale_proof_is_cancelled() {
    let dir = tempfile::tempdir().unwrap();
    let mut node = leader_node(dir.path()).await;
    grant_lease(&mut node, 7, 60).await;
    let deadline = Instant::now() + Duration::from_secs(5);
    node.lease_deadlines.get_mut(&7).unwrap().1 = deadline;

    let (tx, mut reply) = oneshot::channel();
    node.keep_alive(7, None, tx).unwrap();
    node.drive_ready().await.unwrap();
    // Apply reaches the proof index only after the next leader could have set its deadline.
    let extend = node.promote_extend();
    let batch = node.renewals.batches.back_mut().unwrap();
    batch.issued = Instant::now() - extend - Duration::from_millis(1);
    let context = batch.context;
    confirm_read(&mut node, &context).await;
    node.drain_apply().await.unwrap();
    node.drive_ready().await.unwrap();
    let error = reply.try_recv().unwrap().unwrap_err();
    assert!(error.error.contains("renewal cancelled"), "{}", error.error);
    assert_eq!(
        node.lease_deadlines[&7].1, deadline,
        "a stale proof must not move the deadline"
    );
    assert!(node.renewals.is_empty());

    // A fresh proof renews again.
    assert_eq!(keep_alive(&mut node, 7).await.unwrap(), 60);
}

#[tokio::test]
async fn committed_batches_queue_behind_running_apply_in_log_order() {
    let dir = tempfile::tempdir().unwrap();
    let total = 3 * apply::APPLY_ENTRIES + 10;
    let conf_index = 2 * apply::APPLY_ENTRIES + 5;
    let mut store =
        RaftStorage::open(&dir.path().join("raft"), 1, "worker-test", vec![1, 2, 3]).unwrap();
    let entries: Vec<_> = (1..=total)
        .map(|index| Entry {
            index,
            term: 1,
            // An empty change applies as a no-op but still ends the chain of queued jobs.
            entry_type: if index == conf_index {
                EntryType::EntryConfChange as i32
            } else {
                EntryType::EntryNormal as i32
            },
            ..Default::default()
        })
        .collect();
    let hs = HardState {
        term: 1,
        vote: 1,
        commit: total,
    };
    store.persist(&entries, Some(&hs), None).unwrap();
    drop(store);
    let (events, _) = mpsc::channel(256);
    let mut node = Node::open_with_transport(options(dir.path()), events, Some(BTreeMap::new()))
        .await
        .unwrap();
    node.drive_ready().await.unwrap();
    // Raft delivers all committed entries. The worker gets consecutive batches up to the
    // job that holds the configuration change, without a completion between them.
    let targets: Vec<_> = node.apply_jobs.iter().map(|(target, _)| *target).collect();
    assert_eq!(
        targets,
        [
            apply::APPLY_ENTRIES,
            2 * apply::APPLY_ENTRIES,
            3 * apply::APPLY_ENTRIES
        ]
    );
    assert_eq!(node.state.applied, 0);
    // A state query waits for the queue to drain and then sees every queued batch.
    let (tx, query) = oneshot::channel();
    node.enqueue_query(Query::Local("absent".into(), tx));
    node.drive_ready().await.unwrap();
    assert_eq!(node.apply_jobs.len(), 3);
    for target in targets {
        let completed = node.worker.step().await.unwrap().unwrap();
        node.complete_apply(completed).unwrap();
        assert_eq!(node.state.applied, target);
        node.drive_ready().await.unwrap();
    }
    let completed = node.worker.step().await.unwrap().unwrap();
    node.complete_apply(completed).unwrap();
    assert_eq!(
        query.await.unwrap().unwrap()["applied_index"],
        3 * apply::APPLY_ENTRIES
    );
    node.drive_ready().await.unwrap();
    node.drain_apply().await.unwrap();
    assert_eq!(node.state.applied, total);
    assert!(node.apply_jobs.is_empty() && !node.apply_busy);
}

fn seed_puts(dir: &std::path::Path, count: u64) {
    let mut store = RaftStorage::open(&dir.join("raft"), 1, "worker-test", vec![1, 2, 3]).unwrap();
    let entries: Vec<_> = (1..=count)
        .map(|index| Entry {
            index,
            term: 1,
            data: RaftCommand::Diagnostic(Command {
                id: index.to_string(),
                request: Request {
                    operations: vec![crate::Operation::Put {
                        key: "key".into(),
                        value: index.to_string(),
                    }],
                },
            })
            .encode()
            .unwrap(),
            ..Default::default()
        })
        .collect();
    let hs = HardState {
        term: 1,
        vote: 1,
        commit: count,
    };
    store.persist(&entries, Some(&hs), None).unwrap();
}

#[tokio::test]
async fn automatic_checkpoints_wait_for_appended_log_bytes() {
    let dir = tempfile::tempdir().unwrap();
    seed_puts(dir.path(), 100);
    let (events, _) = mpsc::channel(256);
    let mut config = options(dir.path());
    config.snapshot_every = 32;
    config.snapshot_min_bytes = u64::MAX;
    let mut node = Node::open_with_transport(config, events, Some(BTreeMap::new()))
        .await
        .unwrap();
    for _ in 0..1000 {
        node.drive_ready().await.unwrap();
        node.drain_log().await.unwrap();
        node.drain_apply().await.unwrap();
        if node.state.applied == 100 {
            break;
        }
    }
    assert_eq!(node.state.applied, 100);
    // Small writes keep the log until enough bytes justify a synced SlateDB flush.
    assert_eq!(node.raw.store().snapshot_index(), 0);
    assert!(!node.checkpoint_running);
}

#[tokio::test]
async fn automatic_checkpoints_truncate_without_an_image_and_recover_after_a_crash() {
    let dir = tempfile::tempdir().unwrap();
    seed_puts(dir.path(), 100);
    let (events, _) = mpsc::channel(256);
    let mut config = options(dir.path());
    config.snapshot_every = 32;
    let mut node = Node::open_with_transport(config, events, Some(BTreeMap::new()))
        .await
        .unwrap();
    for _ in 0..1000 {
        node.drive_ready().await.unwrap();
        node.drain_log().await.unwrap();
        node.drain_apply().await.unwrap();
        if node.state.applied == 100 && node.raw.store().snapshot_index() >= 96 {
            break;
        }
    }
    let truncated = node.raw.store().snapshot_index();
    // Apply batches can pass the scheduled index before the checkpoint starts.
    assert!(truncated >= 96);
    assert!(!node.raw.store().has_image());
    assert_eq!(node.snapshot_images, 0);
    assert_eq!(node.raw.store().first_index().unwrap(), truncated + 1);
    assert!(node.state.durable_applied >= truncated);
    let raft = dir.path().join("raft");
    let files = || -> Vec<String> {
        std::fs::read_dir(&raft)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|name| name.starts_with("checkpoint-"))
            .collect()
    };
    assert!(files().is_empty());
    // Drop the node without a state flush. Flushed state covers the truncated prefix.
    drop(node);
    let storage = RaftStorage::open(&raft, 1, "worker-test", vec![1, 2, 3]).unwrap();
    assert_eq!(storage.snapshot_index(), truncated);
    assert!(storage.recovery_snapshot().data.is_empty());
    assert!(storage
        .entries(
            truncated,
            truncated + 1,
            None,
            raft::GetEntriesContext::empty(false)
        )
        .is_err());
    drop(storage);
    let (events, _) = mpsc::channel(256);
    let mut node = Node::open_with_transport(options(dir.path()), events, Some(BTreeMap::new()))
        .await
        .unwrap();
    assert!(node.recovered_from_index >= truncated);
    assert_eq!(node.installed_snapshots, 0);
    for _ in 0..100 {
        node.drive_ready().await.unwrap();
        node.drain_log().await.unwrap();
        node.drain_apply().await.unwrap();
    }
    assert_eq!(node.state.applied, 100);
    assert_eq!(
        node.worker
            .state()
            .local_value("key")
            .await
            .unwrap()
            .as_deref(),
        Some("100")
    );
    // A manual checkpoint still writes a complete image for backup and peer transfer.
    let (tx, mut reply) = oneshot::channel();
    node.handle(Event::Checkpoint(tx), &mut vec![])
        .await
        .unwrap();
    for _ in 0..100 {
        node.drive_ready().await.unwrap();
        node.drain_log().await.unwrap();
        node.drain_apply().await.unwrap();
        node.finish_checkpoint_waiters().unwrap();
        if reply.try_recv().is_ok() {
            break;
        }
    }
    assert_eq!(node.raw.store().snapshot_index(), 100);
    assert!(node.raw.store().has_image());
    assert_eq!(files(), vec![format!("checkpoint-{:016x}.data", 100)]);
}

#[tokio::test]
async fn state_behind_a_truncated_log_stops_startup() {
    let dir = tempfile::tempdir().unwrap();
    seed_log(dir.path(), 10);
    let mut storage =
        RaftStorage::open(&dir.path().join("raft"), 1, "worker-test", vec![1, 2, 3]).unwrap();
    // Model lost object storage after a truncation without an image.
    storage
        .publish_checkpoint(
            5,
            None,
            raft::eraftpb::ConfState::from((vec![1, 2, 3], vec![])),
        )
        .unwrap();
    drop(storage);
    let (events, _) = mpsc::channel(256);
    let error = Node::open_with_transport(options(dir.path()), events, Some(BTreeMap::new()))
        .await
        .err()
        .unwrap();
    assert!(error
        .to_string()
        .contains("state storage is behind the log truncation point"));
}

#[tokio::test]
async fn a_snapshot_that_raft_skips_releases_its_staging_reservation() {
    let dir = tempfile::tempdir().unwrap();
    let source = tempfile::tempdir().unwrap();
    // Apply lags a durable commit at 300. Raft skips a snapshot at 200.
    seed_log(dir.path(), 300);
    let (events, _) = mpsc::channel(256);
    let mut node = Node::open_with_transport(options(dir.path()), events, Some(BTreeMap::new()))
        .await
        .unwrap();
    node.drive_ready().await.unwrap();
    assert_eq!(node.state.applied, 0);
    let mut state = StateMachine::open(source.path()).await.unwrap();
    state.location.bind(2, "worker-test", None, None);
    for index in 1..=200 {
        state.apply(index, None).await.unwrap();
    }
    let data = state
        .checkpoint_job(true)
        .await
        .unwrap()
        .await
        .unwrap()
        .unwrap();
    let descriptor = crate::state::transfer::Descriptor::decode(&data)
        .unwrap()
        .unwrap();
    node.location.prepare(&descriptor).await.unwrap();
    let mut snapshot = Snapshot::default();
    snapshot.mut_metadata().index = 200;
    snapshot.mut_metadata().term = 1;
    snapshot.mut_metadata().mut_conf_state().voters = vec![1, 2, 3];
    snapshot.data = data;
    let mut message = Message::default();
    message.set_msg_type(MessageType::MsgSnapshot);
    message.from = 2;
    message.to = 1;
    message.term = 2;
    message.set_snapshot(snapshot);
    let (tx, _rx) = oneshot::channel();
    node.handle(Event::PeerFrame(message, 0, tx), &mut vec![])
        .await
        .unwrap();
    assert!(node.raw.raft.raft_log.unstable_snapshot().is_none());
    // No install follows, so the reservation is gone. A new reservation succeeds.
    assert_eq!(
        node.location.reserve(200, &descriptor.checkpoint),
        Some(true)
    );
    state.close().await.unwrap();
}

/// The warning threshold is the first whole byte count at or above `quota * fraction`. The
/// gauge and the log both compare usage with it.
#[test]
fn quota_warning_threshold_rounds_up() {
    let dir = tempfile::tempdir().unwrap();
    let mut options = options(dir.path());
    options.quota_bytes = 1001;
    options.quota_warning_fraction = 0.8;
    // 1001 * 0.8 is 800.8, so 800 bytes is below the threshold and 801 reaches it.
    let threshold = options.warning_bytes().unwrap();
    assert_eq!(threshold, 801);
    assert!(800 < threshold && 801 >= threshold);
    // A positive threshold below one byte still warns.
    options.quota_warning_fraction = 1e-9;
    assert_eq!(options.warning_bytes(), Some(1));
    options.quota_warning_fraction = 1.0;
    assert_eq!(options.warning_bytes(), Some(1001));
    options.quota_warning_fraction = 0.0;
    assert_eq!(options.warning_bytes(), None);
    options.quota_warning_fraction = 0.5;
    options.quota_bytes = -1;
    assert_eq!(options.warning_bytes(), None);
}
