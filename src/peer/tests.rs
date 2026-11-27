use super::*;
use crate::ApiError;
use serde_json::json;

async fn server() -> (
    String,
    mpsc::Receiver<Event>,
    tokio::task::JoinHandle<Result<()>>,
) {
    let (address, received, task, _) = server_on(None).await;
    (address, received, task)
}

/// A peer server of cluster `stream-test` on shared store `store`. It also returns the counter
/// of refused stores.
async fn server_on(
    store: Option<&str>,
) -> (
    String,
    mpsc::Receiver<Event>,
    tokio::task::JoinHandle<Result<()>>,
    prometheus::IntCounter,
) {
    server_with(store, None).await
}

async fn server_with(
    store: Option<&str>,
    location: Option<std::sync::Arc<crate::state::transfer::Location>>,
) -> (
    String,
    mpsc::Receiver<Event>,
    tokio::task::JoinHandle<Result<()>>,
    prometheus::IntCounter,
) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let (events, received) = mpsc::channel(256);
    let refused = prometheus::IntCounter::new("refused", "Refused peer stores.").unwrap();
    let router = crate::api::router(ApiState {
        location,
        shared: None,
        events,
        peer: Identity {
            cluster: "stream-test".into(),
            store: store.map(str::to_owned),
        },
        refusals: std::sync::Arc::new(crate::api::Refusals::new(refused.clone())),
        clock: None,
        prober: None,
        fault_controls: false,
    });
    let task = tokio::spawn(crate::tls::serve(listener, router, None));
    (address, received, task, refused)
}

fn message(snapshot: bool) -> Message {
    Message {
        from: 1,
        to: 2,
        msg_type: if snapshot {
            MessageType::MsgSnapshot as i32
        } else {
            MessageType::MsgHeartbeat as i32
        },
        ..Default::default()
    }
}

/// The next event that is not the process notice of a stream or request. A sender of this
/// binary names its process. A raw test stream, like an earlier binary, names none.
async fn next_event(events: &mut mpsc::Receiver<Event>) -> Event {
    loop {
        match tokio::time::timeout(Duration::from_secs(3), events.recv())
            .await
            .unwrap()
            .unwrap()
        {
            Event::PeerProcess { process, .. } => {
                assert!(process.is_none_or(|process| process == process_id()))
            }
            event => return event,
        }
    }
}

async fn step(events: &mut mpsc::Receiver<Event>, snapshot: bool) -> crate::node::Response {
    match next_event(events).await {
        Event::PeerFrame(message, _, response) => {
            assert_eq!(message.get_msg_type() == MessageType::MsgSnapshot, snapshot);
            response
        }
        _ => panic!("expected a peer message"),
    }
}

async fn consumed(messages: &mpsc::Sender<Message>, left: usize) {
    tokio::time::timeout(Duration::from_secs(1), async {
        while messages.capacity() != 256 - left {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn stream_window_and_snapshot_storage_ack_bound_the_sender() {
    for snapshot_success in [true, false] {
        let (address, mut received, server) = server().await;
        let (messages, queue) = mpsc::channel(256);
        let (events, mut completed) = mpsc::channel(16);
        let sender = tokio::spawn(transport(
            2,
            address,
            "stream-test".into(),
            queue,
            events,
            crate::tls::Transport::default(),
            Default::default(),
        ));
        for _ in 0..WINDOW {
            messages.send(message(false)).await.unwrap();
        }
        messages.send(message(true)).await.unwrap();
        messages.send(message(false)).await.unwrap();
        let first = step(&mut received, false).await;
        consumed(&messages, 2).await;
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(
            messages.capacity(),
            254,
            "stream exceeded its unconfirmed window"
        );
        first.send(Ok(json!({"accepted": true}))).unwrap();
        for _ in 1..WINDOW {
            step(&mut received, false)
                .await
                .send(Ok(json!({"accepted": true})))
                .unwrap();
        }
        // The snapshot waits for earlier frames. Later heartbeats flow while the follower copies.
        let mut pending = None;
        let mut heartbeat = None;
        for _ in 0..2 {
            match next_event(&mut received).await {
                Event::PeerFrame(message, _, response) => {
                    if message.get_msg_type() == MessageType::MsgSnapshot {
                        pending = Some(response);
                    } else {
                        heartbeat = Some(response);
                    }
                }
                _ => panic!("expected a peer message"),
            }
        }
        heartbeat
            .expect("a heartbeat waited for the snapshot")
            .send(Ok(json!({"accepted": true})))
            .unwrap();
        pending
            .expect("the snapshot was not sent")
            .send(if snapshot_success {
                Ok(json!({"accepted": true}))
            } else {
                Err(ApiError {
                    error: "snapshot storage failed".into(),
                    leader_id: 0,
                })
            })
            .unwrap();
        loop {
            match tokio::time::timeout(Duration::from_secs(3), completed.recv())
                .await
                .unwrap()
                .unwrap()
            {
                Event::NetworkResult {
                    snapshot: true,
                    success,
                    ..
                } => {
                    assert_eq!(success, snapshot_success);
                    break;
                }
                Event::NetworkResult {
                    snapshot: false,
                    success: true,
                    ..
                } => (),
                _ => panic!("unexpected transport failure"),
            }
        }
        drop(messages);
        tokio::time::timeout(Duration::from_secs(3), sender)
            .await
            .unwrap()
            .unwrap();
        server.abort();
        let _ = server.await;
    }
}

#[tokio::test]
async fn rejected_stream_reports_a_waiting_snapshot_and_reconnects() {
    let (address, mut received, server) = server().await;
    let (messages, queue) = mpsc::channel(256);
    let (events, mut completed) = mpsc::channel(16);
    let sender = tokio::spawn(transport(
        2,
        address,
        "stream-test".into(),
        queue,
        events,
        crate::tls::Transport::default(),
        Default::default(),
    ));
    messages.send(message(false)).await.unwrap();
    messages.send(message(true)).await.unwrap();
    let response = step(&mut received, false).await;
    consumed(&messages, 0).await;
    response
        .send(Err(ApiError {
            error: "peer partitioned".into(),
            leader_id: 0,
        }))
        .unwrap();
    for expected_snapshot in [false, true] {
        match tokio::time::timeout(Duration::from_secs(3), completed.recv())
            .await
            .unwrap()
            .unwrap()
        {
            Event::NetworkResult {
                snapshot,
                success: false,
                ..
            } => {
                assert_eq!(snapshot, expected_snapshot);
            }
            _ => panic!("failed stream or snapshot was not reported"),
        }
    }
    messages.send(message(false)).await.unwrap();
    step(&mut received, false)
        .await
        .send(Ok(json!({"accepted": true})))
        .unwrap();
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(3), completed.recv())
            .await
            .unwrap(),
        Some(Event::NetworkResult {
            snapshot: false,
            success: true,
            ..
        })
    ));
    drop(messages);
    sender.await.unwrap();
    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn stream_rejects_wrong_clusters_sequences_payloads_and_snapshots() {
    let (address, mut received, server) = server().await;
    let channel = crate::tls::Transport::default().channel(&address).unwrap();
    let mut client = TransportClient::new(channel);
    let error = client
        .exchange(Request::new(tokio_stream::empty::<Frame>()))
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::PermissionDenied);
    for frame in [
        Frame {
            sequence: 2,
            payload: message(false).encode_to_vec(),
        },
        Frame {
            sequence: 1,
            payload: vec![255],
        },
        Frame {
            sequence: 1,
            payload: message(true).encode_to_vec(),
        },
    ] {
        let mut request = Request::new(tokio_stream::iter([frame]));
        request
            .metadata_mut()
            .insert("x-cluster-id", "stream-test".parse().unwrap());
        let error = client
            .exchange(request)
            .await
            .unwrap()
            .into_inner()
            .message()
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
    }
    assert!(received.try_recv().is_err());
    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn a_stalled_receipt_times_out_and_releases_server_work() {
    let (address, mut received, server) = server().await;
    let (messages, queue) = mpsc::channel(256);
    let (events, mut completed) = mpsc::channel(16);
    let sender = tokio::spawn(transport(
        2,
        address,
        "stream-test".into(),
        queue,
        events,
        crate::tls::Transport::default(),
        Default::default(),
    ));
    messages.send(message(false)).await.unwrap();
    let held = step(&mut received, false).await;
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(4), completed.recv())
            .await
            .unwrap(),
        Some(Event::NetworkResult {
            snapshot: false,
            success: false,
            ..
        })
    ));
    tokio::time::timeout(Duration::from_secs(1), async {
        while !held.is_closed() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    messages.send(message(false)).await.unwrap();
    step(&mut received, false)
        .await
        .send(Ok(json!({"accepted": true})))
        .unwrap();
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(3), completed.recv())
            .await
            .unwrap(),
        Some(Event::NetworkResult {
            snapshot: false,
            success: true,
            ..
        })
    ));
    drop(messages);
    sender.await.unwrap();
    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn large_frames_stop_at_the_independent_byte_target() {
    let (address, mut received, server) = server().await;
    let (messages, queue) = mpsc::channel(256);
    let (events, mut completed) = mpsc::channel(16);
    let sender = tokio::spawn(transport(
        2,
        address,
        "stream-test".into(),
        queue,
        events,
        crate::tls::Transport::default(),
        Default::default(),
    ));
    let mut large = message(false);
    large.entries.push(raft::eraftpb::Entry {
        data: vec![0; 3 * 1024 * 1024],
        ..Default::default()
    });
    for _ in 0..8 {
        messages.send(large.clone()).await.unwrap();
    }
    let first = step(&mut received, false).await;
    // Six 3 MiB frames cross the target. The sender must leave the final two queued.
    consumed(&messages, 2).await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(messages.capacity(), 254);
    first.send(Ok(json!({"accepted": true}))).unwrap();
    for _ in 1..8 {
        step(&mut received, false)
            .await
            .send(Ok(json!({"accepted": true})))
            .unwrap();
    }
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(3), completed.recv())
            .await
            .unwrap(),
        Some(Event::NetworkResult {
            snapshot: false,
            success: true,
            ..
        })
    ));
    drop(messages);
    sender.await.unwrap();
    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn failed_stream_discards_its_backlog_and_rejects_a_queued_snapshot() {
    let (address, mut received, server) = server().await;
    let (messages, queue) = mpsc::channel(256);
    let (events, mut completed) = mpsc::channel(16);
    let sender = tokio::spawn(transport(
        2,
        address,
        "stream-test".into(),
        queue,
        events,
        crate::tls::Transport::default(),
        Default::default(),
    ));
    for _ in 0..WINDOW + 2 {
        messages.send(message(false)).await.unwrap();
    }
    messages.send(message(true)).await.unwrap();
    let first = step(&mut received, false).await;
    consumed(&messages, 3).await;
    first
        .send(Err(ApiError {
            error: "injected peer failure".into(),
            leader_id: 0,
        }))
        .unwrap();
    for expected_snapshot in [false, true] {
        match tokio::time::timeout(Duration::from_secs(3), completed.recv())
            .await
            .unwrap()
            .unwrap()
        {
            Event::NetworkResult {
                snapshot,
                success: false,
                ..
            } => {
                assert_eq!(snapshot, expected_snapshot);
            }
            _ => panic!("failure was not reported"),
        }
    }
    assert_eq!(messages.capacity(), 256);
    while let Ok(event) = received.try_recv() {
        let Event::PeerFrame(_, _, response) = event else {
            continue;
        };
        assert!(
            response.is_closed(),
            "failed stream kept an accepted message waiter"
        );
    }
    messages.send(message(false)).await.unwrap();
    step(&mut received, false)
        .await
        .send(Ok(json!({"accepted": true})))
        .unwrap();
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(3), completed.recv())
            .await
            .unwrap(),
        Some(Event::NetworkResult {
            snapshot: false,
            success: true,
            ..
        })
    ));
    drop(messages);
    sender.await.unwrap();
    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn receiver_pipelines_with_count_bytes_and_ordered_receipt_limits() {
    for (payload_bytes, window) in [(0, WINDOW), (3 * 1024 * 1024, 6)] {
        let (address, mut received, server) = server().await;
        let channel = crate::tls::Transport::default().channel(&address).unwrap();
        let mut client = TransportClient::new(channel).max_encoding_message_size(FRAME_LIMIT);
        let mut message = message(false);
        message.context = vec![0; payload_bytes];
        let frames: Vec<_> = (1..=window + 1)
            .map(|sequence| Frame {
                sequence: sequence as u64,
                payload: message.encode_to_vec(),
            })
            .collect();
        let mut request = Request::new(tokio_stream::iter(frames));
        request
            .metadata_mut()
            .insert("x-cluster-id", "stream-test".parse().unwrap());
        let mut receipts = client.exchange(request).await.unwrap().into_inner();
        let mut held = VecDeque::new();
        for _ in 0..window {
            held.push_back(step(&mut received, false).await);
        }
        assert!(
            tokio::time::timeout(Duration::from_millis(20), received.recv())
                .await
                .is_err()
        );
        held.pop_back().unwrap().send(Ok(json!({}))).unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(20), receipts.message())
                .await
                .is_err()
        );
        held.pop_front().unwrap().send(Ok(json!({}))).unwrap();
        assert_eq!(receipts.message().await.unwrap().unwrap().sequence, 1);
        step(&mut received, false)
            .await
            .send(Ok(json!({})))
            .unwrap();
        for response in held {
            response.send(Ok(json!({}))).unwrap();
        }
        for sequence in 2..=window + 1 {
            assert_eq!(
                receipts.message().await.unwrap().unwrap().sequence,
                sequence as u64
            );
        }
        assert!(receipts.message().await.unwrap().is_none());
        server.abort();
        let _ = server.await;
    }
}

#[tokio::test]
async fn changed_address_applies_when_the_sender_dials_again() {
    let (first_address, mut first, first_server) = server().await;
    let (address, receiver) = tokio::sync::watch::channel(first_address);
    let (messages, queue) = mpsc::channel(256);
    let (events, mut reports) = mpsc::channel(256);
    let sender = tokio::spawn(transport_to(
        2,
        receiver,
        "stream-test".into(),
        queue,
        events,
        crate::tls::Transport::default(),
        Default::default(),
    ));
    let reports = tokio::spawn(async move { while reports.recv().await.is_some() {} });
    messages.send(message(false)).await.unwrap();
    step(&mut first, false)
        .await
        .send(Ok(json!({"accepted": true})))
        .unwrap();

    // A member update to an unreachable URL keeps the working stream, as in etcd.
    address.send_replace("127.0.0.1:1".into());
    messages.send(message(false)).await.unwrap();
    step(&mut first, false)
        .await
        .send(Ok(json!({"accepted": true})))
        .unwrap();

    // After the stream fails, the sender dials the current URL.
    let (second_address, mut second, second_server) = server().await;
    address.send_replace(second_address);
    first_server.abort();
    drop(first);
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            messages.send(message(false)).await.unwrap();
            if let Ok(Some(Event::PeerFrame(_, _, response))) =
                tokio::time::timeout(Duration::from_millis(200), second.recv()).await
            {
                response.send(Ok(json!({"accepted": true}))).unwrap();
                break;
            }
        }
    })
    .await
    .expect("sender did not dial the updated URL");
    drop(messages);
    sender.await.unwrap();
    reports.abort();
    second_server.abort();
}

/// A view snapshot above its checkpoint row, as a leader sends it to `process`.
fn covered_snapshot(process: Option<u64>) -> Message {
    let mut message = message(true);
    let mut snapshot = raft::eraftpb::Snapshot::default();
    snapshot.mut_metadata().index = 9;
    snapshot.data = crate::state::transfer::ViewDescriptor::above_row(uuid::Uuid::new_v4(), 7)
        .encode()
        .unwrap();
    message.set_snapshot(snapshot);
    message.context = process.unwrap_or(0).to_le_bytes().to_vec();
    message
}

/// An earlier binary restarted on the member after the leader built a snapshot above its row.
/// Its server has only the old route, so it answers 404 before its Raft sees the snapshot. The
/// leader then treats the member as an earlier binary and reports the snapshot as failed.
#[tokio::test]
async fn an_earlier_binary_refuses_a_snapshot_above_its_row_before_raft() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let old_route = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let calls = old_route.clone();
    let router = axum::Router::new().route(
        "/raft",
        axum::routing::post(move || {
            calls.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            async { axum::http::StatusCode::NO_CONTENT }
        }),
    );
    tokio::spawn(async move { axum::serve(listener, router).await });
    let (events, mut received) = mpsc::channel(8);
    let traffic = std::sync::Arc::new(crate::metrics::PeerTraffic::default());
    let client = reqwest::Client::new();
    let url = format!("http://{address}/raft");
    spawn_snapshot(
        &client,
        &url,
        &"test".into(),
        covered_snapshot(Some(7)),
        &traffic,
        &events,
        2,
    );
    let first = tokio::time::timeout(Duration::from_secs(3), received.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        first,
        Event::SnapshotRefused {
            member: 2,
            target: 7
        }
    ));
    let second = tokio::time::timeout(Duration::from_secs(3), received.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        second,
        Event::NetworkResult {
            peer: 2,
            snapshot: true,
            success: false
        }
    ));
    assert_eq!(old_route.load(std::sync::atomic::Ordering::Relaxed), 0);
    // A snapshot at its row has no target, and it still takes the old route.
    let mut exact = covered_snapshot(None);
    exact.context.clear();
    let mut snapshot = exact.take_snapshot();
    snapshot.data = crate::state::transfer::ViewDescriptor::new(uuid::Uuid::new_v4())
        .encode()
        .unwrap();
    exact.set_snapshot(snapshot);
    let delivery = post_snapshot(&client, &url, &"test".into(), exact, &traffic).await;
    assert_eq!(delivery, Delivery::Sent);
    assert_eq!(old_route.load(std::sync::atomic::Ordering::Relaxed), 1);
}

/// This binary takes a snapshot above its row only when it names this process. A snapshot for
/// another process of the member gets 409 before Raft, and the sender falls back.
#[tokio::test]
async fn a_snapshot_above_its_row_must_name_the_receiver_process() {
    let (address, mut received, _task) = server().await;
    let client = reqwest::Client::new();
    let url = format!("http://{address}/raft");
    let traffic = crate::metrics::PeerTraffic::default();
    for target in [None, Some(process_id().wrapping_add(1))] {
        let delivery = post_snapshot(
            &client,
            &url,
            &"stream-test".into(),
            covered_snapshot(target),
            &traffic,
        )
        .await;
        assert_eq!(delivery, Delivery::Refused, "target {target:?}");
    }
    assert!(received.try_recv().is_err());
    // The named process passes the check. This test server has no shared state, so the open of
    // the checkpoint fails later with an ordinary error.
    let delivery = post_snapshot(
        &client,
        &url,
        &"stream-test".into(),
        covered_snapshot(Some(process_id())),
        &traffic,
    )
    .await;
    assert_eq!(delivery, Delivery::Failed);
}

/// A member on another bucket or prefix cannot open the checkpoints of this store. Its stream,
/// its snapshots, and its view reports stop before Raft. A peer that names no store, like a
/// replicated member, is refused too. A sender on the same store gets through.
#[tokio::test]
async fn peers_of_another_store_or_no_store_are_refused_before_raft() {
    let (address, mut received, _task, refused) = server_on(Some("store-a")).await;
    let channel = crate::tls::Transport::default().channel(&address).unwrap();
    let mut client = TransportClient::new(channel);
    for store in [None, Some("store-b")] {
        let mut request = Request::new(tokio_stream::iter([Frame {
            sequence: 1,
            payload: message(false).encode_to_vec(),
        }]));
        request
            .metadata_mut()
            .insert(CLUSTER, "stream-test".parse().unwrap());
        if let Some(store) = store {
            request.metadata_mut().insert(STORE, store.parse().unwrap());
        }
        let error = client.exchange(request).await.unwrap_err();
        assert_eq!(error.code(), tonic::Code::PermissionDenied, "{store:?}");
        assert_eq!(error.message(), "peer shared store does not match");
    }
    let http = reqwest::Client::new();
    let mut routes = 0;
    for route in ["/raft", COVERED_ROUTE, "/raft/view-ack"] {
        for store in [None, Some("store-b")] {
            let identity = Identity {
                cluster: "stream-test".into(),
                store: store.map(str::to_owned),
            };
            let request = identity.headers(http.post(format!("http://{address}{route}")));
            let request = match route {
                "/raft/view-ack" => request.json(&json!({"member": 2, "checkpoints": []})),
                _ => request
                    .header(TARGET_PROCESS, process_id().to_string())
                    .body(covered_snapshot(Some(process_id())).encode_to_vec()),
            };
            let response = request.send().await.unwrap();
            assert_eq!(response.status(), reqwest::StatusCode::FORBIDDEN, "{route}");
            routes += 1;
        }
    }
    assert_eq!(refused.get(), 2 + routes);
    assert!(received.try_recv().is_err());

    // A sender on the same store gets a receipt.
    let (messages, queue) = mpsc::channel(256);
    let (events, _completed) = mpsc::channel(16);
    let identity = Identity {
        cluster: "stream-test".into(),
        store: Some("store-a".into()),
    };
    let sender = tokio::spawn(transport(
        2,
        address,
        identity,
        queue,
        events,
        crate::tls::Transport::default(),
        Default::default(),
    ));
    messages.send(message(false)).await.unwrap();
    step(&mut received, false)
        .await
        .send(Ok(json!({})))
        .unwrap();
    assert_eq!(refused.get(), 2 + routes);
    drop(messages);
    sender.await.unwrap();
}

/// The snapshot file route serves the objects of a replicated member. A shared member serves
/// none, with or without the store ID, because its location holds the shared state.
#[tokio::test]
async fn a_shared_member_serves_no_snapshot_file() {
    use slatedb::object_store::{memory::InMemory, ObjectStoreExt};
    let objects: std::sync::Arc<dyn slatedb::object_store::ObjectStore> =
        std::sync::Arc::new(InMemory::new());
    let location = crate::state::transfer::Location::open(
        objects.clone(),
        "state",
        crate::state::transfer::Backend::Local,
        false,
        crate::state::StateCache::default(),
        None,
    )
    .await
    .unwrap();
    let root = location.root();
    let manifest = format!("manifest/{:020}.manifest", 1);
    objects
        .put(
            &format!("{root}/{manifest}").into(),
            b"manifest".to_vec().into(),
        )
        .await
        .unwrap();
    let http = reqwest::Client::new();
    for (store, sent, status) in [
        (None, None, reqwest::StatusCode::OK),
        (Some("store-a"), None, reqwest::StatusCode::NOT_FOUND),
        (
            Some("store-a"),
            Some("store-a"),
            reqwest::StatusCode::NOT_FOUND,
        ),
    ] {
        let (address, _received, _task, _) = server_with(store, Some(location.clone())).await;
        let identity = Identity {
            cluster: "stream-test".into(),
            store: sent.map(str::to_owned),
        };
        let route = crate::state::transfer::FILE_ROUTE;
        let url = format!("http://{address}{route}?root={root}&path={manifest}");
        let response = identity.headers(http.get(url)).send().await.unwrap();
        assert_eq!(response.status(), status, "{store:?} {sent:?}");
    }
}
