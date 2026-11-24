//! Single-voter shared state mode: compare it with replicated mode, and check the publisher,
//! restart, and mode checks. Shared mode uses the local object store here.

use std::sync::Arc;
use std::time::Duration;

use slatedb_etcd::proto::etcdserverpb::{
    compare, kv_client::KvClient, lease_client::LeaseClient, maintenance_client::MaintenanceClient,
    request_op, watch_client::WatchClient, watch_request, CompactionRequest, Compare,
    DeleteRangeRequest, HashKvRequest, LeaseGrantRequest, LeaseRevokeRequest, PutRequest,
    RangeRequest, RequestOp, TxnRequest, WatchCreateRequest, WatchRequest,
};
use slatedb_etcd::server::{Server, ServerConfig};
use slatedb_etcd::state::StateMode;
use tonic::transport::{Channel, Endpoint};

fn config(dir: &std::path::Path, mode: StateMode, view_max_age: Duration) -> ServerConfig {
    let mut config = ServerConfig::single_node(dir);
    config.state_mode = mode;
    config.view_max_age = view_max_age;
    config
}

async fn start(config: ServerConfig) -> (Server, Channel) {
    let server = Server::start(config).await.unwrap();
    server.wait_ready(Duration::from_secs(15)).await.unwrap();
    let channel = Endpoint::from_shared(server.client_url())
        .unwrap()
        .connect()
        .await
        .unwrap();
    (server, channel)
}

fn put(key: &str, value: &str) -> PutRequest {
    PutRequest {
        key: key.as_bytes().to_vec(),
        value: value.as_bytes().to_vec(),
        prev_kv: true,
        ..Default::default()
    }
}

fn all() -> RangeRequest {
    RangeRequest {
        key: vec![0],
        range_end: vec![0],
        ..Default::default()
    }
}

/// Run a fixed workload and return each result without member-specific headers.
async fn workload(channel: Channel) -> Vec<String> {
    let mut kv = KvClient::new(channel.clone());
    let mut lease = LeaseClient::new(channel.clone());
    let mut maintenance = MaintenanceClient::new(channel.clone());
    let mut results = vec![];
    macro_rules! record {
        ($call:expr) => {{
            let mut reply = $call.await.unwrap().into_inner();
            reply.header = None;
            results.push(format!("{reply:?}"));
        }};
    }
    for n in 0..24 {
        record!(kv.put(put(&format!("k{}", n % 7), &format!("v{n}"))));
    }
    let txn = kv
        .txn(TxnRequest {
            compare: vec![Compare {
                key: b"k1".to_vec(),
                target: compare::CompareTarget::Value as i32,
                target_union: Some(compare::TargetUnion::Value(b"v22".to_vec())),
                ..Default::default()
            }],
            success: vec![RequestOp {
                request: Some(request_op::Request::RequestPut(put("k9", "txn"))),
            }],
            failure: vec![],
        })
        .await
        .unwrap()
        .into_inner();
    results.push(format!("txn {}", txn.succeeded));
    record!(kv.delete_range(DeleteRangeRequest {
        key: b"k3".to_vec(),
        range_end: b"k5".to_vec(),
        prev_kv: true,
    }));
    record!(lease.lease_grant(LeaseGrantRequest { ttl: 60, id: 7777 }));
    record!(kv.put(PutRequest {
        lease: 7777,
        ..put("leased", "value")
    }));
    record!(kv.range(all()));
    record!(lease.lease_revoke(LeaseRevokeRequest { id: 7777 }));
    record!(kv.range(all()));
    record!(kv.range(RangeRequest {
        revision: 9,
        ..all()
    }));
    let revision = kv
        .range(all())
        .await
        .unwrap()
        .into_inner()
        .header
        .unwrap()
        .revision;
    results.push(format!("revision {revision}"));
    // Watch replays the history from the first revision.
    let (send, receive) = tokio::sync::mpsc::channel(1);
    send.send(WatchRequest {
        request_union: Some(watch_request::RequestUnion::CreateRequest(
            WatchCreateRequest {
                key: vec![0],
                range_end: vec![0],
                start_revision: 1,
                prev_kv: true,
                ..Default::default()
            },
        )),
    })
    .await
    .unwrap();
    let mut stream = WatchClient::new(channel.clone())
        .watch(tokio_stream::wrappers::ReceiverStream::new(receive))
        .await
        .unwrap()
        .into_inner();
    let mut seen = 0;
    while seen < revision {
        let message = tokio::time::timeout(Duration::from_secs(5), stream.message())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        for event in message.events {
            seen = seen.max(event.kv.as_ref().unwrap().mod_revision);
            results.push(format!("event {event:?}"));
        }
    }
    drop(send);
    // Physical compaction waits for the materialized state in shared mode.
    record!(kv.compact(CompactionRequest {
        revision: 12,
        physical: true,
    }));
    let compacted = kv
        .range(RangeRequest {
            revision: 9,
            ..all()
        })
        .await;
    results.push(format!("{:?}", compacted.unwrap_err().code()));
    record!(kv.range(RangeRequest {
        revision: 12,
        ..all()
    }));
    for revision in [0, 12, revision] {
        record!(maintenance.hash_kv(HashKvRequest { revision }));
    }
    results
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shared_mode_matches_replicated_mode_on_one_voter() {
    let dir = tempfile::tempdir().unwrap();
    let mut outputs = vec![];
    for mode in [StateMode::Replicated, StateMode::Shared] {
        let config = config(
            &dir.path().join(format!("{mode:?}")),
            mode,
            Duration::from_millis(200),
        );
        let (server, channel) = start(config).await;
        outputs.push(workload(channel).await);
        server.shutdown().await.unwrap();
    }
    assert!(outputs[0].len() > 40);
    for (replicated, shared) in outputs[0].iter().zip(&outputs[1]) {
        assert_eq!(replicated, shared);
    }
    assert_eq!(outputs[0].len(), outputs[1].len());
}

/// Read a diagnostics endpoint on the client listener over plain HTTP/1.1.
async fn status(server: &Server) -> serde_json::Value {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut stream = tokio::net::TcpStream::connect(server.client_addr())
        .await
        .unwrap();
    let request = "GET /status HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await.unwrap();
    let response = String::from_utf8(response).unwrap();
    let (head, body) = response.split_once("\r\n\r\n").unwrap();
    assert!(head.starts_with("HTTP/1.1 200"), "{head}");
    let body = &body[body.find('{').unwrap()..=body.rfind('}').unwrap()];
    serde_json::from_str(body).unwrap()
}

/// Return (applied announcements, proposed announcements, base index).
async fn views(server: &Server) -> (u64, u64, u64) {
    let status = status(server).await;
    let shared = &status["shared_state"];
    (
        shared["applied_view_checkpoints"].as_u64().unwrap(),
        shared["writer"]["announced"].as_u64().unwrap(),
        shared["base_applied_index"].as_u64().unwrap(),
    )
}

/// The `view-` checkpoints of the local shared state.
async fn checkpoints(dir: &std::path::Path) -> usize {
    let store = Arc::new(
        slatedb::object_store::local::LocalFileSystem::new_with_prefix(dir.join("objects"))
            .unwrap(),
    );
    slatedb::admin::Admin::builder("shared/state", store)
        .build()
        .list_checkpoints(None)
        .await
        .unwrap()
        .iter()
        .filter(|checkpoint| checkpoint.name.as_deref().unwrap().starts_with("view-"))
        .count()
}

async fn eventually(what: &str, mut check: impl std::ops::AsyncFnMut() -> bool) {
    for _ in 0..300 {
        if check().await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("timed out waiting for {what}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_write_makes_one_checkpoint_and_one_announcement() {
    let dir = tempfile::tempdir().unwrap();
    let age = Duration::from_millis(200);
    let (server, channel) = start(config(dir.path(), StateMode::Shared, age)).await;
    // The first leader publishes the first checkpoint at once, then marks the bootstrap.
    eventually("the bootstrap checkpoint", async || {
        views(&server).await.0 >= 1
    })
    .await;
    assert!(dir.path().join("objects/shared/bootstrap.json").exists());
    // Startup also applies the member's client URLs. Let those publications settle.
    tokio::time::sleep(age * 5).await;
    let (idle, announced, _) = views(&server).await;
    assert_eq!(idle, announced);
    // An idle cluster makes no checkpoints: an announcement does not cause the next one.
    tokio::time::sleep(age * 5).await;
    assert_eq!(views(&server).await.0, idle);
    let mut kv = KvClient::new(channel);
    let revision = kv
        .put(put("key", "value"))
        .await
        .unwrap()
        .into_inner()
        .header
        .unwrap()
        .revision;
    eventually("one announcement", async || views(&server).await.0 > idle).await;
    tokio::time::sleep(age * 5).await;
    let (applied, announced, base) = views(&server).await;
    assert_eq!((applied, announced), (idle + 1, idle + 1));
    // The new base covers the write. The publisher keeps the two newest checkpoints.
    assert!(base > 0);
    assert_eq!(checkpoints(dir.path()).await, 2.min(applied as usize));
    let reply = kv.range(all()).await.unwrap().into_inner();
    assert_eq!(reply.kvs[0].value, b"value");
    assert_eq!(reply.header.unwrap().revision, revision);
    server.shutdown().await.unwrap();
    // A restart opens the newest checkpoint as its base.
    let (server, channel) = start(config(dir.path(), StateMode::Shared, age)).await;
    assert!(views(&server).await.2 >= base);
    let reply = KvClient::new(channel)
        .range(all())
        .await
        .unwrap()
        .into_inner();
    assert_eq!(reply.kvs[0].value, b"value");
    server.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restart_replays_the_log_above_the_newest_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    // A long age keeps every write in the overlay after the bootstrap checkpoint.
    let config = config(dir.path(), StateMode::Shared, Duration::from_secs(60));
    let (server, channel) = start(config.clone()).await;
    eventually("the bootstrap checkpoint", async || {
        views(&server).await.0 >= 1
    })
    .await;
    let mut kv = KvClient::new(channel.clone());
    for n in 0..20 {
        kv.put(put(&format!("k{}", n % 5), &format!("v{n}")))
            .await
            .unwrap();
    }
    let before = kv.range(all()).await.unwrap().into_inner();
    let hash = MaintenanceClient::new(channel)
        .hash_kv(HashKvRequest { revision: 0 })
        .await
        .unwrap()
        .into_inner()
        .hash;
    server.shutdown().await.unwrap();
    for round in 0..2 {
        let (server, channel) = start(config.clone()).await;
        let mut kv = KvClient::new(channel.clone());
        let after = kv.range(all()).await.unwrap().into_inner();
        // Earlier rounds added keys that sort after the first keys.
        assert_eq!(after.kvs.len(), before.kvs.len() + round, "round {round}");
        assert_eq!(after.kvs[..before.kvs.len()], before.kvs, "round {round}");
        let restarted = MaintenanceClient::new(channel)
            .hash_kv(HashKvRequest {
                revision: before.header.as_ref().unwrap().revision,
            })
            .await
            .unwrap()
            .into_inner()
            .hash;
        assert_eq!(restarted, hash);
        kv.put(put(&format!("round{round}"), "x")).await.unwrap();
        server.shutdown().await.unwrap();
    }
    let (server, channel) = start(config).await;
    let reply = KvClient::new(channel)
        .range(all())
        .await
        .unwrap()
        .into_inner();
    assert_eq!(reply.kvs.len(), before.kvs.len() + 2);
    server.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_directory_keeps_its_mode_and_shared_mode_has_one_voter() {
    let dir = tempfile::tempdir().unwrap();
    let age = Duration::from_secs(10);
    let (server, _) = start(config(dir.path(), StateMode::Replicated, age)).await;
    server.shutdown().await.unwrap();
    let error = Server::start(config(dir.path(), StateMode::Shared, age))
        .await
        .err()
        .unwrap();
    assert!(format!("{error:#}").contains("state mode"), "{error:#}");

    let other = tempfile::tempdir().unwrap();
    let mut three = config(other.path(), StateMode::Shared, age);
    three.peers.insert(2, "127.0.0.1:2".into());
    three.peers.insert(3, "127.0.0.1:3".into());
    let error = Server::start(three).await.err().unwrap();
    assert!(format!("{error:#}").contains("one voter"), "{error:#}");
}
