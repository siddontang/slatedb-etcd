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
async fn a_directory_keeps_its_mode_and_members_need_a_shared_store() {
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
    assert!(
        format!("{error:#}").contains("shared object store"),
        "{error:#}"
    );
}

/// Reserve loopback addresses for `members` members.
async fn addresses(members: u64) -> std::collections::BTreeMap<u64, String> {
    let mut peers = std::collections::BTreeMap::new();
    let mut reservations = vec![];
    for id in 1..=members {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        peers.insert(id, listener.local_addr().unwrap().to_string());
        reservations.push(listener);
    }
    peers
}

fn member_config(
    dir: &std::path::Path,
    mode: StateMode,
    id: u64,
    peers: &std::collections::BTreeMap<u64, String>,
) -> ServerConfig {
    let mut config = config(&dir.join(id.to_string()), mode, Duration::from_millis(200));
    config.id = id;
    config.listen = peers[&id].parse().unwrap();
    config.peers = peers.clone();
    config.client_peers = peers.clone();
    // Truncate often, so a late member needs a Raft snapshot.
    config.snapshot_every = 16;
    config.snapshot_min_bytes = 0;
    // The health check needs peer traffic first. This test adds a member at once.
    config.strict_reconfiguration = false;
    if mode == StateMode::Shared {
        config.object_store =
            slatedb_etcd::object::Config::SharedDirectory(dir.join("shared-objects"));
    }
    config
}

async fn channel(server: &Server) -> Channel {
    Endpoint::from_shared(server.client_url())
        .unwrap()
        .connect()
        .await
        .unwrap()
}

async fn leader(server: &Server) -> u64 {
    MaintenanceClient::new(channel(server).await)
        .status(slatedb_etcd::proto::etcdserverpb::StatusRequest {})
        .await
        .unwrap()
        .into_inner()
        .leader
}

/// The local revision and the HashKV result of a member at `revision`.
async fn local_hash(server: &Server, revision: i64) -> Option<u32> {
    let channel = channel(server).await;
    let local = KvClient::new(channel.clone())
        .range(RangeRequest {
            serializable: true,
            ..all()
        })
        .await
        .ok()?
        .into_inner();
    if local.header?.revision < revision {
        return None;
    }
    let hash = MaintenanceClient::new(channel)
        .hash_kv(HashKvRequest { revision })
        .await
        .ok()?
        .into_inner();
    Some(hash.hash)
}

/// Run the workload on three voters, then add a learner. Shared and replicated mode give the
/// same results and hashes. In shared mode the learner joins from a format 6 snapshot.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn three_voters_and_a_learner_share_one_state() {
    use slatedb_etcd::proto::etcdserverpb::{cluster_client::ClusterClient, MemberAddRequest};
    let mut outputs = vec![];
    let mut hashes = vec![];
    for mode in [StateMode::Replicated, StateMode::Shared] {
        let dir = tempfile::tempdir().unwrap();
        let mut peers = addresses(4).await;
        let learner_address = peers.remove(&4).unwrap();
        let mut servers = vec![];
        for id in 1..=3 {
            let config = member_config(dir.path(), mode, id, &peers);
            servers.push(Server::start(config).await.unwrap());
        }
        for server in &servers {
            server.wait_ready(Duration::from_secs(15)).await.unwrap();
        }
        let lead = &servers[leader(&servers[0]).await as usize - 1];
        outputs.push(workload(channel(lead).await).await);
        let revision = KvClient::new(channel(lead).await)
            .range(all())
            .await
            .unwrap()
            .into_inner()
            .header
            .unwrap()
            .revision;
        // Every voter applies the same state. A follower reads its own base and overlay.
        let mut values = vec![];
        for server in &servers {
            let mut hash = None;
            eventually("a member hash", async || {
                hash = local_hash(server, revision).await;
                hash.is_some()
            })
            .await;
            values.push(hash.unwrap());
        }
        assert!(values.iter().all(|hash| *hash == values[0]), "{values:?}");

        // A learner joins after the voters truncated their logs.
        let added = ClusterClient::new(channel(lead).await)
            .member_add(MemberAddRequest {
                peer_ur_ls: vec![format!("http://{learner_address}")],
                is_learner: true,
            })
            .await
            .unwrap()
            .into_inner();
        let id = added.member.unwrap().id;
        let mut joined_peers = peers.clone();
        joined_peers.insert(id, learner_address);
        let mut learner_config = member_config(dir.path(), mode, id, &joined_peers);
        learner_config.join = true;
        let learner = Server::start(learner_config).await.unwrap();
        let mut learner_hash = None;
        eventually("the learner hash", async || {
            learner_hash = local_hash(&learner, revision).await;
            learner_hash.is_some()
        })
        .await;
        assert_eq!(learner_hash.unwrap(), values[0]);
        if mode == StateMode::Shared {
            let joined = status(&learner).await;
            assert!(
                joined["installed_snapshots"].as_u64().unwrap() > 0,
                "{joined}"
            );
            // The leader keeps the transfer and the reports of the members.
            eventually("member reports", async || {
                status(lead).await["shared_state"]["writer"]["reports"]
                    .as_u64()
                    .unwrap()
                    >= 3
            })
            .await;
            assert!(status(lead).await["snapshot_index"].as_u64().unwrap() > 0);
        }
        hashes.push(values[0]);
        learner.shutdown().await.unwrap();
        for server in servers {
            server.shutdown().await.unwrap();
        }
    }
    assert_eq!(outputs[0], outputs[1]);
    assert_eq!(hashes[0], hashes[1]);
}

/// A follower stops while the others truncate their logs past it. Its restart opens the newest
/// checkpoint and adopts it as the snapshot record, because its log ends below the base.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_restarted_follower_adopts_a_checkpoint_above_its_log() {
    let dir = tempfile::tempdir().unwrap();
    let peers = addresses(3).await;
    let configs: Vec<_> = (1..=3)
        .map(|id| member_config(dir.path(), StateMode::Shared, id, &peers))
        .collect();
    let mut servers = vec![];
    for config in &configs {
        servers.push(Some(Server::start(config.clone()).await.unwrap()));
    }
    for server in servers.iter().flatten() {
        server.wait_ready(Duration::from_secs(15)).await.unwrap();
    }
    let lead = leader(servers[0].as_ref().unwrap()).await as usize - 1;
    let follower = (lead + 1) % 3;
    let mut kv = KvClient::new(channel(servers[lead].as_ref().unwrap()).await);
    kv.put(put("before", "stop")).await.unwrap();
    let stopped = servers[follower].take().unwrap();
    let last = status(&stopped).await["last_log_index"].as_u64().unwrap();
    stopped.shutdown().await.unwrap();
    for n in 0..64 {
        kv.put(put(&format!("k{}", n % 8), &format!("v{n}")))
            .await
            .unwrap();
    }
    let lead_server = servers[lead].as_ref().unwrap();
    eventually(
        "a base and a truncation above the stopped log",
        async || {
            let status = status(lead_server).await;
            status["shared_state"]["base_applied_index"]
                .as_u64()
                .unwrap()
                > last
                && status["snapshot_index"].as_u64().unwrap() > last
        },
    )
    .await;
    let revision = kv
        .range(all())
        .await
        .unwrap()
        .into_inner()
        .header
        .unwrap()
        .revision;
    let expected = local_hash(lead_server, revision).await.unwrap();
    let restarted = Server::start(configs[follower].clone()).await.unwrap();
    let mut hash = None;
    eventually("the restarted follower", async || {
        hash = local_hash(&restarted, revision).await;
        hash.is_some()
    })
    .await;
    assert_eq!(hash.unwrap(), expected);
    let joined = status(&restarted).await;
    assert!(
        joined["snapshot_index"].as_u64().unwrap() > last,
        "{joined}"
    );
    restarted.shutdown().await.unwrap();
    for server in servers.into_iter().flatten() {
        server.shutdown().await.unwrap();
    }
}

/// Reads after an idle time longer than the pin limit succeed in both read modes, and Watch
/// replays the history. Each read starts its own pin deadline.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reads_after_an_idle_pin_limit_succeed() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = config(dir.path(), StateMode::Shared, Duration::from_millis(200));
    config.view_pin_max = Duration::from_millis(250);
    let (server, channel) = start(config).await;
    let mut kv = KvClient::new(channel.clone());
    kv.put(put("key", "value")).await.unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;
    for serializable in [true, false] {
        let reply = kv
            .range(RangeRequest {
                key: b"key".to_vec(),
                serializable,
                ..Default::default()
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(reply.kvs[0].value, b"value");
    }
    let (send, receive) = tokio::sync::mpsc::channel(1);
    send.send(WatchRequest {
        request_union: Some(watch_request::RequestUnion::CreateRequest(
            WatchCreateRequest {
                key: b"key".to_vec(),
                start_revision: 1,
                ..Default::default()
            },
        )),
    })
    .await
    .unwrap();
    let mut stream = WatchClient::new(channel)
        .watch(tokio_stream::wrappers::ReceiverStream::new(receive))
        .await
        .unwrap()
        .into_inner();
    let mut events = vec![];
    while events.is_empty() {
        let message = tokio::time::timeout(Duration::from_secs(5), stream.message())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        events.extend(message.events);
    }
    assert_eq!(events[0].kv.as_ref().unwrap().value, b"value");
    drop(send);
    server.shutdown().await.unwrap();
}

/// Three Watch streams with prev_kv read slowly while the pin limit is short. Each stream gets
/// every revision once, with no gap.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn slow_watchers_get_every_revision_once() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = config(dir.path(), StateMode::Shared, Duration::from_millis(200));
    config.view_pin_max = Duration::from_millis(30);
    let (server, channel) = start(config).await;
    let mut streams = vec![];
    let mut senders = vec![];
    for _ in 0..3 {
        // Each stream has its own connection. One connection would share its flow-control
        // window, and a stream that nobody reads would block the others.
        let (send, receive) = tokio::sync::mpsc::channel(1);
        send.send(WatchRequest {
            request_union: Some(watch_request::RequestUnion::CreateRequest(
                WatchCreateRequest {
                    key: b"w/".to_vec(),
                    range_end: b"w0".to_vec(),
                    prev_kv: true,
                    ..Default::default()
                },
            )),
        })
        .await
        .unwrap();
        let mut stream = WatchClient::new(crate::channel(&server).await)
            .watch(tokio_stream::wrappers::ReceiverStream::new(receive))
            .await
            .unwrap()
            .into_inner();
        assert!(stream.message().await.unwrap().unwrap().created);
        streams.push(stream);
        senders.push(send);
    }
    let mut kv = KvClient::new(channel);
    let value = "v".repeat(64 * 1024);
    let mut revisions = vec![];
    for n in 0..48 {
        let reply = kv.put(put(&format!("w/{}", n % 6), &value)).await.unwrap();
        revisions.push(reply.into_inner().header.unwrap().revision);
    }
    let readers = streams.into_iter().map(|mut stream| {
        let count = revisions.len();
        async move {
            let mut seen = vec![];
            while seen.len() < count {
                // A slow consumer: the server blocks on its sends.
                tokio::time::sleep(Duration::from_millis(10)).await;
                let message = tokio::time::timeout(Duration::from_secs(10), stream.message())
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap();
                for event in message.events {
                    seen.push(event.kv.unwrap().mod_revision);
                }
            }
            seen
        }
    });
    for seen in futures::future::join_all(readers).await {
        assert_eq!(seen, revisions);
    }
    drop(senders);
    server.shutdown().await.unwrap();
}

/// Start three shared voters that flush only on request, so their overlays keep every write.
async fn quiet_cluster(dir: &std::path::Path) -> (Vec<ServerConfig>, Vec<Option<Server>>) {
    let peers = addresses(3).await;
    let configs: Vec<_> = (1..=3)
        .map(|id| {
            let mut config = member_config(dir, StateMode::Shared, id, &peers);
            config.view_max_age = Duration::from_secs(60);
            config.overlay_max_bytes = 8 << 20;
            config
        })
        .collect();
    let mut servers = vec![];
    for config in &configs {
        servers.push(Some(Server::start(config.clone()).await.unwrap()));
    }
    for server in servers.iter().flatten() {
        server.wait_ready(Duration::from_secs(15)).await.unwrap();
    }
    (configs, servers)
}

/// Every live member reaches the same hash at the leader's revision.
async fn converge(servers: &[Option<Server>]) {
    let live: Vec<_> = servers.iter().flatten().collect();
    let mut revision = 0;
    for server in &live {
        if let Ok(reply) = KvClient::new(channel(server).await).range(all()).await {
            revision = reply.into_inner().header.unwrap().revision;
            break;
        }
    }
    let mut hashes = vec![];
    for server in live {
        let mut hash = None;
        eventually("a member hash", async || {
            hash = local_hash(server, revision).await;
            hash.is_some()
        })
        .await;
        hashes.push(hash.unwrap());
    }
    assert!(hashes.iter().all(|hash| *hash == hashes[0]), "{hashes:?}");
}

/// A leader transfer first publishes a checkpoint of the applied state. The new leader opens the
/// writer, fences the old one, and serves writes.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn move_leader_publishes_first_and_the_new_leader_writes() {
    use slatedb_etcd::proto::etcdserverpb::MoveLeaderRequest;
    let dir = tempfile::tempdir().unwrap();
    let (_, servers) = quiet_cluster(dir.path()).await;
    let lead = leader(servers[0].as_ref().unwrap()).await;
    let old = servers[lead as usize - 1].as_ref().unwrap();
    let mut kv = KvClient::new(channel(old).await);
    for n in 0..20 {
        kv.put(put(&format!("k{n}"), "before")).await.unwrap();
    }
    let applied = status(old).await["applied_index"].as_u64().unwrap();
    let target = (1..=3).find(|id| *id != lead).unwrap();
    MaintenanceClient::new(channel(old).await)
        .move_leader(MoveLeaderRequest { target_id: target })
        .await
        .unwrap();
    // The old leader published a checkpoint that covers its applied writes before the transfer.
    let base = status(old).await["shared_state"]["base_applied_index"]
        .as_u64()
        .unwrap();
    assert!(base >= applied - 1, "base {base} applied {applied}");
    let new = servers[target as usize - 1].as_ref().unwrap();
    assert_eq!(leader(new).await, target);
    let mut kv = KvClient::new(channel(new).await);
    eventually("a write on the new leader", async || {
        kv.put(put("after", "moved")).await.is_ok()
    })
    .await;
    // The new leader opened the writer, which fenced the old one, and materialized the write.
    eventually("the new writer", async || {
        status(new).await["shared_state"]["writer"]["written"]
            .as_u64()
            .unwrap()
            > applied
    })
    .await;
    converge(&servers).await;
    for server in servers.into_iter().flatten() {
        server.shutdown().await.unwrap();
    }
}

/// The leader stops with a nearly full overlay and no checkpoint of its writes. A new leader
/// opens the writer, materializes its own overlay, and serves the first write. The test prints
/// the time from the stop to that write.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn failover_with_a_full_overlay_serves_the_next_write() {
    let dir = tempfile::tempdir().unwrap();
    let (configs, mut servers) = quiet_cluster(dir.path()).await;
    let lead = leader(servers[0].as_ref().unwrap()).await as usize - 1;
    let mut kv = KvClient::new(channel(servers[lead].as_ref().unwrap()).await);
    // Each value is in a current row and a history row: about 6 MiB against an 8 MiB limit.
    for n in 0..24 {
        kv.put(put(&format!("big{n}"), &"x".repeat(128 * 1024)))
            .await
            .unwrap();
    }
    let follower = servers[(lead + 1) % 3].as_ref().unwrap();
    eventually("the follower overlay", async || {
        status(follower).await["shared_state"]["overlay_bytes"]
            .as_u64()
            .unwrap()
            > 5 << 20
    })
    .await;
    let stopped = std::time::Instant::now();
    servers[lead].take().unwrap().shutdown().await.unwrap();
    let mut elapsed = None;
    for _ in 0..600 {
        for server in servers.iter().flatten() {
            let mut kv = KvClient::new(channel(server).await);
            if kv.put(put("first", "after failover")).await.is_ok() {
                elapsed = Some(stopped.elapsed());
            }
        }
        if elapsed.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let elapsed = elapsed.expect("no write after the failover");
    eprintln!("failover to the first write with a full overlay: {elapsed:?}");
    assert!(elapsed < Duration::from_secs(15), "{elapsed:?}");
    servers[lead] = Some(Server::start(configs[lead].clone()).await.unwrap());
    converge(&servers).await;
    for server in servers.into_iter().flatten() {
        server.shutdown().await.unwrap();
    }
}
