use std::collections::BTreeMap;
use std::time::Duration;

use slatedb_etcd::proto::etcdserverpb::{
    auth_client::AuthClient, kv_client::KvClient, lease_client::LeaseClient,
    maintenance_client::MaintenanceClient, watch_client::WatchClient, watch_request,
    AuthEnableRequest, AuthRoleAddRequest, AuthUserAddRequest, AuthUserGrantRoleRequest,
    LeaseGrantRequest, PutRequest, RangeRequest, StatusRequest, WatchCreateRequest, WatchRequest,
};
use slatedb_etcd::server::{Server, ServerConfig};
use tokio::net::TcpListener;

async fn ready(server: &Server) {
    server.wait_ready(Duration::from_secs(15)).await.unwrap();
}

async fn put(server: &Server, key: &[u8], value: &[u8]) {
    KvClient::connect(server.client_url())
        .await
        .unwrap()
        .put(PutRequest {
            key: key.to_vec(),
            value: value.to_vec(),
            ..Default::default()
        })
        .await
        .unwrap();
}

async fn get(server: &Server, key: &[u8]) -> Vec<u8> {
    let result = KvClient::connect(server.client_url())
        .await
        .unwrap()
        .range(RangeRequest {
            key: key.to_vec(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(result.kvs.len(), 1);
    result.kvs[0].value.clone()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn embedded_watch_lease_shutdown_and_immediate_restart() {
    let dir = tempfile::tempdir().unwrap();
    let server = Server::start(ServerConfig::single_node(dir.path()))
        .await
        .unwrap();
    ready(&server).await;
    let config = server.config().clone();
    let channel = tonic::transport::Endpoint::from_shared(server.client_url())
        .unwrap()
        .connect()
        .await
        .unwrap();
    let lease = LeaseClient::new(channel.clone())
        .lease_grant(LeaseGrantRequest { ttl: 60, id: 0 })
        .await
        .unwrap()
        .into_inner();
    let (send, receive) = tokio::sync::mpsc::channel(1);
    send.send(WatchRequest {
        request_union: Some(watch_request::RequestUnion::CreateRequest(
            WatchCreateRequest {
                key: b"leased".to_vec(),
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
    assert!(stream.message().await.unwrap().unwrap().created);
    let mut client = KvClient::new(channel);
    let written = client
        .put(PutRequest {
            key: b"leased".to_vec(),
            value: b"retained".to_vec(),
            lease: lease.id,
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    let event = tokio::time::timeout(Duration::from_secs(5), stream.message())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(event.events[0].kv.as_ref().unwrap().value, b"retained");
    server.shutdown().await.unwrap();
    let closed = tokio::time::timeout(Duration::from_secs(2), stream.message())
        .await
        .unwrap();
    assert!(closed.is_err() || closed.unwrap().is_none());
    let mut request = tonic::Request::new(RangeRequest {
        key: b"leased".to_vec(),
        ..Default::default()
    });
    request.set_timeout(Duration::from_secs(1));
    assert!(client.range(request).await.is_err());
    for _ in 0..3 {
        let server = Server::start(config.clone()).await.unwrap();
        ready(&server).await;
        let result = KvClient::connect(server.client_url())
            .await
            .unwrap()
            .range(RangeRequest {
                key: b"leased".to_vec(),
                ..Default::default()
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(result.kvs[0].value, b"retained");
        assert_eq!(result.kvs[0].lease, lease.id);
        assert!(result.header.unwrap().revision >= written.header.as_ref().unwrap().revision);
        server.shutdown().await.unwrap();
    }
}

// Keep each address reserved until its member starts. Only retry the initial bind race.
async fn start_three_members(
    dir: &std::path::Path,
    configure: impl Fn(&mut ServerConfig),
) -> Vec<Option<Server>> {
    let attempts = 4;
    'startup: for attempt in 1..=attempts {
        let mut reservations = Vec::new();
        let mut peers = BTreeMap::new();
        for id in 1..=3 {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            peers.insert(id, listener.local_addr().unwrap().to_string());
            reservations.push((id, listener));
        }

        let mut servers = Vec::new();
        for (id, listener) in reservations {
            let mut config = ServerConfig::single_node(dir.join(format!("{attempt}-{id}")));
            config.id = id;
            config.listen = listener.local_addr().unwrap();
            config.peers = peers.clone();
            config.client_peers = peers.clone();
            configure(&mut config);
            drop(listener);
            match Server::start(config).await {
                Ok(server) => servers.push(Some(server)),
                Err(error) => {
                    for server in servers.into_iter().flatten() {
                        server.shutdown().await.unwrap();
                    }
                    let collision = error
                        .downcast_ref::<std::io::Error>()
                        .is_some_and(|err| err.kind() == std::io::ErrorKind::AddrInUse);
                    assert!(
                        collision && attempt < attempts,
                        "member {id} startup: {error:#}"
                    );
                    continue 'startup;
                }
            }
        }
        for server in servers.iter().flatten() {
            ready(server).await;
        }
        return servers;
    }
    unreachable!("the final startup error fails the test")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn embedded_three_members_fail_over_and_rejoin() {
    let dir = tempfile::tempdir().unwrap();
    let mut servers = start_three_members(dir.path(), |_| {}).await;
    let status = MaintenanceClient::connect(servers[0].as_ref().unwrap().client_url())
        .await
        .unwrap()
        .status(StatusRequest {})
        .await
        .unwrap()
        .into_inner();
    let leader = status.leader as usize - 1;
    put(servers[0].as_ref().unwrap(), b"before", b"durable").await;
    let stopped = servers[leader].take().unwrap();
    let restart = stopped.config().clone();
    stopped.shutdown().await.unwrap();
    let survivor = servers.iter().flatten().next().unwrap();
    ready(survivor).await;
    put(survivor, b"after", b"available").await;
    servers[leader] = Some(Server::start(restart).await.unwrap());
    for server in servers.iter().flatten() {
        ready(server).await;
        assert_eq!(get(server, b"before").await, b"durable");
        assert_eq!(get(server, b"after").await, b"available");
    }
    for server in servers.into_iter().flatten() {
        server.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn embedding_validates_before_storage_and_times_out_without_quorum() {
    let dir = tempfile::tempdir().unwrap();
    let busy = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = ServerConfig::single_node(dir.path().join("unused"));
    config.peer_listen = Some(busy.local_addr().unwrap());
    config
        .peers
        .insert(1, busy.local_addr().unwrap().to_string());
    assert!(Server::start(config.clone()).await.is_err());
    assert!(!config.dir.exists());
    config.peer_listen = None;
    config.peers = BTreeMap::from([
        (1, "127.0.0.1:0".into()),
        (2, busy.local_addr().unwrap().to_string()),
        (3, busy.local_addr().unwrap().to_string()),
    ]);
    let server = Server::start(config).await.unwrap();
    assert!(server.wait_ready(Duration::from_millis(300)).await.is_err());
    assert!(Server::start(server.config().clone()).await.is_err());
    let config = server.config().clone();
    server.shutdown().await.unwrap();
    let server = Server::start(config).await.unwrap();
    server.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn readiness_works_with_auth_and_drop_requests_shutdown() {
    let dir = tempfile::tempdir().unwrap();
    let server = Server::start(ServerConfig::single_node(dir.path()))
        .await
        .unwrap();
    ready(&server).await;
    let mut auth = AuthClient::connect(server.client_url()).await.unwrap();
    auth.user_add(AuthUserAddRequest {
        name: "root".into(),
        password: "test-only".into(),
        ..Default::default()
    })
    .await
    .unwrap();
    auth.role_add(AuthRoleAddRequest {
        name: "root".into(),
    })
    .await
    .unwrap();
    auth.user_grant_role(AuthUserGrantRoleRequest {
        user: "root".into(),
        role: "root".into(),
    })
    .await
    .unwrap();
    auth.auth_enable(AuthEnableRequest {}).await.unwrap();
    ready(&server).await;
    assert!(KvClient::connect(server.client_url())
        .await
        .unwrap()
        .range(RangeRequest {
            key: b"denied".to_vec(),
            ..Default::default()
        })
        .await
        .is_err());
    let config = server.config().clone();
    drop(server);
    let reopened = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Ok(server) = Server::start(config.clone()).await {
                break server;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    ready(&reopened).await;
    reopened.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn worker_failure_reaches_host_and_releases_storage() {
    let dir = tempfile::tempdir().unwrap();
    let mut server = Server::start(ServerConfig::single_node(dir.path()))
        .await
        .unwrap();
    ready(&server).await;
    put(&server, b"anchor", b"durable").await;
    let config = server.config().clone();
    let mut conflict = ServerConfig::single_node(dir.path());
    conflict.cluster_id = config.cluster_id.clone();
    assert!(Server::start(conflict).await.is_err());
    reqwest::Client::new()
        .post(format!("{}/debug/log-fault", server.client_url()))
        .json(&serde_json::json!({"fail": true}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    let mut client = KvClient::connect(server.client_url()).await.unwrap();
    assert!(client
        .put(PutRequest {
            key: b"uncertain".to_vec(),
            value: b"value".to_vec(),
            ..Default::default()
        })
        .await
        .is_err());
    let failure = tokio::time::timeout(Duration::from_secs(5), server.wait())
        .await
        .unwrap()
        .unwrap_err();
    assert!(failure.to_string().contains("log"), "{failure:#}");
    drop(server);
    let server = Server::start(config).await.unwrap();
    ready(&server).await;
    assert_eq!(get(&server, b"anchor").await, b"durable");
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn restore_precedes_autotls_and_preserves_the_nonempty_directory_guard() {
    use slatedb_etcd::proto::etcdserverpb::SnapshotRequest;
    use tonic::transport::{Certificate, ClientTlsConfig, Endpoint};

    let dir = tempfile::tempdir().unwrap();
    let source = Server::start(ServerConfig::single_node(dir.path().join("source")))
        .await
        .unwrap();
    ready(&source).await;
    let channel = Endpoint::from_shared(source.client_url())
        .unwrap()
        .connect()
        .await
        .unwrap();
    KvClient::new(channel.clone())
        .put(PutRequest {
            key: b"restored".to_vec(),
            value: b"value".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap();
    let mut snapshot = MaintenanceClient::new(channel)
        .snapshot(SnapshotRequest {})
        .await
        .unwrap()
        .into_inner();
    let mut bytes = Vec::new();
    while let Some(part) = snapshot.message().await.unwrap() {
        bytes.extend(part.blob);
    }
    source.shutdown().await.unwrap();
    let backup = dir.path().join("snapshot");
    std::fs::write(&backup, bytes).unwrap();

    let target = dir.path().join("target");
    let mut config = ServerConfig::single_node(&target);
    config.restore_snapshot = Some(backup);
    config.peer_listen = Some(config.listen);
    config.peers.insert(1, "https://127.0.0.1:0".into());
    config.client_peers = config.peers.clone();
    config.client_tls.auto_tls = std::num::NonZeroU32::new(1);
    config.peer_tls.auto_tls = std::num::NonZeroU32::new(1);
    let restored = Server::start(config.clone()).await.unwrap();
    ready(&restored).await;
    let path = target.join("fixtures/client/cert.pem");
    let cert = std::fs::read(&path).unwrap();
    let endpoint = Endpoint::from_shared(restored.client_url())
        .unwrap()
        .tls_config(ClientTlsConfig::new().ca_certificate(Certificate::from_pem(cert.clone())))
        .unwrap();
    let row = KvClient::new(endpoint.connect().await.unwrap())
        .range(RangeRequest {
            key: b"restored".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(row.kvs[0].value, b"value");
    restored.shutdown().await.unwrap();
    let error = Server::start(config).await.err().unwrap();
    assert!(error.to_string().contains("new, empty data directory"));
    assert_eq!(std::fs::read(path).unwrap(), cert);
}

/// Read one key from the member's local state without a quorum round trip.
async fn local(server: &Server, key: &[u8]) -> Option<Vec<u8>> {
    let result = KvClient::connect(server.client_url())
        .await
        .unwrap()
        .range(RangeRequest {
            key: key.to_vec(),
            serializable: true,
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    result.kvs.first().map(|kv| kv.value.clone())
}

/// Read a diagnostics endpoint on the client listener over plain HTTP/1.1.
async fn diagnostics(server: &Server, path: &str) -> serde_json::Value {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut stream = tokio::net::TcpStream::connect(server.client_addr())
        .await
        .unwrap();
    let request = format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await.unwrap();
    let response = String::from_utf8(response).unwrap();
    let (head, body) = response.split_once("\r\n\r\n").unwrap();
    assert!(head.starts_with("HTTP/1.1 200"), "{head}");
    // Axum may send a chunked body; the JSON object lies between the outer braces.
    let body = &body[body.find('{').unwrap()..=body.rfind('}').unwrap()];
    serde_json::from_str(body).unwrap()
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
async fn follower_behind_an_imageless_truncation_catches_up_from_an_on_demand_image() {
    let dir = tempfile::tempdir().unwrap();
    let mut servers = start_three_members(dir.path(), |config| {
        // Truncate often, as automatic checkpoints do, without waiting for 64 MiB of log.
        config.snapshot_every = 16;
        config.snapshot_min_bytes = 0;
    })
    .await;
    let status = MaintenanceClient::connect(servers[0].as_ref().unwrap().client_url())
        .await
        .unwrap()
        .status(StatusRequest {})
        .await
        .unwrap()
        .into_inner();
    let leader = status.leader as usize - 1;
    let lagging = (leader + 1) % 3;
    put(servers[leader].as_ref().unwrap(), b"before", b"stopped").await;
    let stopped_at = diagnostics(servers[lagging].as_ref().unwrap(), "/status").await
        ["last_log_index"]
        .as_u64()
        .unwrap();
    let stopped = servers[lagging].take().unwrap();
    let restart = stopped.config().clone();
    stopped.shutdown().await.unwrap();

    let value = |i: usize| format!("{i}:{}", "v".repeat(512));
    for i in 0..256 {
        put(
            servers[leader].as_ref().unwrap(),
            format!("lag/{i}").as_bytes(),
            value(i).as_bytes(),
        )
        .await;
    }
    // Both live members must truncate past the stopped follower without building an image.
    for id in [leader, 3 - leader - lagging] {
        let server = servers[id].as_ref().unwrap();
        eventually("imageless truncation", async || {
            let status = diagnostics(server, "/status").await;
            status["snapshot_index"].as_u64().unwrap() > stopped_at
                && !status["snapshot_has_image"].as_bool().unwrap()
                && !status["checkpoint_running"].as_bool().unwrap()
        })
        .await;
        let status = diagnostics(server, "/status").await;
        assert_eq!(status["snapshot_images_built"], 0, "{status}");
    }

    servers[lagging] = Some(Server::start(restart).await.unwrap());
    let follower = servers[lagging].as_ref().unwrap();
    ready(follower).await;
    eventually("snapshot installation", async || {
        diagnostics(follower, "/status").await["installed_snapshots"]
            .as_u64()
            .unwrap()
            > 0
    })
    .await;
    // The leader built the image only because the follower asked for one.
    let status = diagnostics(servers[leader].as_ref().unwrap(), "/status").await;
    assert!(
        status["snapshot_images_built"].as_u64().unwrap() > 0,
        "{status}"
    );
    put(servers[leader].as_ref().unwrap(), b"after", b"caught-up").await;
    eventually("follower convergence", async || {
        local(follower, b"after").await.as_deref() == Some(&b"caught-up"[..])
    })
    .await;
    // Serializable reads come from the follower's own state.
    assert_eq!(local(follower, b"before").await.unwrap(), b"stopped");
    for i in 0..256 {
        let key = format!("lag/{i}");
        let found = local(follower, key.as_bytes()).await;
        assert_eq!(
            found.as_deref(),
            Some(value(i).as_bytes()),
            "follower lost {key}"
        );
    }
    for server in servers.iter().flatten() {
        assert_eq!(get(server, b"lag/255").await, value(255).as_bytes());
    }
    for server in servers.into_iter().flatten() {
        server.shutdown().await.unwrap();
    }
}

/// A host that sets the retention directly gets the flag rules: zero disables automatic
/// compaction in both modes, and a negative revision count fails before storage is created.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn embedded_auto_compaction_retention_is_validated() {
    use slatedb_etcd::compactor::Retention;
    let dir = tempfile::tempdir().unwrap();
    let mut config = ServerConfig::single_node(dir.path().join("negative"));
    config.auto_compaction = Some(Retention::Revision(-2));
    assert!(Server::start(config.clone()).await.is_err());
    assert!(!config.dir.exists());
    for (name, retention) in [
        ("revision", Retention::Revision(0)),
        ("periodic", Retention::Periodic(Duration::ZERO)),
    ] {
        let mut config = ServerConfig::single_node(dir.path().join(name));
        config.auto_compaction = Some(retention);
        let server = Server::start(config).await.unwrap();
        assert_eq!(server.config().auto_compaction, None, "{name}");
        server.shutdown().await.unwrap();
    }
    let mut config = ServerConfig::single_node(dir.path().join("kept"));
    config.auto_compaction = Some(Retention::Revision(5));
    let server = Server::start(config).await.unwrap();
    assert_eq!(
        server.config().auto_compaction,
        Some(Retention::Revision(5))
    );
    server.shutdown().await.unwrap();
}

/// An unpaginated Range larger than 64 MiB returns every row, as etcd sends up to
/// `math.MaxInt32` bytes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unpaginated_range_above_64_mib_returns_every_row() {
    let dir = tempfile::tempdir().unwrap();
    let server = Server::start(ServerConfig::single_node(dir.path()))
        .await
        .unwrap();
    ready(&server).await;
    let value = vec![b'v'; 1024 * 1024];
    for i in 0..72u32 {
        put(&server, format!("big/{i:03}").as_bytes(), &value).await;
    }
    let reply = KvClient::connect(server.client_url())
        .await
        .unwrap()
        .max_decoding_message_size(usize::MAX)
        .range(RangeRequest {
            key: b"big/".to_vec(),
            range_end: b"big0".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!((reply.count, reply.kvs.len(), reply.more), (72, 72, false));
    assert!(reply.kvs.iter().all(|kv| kv.value == value));
}
