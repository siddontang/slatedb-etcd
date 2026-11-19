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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn embedded_three_members_fail_over_and_rejoin() {
    let dir = tempfile::tempdir().unwrap();
    let mut reservations = Vec::new();
    let mut peers = BTreeMap::new();
    for id in 1..=3 {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        peers.insert(id, listener.local_addr().unwrap().to_string());
        reservations.push(listener);
    }
    let configs: Vec<_> = (1..=3)
        .map(|id| {
            let mut config = ServerConfig::single_node(dir.path().join(id.to_string()));
            config.id = id;
            config.listen = peers[&id].parse().unwrap();
            config.peers = peers.clone();
            config.client_peers = peers.clone();
            config
        })
        .collect();
    drop(reservations);
    let mut servers = Vec::new();
    for config in &configs {
        servers.push(Some(Server::start(config.clone()).await.unwrap()));
    }
    for server in servers.iter().flatten() {
        ready(server).await;
    }
    let status = MaintenanceClient::connect(servers[0].as_ref().unwrap().client_url())
        .await
        .unwrap()
        .status(StatusRequest {})
        .await
        .unwrap()
        .into_inner();
    let leader = status.leader as usize - 1;
    put(servers[0].as_ref().unwrap(), b"before", b"durable").await;
    servers[leader].take().unwrap().shutdown().await.unwrap();
    let survivor = servers.iter().flatten().next().unwrap();
    ready(survivor).await;
    put(survivor, b"after", b"available").await;
    servers[leader] = Some(Server::start(configs[leader].clone()).await.unwrap());
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
