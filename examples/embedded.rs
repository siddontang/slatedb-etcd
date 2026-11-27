//! Run with `python3 scripts/rustfs_fixture.py cargo run --example embedded -- <data-directory>`.
//! The member keeps its state on the RustFS server of the fixture.

use std::time::Duration;

use slatedb_etcd::proto::etcdserverpb::{kv_client::KvClient, PutRequest, RangeRequest};
use slatedb_etcd::server::{Server, ServerConfig};

/// The object store that `scripts/rustfs_fixture.py` names in the environment, under `prefix`.
fn object_store(prefix: &str) -> anyhow::Result<slatedb_etcd::object::Config> {
    let endpoint = std::env::var("SLATEDB_S3_TEST_ENDPOINT").ok();
    Ok(slatedb_etcd::object::Config::S3(slatedb_etcd::object::S3 {
        bucket: std::env::var("SLATEDB_S3_TEST_BUCKET")?,
        region: "us-east-1".into(),
        allow_http: endpoint
            .as_deref()
            .is_some_and(|url| url.starts_with("http://")),
        endpoint,
        prefix: prefix.into(),
    }))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let dir = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "tmp/embedded".into());
    let config = ServerConfig::single_node(dir, object_store("examples/embedded")?);
    let server = Server::start(config).await?;
    let result = async {
        server.wait_ready(Duration::from_secs(10)).await?;
        let mut client = KvClient::connect(server.client_url()).await?;
        client
            .put(PutRequest {
                key: b"hello".to_vec(),
                value: b"world".to_vec(),
                ..Default::default()
            })
            .await?;
        let response = client
            .range(RangeRequest {
                key: b"hello".to_vec(),
                ..Default::default()
            })
            .await?
            .into_inner();
        println!(
            "{}: {}",
            server.client_url(),
            String::from_utf8_lossy(&response.kvs[0].value)
        );
        anyhow::Ok(())
    }
    .await;
    let shutdown = server.shutdown().await;
    result.and(shutdown)
}
