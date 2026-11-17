//! Run with `cargo run --example embedded -- <data-directory>`.

use std::time::Duration;

use slatedb_etcd::proto::etcdserverpb::{kv_client::KvClient, PutRequest, RangeRequest};
use slatedb_etcd::server::{Server, ServerConfig};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let dir = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "tmp/embedded".into());
    let server = Server::start(ServerConfig::single_node(dir)).await?;
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
