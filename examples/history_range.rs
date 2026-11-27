//! Time paginated historical LIST pages while the history grows.
//!
//! Run with `python3 scripts/rustfs_fixture.py cargo run --release --example history_range --
//! [keys] [rounds] [page] [dir]`. The member keeps its state on the RustFS server of the fixture.
//! The member data goes to a new temporary child of `dir`, which the run removes at exit.
//! The example writes `keys` keys, then overwrites every key `rounds` times. After some rounds it
//! reads all keys in pages of `page` rows at the first write (`old`) and at the latest write
//! (`new`). The `churn` read uses the latest write while 100 writes to other keys land between
//! pages, as Kubernetes Events do. One JSON line reports the page latencies of each read.

use std::time::{Duration, Instant};

use slatedb_etcd::proto::etcdserverpb::{
    kv_client::KvClient, request_op::Request, PutRequest, RangeRequest, RequestOp, TxnRequest,
};
use slatedb_etcd::server::{Server, ServerConfig};
use tonic::transport::Channel;

const VALUE: usize = 256;
const TXN_OPS: usize = 100;

fn arg(index: usize, default: u64) -> u64 {
    std::env::args()
        .nth(index)
        .map_or(default, |value| value.parse().expect("numeric argument"))
}

fn key(index: u64) -> Vec<u8> {
    format!("/registry/pods/default/pod-{index:08}").into_bytes()
}

/// Write one version of every key and return the revision of the last write.
async fn write_round(client: &mut KvClient<Channel>, keys: u64, round: u64) -> anyhow::Result<i64> {
    let mut revision = 0;
    let value = vec![b'a' + (round % 26) as u8; VALUE];
    for chunk in (0..keys).collect::<Vec<_>>().chunks(TXN_OPS) {
        let success = chunk
            .iter()
            .map(|&index| RequestOp {
                request: Some(Request::RequestPut(PutRequest {
                    key: key(index),
                    value: value.clone(),
                    ..Default::default()
                })),
            })
            .collect();
        let reply = client
            .txn(TxnRequest {
                success,
                ..Default::default()
            })
            .await?
            .into_inner();
        revision = reply.header.unwrap().revision;
    }
    Ok(revision)
}

/// Read every key at `revision` in pages and return the sorted page latencies.
/// `churn` writes this many other keys between pages.
async fn list(
    client: &mut KvClient<Channel>,
    revision: i64,
    page: i64,
    keys: u64,
    churn: u64,
) -> anyhow::Result<Vec<Duration>> {
    let end = b"/registry/pods0".to_vec();
    let mut start = b"/registry/pods/".to_vec();
    let mut latencies = vec![];
    let mut seen = 0;
    loop {
        let begin = Instant::now();
        let reply = client
            .range(RangeRequest {
                key: start.clone(),
                range_end: end.clone(),
                revision,
                limit: page,
                serializable: true,
                ..Default::default()
            })
            .await?
            .into_inner();
        latencies.push(begin.elapsed());
        seen += reply.kvs.len() as u64;
        if !reply.more {
            break;
        }
        for index in 0..churn {
            client
                .put(PutRequest {
                    key: format!("/registry/events/event-{index:04}").into_bytes(),
                    value: vec![b'e'; VALUE],
                    ..Default::default()
                })
                .await?;
        }
        start = [reply.kvs.last().unwrap().key.as_slice(), b"\0"].concat();
    }
    anyhow::ensure!(seen == keys, "listed {seen} of {keys} keys");
    latencies.sort();
    Ok(latencies)
}

fn millis(latencies: &[Duration], quantile: f64) -> f64 {
    let index = ((latencies.len() as f64 * quantile).ceil() as usize).clamp(1, latencies.len());
    latencies[index - 1].as_secs_f64() * 1000.0
}

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
    let keys = arg(1, 20_000);
    let rounds = arg(2, 20);
    let page = arg(3, 500) as i64;
    let parent = std::env::args()
        .nth(4)
        .unwrap_or_else(|| "tmp/history-range".into());
    // The member data goes to a new child of `dir`, so the run never deletes existing data.
    std::fs::create_dir_all(&parent)?;
    let dir = tempfile::Builder::new()
        .prefix("member-")
        .tempdir_in(&parent)?;
    let name = dir.path().file_name().unwrap().to_string_lossy();
    let store = object_store(&format!("examples/history-range/{name}"))?;
    let server = Server::start(ServerConfig::single_node(dir.path(), store)).await?;
    let result = async {
        server.wait_ready(Duration::from_secs(10)).await?;
        let mut client = KvClient::connect(server.client_url())
            .await?
            .max_decoding_message_size(64 << 20);
        let old = write_round(&mut client, keys, 0).await?;
        let mut report = (1..=rounds).filter(|round| round.is_power_of_two() || *round == rounds);
        let mut next = report.next();
        for round in 1..=rounds {
            let new = write_round(&mut client, keys, round).await?;
            if Some(round) != next {
                continue;
            }
            next = report.next();
            for (name, revision, churn) in [("old", old, 0), ("new", new, 0), ("churn", new, 100)] {
                // The first pass warms the block cache. The second pass is the measurement.
                list(&mut client, revision, page, keys, 0).await?;
                let latencies = list(&mut client, revision, page, keys, churn).await?;
                let total: Duration = latencies.iter().sum();
                println!(
                    "{{\"keys\":{keys},\"versions\":{},\"revision\":\"{name}\",\"pages\":{},\
                     \"p50_ms\":{:.3},\"p99_ms\":{:.3},\"max_ms\":{:.3},\"list_ms\":{:.1}}}",
                    round + 1,
                    latencies.len(),
                    millis(&latencies, 0.5),
                    millis(&latencies, 0.99),
                    millis(&latencies, 1.0),
                    total.as_secs_f64() * 1000.0,
                );
            }
        }
        anyhow::Ok(())
    }
    .await;
    let shutdown = server.shutdown().await;
    result.and(shutdown)
}
