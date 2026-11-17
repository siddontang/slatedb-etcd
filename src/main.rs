use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::{ensure, Context, Result};
use clap::Parser;
use slatedb_etcd::api::{router, ApiState};
use slatedb_etcd::node::{LogMemory, Node, NodeOptions};
use tokio::sync::mpsc;

#[derive(Parser)]
#[command(about = "Three-voter SlateDB and Raft proof of concept")]
struct Args {
    #[arg(long)]
    id: u64,
    #[arg(long)]
    listen: SocketAddr,
    /// Repeat as --peer 1=127.0.0.1:7001. Include this node.
    #[arg(long = "peer", required = true)]
    peers: Vec<String>,
    #[arg(long)]
    dir: PathBuf,
    #[arg(long, default_value = "slatedb-raft-poc")]
    cluster_id: String,
    /// Zero disables automatic snapshots. The checkpoint endpoint remains available.
    #[arg(long, default_value_t = 1000)]
    snapshot_every: u64,
    #[arg(long, default_value_t = 8 * 1024 * 1024)]
    log_cache_bytes: usize,
    #[arg(long, default_value_t = 32 * 1024 * 1024)]
    log_pending_bytes: usize,
    #[arg(long, default_value_t = 16 * 1024 * 1024)]
    log_read_bytes: usize,
    #[arg(long, default_value_t = 1024 * 1024)]
    log_term_bytes: usize,
}

#[tokio::main(worker_threads = 4)]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .with_writer(std::io::stderr)
        .init();
    let args = Args::parse();
    ensure!(
        args.listen.ip().is_loopback(),
        "the unauthenticated PoC must bind to loopback"
    );
    let mut peers = BTreeMap::new();
    for peer in args.peers {
        let (id, address) = peer.split_once('=').context("peer must be ID=IP:PORT")?;
        let id = id.parse()?;
        let socket: SocketAddr = address.parse()?;
        ensure!(
            socket.ip().is_loopback(),
            "PoC peers must use loopback addresses"
        );
        ensure!(
            peers.insert(id, socket.to_string()).is_none(),
            "duplicate peer id"
        );
    }
    ensure!(
        peers.get(&args.id) == Some(&args.listen.to_string()),
        "listen address differs from peer address"
    );
    let listener = tokio::net::TcpListener::bind(args.listen).await?;
    let (tx, rx) = mpsc::channel(2048);
    let grpc = slatedb_etcd::grpc::service(args.id, tx.clone(), &peers);
    let node = Node::open(
        NodeOptions {
            id: args.id,
            cluster_id: args.cluster_id.clone(),
            peers,
            dir: args.dir,
            snapshot_every: args.snapshot_every,
            log_memory: LogMemory {
                cache_bytes: args.log_cache_bytes,
                pending_bytes: args.log_pending_bytes,
                read_bytes: args.log_read_bytes,
                term_bytes: args.log_term_bytes,
            },
        },
        tx.clone(),
    )
    .await?;
    let app = router(ApiState {
        events: tx,
        cluster_id: args.cluster_id,
    })
    .fallback_service(grpc);
    tracing::info!(node = args.id, listen = %args.listen, "Node started");
    tokio::select! {
        result = node.run(rx) => result,
        result = axum::serve(listener, app) => result.map_err(Into::into),
        result = tokio::signal::ctrl_c() => result.map_err(Into::into),
    }
}
