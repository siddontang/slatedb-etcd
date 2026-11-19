#[cfg(feature = "write-profile")]
#[global_allocator]
static ALLOCATOR: slatedb_etcd::write_profile::StageAllocator =
    slatedb_etcd::write_profile::StageAllocator;

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::{ensure, Context, Result};
use clap::Parser;
use slatedb_etcd::node::LogMemory;
use slatedb_etcd::server::{Server, ServerConfig};

static LONG_VERSION: std::sync::LazyLock<String> = std::sync::LazyLock::new(|| {
    format!(
        "{}\netcd API version: {}",
        env!("CARGO_PKG_VERSION"),
        slatedb_etcd::ETCD_API_VERSION
    )
});

#[derive(Parser)]
#[group(id = "ServerArgs")]
#[command(version, long_version = LONG_VERSION.as_str(),
          about = "Experimental etcd v3 service with Raft and SlateDB")]
struct Args {
    /// Async worker count. Log writes use a separate thread.
    #[arg(long, default_value = "2")]
    runtime_workers: std::num::NonZeroUsize,
    /// Persistent member ID. Keep it with its data directory.
    #[arg(long)]
    id: u64,
    #[command(flatten)]
    object_store: slatedb_etcd::object::Args,
    /// Client listener address. Remote listeners require --allow-remote.
    #[arg(long)]
    listen: SocketAddr,
    /// Allow remote hosts with separate client and peer listeners and mutual TLS.
    #[arg(long)]
    allow_remote: bool,
    /// Separate peer listener address. Required when TLS is enabled.
    #[arg(long)]
    peer_listen: Option<SocketAddr>,
    /// Comma-separated HTTP or HTTPS listeners for Prometheus scrapes.
    #[arg(long, value_delimiter = ',')]
    listen_metrics_urls: Vec<String>,
    /// Select extensive to measure gRPC handling duration by method.
    #[arg(long, value_enum, default_value = "basic")]
    metrics: slatedb_etcd::server::MetricsLevel,
    /// Advertised client URL. Repeat as --client-peer ID=URL.
    #[arg(long = "client-peer")]
    client_peers: Vec<String>,
    /// Client listener certificate chain in PEM format.
    #[arg(long)]
    cert_file: Option<PathBuf>,
    /// Client listener private key in PEM format.
    #[arg(long)]
    key_file: Option<PathBuf>,
    /// Trusted CA certificates for client authentication.
    #[arg(long)]
    trusted_ca_file: Option<PathBuf>,
    /// Require verified client certificates.
    #[arg(long)]
    client_cert_auth: bool,
    /// Peer certificate chain for incoming and outgoing connections.
    #[arg(long)]
    peer_cert_file: Option<PathBuf>,
    /// Peer private key in PEM format.
    #[arg(long)]
    peer_key_file: Option<PathBuf>,
    /// Trusted peer CA certificates. Peer TLS requires mutual authentication.
    #[arg(long)]
    peer_trusted_ca_file: Option<PathBuf>,
    /// Repeat as --peer 1=127.0.0.1:7001. Include this node.
    #[arg(long = "peer", required = true)]
    peers: Vec<String>,
    /// Data directory for this member's Raft log and SlateDB objects.
    #[arg(long)]
    dir: PathBuf,
    /// Shared cluster identity. Preserve it when restarting existing data.
    #[arg(long, default_value = "slatedb-raft-poc")]
    cluster_id: String,
    /// Zero disables automatic snapshots. The checkpoint endpoint remains available.
    #[arg(long, default_value_t = 1000)]
    snapshot_every: u64,
    /// Interval between idle Watch progress responses. Positive values have a 100 ms minimum.
    #[arg(long, alias = "experimental-watch-progress-notify-interval", default_value = "10s",
          value_parser = humantime::parse_duration)]
    watch_progress_notify_interval: std::time::Duration,
    /// Log entry cache budget in bytes.
    #[arg(long, default_value_t = 8 * 1024 * 1024)]
    log_cache_bytes: usize,
    /// Logical quota in bytes. Zero selects 2 GiB. A negative value disables the quota.
    #[arg(long, default_value_t = 0, allow_negative_numbers = true)]
    quota_backend_bytes: i64,
    /// Token mode: simple, or jwt with RS256 public-key and private-key paths.
    #[arg(long, default_value = "simple")]
    auth_token: String,
    /// Reject member changes that would violate the quorum health checks.
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    strict_reconfig_check: bool,
    /// Join after MemberAdd, with a new directory and the assigned member ID.
    #[arg(long)]
    join: bool,
    /// Restore a native snapshot into a new data directory.
    #[arg(long, conflicts_with = "join")]
    restore_snapshot: Option<PathBuf>,
    /// Pending log entry budget in bytes, including unapplied entries.
    #[arg(long, default_value_t = 32 * 1024 * 1024)]
    log_pending_bytes: usize,
    /// Asynchronous log read result budget in bytes.
    #[arg(long, default_value_t = 16 * 1024 * 1024)]
    log_read_bytes: usize,
    /// Compressed log term metadata budget in bytes.
    #[arg(long, default_value_t = 1024 * 1024)]
    log_term_bytes: usize,
}

fn main() -> Result<()> {
    let args = Args::parse();
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(args.runtime_workers.get())
        .enable_all()
        .build()?
        .block_on(run(args))
}

async fn run(args: Args) -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .with_writer(std::io::stderr)
        .init();
    let config = ServerConfig {
        id: args.id,
        cluster_id: args.cluster_id,
        dir: args.dir,
        listen: args.listen,
        peer_listen: args.peer_listen,
        listen_metrics_urls: args.listen_metrics_urls,
        metrics: args.metrics,
        peers: parse_peers(args.peers)?,
        client_peers: parse_peers(args.client_peers)?,
        allow_remote: args.allow_remote,
        client_tls: slatedb_etcd::tls::Options {
            cert: args.cert_file,
            key: args.key_file,
            ca: args.trusted_ca_file,
            client_auth: args.client_cert_auth,
        },
        peer_tls: slatedb_etcd::tls::Options {
            client_auth: args.peer_cert_file.is_some(),
            cert: args.peer_cert_file,
            key: args.peer_key_file,
            ca: args.peer_trusted_ca_file,
        },
        object_store: args.object_store.config()?,
        snapshot_every: args.snapshot_every,
        watch_progress_notify_interval: args.watch_progress_notify_interval,
        log_memory: LogMemory {
            cache_bytes: args.log_cache_bytes,
            pending_bytes: args.log_pending_bytes,
            read_bytes: args.log_read_bytes,
            term_bytes: args.log_term_bytes,
        },
        quota_bytes: args.quota_backend_bytes,
        auth_token: args.auth_token,
        strict_reconfiguration: args.strict_reconfig_check,
        join: args.join,
        restore_snapshot: args.restore_snapshot,
    };
    let mut server = Server::start(config).await?;
    tracing::info!(node = args.id, listen = %server.client_addr(), "Node started");
    tokio::select! {
        result = server.wait() => result,
        result = tokio::signal::ctrl_c() => {
            result?;
            server.shutdown().await
        },
    }
}

fn parse_peers(peers: Vec<String>) -> Result<BTreeMap<u64, String>> {
    let mut result = BTreeMap::new();
    for peer in peers {
        let (id, address) = peer.split_once('=').context("peer must be ID=URL")?;
        ensure!(
            result.insert(id.parse()?, address.to_owned()).is_none(),
            "duplicate peer id"
        );
    }
    Ok(result)
}
