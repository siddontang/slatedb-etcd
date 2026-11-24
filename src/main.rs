#[cfg(feature = "write-profile")]
#[global_allocator]
static ALLOCATOR: slatedb_etcd::write_profile::StageAllocator =
    slatedb_etcd::write_profile::StageAllocator;

// The write profiler counts allocations through its own wrapper, so it keeps the system allocator.
#[cfg(all(
    feature = "jemalloc",
    not(feature = "write-profile"),
    target_os = "linux",
    target_env = "gnu"
))]
#[global_allocator]
static ALLOCATOR: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[cfg(all(
    feature = "jemalloc",
    not(feature = "write-profile"),
    target_os = "linux",
    target_env = "gnu"
))]
mod malloc_conf {
    /// A C string pointer that jemalloc reads before `main`.
    #[repr(transparent)]
    struct Conf(*const std::ffi::c_char);

    // SAFETY: the pointer refers to an immutable static C string.
    unsafe impl Sync for Conf {}

    /// Default jemalloc options. `_RJEM_MALLOC_CONF` still overrides them at startup.
    ///
    /// jemalloc keeps freed dirty pages for 10 s by default. Memtable flushes and compactions
    /// free hundreds of MiB at once, so RSS stayed near the earlier peak for 10 s.
    ///
    /// Without a background thread, jemalloc purges an arena only when that arena allocates.
    /// Arenas that went idle after a flush kept their dirty pages: in put-c256, jemalloc held
    /// 100 MiB of resident pages above its active pages several seconds after the load ended.
    /// Background threads purge on a timer, and dirty pages return after 200 ms.
    /// Muzzy decay keeps the jemalloc 5.3 default of 0.
    #[export_name = "_rjem_malloc_conf"]
    static CONF: Conf =
        Conf(c"background_thread:true,dirty_decay_ms:200,muzzy_decay_ms:0".as_ptr());
}

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
    /// The default is the CPU count that the process may use (affinity mask and cgroup quota).
    #[arg(long)]
    runtime_workers: Option<std::num::NonZeroUsize>,
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
    /// Generate a self-signed client listener certificate.
    #[arg(long)]
    auto_tls: bool,
    /// Generate a self-signed peer listener certificate.
    #[arg(long)]
    peer_auto_tls: bool,
    /// Validity of generated certificates in years.
    #[arg(long, default_value = "1")]
    self_signed_cert_validity: std::num::NonZeroU32,
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
    #[arg(long, default_value_t = 10_000)]
    snapshot_every: u64,
    /// Interval between idle Watch progress responses. Positive values have a 100 ms minimum.
    #[arg(long, alias = "experimental-watch-progress-notify-interval", default_value = "10s",
          value_parser = humantime::parse_duration)]
    watch_progress_notify_interval: std::time::Duration,
    /// Automatic snapshots also wait for this many appended log bytes. Zero uses only the entry interval.
    #[arg(long, default_value_t = slatedb_etcd::node::DEFAULT_SNAPSHOT_MIN_BYTES)]
    snapshot_min_bytes: u64,
    /// Log entry cache budget in bytes.
    #[arg(long, default_value_t = 8 * 1024 * 1024)]
    log_cache_bytes: usize,
    /// State cache budget for SST data blocks, in bytes.
    #[arg(long, default_value_t = 64 * 1024 * 1024)]
    state_block_cache_bytes: u64,
    /// State cache budget for SST indexes, filters, and stats, in bytes.
    #[arg(long, default_value_t = 64 * 1024 * 1024)]
    state_meta_cache_bytes: u64,
    /// Budget for current rows of linearizable point reads, in bytes. Zero disables the row cache.
    #[arg(long, default_value_t = 64 * 1024 * 1024)]
    state_row_cache_bytes: u64,
    /// Logical quota in bytes. Zero selects 2 GiB. A negative value disables the quota.
    #[arg(long, default_value_t = 0, allow_negative_numbers = true)]
    quota_backend_bytes: i64,
    /// Encoded bytes of rows one Range or read-only Txn response may return. Larger reads fail
    /// with ResourceExhausted before the member holds the whole result. At most 2 GiB - 1 MiB.
    #[arg(long, default_value_t = slatedb_etcd::state::DEFAULT_MAX_RESPONSE_BYTES as u64,
          value_parser = clap::value_parser!(u64).range(1..=i32::MAX as u64 - 1024 * 1024))]
    max_response_bytes: u64,
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
        .worker_threads(args.runtime_workers.unwrap_or_else(default_workers).get())
        .enable_all()
        .build()?
        .block_on(run(args))
}

/// Linux derives this from `sched_getaffinity` and the cgroup CPU quota. Two remains the fallback.
fn default_workers() -> std::num::NonZeroUsize {
    std::thread::available_parallelism().unwrap_or(std::num::NonZeroUsize::new(2).unwrap())
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
            auto_tls: args.auto_tls.then_some(args.self_signed_cert_validity),
            cert: args.cert_file,
            key: args.key_file,
            ca: args.trusted_ca_file,
            client_auth: args.client_cert_auth,
        },
        peer_tls: slatedb_etcd::tls::Options {
            client_auth: args.peer_cert_file.is_some(),
            auto_tls: args.peer_auto_tls.then_some(args.self_signed_cert_validity),
            cert: args.peer_cert_file,
            key: args.peer_key_file,
            ca: args.peer_trusted_ca_file,
        },
        object_store: args.object_store.config()?,
        snapshot_every: args.snapshot_every,
        watch_progress_notify_interval: args.watch_progress_notify_interval,
        snapshot_min_bytes: args.snapshot_min_bytes,
        log_memory: LogMemory {
            cache_bytes: args.log_cache_bytes,
            pending_bytes: args.log_pending_bytes,
            read_bytes: args.log_read_bytes,
            term_bytes: args.log_term_bytes,
        },
        state_cache: slatedb_etcd::state::StateCache {
            block_bytes: args.state_block_cache_bytes,
            meta_bytes: args.state_meta_cache_bytes,
            row_bytes: args.state_row_cache_bytes,
        },
        quota_bytes: args.quota_backend_bytes,
        max_response_bytes: args.max_response_bytes as usize,
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
