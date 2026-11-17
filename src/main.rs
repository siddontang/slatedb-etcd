use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::{ensure, Context, Result};
use clap::Parser;
use slatedb_etcd::api::{router, ApiState};
use slatedb_etcd::node::{LogMemory, Node, NodeOptions};
use tokio::sync::mpsc;

#[derive(Parser)]
#[command(about = "Experimental etcd v3 service with Raft and SlateDB")]
struct Args {
    /// Persistent member ID. Keep it with its data directory.
    #[arg(long)]
    id: u64,
    /// Client listener address. Use a loopback IP and port.
    #[arg(long)]
    listen: SocketAddr,
    /// Separate peer listener address. Required when TLS is enabled.
    #[arg(long)]
    peer_listen: Option<SocketAddr>,
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
    /// Log entry cache budget in bytes.
    #[arg(long, default_value_t = 8 * 1024 * 1024)]
    log_cache_bytes: usize,
    /// Logical data quota in bytes. Zero disables the quota.
    #[arg(long, default_value_t = 0)]
    quota_backend_bytes: u64,
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

#[tokio::main(worker_threads = 4)]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .with_writer(std::io::stderr)
        .init();
    let args = Args::parse();
    let _ = tokio_rustls::rustls::crypto::ring::default_provider().install_default();
    let client_tls = slatedb_etcd::tls::Options {
        cert: args.cert_file.clone(),
        key: args.key_file.clone(),
        ca: args.trusted_ca_file.clone(),
        client_auth: args.client_cert_auth,
    }
    .server()?;
    let peer_options = slatedb_etcd::tls::Options {
        cert: args.peer_cert_file.clone(),
        key: args.peer_key_file.clone(),
        ca: args.peer_trusted_ca_file.clone(),
        client_auth: args.peer_cert_file.is_some(),
    };
    let peer_tls = peer_options.server()?;
    let transport = peer_options.transport()?;
    ensure!(
        args.peer_listen.is_some() || (client_tls.is_none() && peer_tls.is_none()),
        "TLS requires a separate peer listener"
    );
    ensure!(
        args.listen.ip().is_loopback(),
        "the service must bind to loopback"
    );
    let mut peers = BTreeMap::new();
    for peer in args.peers {
        let (id, address) = peer.split_once('=').context("peer must be ID=IP:PORT")?;
        let id = id.parse()?;
        let url = reqwest::Url::parse(&slatedb_etcd::cluster::url(address))?;
        let host = url.host_str().context("peer URL has no host")?;
        let socket: SocketAddr = format!(
            "{}:{}",
            host,
            url.port_or_known_default()
                .context("peer URL has no port")?
        )
        .parse()?;
        ensure!(
            socket.ip().is_loopback(),
            "PoC peers must use loopback addresses"
        );
        ensure!(
            peers
                .insert(
                    id,
                    if address.contains("://") {
                        address.to_owned()
                    } else {
                        socket.to_string()
                    }
                )
                .is_none(),
            "duplicate peer id"
        );
    }
    let peer_listen = args.peer_listen.unwrap_or(args.listen);
    ensure!(
        peer_listen.ip().is_loopback(),
        "peer listener must bind to loopback"
    );
    let own_peer = reqwest::Url::parse(&slatedb_etcd::cluster::url(
        peers.get(&args.id).context("missing local peer")?,
    ))?;
    ensure!(
        own_peer.port_or_known_default() == Some(peer_listen.port()),
        "peer listener differs from advertised peer port"
    );
    let mut client_peers = BTreeMap::new();
    for peer in args.client_peers {
        let (id, address) = peer.split_once('=').context("client peer must be ID=URL")?;
        client_peers.insert(id.parse()?, address.to_owned());
    }
    let listener = tokio::net::TcpListener::bind(args.listen).await?;
    if let Some(backup) = &args.restore_snapshot {
        let mut members = slatedb_etcd::cluster::Store::initial(&peers);
        for (&id, address) in &client_peers {
            if let Some(member) = members.members.get_mut(&id) {
                member.client_ur_ls = vec![slatedb_etcd::cluster::url(address)];
            }
        }
        slatedb_etcd::state::StateMachine::restore_backup(
            backup,
            &args.dir,
            args.id,
            &args.cluster_id,
            &members,
        )?;
    }
    let (tx, rx) = mpsc::channel(2048);
    let grpc = slatedb_etcd::grpc::service(
        args.id,
        tx.clone(),
        &peers,
        slatedb_etcd::auth::TokenIssuer::parse(&args.auth_token)?,
        transport.clone(),
    )?;
    let node = Node::open(
        NodeOptions {
            client_peers,
            transport,
            strict_reconfiguration: args.strict_reconfig_check,
            join: args.join,
            quota_bytes: args.quota_backend_bytes,
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
    let diagnostics = router(ApiState {
        events: tx,
        cluster_id: args.cluster_id,
    });
    let peer_app = diagnostics.clone().fallback_service(
        grpc.clone()
            .layer(axum::Extension(slatedb_etcd::tls::PeerServer)),
    );
    let app = if args.peer_listen.is_some() {
        grpc.layer(axum::Extension(slatedb_etcd::tls::ClientServer))
    } else {
        diagnostics.fallback_service(grpc)
    };
    let peer_server = async {
        if let Some(address) = args.peer_listen {
            let listener = tokio::net::TcpListener::bind(address).await?;
            slatedb_etcd::tls::serve(listener, peer_app, peer_tls).await
        } else {
            std::future::pending::<Result<()>>().await
        }
    };
    tracing::info!(node = args.id, listen = %args.listen, "Node started");
    tokio::select! {
        result = node.run(rx) => result,
        result = slatedb_etcd::tls::serve(listener, app, client_tls) => result,
        result = peer_server => result,
        result = tokio::signal::ctrl_c() => result.map_err(Into::into),
    }
}
