//! Start an etcd-compatible member inside an existing Tokio runtime.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{ensure, Context, Result};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;

use crate::api::{router, ApiState};
use crate::grpc::app::{App, Listener};
use crate::node::{Event, LogMemory, Node, NodeOptions};
use crate::proto::etcdserverpb::{request_op, RangeRequest, RequestOp};

/// Select the etcd Prometheus collection level.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, clap::ValueEnum)]
pub enum MetricsLevel {
    #[default]
    Basic,
    Extensive,
}

/// The default age limit of unflushed shared-mode writes.
pub const DEFAULT_VIEW_MAX_AGE: Duration = Duration::from_secs(10);
/// The default time that a shared-mode read view may pin its base checkpoint.
pub const DEFAULT_VIEW_PIN_MAX: Duration = Duration::from_secs(5 * 60);
/// The default soft limit of the shared-mode overlay.
pub const DEFAULT_OVERLAY_MAX_BYTES: u64 = 1 << 30;

/// The default quota usage fraction for the early warning.
pub const DEFAULT_QUOTA_WARNING_FRACTION: f64 = 0.8;

/// Configuration for one member. Preserve its ID, cluster ID, and directory across restarts.
#[derive(Clone)]
pub struct ServerConfig {
    pub id: u64,
    pub cluster_id: String,
    pub dir: PathBuf,
    pub listen: SocketAddr,
    pub peer_listen: Option<SocketAddr>,
    /// Extra HTTP or HTTPS listeners for Prometheus scrapes.
    pub listen_metrics_urls: Vec<String>,
    pub metrics: MetricsLevel,
    pub peers: BTreeMap<u64, String>,
    pub client_peers: BTreeMap<u64, String>,
    pub allow_remote: bool,
    pub client_tls: crate::tls::Options,
    pub peer_tls: crate::tls::Options,
    pub object_store: crate::object::Config,
    pub snapshot_every: u64,
    /// Interval between idle Watch progress responses.
    pub watch_progress_notify_interval: Duration,
    /// Automatic log truncation waits for this many appended log bytes.
    pub snapshot_min_bytes: u64,
    /// Size of one Raft log file.
    pub log_file_bytes: u64,
    pub log_memory: LogMemory,
    /// Weight budgets for cached SST blocks and SST metadata.
    pub state_cache: crate::state::StateCache,
    /// Zero selects the default 2 GiB quota. A negative value disables the quota.
    pub quota_bytes: i64,
    /// Quota usage fraction that raises the early warning. Zero disables the warning.
    pub quota_warning_fraction: f64,
    /// Automatic compaction retention, as etcd's `--auto-compaction-*` flags select it.
    /// `None` disables automatic compaction.
    pub auto_compaction: Option<crate::compactor::Retention>,
    pub auth_token: String,
    pub strict_reconfiguration: bool,
    pub join: bool,
    pub restore_snapshot: Option<PathBuf>,
    /// The state mode of a new cluster. Shared mode keeps one SlateDB for the cluster. Phase 1
    /// supports it for one voter only. An existing directory keeps its mode.
    pub state_mode: crate::state::StateMode,
    /// Shared mode: flush and publish a checkpoint when the oldest unflushed write is this old.
    pub view_max_age: Duration,
    /// Shared mode: a read fails with a retryable error when its view is older than this.
    pub view_pin_max: Duration,
    /// Shared mode: the leader rejects client proposals above this overlay size, and apply
    /// waits above twice this size.
    pub overlay_max_bytes: u64,
    /// Shared mode: a new leader does not start its writer while its clock is behind the
    /// shared writer clock by more than this.
    pub max_clock_skew: Duration,
    /// Shared mode: whether the leader deletes old view checkpoints. Off by default.
    pub view_retention: crate::state::ViewRetention,
}

impl ServerConfig {
    /// Create a local, single-member configuration with an assigned loopback port.
    pub fn single_node(dir: impl Into<PathBuf>) -> Self {
        let address = "127.0.0.1:0";
        Self {
            id: 1,
            cluster_id: "slatedb-raft-poc".into(),
            dir: dir.into(),
            listen: address.parse().unwrap(),
            peer_listen: None,
            listen_metrics_urls: Vec::new(),
            metrics: MetricsLevel::Basic,
            peers: BTreeMap::from([(1, address.into())]),
            client_peers: BTreeMap::from([(1, address.into())]),
            allow_remote: false,
            client_tls: crate::tls::Options::default(),
            peer_tls: crate::tls::Options::default(),
            object_store: crate::object::Config::Local,
            watch_progress_notify_interval: crate::grpc::DEFAULT_WATCH_PROGRESS_INTERVAL,
            snapshot_every: 10_000,
            snapshot_min_bytes: crate::node::DEFAULT_SNAPSHOT_MIN_BYTES,
            log_file_bytes: crate::storage::DEFAULT_LOG_FILE_BYTES,
            log_memory: LogMemory::default(),
            state_cache: crate::state::StateCache::default(),
            quota_bytes: 0,
            quota_warning_fraction: DEFAULT_QUOTA_WARNING_FRACTION,
            auto_compaction: None,
            auth_token: "simple".into(),
            strict_reconfiguration: true,
            join: false,
            restore_snapshot: None,
            state_mode: crate::state::StateMode::Replicated,
            view_max_age: DEFAULT_VIEW_MAX_AGE,
            view_pin_max: DEFAULT_VIEW_PIN_MAX,
            overlay_max_bytes: DEFAULT_OVERLAY_MAX_BYTES,
            max_clock_skew: Duration::from_secs(5),
            view_retention: crate::state::ViewRetention::Off,
        }
    }
}

/// An owned member. Call `shutdown` and await it before reusing its directory.
///
/// Dropping this handle requests shutdown. It cannot wait for background tasks.
/// The host must keep its Tokio runtime alive until shutdown finishes.
pub struct Server {
    config: ServerConfig,
    events: mpsc::Sender<Event>,
    stop: watch::Sender<bool>,
    task: Option<JoinHandle<Result<()>>>,
}

impl Server {
    /// Bind all listeners and recover storage. This does not wait for a Raft quorum.
    ///
    /// The host owns logging, process signals, and the Tokio runtime.
    pub async fn start(config: ServerConfig) -> Result<Self> {
        // A cancelled caller must not interrupt storage initialization or cleanup.
        tokio::spawn(Self::start_inner(config))
            .await
            .context("server startup panicked")?
    }

    async fn start_inner(mut config: ServerConfig) -> Result<Self> {
        let _ = tokio_rustls::rustls::crypto::ring::default_provider().install_default();
        config.log_memory.validate()?;
        config.state_cache.validate()?;
        ensure!(
            (0.0..=1.0).contains(&config.quota_warning_fraction),
            "the quota warning fraction must be from 0 to 1"
        );
        // A directly built retention gets the same zero and sign rules as the flags.
        config.auto_compaction = config
            .auto_compaction
            .map(crate::compactor::Retention::validate)
            .transpose()?
            .flatten();
        ensure!(
            !config.join || config.restore_snapshot.is_none(),
            "join cannot restore a snapshot"
        );
        ensure!(
            config.id != 0 && !config.peers.contains_key(&0),
            "invalid member ID"
        );
        let peer_address = config.peer_listen.unwrap_or(config.listen);
        let mut client_options = config.client_tls.clone();
        let mut peer_options = config.peer_tls.clone();
        let mut client_tls = client_options.server()?;
        let mut peer_tls = peer_options.server()?;
        let client_auto = client_tls.is_none() && client_options.auto_tls.is_some();
        let peer_auto = peer_tls.is_none() && peer_options.auto_tls.is_some();
        let client_secure = client_tls.is_some() || client_auto;
        let peer_secure = peer_tls.is_some() || peer_auto;
        if !peer_auto {
            peer_options.auto_tls = None;
        }
        let mut transport = peer_options.transport()?;
        let tokens = crate::auth::TokenIssuer::parse(&config.auth_token)?;
        ensure!(
            config.peer_listen.is_some() || (!client_secure && !peer_secure),
            "TLS requires a separate peer listener"
        );
        ensure!(
            !peer_secure || peer_options.client_auth || peer_auto,
            "peer TLS requires mutual authentication"
        );
        if config.allow_remote {
            ensure!(
                config.peer_listen.is_some()
                    && client_tls.is_some()
                    && config.client_tls.client_auth
                    && peer_tls.is_some()
                    && peer_options.client_auth,
                "remote mode requires separate listeners and mutual client and peer TLS"
            );
        }
        ensure!(
            config.allow_remote || config.listen.ip().is_loopback(),
            "remote listeners require allow_remote"
        );
        ensure!(
            config.allow_remote || peer_address.ip().is_loopback(),
            "remote peer listeners require allow_remote"
        );
        for address in config.peers.values() {
            crate::tls::validate_url(address, config.allow_remote, peer_secure)?;
        }
        for address in config.client_peers.values() {
            crate::tls::validate_url(address, config.allow_remote, client_secure)?;
        }
        ensure!(
            !config.allow_remote
                || config
                    .peers
                    .keys()
                    .all(|id| config.client_peers.contains_key(id)),
            "remote mode requires a client URL for every initial peer"
        );
        let own_peer = config.peers.get(&config.id).context("missing local peer")?;
        let own_peer = reqwest::Url::parse(&crate::cluster::url(own_peer))?;
        ensure!(
            own_peer.port_or_known_default() == Some(peer_address.port()),
            "peer listener differs from advertised peer port"
        );
        // Bind both sockets before opening storage or starting any member tasks.
        let mut metrics_listeners = Vec::with_capacity(config.listen_metrics_urls.len());
        for address in &mut config.listen_metrics_urls {
            let url = reqwest::Url::parse(address)?;
            crate::tls::validate_url(address, config.allow_remote, false)?;
            let secure = url.scheme() == "https";
            ensure!(
                !secure || client_secure,
                "HTTPS metrics require client TLS settings"
            );
            let host = url
                .host_str()
                .context("metrics URL has no host")?
                .trim_matches(['[', ']']);
            let port = url
                .port_or_known_default()
                .context("metrics URL has no port")?;
            let listener = TcpListener::bind((host, port)).await?;
            let bound = listener.local_addr()?;
            ensure!(
                bound.ip().is_loopback() || (config.allow_remote && secure),
                "remote metrics require remote mode and client TLS"
            );
            *address = format!("{}://{bound}", url.scheme());
            metrics_listeners.push((listener, secure));
        }
        let listener = TcpListener::bind(config.listen).await?;
        config.listen = listener.local_addr()?;
        let peer_listener = match config.peer_listen {
            Some(address) => {
                let listener = TcpListener::bind(address).await?;
                config.peer_listen = Some(listener.local_addr()?);
                Some(listener)
            }
            None => None,
        };
        resolve_port(
            config.peers.get_mut(&config.id).unwrap(),
            config.peer_listen.unwrap_or(config.listen),
        )?;
        if let Some(address) = config.client_peers.get_mut(&config.id) {
            resolve_port(address, config.listen)?;
        }
        let mut restore = None;
        if let Some(backup) = config.restore_snapshot.take() {
            let mut members = crate::cluster::Store::initial(&config.peers);
            for (&id, address) in &config.client_peers {
                if let Some(member) = members.members.get_mut(&id) {
                    member.client_ur_ls = vec![crate::cluster::url(address)];
                }
            }
            if config.state_mode.is_replicated() {
                crate::state::StateMachine::restore_backup(
                    &backup,
                    &config.dir,
                    config.id,
                    &config.cluster_id,
                    &members,
                )?;
            } else {
                // A new shared cluster gets the rows in its shared database when it opens.
                restore = Some(crate::state::StateMachine::restore_shared(
                    &backup,
                    &config.dir,
                    config.id,
                    &members,
                )?);
            }
        }
        // Snapshot restore must see the original data directory before AutoTLS creates fixtures.
        if client_auto {
            client_options.prepare(&config.dir, "client", config.listen.ip())?;
            client_tls = client_options.server()?;
        }
        if peer_auto {
            peer_options.prepare(&config.dir, "peer", peer_address.ip())?;
            peer_tls = peer_options.server()?;
            transport = peer_options.transport()?;
        }
        let (events, receiver) = mpsc::channel(2048);
        let servers = crate::grpc::Servers::new(
            config.id,
            events.clone(),
            &config.peers,
            tokens,
            transport.clone(),
            config.watch_progress_notify_interval,
        )?;
        let node = Node::open(
            NodeOptions {
                id: config.id,
                cluster_id: config.cluster_id.clone(),
                dir: config.dir.clone(),
                peers: config.peers.clone(),
                client_peers: config.client_peers.clone(),
                snapshot_every: config.snapshot_every,
                snapshot_min_bytes: config.snapshot_min_bytes,
                log_file_bytes: config.log_file_bytes,
                log_memory: config.log_memory,
                state_cache: config.state_cache,
                quota_bytes: config.quota_bytes,
                quota_warning_fraction: config.quota_warning_fraction,
                auto_compaction: config.auto_compaction,
                strict_reconfiguration: config.strict_reconfiguration,
                join: config.join,
                object_store: config.object_store.clone(),
                require_peer_tls: config.allow_remote,
                state_mode: config.state_mode,
                view_max_age: config.view_max_age,
                view_pin_max: config.view_pin_max,
                overlay_max_bytes: config.overlay_max_bytes,
                max_clock_skew: config.max_clock_skew,
                view_retention: config.view_retention,
                restore,
                transport,
            },
            events.clone(),
        )
        .await?;
        let diagnostics = router(ApiState {
            events: events.clone(),
            cluster_id: config.cluster_id.clone(),
            location: Some(node.location.clone()),
            shared: node.shared_source(),
        });
        // Peer gRPC has no RPC metrics; diagnostics paths never collide with gRPC services.
        let peer_app = App::new(
            servers.clone(),
            Some(Listener::Peer),
            None,
            None,
            diagnostics.clone(),
        );
        let metrics_app = node.metrics.scrape_router(events.clone());
        let (watch_meters, rpc) = node.metrics.rpc(config.metrics)?;
        let scrape = node.metrics.scrape_router(events.clone());
        let client_app = if peer_listener.is_some() {
            App::new(
                servers,
                Some(Listener::Client),
                Some(watch_meters),
                Some(rpc),
                scrape,
            )
        } else {
            App::new(
                servers,
                None,
                Some(watch_meters),
                Some(rpc),
                diagnostics.fallback_service(scrape),
            )
        };
        let (stop, _) = watch::channel(false);
        let mut tasks = tokio::task::JoinSet::new();
        for (metrics_listener, secure) in metrics_listeners {
            let tls = if secure { client_tls.clone() } else { None };
            tasks.spawn(crate::tls::serve_until(
                metrics_listener,
                metrics_app.clone(),
                tls,
                stopped(stop.subscribe()),
            ));
        }
        tasks.spawn(node.run_until(receiver, stopped(stop.subscribe())));
        tasks.spawn(crate::tls::serve_until(
            listener,
            client_app,
            client_tls,
            stopped(stop.subscribe()),
        ));
        if let Some(listener) = peer_listener {
            tasks.spawn(crate::tls::serve_until(
                listener,
                peer_app,
                peer_tls,
                stopped(stop.subscribe()),
            ));
        }
        let shutdown = stop.clone();
        let task = tokio::spawn(async move {
            let first = tasks.join_next().await.context("server has no tasks")?;
            shutdown.send_replace(true);
            let mut result = first.context("server task panicked").and_then(|r| r);
            while let Some(next) = tasks.join_next().await {
                result = result.and(next.context("server task panicked").and_then(|r| r));
            }
            result
        });
        Ok(Self {
            config,
            events,
            stop,
            task: Some(task),
        })
    }

    /// Get the effective configuration, including assigned ports.
    pub fn config(&self) -> &ServerConfig {
        &self.config
    }

    /// Get the bound client socket address.
    pub fn client_addr(&self) -> SocketAddr {
        self.config.listen
    }

    /// Get the bound peer socket address.
    pub fn peer_addr(&self) -> SocketAddr {
        self.config.peer_listen.unwrap_or(self.config.listen)
    }

    /// Get the local client endpoint. Remote hosts must use the advertised client URL.
    pub fn client_url(&self) -> String {
        let scheme =
            if self.config.client_tls.cert.is_some() || self.config.client_tls.auto_tls.is_some() {
                "https"
            } else {
                "http"
            };
        format!("{scheme}://{}", self.client_addr())
    }

    /// Wait for a quorum-confirmed read and local apply progress within the given timeout.
    ///
    /// Readiness is a point-in-time result. It does not guarantee future availability.
    pub async fn wait_ready(&self, timeout: Duration) -> Result<()> {
        tokio::time::timeout(timeout, async {
            loop {
                ensure!(!*self.stop.borrow(), "server stopped");
                let (tx, rx) = oneshot::channel();
                // This internal read returns no user data and does not need client credentials.
                let request = RequestOp {
                    request: Some(request_op::Request::RequestRange(RangeRequest {
                        key: vec![0],
                        count_only: true,
                        limit: 1,
                        ..Default::default()
                    })),
                };
                self.events
                    .send(Event::LinearRead(request, tx))
                    .await
                    .context("server stopped")?;
                if let Ok(value) = rx.await.context("server stopped")? {
                    let outcome: crate::mvcc::Outcome = serde_json::from_value(value)?;
                    if outcome.result.is_ok() {
                        return Ok(());
                    }
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .context("server readiness timed out")?
    }

    /// Wait for termination and return any server failure. Cancellation leaves the handle usable.
    pub async fn wait(&mut self) -> Result<()> {
        let result = self
            .task
            .as_mut()
            .context("server termination already collected")?
            .await;
        self.task.take();
        result.context("server supervisor panicked")?
    }

    /// Stop connections and workers, then release the data directory lock.
    ///
    /// In-flight requests can fail with an unknown result. Acknowledged writes remain in Raft.
    /// Storage I/O can delay shutdown. Keep this future running until it returns.
    pub async fn shutdown(mut self) -> Result<()> {
        self.stop.send_replace(true);
        self.wait().await
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.stop.send_replace(true);
    }
}

async fn stopped(mut stop: watch::Receiver<bool>) {
    let _ = stop.wait_for(|value| *value).await;
}

fn resolve_port(address: &mut String, listener: SocketAddr) -> Result<()> {
    let mut url = reqwest::Url::parse(&crate::cluster::url(address))?;
    if url.port() == Some(0) {
        url.set_port(Some(listener.port()))
            .map_err(|_| anyhow::anyhow!("invalid service port"))?;
        *address = url.as_str().trim_end_matches('/').to_owned();
    }
    Ok(())
}
