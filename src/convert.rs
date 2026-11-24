//! Convert a stopped replicated S3 cluster to shared state mode, offline.
//!
//! Stop every member first. Then run the conversion once on each member host, with the data
//! directory of that member.
//!
//! 1. Preflight. The first run reads the flushed index of each replicated member namespace from
//!    a detached checkpoint, and selects the member with the highest index. Different indexes log
//!    a warning. It checks the index, the term at the index, the membership, and the sequence of
//!    the copied manifest. A failed check changes no claim, so the replicated cluster stays usable.
//! 2. Plan. The run writes `shared/conversion.json` with `PutMode::Create`. It records the source
//!    member, its checkpoint, the index, the term, and the active membership. Every later or
//!    concurrent run uses this plan. A run that loses the claim deletes only its own detached
//!    checkpoints.
//! 3. Seed. Until the bootstrap marker exists, a run copies the SSTs and the manifest of the
//!    planned checkpoint into `shared/state`, changes the mode claim to shared, writes the cluster
//!    claim with the bootstrap voters, publishes the first view checkpoint, retires the owner
//!    claims of members outside the active membership, and writes the marker. Each step can run
//!    again. No step deletes a view checkpoint.
//! 4. Member. Each run adopts the view checkpoint as the Raft snapshot record of its local log,
//!    writes the shared binding claim, rewrites the local binding, and deletes its replicated
//!    owner claim. A shared member refuses to start while an owner claim remains, so the cluster
//!    starts only after every active member is converted.
//!
//! The replicated state objects stay in place. Delete them after a check.

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{bail, ensure, Context, Result};
use serde::{Deserialize, Serialize};
use slatedb::object_store::path::Path;
use slatedb::object_store::{ObjectStore, ObjectStoreExt, PutMode};

use raft::Storage as _;

use crate::state::transfer::{Backend, Location, ViewDescriptor};
use crate::state::{shared, StateCache, StateMode};

/// The cluster-wide conversion plan, under the cluster prefix.
const PLAN: &str = "shared/conversion.json";
/// The name of the detached checkpoints of the conversion.
const CHECKPOINT: &str = "convert-to-shared";

/// One member's conversion.
pub struct Conversion {
    pub id: u64,
    pub dir: PathBuf,
    pub cluster_id: String,
    /// The quota of the shared cluster claim, in bytes.
    pub quota: u64,
    /// A test store in place of S3.
    #[cfg(test)]
    pub(crate) store: Option<Arc<dyn ObjectStore>>,
    /// A test pause after the plan read: (reached, resume).
    #[cfg(test)]
    pub(crate) pause: Option<(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>)>,
    /// A test that hides the applied term of the source state.
    #[cfg(test)]
    pub(crate) hide_term: bool,
}

/// The result of one member's conversion.
#[derive(Debug)]
pub struct Converted {
    /// The Raft index and term of the base checkpoint.
    pub index: u64,
    pub term: u64,
    /// The member whose state seeded the shared state.
    pub source: u64,
    /// True when this run wrote the conversion plan.
    pub planned: bool,
}

/// The conversion plan. See the module comment.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct Plan {
    version: u32,
    cluster: String,
    source: u64,
    /// The source database root and its detached checkpoint.
    root: String,
    checkpoint: uuid::Uuid,
    manifest: u64,
    ssts: Vec<String>,
    index: u64,
    term: u64,
    /// The active membership of the copied state, as JSON.
    members: Vec<u8>,
    /// The bootstrap voters of the Raft logs. Restarts compare them with the cluster claim.
    voters: Vec<u64>,
}

impl Conversion {
    pub fn new(id: u64, dir: PathBuf, cluster_id: String, quota: u64) -> Self {
        Self {
            id,
            dir,
            cluster_id,
            quota,
            #[cfg(test)]
            store: None,
            #[cfg(test)]
            pause: None,
            #[cfg(test)]
            hide_term: false,
        }
    }

    pub async fn run(&self) -> Result<Converted> {
        let raft = self.dir.join("raft");
        ensure!(raft.exists(), "the data directory has no Raft log");
        // The Raft log lock refuses a running member.
        let (_, _, voters) = crate::storage::RaftStorage::stored_identity(&raft)
            .context("cannot open the Raft log; stop the member first")?;
        let mut storage =
            crate::storage::RaftStorage::open(&raft, self.id, &self.cluster_id, voters.clone())?;
        let (s3, mode) = crate::object::converted_s3(&self.dir, &self.cluster_id, self.id)?;
        #[cfg(test)]
        let store = match &self.store {
            Some(store) => store.clone(),
            None => s3.store()?,
        };
        #[cfg(not(test))]
        let store = s3.store()?;
        let root = s3.cluster_prefix(&self.cluster_id);
        let backend = Backend::S3 {
            bucket: s3.bucket.clone(),
            endpoint: s3.endpoint.clone(),
            region: s3.region.clone(),
        };
        let cache = StateCache::default();
        let source = shared::offline_source(store.clone(), &root, cache)?;
        let mut planned = false;
        let plan = match read_plan(store.as_ref(), &root).await? {
            Some(plan) => plan,
            None => {
                #[cfg(test)]
                if let Some((reached, resume)) = &self.pause {
                    reached.notify_one();
                    resume.notified().await;
                }
                ensure!(
                    mode == StateMode::Replicated,
                    "the member is converted, but the cluster has no conversion plan"
                );
                let preflight = self
                    .preflight(&store, &root, &backend, cache, &storage, voters)
                    .await;
                match preflight {
                    Ok(plan) => match create_plan(store.as_ref(), &root, &plan).await? {
                        None => {
                            planned = true;
                            plan
                        }
                        // Another run planned first. Its plan holds.
                        Some(other) => {
                            delete_checkpoint(&store, &plan.root, plan.checkpoint).await?;
                            other
                        }
                    },
                    // A concurrent run can plan and seed while this preflight runs.
                    Err(error) => match read_plan(store.as_ref(), &root).await? {
                        Some(plan) => plan,
                        None => return Err(error),
                    },
                }
            }
        };
        ensure!(
            plan.cluster == self.cluster_id,
            "the conversion plan belongs to another cluster"
        );
        let members: crate::cluster::Store = serde_json::from_slice(&plan.members)?;
        ensure!(
            members.members.contains_key(&self.id),
            "member {} is not in the converted membership",
            self.id
        );
        if !source.bootstrapped().await? {
            self.seed(&store, &root, &source, &plan, &members).await?;
        }
        let checkpoint = view_checkpoint(&source, plan.index)
            .await?
            .context("the shared state has no view checkpoint at the planned index")?;
        let mut snapshot = raft::eraftpb::Snapshot::default();
        snapshot.mut_metadata().index = plan.index;
        snapshot.mut_metadata().term = plan.term;
        snapshot.mut_metadata().set_conf_state(members.conf());
        snapshot.data = ViewDescriptor::new(checkpoint).encode()?;
        // The log keeps its entries above the checkpoint when its term matches there. A record
        // that an earlier run installed stays, because it is equal.
        storage.replace_record(&snapshot)?;
        drop(storage);
        crate::object::convert_binding(&self.dir, store.as_ref(), &self.cluster_id, self.id)
            .await?;
        Ok(Converted {
            index: plan.index,
            term: plan.term,
            source: plan.source,
            planned,
        })
    }

    /// Select the source and check everything before any claim changes. Each member namespace
    /// gets a detached checkpoint. Only the one of the selected source stays.
    async fn preflight(
        &self,
        store: &Arc<dyn ObjectStore>,
        root: &str,
        backend: &Backend,
        cache: StateCache,
        storage: &crate::storage::RaftStorage,
        voters: Vec<u64>,
    ) -> Result<Plan> {
        // The plan keeps the bootstrap voters for the restart checks. A member that joined later
        // has none in its log identity.
        ensure!(
            !voters.is_empty(),
            "member {} joined after the bootstrap; run the first conversion on a bootstrap voter",
            self.id
        );
        let namespaces = replicated_members(store.as_ref(), root).await?;
        ensure!(
            !namespaces.is_empty(),
            "the cluster has no replicated member namespace"
        );
        let options = slatedb::config::CheckpointOptions {
            name: Some(CHECKPOINT.to_owned()),
            ..Default::default()
        };
        let mut states = vec![];
        let mut checkpoints = vec![];
        let result = async {
            for &member in &namespaces {
                let location = Location::open(
                    store.clone(),
                    &format!("{root}/member-{member}/state"),
                    backend.clone(),
                    cache,
                )
                .await?;
                let db = location.root();
                let admin = slatedb::admin::Admin::builder(db.as_str(), store.clone()).build();
                let checkpoint = admin.create_detached_checkpoint(&options).await?;
                checkpoints.push((db.clone(), checkpoint.id));
                let base = crate::state::view::Base::open(
                    store.clone(),
                    &db,
                    checkpoint.id,
                    cache.build(),
                )
                .await?;
                states.push((base.applied, member, location, db, checkpoint, base));
            }
            states.sort_by_key(|(applied, member, ..)| (*applied, std::cmp::Reverse(*member)));
            let (applied, member, location, db, checkpoint, base) = states.pop().unwrap();
            ensure!(
                applied > 0,
                "no member has a flushed state; run POST /checkpoint on a member before the stop"
            );
            if states.iter().any(|(other, ..)| *other != applied) {
                let indexes: Vec<_> = states.iter().map(|(a, m, ..)| (*m, *a)).collect();
                tracing::warn!(
                    member,
                    applied,
                    ?indexes,
                    "the members have different flushed indexes; using the highest"
                );
            }
            let (term, members) = shared::identity_parts(&base).await?;
            #[cfg(test)]
            let term = if self.hide_term { 0 } else { term };
            // An older binary did not store the term. The local log can still hold it.
            let term = match term {
                0 => local_term(storage, applied).with_context(|| {
                    format!(
                        "the state at index {applied} has no term, and this member's log does \
                         not hold it; run the conversion on member {member} first"
                    )
                })?,
                term => term,
            };
            let members = members.context("the source state has no membership")?;
            let membership: crate::cluster::Store = serde_json::from_slice(&members)?;
            ensure!(
                membership.members.contains_key(&self.id),
                "member {} is not in the membership of the source state",
                self.id
            );
            let admin = slatedb::admin::Admin::builder(db.as_str(), store.clone()).build();
            let manifest = admin
                .read_manifest(Some(checkpoint.manifest_id))
                .await?
                .context("missing source manifest")?;
            ensure!(
                manifest.last_l0_seq() == applied,
                "the source state ends at sequence {}, not at index {applied}",
                manifest.last_l0_seq()
            );
            let descriptor = location
                .describe(
                    db.clone(),
                    checkpoint.id,
                    checkpoint.manifest_id,
                    applied,
                    vec![],
                )
                .await?;
            let descriptor = crate::state::transfer::Descriptor::decode(&descriptor)?
                .context("invalid conversion descriptor")?;
            Ok(Plan {
                version: 1,
                cluster: self.cluster_id.clone(),
                source: member,
                root: db,
                checkpoint: checkpoint.id,
                manifest: checkpoint.manifest_id,
                ssts: descriptor.ssts,
                index: applied,
                term,
                members,
                voters,
            })
        }
        .await;
        // Keep only the checkpoint of the plan.
        let keep = result.as_ref().ok().map(|plan| plan.checkpoint);
        for (db, id) in checkpoints {
            if Some(id) != keep {
                delete_checkpoint(store, &db, id).await?;
            }
        }
        result
    }

    /// Seed `shared/state` from the plan. Each step can run again, also at the same time on
    /// another host. No step deletes a view checkpoint.
    async fn seed(
        &self,
        store: &Arc<dyn ObjectStore>,
        root: &str,
        source: &shared::Source,
        plan: &Plan,
        members: &crate::cluster::Store,
    ) -> Result<()> {
        let target = source.state();
        let mut objects = plan.ssts.clone();
        objects.push(format!("manifest/{:020}.manifest", plan.manifest));
        let admin = slatedb::admin::Admin::builder(target.as_str(), store.clone()).build();
        if admin.read_manifest(None).await?.is_none() {
            for relative in objects {
                store
                    .copy(
                        &Path::from(format!("{}/{relative}", plan.root)),
                        &Path::from(format!("{target}/{relative}")),
                    )
                    .await?;
            }
        }
        // The copied manifest lists the source checkpoints. Their manifests were not copied.
        for listed in admin.list_checkpoints(None).await? {
            if !listed.name.as_deref().unwrap_or("").starts_with("view-") {
                admin.delete_checkpoint(listed.id).await?;
            }
        }
        shared::convert_mode(store.as_ref(), root, &self.cluster_id).await?;
        shared::claim(
            store.as_ref(),
            root,
            &self.cluster_id,
            self.quota,
            &plan.voters,
        )
        .await?;
        let view = match view_checkpoint(source, plan.index).await? {
            Some(view) => view,
            None => {
                let writer = source.open_writer().await?;
                let last = writer.manifest().last_l0_seq();
                ensure!(
                    last == plan.index,
                    "the copied state ends at sequence {last}, not at index {}",
                    plan.index
                );
                let options = slatedb::config::CheckpointOptions {
                    name: Some(shared::view_name(last)),
                    ..Default::default()
                };
                let view = writer
                    .create_checkpoint(slatedb::config::CheckpointScope::Durable, &options)
                    .await?;
                writer.close().await?;
                view.id
            }
        };
        // Members outside the active membership were removed. They convert no more.
        for member in replicated_members(store.as_ref(), root).await? {
            if !members.members.contains_key(&member) {
                tracing::info!(member, "retire the owner claim of a removed member");
                delete(
                    store.as_ref(),
                    &format!("{root}/member-{member}/owner.json"),
                )
                .await?;
            }
        }
        if let Err(error) = source.mark_bootstrapped(view).await {
            if !source.bootstrapped().await? {
                return Err(error);
            }
        }
        delete_checkpoint(store, &plan.root, plan.checkpoint).await
    }
}

/// The Raft snapshot record of a stopped member: its index, term, and, in shared mode, the view
/// checkpoint that it names. Retention and manual cleanup must keep that checkpoint.
pub fn member_record(dir: &std::path::Path) -> Result<serde_json::Value> {
    let raft = dir.join("raft");
    ensure!(raft.exists(), "the data directory has no Raft log");
    let storage = crate::storage::RaftStorage::open_existing(&raft)
        .context("cannot open the Raft log; stop the member first")?;
    let record = storage.recovery_snapshot();
    let checkpoint = ViewDescriptor::decode(record.get_data())?
        .map(|view| view.checkpoint())
        .transpose()?;
    Ok(serde_json::json!({
        "index": record.get_metadata().index,
        "term": record.get_metadata().term,
        "checkpoint": checkpoint,
    }))
}

/// The term of `index` in a local log, only with committed evidence: the snapshot record at
/// `index`, or an entry at `index` at or below the durable commit index. An uncommitted suffix
/// can differ from the committed entry.
pub(crate) fn local_term(storage: &crate::storage::RaftStorage, index: u64) -> Option<u64> {
    if storage.snapshot_index() == index {
        return Some(storage.recovery_snapshot().get_metadata().term);
    }
    if storage.commit_index() < index {
        return None;
    }
    storage.term(index).ok().filter(|term| *term != 0)
}

/// The newest view checkpoint at `index`.
async fn view_checkpoint(source: &shared::Source, index: u64) -> Result<Option<uuid::Uuid>> {
    let admin =
        slatedb::admin::Admin::builder(source.state().as_str(), source.store.clone()).build();
    Ok(shared::view_checkpoints(&admin)
        .await?
        .into_iter()
        .rev()
        .find(|(seq, _)| *seq == index)
        .map(|(_, checkpoint)| checkpoint.id))
}

async fn read_plan(store: &dyn ObjectStore, root: &str) -> Result<Option<Plan>> {
    match store.get(&Path::from(format!("{root}/{PLAN}"))).await {
        Ok(result) => Ok(Some(
            serde_json::from_slice(&result.bytes().await?).context("invalid conversion plan")?,
        )),
        Err(slatedb::object_store::Error::NotFound { .. }) => Ok(None),
        Err(error) => Err(error.into()),
    }
}

/// Write `plan` once. Return the plan of an earlier run, if one exists.
async fn create_plan(store: &dyn ObjectStore, root: &str, plan: &Plan) -> Result<Option<Plan>> {
    let path = Path::from(format!("{root}/{PLAN}"));
    match store
        .put_opts(
            &path,
            serde_json::to_vec(plan)?.into(),
            PutMode::Create.into(),
        )
        .await
    {
        Ok(_) => Ok(None),
        Err(slatedb::object_store::Error::AlreadyExists { .. }) => {
            match read_plan(store, root).await? {
                Some(other) => Ok(Some(other)),
                None => bail!("the conversion plan disappeared"),
            }
        }
        Err(error) => Err(error.into()),
    }
}

async fn delete_checkpoint(store: &Arc<dyn ObjectStore>, db: &str, id: uuid::Uuid) -> Result<()> {
    let admin = slatedb::admin::Admin::builder(db, store.clone()).build();
    if admin
        .list_checkpoints(None)
        .await?
        .iter()
        .any(|c| c.id == id)
    {
        admin.delete_checkpoint(id).await?;
    }
    Ok(())
}

async fn delete(store: &dyn ObjectStore, path: &str) -> Result<()> {
    match store.delete(&Path::from(path)).await {
        Ok(()) | Err(slatedb::object_store::Error::NotFound { .. }) => Ok(()),
        Err(error) => Err(error.into()),
    }
}

/// The members with a replicated owner claim under `root`.
async fn replicated_members(store: &dyn ObjectStore, root: &str) -> Result<Vec<u64>> {
    let listing = store.list_with_delimiter(Some(&Path::from(root))).await?;
    let mut members = vec![];
    for prefix in listing.common_prefixes {
        let Some(member) = prefix
            .filename()
            .and_then(|name| name.strip_prefix("member-"))
            .and_then(|id| id.parse::<u64>().ok())
        else {
            continue;
        };
        match store.head(&prefix.clone().join("owner.json")).await {
            Ok(_) => members.push(member),
            Err(slatedb::object_store::Error::NotFound { .. }) => {}
            Err(error) => return Err(error.into()),
        }
    }
    members.sort_unstable();
    Ok(members)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::etcdserverpb::{
        kv_client::KvClient, maintenance_client::MaintenanceClient, HashKvRequest, PutRequest,
        RangeRequest, StatusRequest,
    };
    use crate::server::{Server, ServerConfig};
    use slatedb::object_store::memory::InMemory;
    use std::time::Duration;

    fn s3() -> crate::object::S3 {
        crate::object::S3 {
            bucket: "test-bucket".into(),
            region: "us-east-1".into(),
            endpoint: Some("http://127.0.0.1:9000".into()),
            prefix: "slatedb-etcd".into(),
            allow_http: true,
        }
    }

    /// A free local port below the Linux ephemeral range, never given out twice in this process.
    /// A port 0 listener returns its port to the pool, and a parallel test can take it.
    fn port() -> String {
        use std::sync::atomic::{AtomicU16, Ordering};
        static NEXT: AtomicU16 = AtomicU16::new(0);
        let _ = NEXT.compare_exchange(
            0,
            21_000 + (std::process::id() % 1_000) as u16 * 10,
            Ordering::Relaxed,
            Ordering::Relaxed,
        );
        loop {
            let port = NEXT.fetch_add(1, Ordering::Relaxed);
            assert!(port < 32_000, "no free test port");
            if std::net::TcpListener::bind(("127.0.0.1", port)).is_ok() {
                return format!("127.0.0.1:{port}");
            }
        }
    }

    async fn addresses() -> std::collections::BTreeMap<u64, String> {
        (1..=3).map(|id| (id, port())).collect()
    }

    fn config(
        dir: &std::path::Path,
        id: u64,
        peers: &std::collections::BTreeMap<u64, String>,
        store: &Arc<dyn ObjectStore>,
        mode: StateMode,
    ) -> ServerConfig {
        let mut config = ServerConfig::single_node(dir.join(id.to_string()));
        config.id = id;
        config.listen = peers[&id].parse().unwrap();
        config.peers = peers.clone();
        config.client_peers = peers.clone();
        config.object_store = crate::object::Config::TestS3(store.clone(), s3());
        config.state_mode = mode;
        config.view_max_age = Duration::from_millis(200);
        // The health check needs peer traffic first. A test removes a member at once.
        config.strict_reconfiguration = false;
        config
    }

    async fn start(configs: &[ServerConfig]) -> Vec<Server> {
        let mut servers = vec![];
        for config in configs {
            servers.push(Server::start(config.clone()).await.unwrap());
        }
        for server in &servers {
            server.wait_ready(Duration::from_secs(20)).await.unwrap();
        }
        servers
    }

    async fn client(server: &Server) -> tonic::transport::Channel {
        tonic::transport::Endpoint::from_shared(server.client_url())
            .unwrap()
            .connect()
            .await
            .unwrap()
    }

    /// Write keys through the leader and return the hash of every member at the last revision.
    async fn hashes(servers: &[Server], revision: i64) -> Vec<u32> {
        let mut hashes = vec![];
        for server in servers {
            let mut hash = None;
            for _ in 0..200 {
                let channel = client(server).await;
                let local = KvClient::new(channel.clone())
                    .range(RangeRequest {
                        key: vec![0],
                        range_end: vec![0],
                        serializable: true,
                        ..Default::default()
                    })
                    .await
                    .unwrap()
                    .into_inner();
                if local.header.unwrap().revision >= revision {
                    hash = Some(
                        MaintenanceClient::new(channel)
                            .hash_kv(HashKvRequest { revision })
                            .await
                            .unwrap()
                            .into_inner()
                            .hash,
                    );
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            hashes.push(hash.expect("a member reached the revision"));
        }
        hashes
    }

    /// The position of the leader in `servers`.
    async fn leader(servers: &[Server]) -> usize {
        for _ in 0..100 {
            for (position, server) in servers.iter().enumerate() {
                let status = MaintenanceClient::new(client(server).await)
                    .status(StatusRequest {})
                    .await
                    .unwrap()
                    .into_inner();
                if status.leader != 0 && status.header.unwrap().member_id == status.leader {
                    return position;
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("no leader");
    }

    async fn write(servers: &[Server], count: usize) -> i64 {
        let mut kv = KvClient::new(client(&servers[leader(servers).await]).await);
        let mut revision = 0;
        for n in 0..count {
            revision = kv
                .put(PutRequest {
                    key: format!("key-{n}").into_bytes(),
                    value: format!("value-{n}").into_bytes(),
                    ..Default::default()
                })
                .await
                .unwrap()
                .into_inner()
                .header
                .unwrap()
                .revision;
        }
        revision
    }

    fn conversion(dir: &std::path::Path, store: &Arc<dyn ObjectStore>, id: u64) -> Conversion {
        let mut conversion =
            Conversion::new(id, dir.join(id.to_string()), "slatedb-raft-poc".into(), 0);
        conversion.store = Some(store.clone());
        conversion
    }

    /// Frequent log truncation flushes each member's state at its own index. The conversion
    /// takes the highest one, and the other members replay their logs above it.
    fn replicated(
        dir: &std::path::Path,
        peers: &std::collections::BTreeMap<u64, String>,
        store: &Arc<dyn ObjectStore>,
    ) -> Vec<ServerConfig> {
        (1..=3)
            .map(|id| {
                let mut config = config(dir, id, peers, store, StateMode::Replicated);
                config.snapshot_every = 16;
                config.snapshot_min_bytes = 0;
                config
            })
            .collect()
    }

    async fn mode(store: &Arc<dyn ObjectStore>) -> String {
        let root = s3().cluster_prefix("slatedb-raft-poc");
        let data = store
            .get(&Path::from(format!("{root}/mode.json")))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        serde_json::from_slice::<serde_json::Value>(&data).unwrap()["mode"]
            .as_str()
            .unwrap()
            .to_owned()
    }

    /// A stopped three-member replicated cluster converts to shared mode. The converter refuses
    /// a running member, and a shared member does not start before every member is converted.
    /// The first run finds no term in the source state and takes it from its own log. After the
    /// conversion the members serve the same data and hash, and accept writes.
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn a_stopped_replicated_cluster_converts_to_shared_mode() {
        let dir = tempfile::tempdir().unwrap();
        let faults = Arc::new(crate::object::faults::FaultStore::new(Arc::new(
            InMemory::new(),
        )));
        let store: Arc<dyn ObjectStore> = faults.clone();
        let peers = addresses().await;
        let servers = start(&replicated(dir.path(), &peers, &store)).await;
        let revision = write(&servers, 50).await;
        let before = hashes(&servers, revision).await;
        let error = conversion(dir.path(), &store, 1).run().await.unwrap_err();
        assert!(error.to_string().contains("stop the member"), "{error:#}");
        for server in servers {
            server.shutdown().await.unwrap();
        }
        let shared: Vec<_> = (1..=3)
            .map(|id| config(dir.path(), id, &peers, &store, StateMode::Shared))
            .collect();
        faults.written.lock().unwrap().clear();
        let mut first = conversion(dir.path(), &store, 1);
        first.hide_term = true;
        let first = first.run().await.unwrap();
        assert!(first.planned && first.term > 0);
        // Members 2 and 3 still have replicated owner claims.
        let error = Server::start(shared[0].clone()).await.err().unwrap();
        assert!(
            format!("{error:#}").contains("Replicated state mode"),
            "{error:#}"
        );
        for id in 2..=3 {
            let converted = conversion(dir.path(), &store, id).run().await.unwrap();
            assert_eq!(
                (converted.index, converted.term, converted.planned),
                (first.index, first.term, false)
            );
        }
        // The converter writes the shared keys, the mode claim, and member keys: bindings, and
        // the manifests of the detached checkpoints in the replicated member states.
        let root = format!("{}/", s3().cluster_prefix("slatedb-raft-poc"));
        let written: Vec<_> = std::mem::take(&mut *faults.written.lock().unwrap())
            .into_iter()
            .map(|key| key.strip_prefix(&root).unwrap().to_owned())
            .collect();
        assert!(
            written.iter().all(|key| key.starts_with("shared/")
                || key == "mode.json"
                || key.starts_with("member-")),
            "{written:?}"
        );
        assert!(written
            .iter()
            .any(|key| key.starts_with("member-") && key.contains("/manifest/")));
        let servers = start(&shared).await;
        assert_eq!(hashes(&servers, revision).await, before);
        let revision = write(&servers, 1).await;
        let after = hashes(&servers, revision).await;
        assert!(after.iter().all(|hash| *hash == after[0]), "{after:?}");
        for server in servers {
            server.shutdown().await.unwrap();
        }
        // The offline record names a view checkpoint of the shared state.
        let record = member_record(&dir.path().join("1")).unwrap();
        assert!(record["index"].as_u64().unwrap() >= first.index, "{record}");
        let checkpoint: uuid::Uuid = serde_json::from_value(record["checkpoint"].clone()).unwrap();
        let source = shared::offline_source(
            store.clone(),
            &s3().cluster_prefix("slatedb-raft-poc"),
            StateCache::default(),
        )
        .unwrap();
        source.open_base(checkpoint).await.unwrap();
    }

    /// Two hosts convert at once. Host 2 pauses after it found no plan. Host 1 plans, seeds,
    /// and finishes. Host 2 then loses the plan claim, deletes only its own checkpoints, and
    /// uses the plan of host 1. The view checkpoint of host 1 stays, and the cluster starts.
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn concurrent_conversions_share_one_plan() {
        let dir = tempfile::tempdir().unwrap();
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let peers = addresses().await;
        let servers = start(&replicated(dir.path(), &peers, &store)).await;
        let revision = write(&servers, 50).await;
        let before = hashes(&servers, revision).await;
        for server in servers {
            server.shutdown().await.unwrap();
        }
        let reached = Arc::new(tokio::sync::Notify::new());
        let resume = Arc::new(tokio::sync::Notify::new());
        let mut second = conversion(dir.path(), &store, 2);
        second.pause = Some((reached.clone(), resume.clone()));
        let second = tokio::spawn(async move { second.run().await });
        reached.notified().await;
        let first = conversion(dir.path(), &store, 1).run().await.unwrap();
        assert!(first.planned);
        resume.notify_one();
        let second = second.await.unwrap().unwrap();
        assert_eq!((second.index, second.planned), (first.index, false));
        conversion(dir.path(), &store, 3).run().await.unwrap();
        let shared: Vec<_> = (1..=3)
            .map(|id| config(dir.path(), id, &peers, &store, StateMode::Shared))
            .collect();
        let servers = start(&shared).await;
        assert_eq!(hashes(&servers, revision).await, before);
        for server in servers {
            server.shutdown().await.unwrap();
        }
    }

    /// A legacy state has no term. Only a log with committed evidence at the index supplies it:
    /// an entry at or below the durable commit index, or the snapshot record there.
    #[test]
    fn a_term_needs_committed_evidence() {
        let dir = tempfile::tempdir().unwrap();
        let mut storage =
            crate::storage::RaftStorage::open(dir.path(), 1, "cluster", vec![1]).unwrap();
        let entry = raft::eraftpb::Entry {
            index: 1,
            term: 2,
            ..Default::default()
        };
        storage
            .persist(
                &[entry],
                Some(&raft::eraftpb::HardState {
                    term: 2,
                    commit: 0,
                    ..Default::default()
                }),
                None,
            )
            .unwrap();
        assert_eq!(local_term(&storage, 1), None);
        storage.persist_commit(1).unwrap();
        assert_eq!(local_term(&storage, 1), Some(2));
        assert_eq!(local_term(&storage, 2), None);
    }

    /// A learner joined after the bootstrap. Its log identity holds no bootstrap voters, so it
    /// cannot plan the conversion, and its refusal changes no claim. A bootstrap voter plans it.
    /// The learner then converts with that plan, and all four members start with their flags.
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn a_learner_converts_only_after_a_bootstrap_voter_planned() {
        use crate::proto::etcdserverpb::{cluster_client::ClusterClient, MemberAddRequest};
        let dir = tempfile::tempdir().unwrap();
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let peers = addresses().await;
        let configs = replicated(dir.path(), &peers, &store);
        let mut servers = start(&configs).await;
        write(&servers, 20).await;
        let address = port();
        let lead = leader(&servers).await;
        let id = ClusterClient::new(client(&servers[lead]).await)
            .member_add(MemberAddRequest {
                peer_ur_ls: vec![format!("http://{address}")],
                is_learner: true,
            })
            .await
            .unwrap()
            .into_inner()
            .member
            .unwrap()
            .id;
        let mut joined = peers.clone();
        joined.insert(id, address);
        let mut learner = config(dir.path(), id, &joined, &store, StateMode::Replicated);
        learner.join = true;
        servers.push(Server::start(learner.clone()).await.unwrap());
        let revision = write(&servers[..3], 10).await;
        let before = hashes(&servers, revision).await;
        for server in servers {
            server.shutdown().await.unwrap();
        }
        let error = conversion(dir.path(), &store, id).run().await.unwrap_err();
        assert!(error.to_string().contains("bootstrap voter"), "{error:#}");
        assert_eq!(mode(&store).await, "replicated");
        for member in [1, 2, 3, id] {
            conversion(dir.path(), &store, member).run().await.unwrap();
        }
        let mut shared: Vec<_> = (1..=3)
            .map(|id| config(dir.path(), id, &peers, &store, StateMode::Shared))
            .collect();
        learner.state_mode = StateMode::Shared;
        shared.push(learner);
        let servers = start(&shared).await;
        assert_eq!(hashes(&servers, revision).await, before);
        for server in servers {
            server.shutdown().await.unwrap();
        }
    }

    /// A voter was removed before the stop. Its own conversion fails in preflight and changes no
    /// claim, so the replicated cluster still restarts. The two active members convert. The
    /// seed retires the owner claim of the removed member, and the two members start with their
    /// original flags.
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn a_removed_voter_does_not_block_the_conversion() {
        use crate::proto::etcdserverpb::{cluster_client::ClusterClient, MemberRemoveRequest};
        let dir = tempfile::tempdir().unwrap();
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let peers = addresses().await;
        let configs = replicated(dir.path(), &peers, &store);
        let mut servers = start(&configs).await;
        write(&servers, 20).await;
        let lead = leader(&servers).await;
        let removed = (0..3).find(|i| *i != lead).unwrap();
        ClusterClient::new(client(&servers[lead]).await)
            .member_remove(MemberRemoveRequest {
                id: removed as u64 + 1,
            })
            .await
            .unwrap();
        servers.remove(removed).shutdown().await.unwrap();
        let revision = write(&servers, 20).await;
        let before = hashes(&servers, revision).await;
        for server in servers {
            server.shutdown().await.unwrap();
        }
        let error = conversion(dir.path(), &store, removed as u64 + 1)
            .run()
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("not in the membership"),
            "{error:#}"
        );
        assert_eq!(mode(&store).await, "replicated");
        let active: Vec<_> = configs
            .iter()
            .filter(|config| config.id != removed as u64 + 1)
            .cloned()
            .collect();
        // The replicated cluster still starts after the failed preflight.
        for server in start(&active).await {
            server.shutdown().await.unwrap();
        }
        for config in &active {
            conversion(dir.path(), &store, config.id)
                .run()
                .await
                .unwrap();
        }
        assert_eq!(mode(&store).await, "shared");
        let shared: Vec<_> = active
            .iter()
            .map(|config| {
                let mut config = config.clone();
                config.state_mode = StateMode::Shared;
                config.snapshot_every = ServerConfig::single_node(".").snapshot_every;
                config
            })
            .collect();
        let servers = start(&shared).await;
        assert_eq!(hashes(&servers, revision).await, before);
        for server in servers {
            server.shutdown().await.unwrap();
        }
    }
}
