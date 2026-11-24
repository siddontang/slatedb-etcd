//! Convert a stopped replicated S3 cluster to shared state mode, offline.
//!
//! Stop every member first. Then run the conversion once on each member host, with the data
//! directory of that member. The first run seeds the shared state:
//!
//! 1. It reads the durable applied index of each replicated member namespace from a detached
//!    checkpoint, and selects the member with the highest index. Different indexes log a
//!    warning. The other members replay their own Raft logs above that index later.
//! 2. It copies the SSTs and the manifest of that checkpoint into `shared/state`, with the
//!    object copy of the snapshot transfer.
//! 3. It changes the mode claim to shared, writes the cluster claim with the voters of the
//!    copied membership, publishes the first view checkpoint, and writes the bootstrap marker.
//!
//! Each run then makes its member a shared-mode member: it adopts the view checkpoint as the
//! Raft snapshot record of the local log, writes the shared binding claim, rewrites the local
//! binding, and deletes the replicated owner claim. A shared member refuses to start while any
//! member namespace keeps a replicated owner claim, so the cluster starts only after every member
//! is converted. The replicated state objects stay in place. Delete them after a check.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{ensure, Context, Result};
use slatedb::object_store::path::Path;
use slatedb::object_store::{ObjectStore, ObjectStoreExt};

use crate::state::transfer::{Backend, Location, ViewDescriptor};
use crate::state::{shared, StateCache, StateMode};

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
}

/// The result of one member's conversion.
#[derive(Debug)]
pub struct Converted {
    /// The Raft index and term of the base checkpoint.
    pub index: u64,
    pub term: u64,
    /// The member whose state seeded the shared state, if this run seeded it.
    pub source: Option<u64>,
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
        }
    }

    pub async fn run(&self) -> Result<Converted> {
        let raft = self.dir.join("raft");
        ensure!(raft.exists(), "the data directory has no Raft log");
        // The Raft log lock refuses a running member.
        let voters = crate::storage::RaftStorage::stored_voters(&raft)
            .context("cannot open the Raft log; stop the member first")?;
        let mut storage =
            crate::storage::RaftStorage::open(&raft, self.id, &self.cluster_id, voters)?;
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
        let mut seeded = None;
        if !source.bootstrapped().await? {
            ensure!(
                mode == StateMode::Replicated,
                "the member is converted, but the shared state has no bootstrap marker"
            );
            seeded = Some(self.seed(&store, &root, &backend, cache, &source).await?);
        }
        let (checkpoint, base, term, members) = shared::newest_view(&source).await?;
        ensure!(
            members.members.contains_key(&self.id),
            "member {} is not in the converted membership",
            self.id
        );
        let mut snapshot = raft::eraftpb::Snapshot::default();
        snapshot.mut_metadata().index = base.applied;
        snapshot.mut_metadata().term = term;
        snapshot.mut_metadata().set_conf_state(members.conf());
        snapshot.data = ViewDescriptor::new(checkpoint).encode()?;
        // The log keeps its entries above the checkpoint when its term matches there.
        storage.replace_record(&snapshot)?;
        drop(storage);
        crate::object::convert_binding(&self.dir, store.as_ref(), &self.cluster_id, self.id)
            .await?;
        Ok(Converted {
            index: base.applied,
            term,
            source: seeded,
        })
    }

    /// Copy the newest replicated member state into `shared/state`, and publish it as the first
    /// view checkpoint. Return the source member.
    async fn seed(
        &self,
        store: &Arc<dyn ObjectStore>,
        root: &str,
        backend: &Backend,
        cache: StateCache,
        source: &shared::Source,
    ) -> Result<u64> {
        let members = replicated_members(store.as_ref(), root).await?;
        ensure!(
            !members.is_empty(),
            "the cluster has no replicated member namespace"
        );
        let options = slatedb::config::CheckpointOptions {
            lifetime: Some(Duration::from_secs(3600)),
            name: Some("convert-to-shared".to_owned()),
            ..Default::default()
        };
        // (applied, member, location, database root, checkpoint)
        let mut states = vec![];
        for &member in &members {
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
            let base =
                crate::state::view::Base::open(store.clone(), &db, checkpoint.id, cache.build())
                    .await?;
            states.push((base.applied, member, location, db, checkpoint));
        }
        let detached: Vec<_> = states
            .iter()
            .map(|(_, _, _, db, checkpoint)| (db.clone(), checkpoint.id))
            .collect();
        states.sort_by_key(|(applied, member, ..)| (*applied, std::cmp::Reverse(*member)));
        let (applied, member, location, db, checkpoint) = states.pop().unwrap();
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
                "the members have different durable indexes; using the highest"
            );
        }
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
        let target = source.state();
        let mut objects = descriptor.ssts.clone();
        objects.push(descriptor.manifest_path());
        for relative in objects {
            store
                .copy(
                    &Path::from(format!("{db}/{relative}")),
                    &Path::from(format!("{target}/{relative}")),
                )
                .await?;
        }
        // The copied manifest lists the source checkpoints. Their manifests were not copied.
        let admin = slatedb::admin::Admin::builder(target.as_str(), store.clone()).build();
        for listed in admin.list_checkpoints(None).await? {
            admin.delete_checkpoint(listed.id).await?;
        }
        for (other, id) in detached {
            slatedb::admin::Admin::builder(other.as_str(), store.clone())
                .build()
                .delete_checkpoint(id)
                .await?;
        }
        shared::convert_mode(store.as_ref(), root, &self.cluster_id).await?;
        let writer = source.open_writer().await?;
        let last = writer.manifest().last_l0_seq();
        ensure!(
            last == applied,
            "the copied state ends at sequence {last}, not at index {applied}"
        );
        let options = slatedb::config::CheckpointOptions {
            name: Some(shared::view_name(last)),
            ..Default::default()
        };
        let view = writer
            .create_checkpoint(slatedb::config::CheckpointScope::Durable, &options)
            .await?;
        writer.close().await?;
        let base = source.open_base(view.id).await?;
        let (_, members) = shared::identity(&base).await?;
        let voters: Vec<u64> = members
            .members
            .iter()
            .filter(|(_, member)| !member.is_learner)
            .map(|(&id, _)| id)
            .collect();
        shared::claim(store.as_ref(), root, &self.cluster_id, self.quota, &voters).await?;
        source.mark_bootstrapped(view.id).await?;
        Ok(member)
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

    fn s3() -> crate::object::S3 {
        crate::object::S3 {
            bucket: "test-bucket".into(),
            region: "us-east-1".into(),
            endpoint: Some("http://127.0.0.1:9000".into()),
            prefix: "slatedb-etcd".into(),
            allow_http: true,
        }
    }

    async fn addresses() -> std::collections::BTreeMap<u64, String> {
        let mut peers = std::collections::BTreeMap::new();
        let mut reserved = vec![];
        for id in 1..=3 {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            peers.insert(id, listener.local_addr().unwrap().to_string());
            reserved.push(listener);
        }
        peers
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

    async fn leader(servers: &[Server]) -> usize {
        let leader = MaintenanceClient::new(client(&servers[0]).await)
            .status(StatusRequest {})
            .await
            .unwrap()
            .into_inner()
            .leader;
        leader as usize - 1
    }

    /// A stopped three-member replicated cluster converts to shared mode. The converter refuses
    /// a running member, and a shared member does not start before every member is converted.
    /// After the conversion the members serve the same data and hash, and accept writes.
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn a_stopped_replicated_cluster_converts_to_shared_mode() {
        let dir = tempfile::tempdir().unwrap();
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let peers = addresses().await;
        // Frequent log truncation flushes each member's state at its own index. The conversion
        // takes the highest one, and the other members replay their logs above it.
        let replicated: Vec<_> = (1..=3)
            .map(|id| {
                let mut config = config(dir.path(), id, &peers, &store, StateMode::Replicated);
                config.snapshot_every = 16;
                config.snapshot_min_bytes = 0;
                config
            })
            .collect();
        let servers = start(&replicated).await;
        let mut kv = KvClient::new(client(&servers[leader(&servers).await]).await);
        let mut revision = 0;
        for n in 0..50 {
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
        let before = hashes(&servers, revision).await;
        let conversion = |id: u64| {
            let mut conversion = Conversion::new(
                id,
                dir.path().join(id.to_string()),
                "slatedb-raft-poc".into(),
                0,
            );
            conversion.store = Some(store.clone());
            conversion
        };
        let error = conversion(1).run().await.unwrap_err();
        assert!(error.to_string().contains("stop the member"), "{error:#}");
        for server in servers {
            server.shutdown().await.unwrap();
        }
        let shared: Vec<_> = (1..=3)
            .map(|id| config(dir.path(), id, &peers, &store, StateMode::Shared))
            .collect();
        let first = conversion(1).run().await.unwrap();
        assert!(first.source.is_some());
        // Members 2 and 3 still have replicated owner claims.
        let error = Server::start(shared[0].clone()).await.err().unwrap();
        assert!(
            format!("{error:#}").contains("Replicated state mode"),
            "{error:#}"
        );
        for id in 2..=3 {
            let converted = conversion(id).run().await.unwrap();
            assert_eq!((converted.index, converted.source), (first.index, None));
        }
        let servers = start(&shared).await;
        assert_eq!(hashes(&servers, revision).await, before);
        let mut kv = KvClient::new(client(&servers[leader(&servers).await]).await);
        let reply = kv
            .put(PutRequest {
                key: b"after".to_vec(),
                value: b"conversion".to_vec(),
                ..Default::default()
            })
            .await
            .unwrap()
            .into_inner();
        let after = hashes(&servers, reply.header.unwrap().revision).await;
        assert!(after.iter().all(|hash| *hash == after[0]), "{after:?}");
        for server in servers {
            server.shutdown().await.unwrap();
        }
    }
}
