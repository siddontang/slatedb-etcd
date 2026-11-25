//! Transfer Raft snapshots as SlateDB checkpoint objects. See `docs/object-snapshots.md`.

use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, LazyLock, Mutex, Weak};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{bail, ensure, Context, Result};
use futures::{StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use slatedb::config::{CheckpointOptions, CheckpointScope};
use slatedb::object_store::path::Path;
use slatedb::object_store::{GetResult, ObjectStore, ObjectStoreExt, PutPayload, WriteMultipart};
use slatedb::Db;

/// The Raft snapshot format that holds a checkpoint descriptor instead of rows.
pub(crate) const OBJECT_SNAPSHOT_FORMAT: u32 = 5;
/// SlateDB removes a transfer checkpoint after this time.
const CHECKPOINT_LIFETIME: Duration = Duration::from_secs(60 * 60);
/// A node sends its descriptor for this time. Then it builds a new checkpoint.
pub(crate) const IMAGE_REUSE: Duration = Duration::from_secs(20 * 60);
const CHECKPOINT_NAME: &str = "raft-snapshot";
/// The suffix of the object that names the current database: `<first>.current`.
const POINTER: &str = ".current";
/// The suffix of the object that holds the stable random ID of a database family:
/// `<first>.namespace`.
const NAMESPACE: &str = ".namespace";
const COPIES: usize = 8;
const CHUNK_BYTES: usize = 8 * 1024 * 1024;
/// The HTTP path that serves checkpoint objects to peers.
pub const FILE_ROUTE: &str = "/raft/snapshot-file";

/// The identity of an object backend. Equal S3 identities share one object namespace.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum Backend {
    Local,
    S3 {
        bucket: String,
        endpoint: Option<String>,
        region: String,
    },
}

/// The Raft snapshot format of shared state mode. It names a `view-` checkpoint of the shared
/// database. Every member reads the same objects, so any member can send it.
pub(crate) const VIEW_SNAPSHOT_FORMAT: u32 = 6;

/// The Raft snapshot data of format 6. The snapshot metadata holds the index, term, and
/// ConfState from the checkpoint.
///
/// A record above the checkpoint row also holds that row `F` after the encoded fields. Earlier
/// binaries ignore the trailing bytes, and they never get such a record. See `node::shared`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ViewDescriptor {
    pub version: u32,
    pub checkpoint: String,
    /// The checkpoint row, when the record index is above it. `None` means the record index.
    #[serde(skip)]
    pub row: Option<u64>,
}

impl ViewDescriptor {
    pub fn new(checkpoint: uuid::Uuid) -> Self {
        Self {
            version: VIEW_SNAPSHOT_FORMAT,
            checkpoint: checkpoint.to_string(),
            row: None,
        }
    }

    /// A record of `checkpoint` at an index above its row `row`.
    pub fn above_row(checkpoint: uuid::Uuid, row: u64) -> Self {
        Self {
            row: Some(row),
            ..Self::new(checkpoint)
        }
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut data = crate::codec::encode(self)?;
        if let Some(row) = self.row {
            data.extend(crate::codec::encode(&row)?);
        }
        Ok(data)
    }

    /// Decode format 6 data. Other formats return `None`.
    pub fn decode(data: &[u8]) -> Result<Option<Self>> {
        if format(data) != Some(VIEW_SNAPSHOT_FORMAT) {
            return Ok(None);
        }
        let (mut descriptor, used): (Self, _) =
            crate::codec::decode_prefix(data).context("invalid view descriptor")?;
        if used < data.len() {
            descriptor.row =
                Some(crate::codec::decode(&data[used..]).context("invalid view descriptor row")?);
        }
        descriptor.checkpoint()?;
        Ok(Some(descriptor))
    }

    pub fn checkpoint(&self) -> Result<uuid::Uuid> {
        uuid::Uuid::parse_str(&self.checkpoint).context("invalid view checkpoint")
    }
}

/// Return true for a view snapshot above its checkpoint row. An earlier binary cannot install
/// it, so it travels on its own route. See `peer::COVERED_ROUTE`.
pub(crate) fn above_row(snapshot: &raft::eraftpb::Snapshot) -> bool {
    matches!(
        ViewDescriptor::decode(snapshot.get_data()),
        Ok(Some(view)) if view.row.is_some_and(|row| row < snapshot.get_metadata().index)
    )
}

/// The Raft snapshot data of format 5.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Descriptor {
    pub version: u32,
    pub applied: u64,
    pub member: u64,
    /// The stable ID of the source namespace. It survives a source restart.
    pub namespace: String,
    pub backend: Backend,
    pub peer_url: Option<String>,
    pub root: String,
    pub checkpoint: String,
    pub manifest: u64,
    pub ssts: Vec<String>,
    pub members: Vec<u8>,
    pub created_ms: u64,
}

impl Descriptor {
    pub fn encode(&self) -> Result<Vec<u8>> {
        Ok(crate::codec::encode(self)?)
    }

    /// Decode format 5 data. Other formats return `None`.
    pub fn decode(data: &[u8]) -> Result<Option<Self>> {
        if format(data) != Some(OBJECT_SNAPSHOT_FORMAT) {
            return Ok(None);
        }
        let descriptor: Self = crate::codec::decode(data).context("invalid snapshot descriptor")?;
        for path in &descriptor.ssts {
            ensure!(sst_path(path), "invalid snapshot SST path");
        }
        uuid::Uuid::parse_str(&descriptor.checkpoint).context("invalid snapshot checkpoint")?;
        Ok(Some(descriptor))
    }

    pub(crate) fn manifest_path(&self) -> String {
        format!("manifest/{:020}.manifest", self.manifest)
    }
}

/// The snapshot format of `data`, if it has one.
pub(crate) fn format(data: &[u8]) -> Option<u32> {
    crate::codec::decode::<u32>(data).ok()
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Return true when `member` can send this snapshot data to a peer.
///
/// A row image is always transferable. A descriptor is transferable only by the member that
/// built it, and only while its checkpoint is young enough to outlive a transfer.
pub(crate) fn sendable(data: &[u8], member: u64) -> bool {
    if data.is_empty() {
        return false;
    }
    if format(data) == Some(VIEW_SNAPSHOT_FORMAT) {
        return ViewDescriptor::decode(data).is_ok();
    }
    match Descriptor::decode(data) {
        Ok(Some(descriptor)) => {
            descriptor.member == member
                && now_ms().saturating_sub(descriptor.created_ms) < IMAGE_REUSE.as_millis() as u64
        }
        Ok(None) => true,
        Err(_) => false,
    }
}

/// Accept only the relative object paths that a SlateDB checkpoint holds.
fn sst_path(path: &str) -> bool {
    path.strip_prefix("compacted/")
        .and_then(|name| name.strip_suffix(".sst"))
        .is_some_and(|id| id.len() == 26 && id.bytes().all(|b| b.is_ascii_alphanumeric()))
}

fn manifest_path(path: &str) -> bool {
    path.strip_prefix("manifest/")
        .and_then(|name| name.strip_suffix(".manifest"))
        .is_some_and(|id| id.len() == 20 && id.bytes().all(|b| b.is_ascii_digit()))
}

/// The node identity that a peer copy needs.
#[derive(Clone)]
struct Peer {
    member: u64,
    cluster: String,
    peer_url: Option<String>,
    http: Option<reqwest::Client>,
}

/// In-process sources by namespace ID. Embedded clusters and tests read peer objects directly.
static SOURCES: LazyLock<Mutex<HashMap<String, Weak<Location>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// The object namespace of one member and its current database.
pub struct Location {
    store: Arc<dyn ObjectStore>,
    base: String,
    backend: Backend,
    namespace: String,
    /// The name of the first database. Later databases are `<first>-N`.
    first: String,
    pub(crate) cache: super::StateCache,
    current: Mutex<String>,
    /// Staging state. One lock covers admission and reuse, so they cannot interleave.
    staging: Mutex<Staging>,
    peer: Mutex<Peer>,
    prepare: tokio::sync::Mutex<()>,
    /// A test pauses a copy after its scan here: (reached, resume).
    #[cfg(test)]
    pub(crate) pause_after_scan:
        Mutex<Option<(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>)>>,
}

impl Location {
    /// Open the namespace that holds `prefix`. The last path segment names the first database.
    pub(crate) async fn open(
        store: Arc<dyn ObjectStore>,
        prefix: &str,
        backend: Backend,
        cache: super::StateCache,
    ) -> Result<Arc<Self>> {
        let (base, first) = match prefix.rsplit_once('/') {
            Some((base, first)) => (base.to_owned(), first.to_owned()),
            None => (String::new(), prefix.to_owned()),
        };
        ensure!(
            !first.is_empty() && !first.contains('.'),
            "invalid state path"
        );
        // Each family has its own pointer and identity. A sibling prefix such as `state-5`
        // opens as an independent family.
        let pointer = Path::from(join(&base, &format!("{first}{POINTER}")));
        let current = match store.get(&pointer).await {
            Ok(result) => {
                let name = String::from_utf8(result.bytes().await?.to_vec())?;
                ensure!(
                    generation(&first, &name).is_some(),
                    "invalid current state name"
                );
                name
            }
            Err(slatedb::object_store::Error::NotFound { .. }) => first.clone(),
            Err(error) => return Err(error.into()),
        };
        let namespace =
            namespace(store.as_ref(), &join(&base, &format!("{first}{NAMESPACE}"))).await?;
        let location = Arc::new(Self {
            store,
            base,
            backend,
            namespace,
            first,
            cache,
            current: Mutex::new(current),
            staging: Mutex::new(Staging::default()),
            peer: Mutex::new(Peer {
                member: 0,
                cluster: String::new(),
                peer_url: None,
                http: None,
            }),
            prepare: tokio::sync::Mutex::new(()),
            #[cfg(test)]
            pause_after_scan: Mutex::new(None),
        });
        let mut sources = SOURCES.lock().unwrap();
        sources.retain(|_, source| source.strong_count() > 0);
        sources.insert(location.namespace.clone(), Arc::downgrade(&location));
        Ok(location)
    }

    /// Record the node identity. Snapshot copies need it.
    pub(crate) fn bind(
        &self,
        member: u64,
        cluster: &str,
        peer_url: Option<String>,
        http: Option<reqwest::Client>,
    ) {
        *self.peer.lock().unwrap() = Peer {
            member,
            cluster: cluster.to_owned(),
            peer_url,
            http,
        };
    }

    pub(crate) fn store(&self) -> Arc<dyn ObjectStore> {
        self.store.clone()
    }

    /// The object path of the current database.
    pub(crate) fn root(&self) -> String {
        join(&self.base, &self.current.lock().unwrap())
    }

    pub(crate) fn path(&self, name: &str) -> String {
        join(&self.base, name)
    }

    fn current(&self) -> String {
        self.current.lock().unwrap().clone()
    }

    /// List the databases of this family with their ownership and ready markers.
    async fn scan(&self) -> Result<Vec<Database>> {
        let base = (!self.base.is_empty()).then(|| Path::from(self.base.as_str()));
        let listing = self.store.list_with_delimiter(base.as_ref()).await?;
        let mut names = BTreeSet::new();
        for prefix in listing.common_prefixes {
            if let Some(name) = prefix.filename() {
                names.insert(name.to_owned());
            }
        }
        for object in listing.objects {
            if let Some(name) = object.location.filename() {
                let name = name.strip_suffix(".ready").unwrap_or(name);
                names.insert(name.strip_suffix(".owner").unwrap_or(name).to_owned());
            }
        }
        let mut databases = vec![];
        for name in names {
            let Some(generation) = generation(&self.first, &name) else {
                continue;
            };
            let root = self.path(&name);
            let owner = read(self.store.as_ref(), &format!("{root}.owner")).await?;
            databases.push(Database {
                owned: owner.as_deref() == Some(self.namespace.as_str()),
                ready: read(self.store.as_ref(), &format!("{root}.ready")).await?,
                name,
                generation,
            });
        }
        Ok(databases)
    }

    /// Mark `name` as a database of this namespace. Only marked databases are deleted.
    async fn own(&self, name: &str) -> Result<()> {
        self.store
            .put(
                &Path::from(format!("{}.owner", self.path(name))),
                PutPayload::from(self.namespace.clone().into_bytes()),
            )
            .await?;
        Ok(())
    }

    /// Keep the staging database of a snapshot that Raft accepts until its install finishes.
    pub(crate) fn protect(&self, index: u64, checkpoint: &str) -> bool {
        let mut staging = self.staging.lock().unwrap();
        if staging.protected.iter().any(|(_, id)| id == checkpoint) {
            return false;
        }
        staging.protected.push((index, checkpoint.to_owned()));
        true
    }

    /// Protect a prepared snapshot for Raft. Return `Some(true)` for a new reservation,
    /// `Some(false)` for an existing one, and `None` when no complete staging copy exists.
    ///
    /// A copy claims a staging database for reuse under the same lock, so a reserved copy
    /// stays intact and a claimed copy cannot be reserved.
    pub(crate) fn reserve(&self, index: u64, checkpoint: &str) -> Option<bool> {
        let mut staging = self.staging.lock().unwrap();
        if !staging.prepared.contains_key(checkpoint) {
            return None;
        }
        if staging.protected.iter().any(|(_, id)| id == checkpoint) {
            return Some(false);
        }
        staging.protected.push((index, checkpoint.to_owned()));
        Some(true)
    }

    /// Release a reservation that Raft did not use.
    pub(crate) fn release(&self, checkpoint: &str) {
        self.staging
            .lock()
            .unwrap()
            .protected
            .retain(|(_, id)| id != checkpoint);
    }

    /// Start a checkpoint of the applied prefix. Call it between apply jobs.
    pub(crate) async fn begin(
        self: &Arc<Self>,
        db: &Db,
        applied: u64,
        members: Vec<u8>,
    ) -> Result<impl std::future::Future<Output = Result<Vec<u8>>> + Send + 'static> {
        let root = self.root();
        let handle = db
            .begin_checkpoint(
                CheckpointScope::All,
                &CheckpointOptions {
                    lifetime: Some(CHECKPOINT_LIFETIME),
                    name: Some(CHECKPOINT_NAME.to_owned()),
                    ..Default::default()
                },
            )
            .await?;
        let location = self.clone();
        Ok(async move {
            let checkpoint = handle.wait().await?;
            location
                .describe(
                    root,
                    checkpoint.id,
                    checkpoint.manifest_id,
                    applied,
                    members,
                )
                .await
        })
    }

    pub(crate) async fn describe(
        &self,
        root: String,
        checkpoint: uuid::Uuid,
        manifest: u64,
        applied: u64,
        members: Vec<u8>,
    ) -> Result<Vec<u8>> {
        let admin = slatedb::admin::Admin::builder(root.as_str(), self.store.clone()).build();
        let versioned = admin
            .read_manifest(Some(manifest))
            .await?
            .context("missing checkpoint manifest")?;
        ensure!(
            versioned.external_dbs().is_empty(),
            "a cloned database cannot be a snapshot source"
        );
        let resolver = slatedb::PathResolver::new(root.as_str(), &versioned);
        let prefix = format!("{root}/");
        let mut ssts = BTreeSet::new();
        for view in versioned.all_sst_views() {
            let path = resolver.sst_path(&view.sst.id).to_string();
            let relative = path
                .strip_prefix(&prefix)
                .context("SST is outside the database")?;
            ensure!(sst_path(relative), "unexpected SST path {relative}");
            ssts.insert(relative.to_owned());
        }
        let peer = self.peer.lock().unwrap().clone();
        // A member update can change the peer URL after startup. Prefer the applied membership.
        let peer_url = serde_json::from_slice::<crate::cluster::Store>(&members)
            .ok()
            .and_then(|store| store.members.get(&peer.member)?.peer_ur_ls.first().cloned())
            .or(peer.peer_url);
        Descriptor {
            version: OBJECT_SNAPSHOT_FORMAT,
            applied,
            member: peer.member,
            namespace: self.namespace.clone(),
            backend: self.backend.clone(),
            peer_url,
            root,
            checkpoint: checkpoint.to_string(),
            manifest,
            ssts: ssts.into_iter().collect(),
            members,
            created_ms: now_ms(),
        }
        .encode()
    }

    /// Copy the checkpoint objects into the staging database. Return the staging name.
    ///
    /// The copy is idempotent. A retry keeps the SSTs that an earlier attempt copied.
    pub(crate) async fn prepare(&self, descriptor: &Descriptor) -> Result<String> {
        let _guard = self.prepare.lock().await;
        let current = generation(&self.first, &self.current()).unwrap_or(0);
        let databases = self.scan().await?;
        #[cfg(test)]
        {
            let pause = self.pause_after_scan.lock().unwrap().take();
            if let Some((reached, resume)) = pause {
                reached.notify_one();
                resume.notified().await;
            }
        }
        let staged = databases
            .iter()
            .filter(|db| db.owned && db.generation > current);
        if let Some(db) = staged
            .clone()
            .find(|db| db.ready.as_deref() == Some(descriptor.checkpoint.as_str()))
        {
            self.staging
                .lock()
                .unwrap()
                .prepared
                .insert(descriptor.checkpoint.clone(), db.name.clone());
            return Ok(db.name.clone());
        }
        // Reuse the newest staging database that no accepted snapshot needs. It keeps its SSTs.
        // Select and claim under the reservation lock: Raft cannot reserve a claimed copy.
        let mut reusable: Vec<&Database> = {
            let mut lock = self.staging.lock().unwrap();
            // An install can switch the current database after the scan. `switch` changes it
            // under this lock, so read it again here and never claim it.
            let active = generation(&self.first, &self.current()).unwrap_or(0);
            let reusable: Vec<&Database> = staged
                .filter(|db| db.generation > active)
                .filter(|db| {
                    db.ready.as_ref().is_none_or(|id| {
                        !lock.protected.iter().any(|(_, protected)| protected == id)
                    })
                })
                .collect();
            lock.prepared.retain(|_, name| {
                !reusable.iter().any(|db| db.name == *name)
                    && databases.iter().any(|db| db.name == *name)
            });
            reusable
        };
        reusable.sort_by_key(|db| db.generation);
        let staging = match reusable.pop() {
            Some(db) => db.name.clone(),
            None => {
                let next = databases.iter().map(|db| db.generation).max().unwrap_or(0);
                let name = format!("{}-{}", self.first, next.max(current) + 1);
                self.own(&name).await?;
                name
            }
        };
        for db in reusable {
            self.remove(&db.name).await?;
        }
        let root = self.path(&staging);
        let ready = Path::from(format!("{root}.ready"));
        delete(self.store.as_ref(), &ready).await?;
        // Keep copied SSTs. Remove manifests, boundaries, and compaction state of other attempts.
        let compacted = format!("{root}/compacted/");
        for path in list(self.store.as_ref(), &root).await? {
            if !path.as_ref().starts_with(&compacted) {
                delete(self.store.as_ref(), &path).await?;
            }
        }
        let source = self.source(descriptor);
        type Copy<'a> =
            std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>>;
        let copies: Vec<Copy<'_>> = descriptor
            .ssts
            .iter()
            .map(|relative| {
                Box::pin(self.copy_missing(&source, descriptor, &root, relative)) as Copy<'_>
            })
            .collect();
        futures::stream::iter(copies)
            .buffer_unordered(COPIES)
            .try_collect::<Vec<_>>()
            .await?;
        let manifest = descriptor.manifest_path();
        self.copy(
            &source,
            descriptor,
            &manifest,
            &Path::from(format!("{root}/{manifest}")),
        )
        .await?;
        // The copied manifest lists source checkpoints whose manifests were not copied.
        let admin = slatedb::admin::Admin::builder(root.as_str(), self.store.clone()).build();
        let versioned = admin
            .read_manifest(None)
            .await?
            .context("missing copied manifest")?;
        ensure!(
            versioned.id() == descriptor.manifest && versioned.external_dbs().is_empty(),
            "copied manifest differs from the descriptor"
        );
        for checkpoint in admin.list_checkpoints(None).await? {
            admin.delete_checkpoint(checkpoint.id).await?;
        }
        crate::failpoint::hit("snapshot-before-ready");
        self.store
            .put(
                &ready,
                PutPayload::from(descriptor.checkpoint.clone().into_bytes()),
            )
            .await?;
        self.staging
            .lock()
            .unwrap()
            .prepared
            .insert(descriptor.checkpoint.clone(), staging.clone());
        Ok(staging)
    }

    async fn copy_missing(
        &self,
        source: &Source,
        descriptor: &Descriptor,
        root: &str,
        relative: &str,
    ) -> Result<()> {
        let destination = Path::from(format!("{root}/{relative}"));
        match self.store.head(&destination).await {
            // SST IDs are unique and a PUT is atomic. An existing SST is complete.
            Ok(_) => Ok(()),
            Err(slatedb::object_store::Error::NotFound { .. }) => {
                self.copy(source, descriptor, relative, &destination).await
            }
            Err(error) => Err(error.into()),
        }
    }

    fn source(&self, descriptor: &Descriptor) -> Source {
        if matches!(descriptor.backend, Backend::S3 { .. }) && descriptor.backend == self.backend {
            return Source::Shared;
        }
        let local = SOURCES
            .lock()
            .unwrap()
            .get(&descriptor.namespace)
            .and_then(Weak::upgrade);
        match local {
            Some(location) => Source::Store(location.store()),
            None => Source::Http,
        }
    }

    async fn copy(
        &self,
        source: &Source,
        descriptor: &Descriptor,
        relative: &str,
        destination: &Path,
    ) -> Result<()> {
        let from = Path::from(format!("{}/{relative}", descriptor.root));
        let result = match source {
            Source::Shared => self
                .store
                .copy(&from, destination)
                .await
                .map_err(anyhow::Error::from),
            Source::Store(store) => match store.get(&from).await {
                Ok(result) => self.write(destination, result).await,
                Err(error) => Err(error.into()),
            },
            Source::Http => return self.fetch(descriptor, relative, destination).await,
        };
        match result {
            Ok(()) => Ok(()),
            // A peer that denies cross-prefix reads still serves its objects over HTTP.
            Err(error)
                if self.peer.lock().unwrap().http.is_some() && descriptor.peer_url.is_some() =>
            {
                tracing::warn!(%error, "direct snapshot copy failed; using HTTP");
                self.fetch(descriptor, relative, destination).await
            }
            Err(error) => Err(error),
        }
    }

    async fn write(&self, destination: &Path, result: GetResult) -> Result<()> {
        let upload = self.store.put_multipart(destination).await?;
        let mut writer = WriteMultipart::new_with_chunk_size(upload, CHUNK_BYTES);
        let mut stream = result.into_stream();
        while let Some(chunk) = stream.next().await {
            match chunk {
                Ok(chunk) => {
                    writer.wait_for_capacity(2).await?;
                    writer.put(chunk);
                }
                Err(error) => {
                    let _ = writer.abort().await;
                    return Err(error.into());
                }
            }
        }
        writer.finish().await?;
        Ok(())
    }

    async fn fetch(
        &self,
        descriptor: &Descriptor,
        relative: &str,
        destination: &Path,
    ) -> Result<()> {
        let peer = self.peer.lock().unwrap().clone();
        let (Some(http), Some(url)) = (peer.http, descriptor.peer_url.as_ref()) else {
            bail!("no route to the snapshot source");
        };
        let url = reqwest::Url::parse_with_params(
            &format!("{url}{FILE_ROUTE}"),
            [("root", descriptor.root.as_str()), ("path", relative)],
        )?;
        let mut response = http
            .get(url)
            .header("x-cluster-id", peer.cluster)
            .timeout(Duration::from_secs(600))
            .send()
            .await?;
        ensure!(
            response.status().is_success(),
            "snapshot source returned {}",
            response.status()
        );
        let upload = self.store.put_multipart(destination).await?;
        let mut writer = WriteMultipart::new_with_chunk_size(upload, CHUNK_BYTES);
        loop {
            match response.chunk().await {
                Ok(Some(chunk)) => {
                    writer.wait_for_capacity(2).await?;
                    writer.put(chunk);
                }
                Ok(None) => break,
                Err(error) => {
                    let _ = writer.abort().await;
                    return Err(error.into());
                }
            }
        }
        writer.finish().await?;
        Ok(())
    }

    /// Read one object of the current database for a peer copy.
    pub async fn serve(&self, root: &str, relative: &str) -> Result<Option<GetResult>> {
        if root != self.root() || !(sst_path(relative) || manifest_path(relative)) {
            return Ok(None);
        }
        match self
            .store
            .get(&Path::from(format!("{root}/{relative}")))
            .await
        {
            Ok(result) => Ok(Some(result)),
            Err(slatedb::object_store::Error::NotFound { .. }) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    /// Make the prepared staging database of snapshot `index` current with one atomic PUT.
    pub(crate) async fn switch(&self, name: &str, index: u64) -> Result<String> {
        ensure!(
            generation(&self.first, name).is_some(),
            "invalid state name"
        );
        // A restart after the switch deletes the old database only when it carries this mark.
        self.own(&self.current()).await?;
        self.store
            .put(
                &Path::from(join(&self.base, &format!("{}{POINTER}", self.first))),
                PutPayload::from(name.to_owned().into_bytes()),
            )
            .await?;
        // Change the current database and release its reservation in one step. A copy that
        // claims a staging database takes the same lock, so it sees the new current database.
        let mut staging = self.staging.lock().unwrap();
        let old = std::mem::replace(&mut *self.current.lock().unwrap(), name.to_owned());
        staging.prepared.retain(|_, staged| staged != name);
        staging.protected.retain(|(i, _)| *i > index);
        drop(staging);
        Ok(old)
    }

    /// Close a replaced database after its readers finish, then delete its objects.
    pub(crate) fn retire(self: &Arc<Self>, db: Arc<Db>, name: String) {
        let location = self.clone();
        tokio::spawn(async move {
            // Each read view holds the database. Wait for the views instead of a fixed time,
            // because a closed database fails the reads of a pinned snapshot.
            let start = tokio::time::Instant::now();
            let mut warned = false;
            while Arc::strong_count(&db) > 1 {
                if !warned && start.elapsed() > Duration::from_secs(60) {
                    warned = true;
                    tracing::warn!(name, "a replaced state database still has readers");
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            let _ = db
                .close_with_options(slatedb::config::CloseOptions { flush_type: None })
                .await;
            drop(db);
            if let Err(error) = location.remove(&name).await {
                tracing::warn!(%error, name, "cannot delete a replaced state database");
            }
        });
    }

    /// Delete replaced databases of this namespace. Other databases in the store stay intact.
    pub(crate) async fn remove_orphans(&self) -> Result<()> {
        let current = generation(&self.first, &self.current()).unwrap_or(0);
        for db in self.scan().await? {
            if db.owned && db.generation < current {
                self.remove(&db.name).await?;
            }
        }
        Ok(())
    }

    async fn remove(&self, name: &str) -> Result<()> {
        let root = self.path(name);
        for path in list(self.store.as_ref(), &root).await? {
            delete(self.store.as_ref(), &path).await?;
        }
        delete(self.store.as_ref(), &Path::from(format!("{root}.ready"))).await?;
        // The ownership mark goes last, so an interrupted delete can finish after a restart.
        delete(self.store.as_ref(), &Path::from(format!("{root}.owner"))).await
    }
}

#[derive(Default)]
struct Staging {
    /// Staging databases that hold a checkpoint completely, by checkpoint ID.
    prepared: HashMap<String, String>,
    /// Snapshots that Raft accepted but the state machine has not installed: (index, checkpoint).
    /// A new copy must not reuse their staging databases.
    protected: Vec<(u64, String)>,
}

struct Database {
    name: String,
    generation: u64,
    owned: bool,
    ready: Option<String>,
}

async fn read(store: &dyn ObjectStore, path: &str) -> Result<Option<String>> {
    match store.get(&Path::from(path)).await {
        Ok(result) => Ok(Some(String::from_utf8(result.bytes().await?.to_vec())?)),
        Err(slatedb::object_store::Error::NotFound { .. }) => Ok(None),
        Err(error) => Err(error.into()),
    }
}

enum Source {
    /// The same object namespace. The store copies on the server.
    Shared,
    /// A store in the same process.
    Store(Arc<dyn ObjectStore>),
    /// The source member serves its objects.
    Http,
}

/// Read the namespace ID, or create it with a conditional PUT.
async fn namespace(store: &dyn ObjectStore, path: &str) -> Result<String> {
    let path = Path::from(path);
    loop {
        match store.get(&path).await {
            Ok(result) => return Ok(String::from_utf8(result.bytes().await?.to_vec())?),
            Err(slatedb::object_store::Error::NotFound { .. }) => {}
            Err(error) => return Err(error.into()),
        }
        let id = uuid::Uuid::new_v4().to_string();
        match store
            .put_opts(
                &path,
                PutPayload::from(id.clone().into_bytes()),
                slatedb::object_store::PutMode::Create.into(),
            )
            .await
        {
            Ok(_) => return Ok(id),
            // Another open created it first, or a lost reply hid this PUT. Read it again.
            Err(slatedb::object_store::Error::AlreadyExists { .. }) => {}
            Err(error) => return Err(error.into()),
        }
    }
}

async fn list(store: &dyn ObjectStore, root: &str) -> Result<Vec<Path>> {
    let mut stream = store.list(Some(&Path::from(root)));
    let mut paths = vec![];
    while let Some(meta) = stream.next().await {
        paths.push(meta?.location);
    }
    Ok(paths)
}

async fn delete(store: &dyn ObjectStore, path: &Path) -> Result<()> {
    match store.delete(path).await {
        Ok(()) | Err(slatedb::object_store::Error::NotFound { .. }) => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn join(base: &str, name: &str) -> String {
    if base.is_empty() {
        name.to_owned()
    } else {
        format!("{base}/{name}")
    }
}

/// The generation of a database name: `<first>` is 0 and `<first>-N` is N.
fn generation(first: &str, name: &str) -> Option<u64> {
    if name == first {
        return Some(0);
    }
    name.strip_prefix(first)
        .and_then(|n| n.strip_prefix('-'))
        .filter(|n| !n.starts_with('0') && n.bytes().all(|b| b.is_ascii_digit()))
        .and_then(|n| n.parse().ok())
}

#[cfg(test)]
#[path = "transfer_tests.rs"]
mod tests;
