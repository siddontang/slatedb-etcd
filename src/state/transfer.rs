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
/// The object that names the current database of a member.
const POINTER: &str = "current-state";
/// The object that holds the stable random ID of a member namespace.
const NAMESPACE: &str = "namespace-id";
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

    fn manifest_path(&self) -> String {
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
    /// The checkpoint that the staging database holds completely.
    prepared: Mutex<Option<String>>,
    peer: Mutex<Peer>,
    prepare: tokio::sync::Mutex<()>,
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
            !first.is_empty() && !first.contains('.') && first != POINTER && first != NAMESPACE,
            "invalid state path"
        );
        let pointer = Path::from(join(&base, POINTER));
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
        let namespace = namespace(store.as_ref(), &base).await?;
        let location = Arc::new(Self {
            store,
            base,
            backend,
            namespace,
            first,
            cache,
            current: Mutex::new(current),
            prepared: Mutex::new(None),
            peer: Mutex::new(Peer {
                member: 0,
                cluster: String::new(),
                peer_url: None,
                http: None,
            }),
            prepare: tokio::sync::Mutex::new(()),
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

    fn staging(&self) -> String {
        let current = self.current.lock().unwrap();
        format!(
            "{}-{}",
            self.first,
            generation(&self.first, &current).unwrap_or(0) + 1
        )
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

    async fn describe(
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
        Descriptor {
            version: OBJECT_SNAPSHOT_FORMAT,
            applied,
            member: peer.member,
            namespace: self.namespace.clone(),
            backend: self.backend.clone(),
            peer_url: peer.peer_url,
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
        let staging = self.staging();
        let root = self.path(&staging);
        let ready = Path::from(format!("{root}.ready"));
        match self.store.get(&ready).await {
            Ok(result) => {
                if result.bytes().await? == descriptor.checkpoint.as_bytes() {
                    *self.prepared.lock().unwrap() = Some(descriptor.checkpoint.clone());
                    return Ok(staging);
                }
            }
            Err(slatedb::object_store::Error::NotFound { .. }) => {}
            Err(error) => return Err(error.into()),
        }
        *self.prepared.lock().unwrap() = None;
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
        *self.prepared.lock().unwrap() = Some(descriptor.checkpoint.clone());
        Ok(staging)
    }

    /// Return true when the staging database holds `checkpoint` completely.
    pub(crate) fn is_prepared(&self, checkpoint: &str) -> bool {
        self.prepared.lock().unwrap().as_deref() == Some(checkpoint)
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

    /// Make the prepared staging database current with one atomic PUT.
    pub(crate) async fn switch(&self, name: &str) -> Result<String> {
        ensure!(
            generation(&self.first, name).is_some(),
            "invalid state name"
        );
        self.store
            .put(
                &Path::from(join(&self.base, POINTER)),
                PutPayload::from(name.to_owned().into_bytes()),
            )
            .await?;
        let old = std::mem::replace(&mut *self.current.lock().unwrap(), name.to_owned());
        *self.prepared.lock().unwrap() = None;
        Ok(old)
    }

    /// Close a replaced database after its readers finish, then delete its objects.
    pub(crate) fn retire(self: &Arc<Self>, db: Arc<Db>, name: String) {
        let location = self.clone();
        tokio::spawn(async move {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
            while Arc::strong_count(&db) > 1 && tokio::time::Instant::now() < deadline {
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

    /// Delete databases other than the current and staging databases.
    pub(crate) async fn remove_orphans(&self) -> Result<()> {
        let current = self.current.lock().unwrap().clone();
        let keep = [current, self.staging()];
        let base = (!self.base.is_empty()).then(|| Path::from(self.base.as_str()));
        let listing = self.store.list_with_delimiter(base.as_ref()).await?;
        let mut names = BTreeSet::new();
        for prefix in listing.common_prefixes {
            if let Some(name) = prefix.filename() {
                names.insert(name.to_owned());
            }
        }
        for object in listing.objects {
            if let Some(name) = object
                .location
                .filename()
                .and_then(|n| n.strip_suffix(".ready"))
            {
                names.insert(name.to_owned());
            }
        }
        for name in names {
            if generation(&self.first, &name).is_some() && !keep.contains(&name) {
                self.remove(&name).await?;
            }
        }
        Ok(())
    }

    async fn remove(&self, name: &str) -> Result<()> {
        let root = self.path(name);
        for path in list(self.store.as_ref(), &root).await? {
            delete(self.store.as_ref(), &path).await?;
        }
        delete(self.store.as_ref(), &Path::from(format!("{root}.ready"))).await
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
async fn namespace(store: &dyn ObjectStore, base: &str) -> Result<String> {
    let path = Path::from(join(base, NAMESPACE));
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
