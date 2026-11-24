//! Shared state mode: one SlateDB for the whole cluster. See `docs/shared-state.md`.
//!
//! This module owns the apply side: the cluster claim, the overlay, the base checkpoint, and
//! base switches. The leader tasks that write the shared `Db` are in `node/shared`.
//!
//! No member opens the writer here. The leader opens it in `node/shared` when it takes the lead.
//! Phase 3 adds the fencing checks, the clock checks, and the reopen after a fence.

use std::collections::HashSet;
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use anyhow::{bail, ensure, Context, Result};
use serde::{Deserialize, Serialize};
use slatedb::object_store::path::Path;
use slatedb::object_store::{ObjectStore, ObjectStoreExt, PutMode};
use slatedb::Db;

use super::overlay::Overlay;
use super::view::{Base, SharedView, StateView};
use super::*;

/// The state layout and replication mode of a cluster.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize, clap::ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum StateMode {
    /// Each member keeps its own SlateDB.
    #[default]
    Replicated,
    /// The leader writes one SlateDB. Members read checkpoints of it under an overlay.
    Shared,
}

/// How the leader deletes old `view-` checkpoints.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, clap::ValueEnum)]
pub enum ViewRetention {
    /// Keep every checkpoint. Each one pins its SSTs, so storage grows until a manual cleanup.
    #[default]
    Off,
    /// Delete old checkpoints that no member pins. SlateDB deletes without an epoch check, so a
    /// deposed leader can still race a new one between its epoch check and the deletion.
    Fenced,
}

impl StateMode {
    pub fn is_replicated(&self) -> bool {
        *self == Self::Replicated
    }
}

/// The logical size after each shared-mode batch. Install reads it instead of a full scan.
pub(crate) const LOGICAL_BYTES: &[u8] = b"meta/logical-bytes";
/// The name prefix of the checkpoints that the publisher creates.
const VIEW: &str = "view-";
const CLAIM: &str = "shared/owner.json";
const BOOTSTRAP: &str = "shared/bootstrap.json";
const STATE: &str = "shared/state";
/// A received snapshot that Raft did not install releases its checkpoint after this time.
const RECEIPT_LIFETIME: Duration = Duration::from_secs(60);
/// The cluster-wide mode claim. Both modes create it before they write state on S3.
const MODE: &str = "mode.json";

/// The cluster claim. The first member creates it, and every member checks it.
#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
struct Claim {
    version: u32,
    cluster: String,
    mode: StateMode,
    /// The logical quota of the cluster. Zero disables it. Every member applies this value.
    quota: u64,
    /// The initial voters. Members with another bootstrap set refuse to open the writer.
    voters: Vec<u64>,
}

/// The mode claim of one cluster namespace.
#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
struct ModeClaim {
    version: u32,
    cluster: String,
    mode: StateMode,
}

/// Create or check the mode claim under `root`. Concurrent first starts in different modes
/// cannot both pass, because only one `PutMode::Create` succeeds.
pub(crate) async fn claim_mode(
    store: &dyn ObjectStore,
    root: &str,
    cluster: &str,
    mode: StateMode,
) -> Result<()> {
    let path = Path::from(join(root, MODE));
    let claim = ModeClaim {
        version: 1,
        cluster: cluster.to_owned(),
        mode,
    };
    match store
        .put_opts(
            &path,
            serde_json::to_vec(&claim)?.into(),
            PutMode::Create.into(),
        )
        .await
    {
        Ok(_) => Ok(()),
        Err(slatedb::object_store::Error::AlreadyExists { .. }) => {
            let data = store.get(&path).await?.bytes().await?;
            let existing: ModeClaim =
                serde_json::from_slice(&data).context("invalid cluster mode claim")?;
            ensure!(
                existing.version == 1 && existing.cluster == cluster,
                "the namespace belongs to another cluster"
            );
            ensure!(
                existing.mode == mode,
                "the cluster uses {:?} state mode",
                existing.mode
            );
            Ok(())
        }
        Err(error) => Err(error).context("cannot claim the cluster mode"),
    }
}

/// Return true when `root` holds a replicated member namespace. Clusters from before the mode
/// claim have only these namespaces.
pub(crate) async fn has_replicated_member(store: &dyn ObjectStore, root: &str) -> Result<bool> {
    let listing = store.list_with_delimiter(Some(&Path::from(root))).await?;
    for prefix in listing.common_prefixes {
        if !prefix
            .filename()
            .is_some_and(|name| name.starts_with("member-"))
        {
            continue;
        }
        match store.head(&prefix.clone().join("owner.json")).await {
            Ok(_) => return Ok(true),
            Err(slatedb::object_store::Error::NotFound { .. }) => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(false)
}

/// Read the quota of an existing claim. A joining member uses this, because it is not one of the
/// initial voters.
pub(crate) async fn claimed_quota(
    store: &dyn ObjectStore,
    root: &str,
    cluster: &str,
) -> Result<u64> {
    let data = match store.get(&Path::from(join(root, CLAIM))).await {
        Ok(result) => result.bytes().await?,
        Err(slatedb::object_store::Error::NotFound { .. }) => {
            bail!("a joining member needs an existing shared cluster")
        }
        Err(error) => return Err(error.into()),
    };
    let existing: Claim = serde_json::from_slice(&data).context("invalid cluster claim")?;
    ensure!(
        existing.version == 1 && existing.cluster == cluster && existing.mode == StateMode::Shared,
        "the shared state belongs to another cluster"
    );
    Ok(existing.quota)
}

/// Create or check the shared state claim under `root`. Return the cluster quota.
///
/// The claim holds the initial voters. Two members that each bootstrap a separate one-voter
/// cluster in the same namespace race on one `PutMode::Create`, so only one opens the writer.
pub(crate) async fn claim(
    store: &dyn ObjectStore,
    root: &str,
    cluster: &str,
    quota: u64,
    voters: &[u64],
) -> Result<u64> {
    let path = Path::from(join(root, CLAIM));
    let mut voters = voters.to_vec();
    voters.sort_unstable();
    let claim = Claim {
        version: 1,
        cluster: cluster.to_owned(),
        mode: StateMode::Shared,
        quota,
        voters,
    };
    match store
        .put_opts(
            &path,
            serde_json::to_vec(&claim)?.into(),
            PutMode::Create.into(),
        )
        .await
    {
        Ok(_) => Ok(quota),
        Err(slatedb::object_store::Error::AlreadyExists { .. }) => {
            let data = store.get(&path).await?.bytes().await?;
            let existing: Claim = serde_json::from_slice(&data).context("invalid cluster claim")?;
            ensure!(
                existing.version == 1 && existing.cluster == cluster,
                "the shared state belongs to another cluster"
            );
            ensure!(
                existing.mode == StateMode::Shared,
                "the cluster uses another state mode"
            );
            ensure!(
                existing.voters == claim.voters,
                "the shared state belongs to other initial voters {:?}",
                existing.voters
            );
            if existing.quota != quota {
                tracing::warn!(
                    configured = quota,
                    cluster = existing.quota,
                    "the cluster claim sets the quota"
                );
            }
            Ok(existing.quota)
        }
        Err(error) => Err(error).context("cannot claim the shared state"),
    }
}

fn join(root: &str, name: &str) -> String {
    if root.is_empty() {
        name.to_owned()
    } else {
        format!("{root}/{name}")
    }
}

/// The objects of the shared state and the member cache that reads them.
pub(crate) struct Source {
    pub store: Arc<dyn ObjectStore>,
    /// `<prefix>/<sha256(cluster)>`, or empty for a store that holds one cluster.
    root: String,
    pub cache: Arc<dyn slatedb::db_cache::DbCache>,
    /// Opens and deletions of checkpoints. One lock orders them. See `Pins`.
    pins: Mutex<Pins>,
    /// Bases of received snapshots, with their receive time. Install takes the base. Raft can
    /// also drop a snapshot, so a receipt expires after `RECEIPT_LIFETIME`.
    received: Mutex<std::collections::HashMap<uuid::Uuid, (std::time::Instant, Arc<Base>)>>,
}

/// The checkpoints that this member uses, and the ones that retention deletes.
///
/// An open reserves its checkpoint before any storage I/O, and retention marks its candidates
/// before it deletes them. Both steps take the same lock. So an open in flight protects its
/// checkpoint, and an open that starts after the selection fails instead of publishing a base
/// whose checkpoint is going away.
#[derive(Default)]
struct Pins {
    /// Bases that opened. A base with a live reference pins its checkpoint.
    bases: Vec<(uuid::Uuid, Weak<Base>)>,
    /// Opens in flight, by checkpoint.
    opening: std::collections::HashMap<uuid::Uuid, usize>,
    /// Checkpoints that retention selected and deletes.
    retiring: HashSet<uuid::Uuid>,
}

impl Pins {
    fn pinned(&mut self) -> HashSet<uuid::Uuid> {
        self.bases.retain(|(_, base)| base.strong_count() > 0);
        self.bases
            .iter()
            .map(|(id, _)| *id)
            .chain(self.opening.keys().copied())
            .collect()
    }
}

/// An open in flight. Dropping it ends the reservation.
pub(crate) struct Opening<'a> {
    source: &'a Source,
    id: uuid::Uuid,
}

impl Drop for Opening<'_> {
    fn drop(&mut self) {
        let mut pins = self.source.pins.lock().unwrap();
        if let Some(count) = pins.opening.get_mut(&self.id) {
            *count -= 1;
            if *count == 0 {
                pins.opening.remove(&self.id);
            }
        }
    }
}

impl Source {
    /// The path of the shared SlateDB.
    pub fn state(&self) -> String {
        join(&self.root, STATE)
    }

    /// Reserve checkpoint `id` for an open. Fail when retention is deleting it.
    pub(crate) fn reserve_open(&self, id: uuid::Uuid) -> Result<Opening<'_>> {
        let mut pins = self.pins.lock().unwrap();
        ensure!(
            !pins.retiring.contains(&id),
            "view checkpoint {id} is being deleted"
        );
        *pins.opening.entry(id).or_default() += 1;
        Ok(Opening { source: self, id })
    }

    pub async fn open_base(&self, id: uuid::Uuid) -> Result<Arc<Base>> {
        let opening = self.reserve_open(id)?;
        let base = Base::open(self.store.clone(), &self.state(), id, self.cache.clone()).await?;
        let base = Arc::new(base);
        // The reservation ends only after the base is pinned.
        self.pins
            .lock()
            .unwrap()
            .bases
            .push((id, Arc::downgrade(&base)));
        drop(opening);
        Ok(base)
    }

    /// The checkpoints that this member uses: live bases and opens in flight.
    pub fn pinned(&self) -> HashSet<uuid::Uuid> {
        self.received
            .lock()
            .unwrap()
            .retain(|_, (at, _)| at.elapsed() < RECEIPT_LIFETIME);
        self.pins.lock().unwrap().pinned()
    }

    /// Select the checkpoints to delete and mark them, under the lock that opens take. A later
    /// open of a marked checkpoint fails. Call `end_retire` after the deletions.
    pub(crate) fn begin_retire(
        &self,
        views: &[(u64, uuid::Uuid)],
        reported: &HashSet<uuid::Uuid>,
        keep: usize,
    ) -> Vec<uuid::Uuid> {
        let mut pins = self.pins.lock().unwrap();
        let pinned = pins.pinned();
        let candidates = retired(views, &pinned, reported, keep);
        pins.retiring.extend(candidates.iter().copied());
        candidates
    }

    pub(crate) fn end_retire(&self, ids: &[uuid::Uuid]) {
        let mut pins = self.pins.lock().unwrap();
        for id in ids {
            pins.retiring.remove(id);
        }
    }

    /// Open the checkpoint of a received snapshot before Raft accepts it. A failed open fails the
    /// request, and the leader retries.
    pub async fn receive(&self, id: uuid::Uuid) -> Result<()> {
        if self.received.lock().unwrap().contains_key(&id) {
            return Ok(());
        }
        let base = self.open_base(id).await?;
        // Raft can drop an older snapshot without an install. Keep only the newest one.
        *self.received.lock().unwrap() =
            std::collections::HashMap::from([(id, (std::time::Instant::now(), base))]);
        Ok(())
    }

    fn take_received(&self, id: uuid::Uuid) -> Option<Arc<Base>> {
        self.received
            .lock()
            .unwrap()
            .remove(&id)
            .map(|(_, base)| base)
    }

    /// Open the writable shared database. Opening it fences an older writer.
    pub async fn open_writer(&self) -> Result<Db> {
        Ok(Db::builder(self.state(), self.store.clone())
            .with_db_cache(self.cache.clone(), 0)
            .with_settings(slatedb_settings())
            .with_block_cache_policy(block_cache_policy())
            .build()
            .await?)
    }

    /// Return true after the first checkpoint was published.
    pub async fn bootstrapped(&self) -> Result<bool> {
        match self
            .store
            .head(&Path::from(join(&self.root, BOOTSTRAP)))
            .await
        {
            Ok(_) => Ok(true),
            Err(slatedb::object_store::Error::NotFound { .. }) => Ok(false),
            Err(error) => Err(error.into()),
        }
    }

    /// Record that a checkpoint exists. A member never starts from an empty base after this.
    pub async fn mark_bootstrapped(&self, checkpoint: uuid::Uuid) -> Result<()> {
        let data = serde_json::to_vec(&serde_json::json!({ "first_checkpoint": checkpoint }))?;
        match self
            .store
            .put_opts(
                &Path::from(join(&self.root, BOOTSTRAP)),
                data.into(),
                PutMode::Create.into(),
            )
            .await
        {
            Ok(_) | Err(slatedb::object_store::Error::AlreadyExists { .. }) => Ok(()),
            Err(error) => Err(error.into()),
        }
    }
}

/// The checkpoint name for a publication that started at `last_l0_seq`.
pub(crate) fn view_name(seq: u64) -> String {
    format!("{VIEW}{seq:020}")
}

/// The `view-` checkpoints, ordered by the sequence in their names.
pub(crate) async fn view_checkpoints(
    admin: &slatedb::admin::Admin,
) -> Result<Vec<(u64, slatedb::Checkpoint)>> {
    // The first leader creates the database. Before that it has no manifest.
    if admin.read_manifest(None).await?.is_none() {
        return Ok(vec![]);
    }
    let mut views: Vec<_> = admin
        .list_checkpoints(None)
        .await?
        .into_iter()
        .filter_map(|checkpoint| {
            let seq = checkpoint
                .name
                .as_deref()?
                .strip_prefix(VIEW)?
                .parse()
                .ok()?;
            Some((seq, checkpoint))
        })
        .collect();
    views.sort_by_key(|(seq, _)| *seq);
    Ok(views)
}

/// Return the view checkpoints to delete: every checkpoint except the newest `keep`, and only
/// those older than the oldest checkpoint in `pinned`. A base pins memory, not objects, so a
/// checkpoint under a live view must survive GC. `reported` holds the checkpoints that other
/// members reported in `ViewAck`; they also survive.
pub(crate) fn retired(
    views: &[(u64, uuid::Uuid)],
    pinned: &HashSet<uuid::Uuid>,
    reported: &HashSet<uuid::Uuid>,
    keep: usize,
) -> Vec<uuid::Uuid> {
    let oldest_pin = views
        .iter()
        .filter(|(_, id)| pinned.contains(id))
        .map(|(seq, _)| *seq)
        .min()
        .unwrap_or(u64::MAX);
    let old = views.len().saturating_sub(keep);
    views[..old]
        .iter()
        .filter(|(seq, id)| *seq < oldest_pin && !reported.contains(id))
        .map(|(_, id)| *id)
        .collect()
}

/// Delete the old `view-` checkpoints that no live base of this member pins and that no member
/// reported. Other checkpoints are not ours.
///
/// Deletion writes the manifest without an epoch check, so a deposed writer could delete a
/// checkpoint that the new leader's members pin. With `epoch`, retention reads the manifest
/// before each deletion and stops when another writer holds a newer epoch.
pub(crate) async fn retain(
    admin: &slatedb::admin::Admin,
    source: &Source,
    reported: &HashSet<uuid::Uuid>,
    keep: usize,
    epoch: Option<u64>,
) -> Result<()> {
    let views: Vec<_> = view_checkpoints(admin)
        .await?
        .into_iter()
        .map(|(seq, checkpoint)| (seq, checkpoint.id))
        .collect();
    let candidates = source.begin_retire(&views, reported, keep);
    let mut result = Ok(());
    for id in &candidates {
        if let Some(epoch) = epoch {
            let current = admin.read_manifest(None).await.map_err(anyhow::Error::from);
            match current.map(|manifest| manifest.map(|manifest| manifest.writer_epoch())) {
                Ok(Some(current)) if current == epoch => {}
                Ok(current) => {
                    tracing::warn!(?current, epoch, "a newer writer holds the shared state");
                    break;
                }
                Err(error) => {
                    result = Err(error);
                    break;
                }
            }
        }
        if let Err(error) = admin.delete_checkpoint(*id).await {
            result = Err(error.into());
            break;
        }
    }
    source.end_retire(&candidates);
    result
}

/// Open the newest `view-` checkpoint at or below index `limit` that holds the membership.
async fn newest_base_through(
    admin: &slatedb::admin::Admin,
    source: &Source,
    limit: u64,
) -> Result<Option<Arc<Base>>> {
    for (seq, checkpoint) in view_checkpoints(admin).await?.into_iter().rev() {
        // The name holds a lower bound of `F`. The base holds `F` itself.
        if seq > limit {
            continue;
        }
        let base = source.open_base(checkpoint.id).await?;
        if base.applied <= limit && base.get(crate::cluster::KEY).await?.is_some() {
            return Ok(Some(base));
        }
    }
    Ok(None)
}

/// Open the newest `view-` checkpoint that holds the membership. The publisher deletes a
/// checkpoint without it, but a stop can leave one behind.
async fn newest_base(admin: &slatedb::admin::Admin, source: &Source) -> Result<Option<Arc<Base>>> {
    for (_, checkpoint) in view_checkpoints(admin).await?.into_iter().rev() {
        let base = source.open_base(checkpoint.id).await?;
        if base.get(crate::cluster::KEY).await?.is_some() {
            return Ok(Some(base));
        }
        tracing::warn!(checkpoint = %checkpoint.id, "skip a view checkpoint without membership");
    }
    Ok(None)
}

/// Return true when `error` comes from an object that the store no longer has, such as an SST
/// that GC deleted under an old base. Other storage errors still stop the member.
///
/// A SlateDB error keeps its kind and its object store source, so the check uses them. Apply
/// reports some storage errors as text through `KvError`, so the check also matches that text.
pub(crate) fn missing_object(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        if let Some(error) = cause.downcast_ref::<slatedb::Error>() {
            let mut source = std::error::Error::source(error);
            while let Some(inner) = source {
                if matches!(
                    inner.downcast_ref::<slatedb::object_store::Error>(),
                    Some(slatedb::object_store::Error::NotFound { .. })
                ) {
                    return error.kind() == slatedb::ErrorKind::Data;
                }
                source = inner.source();
            }
        }
        let text = cause.to_string();
        text.contains("Data error") && text.contains("not found")
    })
}

/// Bases that opened in the background. Apply takes the newest one.
pub(crate) struct Opened {
    slot: Mutex<Option<Arc<Base>>>,
    /// The `F` of the newest offered base. The node starts a switch when it goes up.
    ready: tokio::sync::watch::Sender<u64>,
}

impl Default for Opened {
    fn default() -> Self {
        Self {
            slot: Mutex::default(),
            ready: tokio::sync::watch::channel(0).0,
        }
    }
}

impl Opened {
    /// Keep `base` unless an opened base already covers more. Opens can finish out of order.
    pub(crate) fn offer(&self, base: Arc<Base>) {
        let applied = base.applied;
        let mut slot = self.slot.lock().unwrap();
        if slot.as_ref().is_some_and(|old| old.applied >= applied) {
            return;
        }
        *slot = Some(base);
        drop(slot);
        self.ready.send_if_modified(|old| {
            let newer = *old < applied;
            *old = (*old).max(applied);
            newer
        });
    }

    fn take_through(&self, pushed: u64) -> Option<Arc<Base>> {
        let mut slot = self.slot.lock().unwrap();
        // A base above the applied state needs an adopt: a snapshot install or a restart.
        if slot.as_ref().is_some_and(|base| base.applied <= pushed) {
            return slot.take();
        }
        None
    }

    pub(crate) fn subscribe(&self) -> tokio::sync::watch::Receiver<u64> {
        self.ready.subscribe()
    }

    /// The `F` of the opened base that waits in the slot.
    #[cfg(test)]
    pub(crate) fn newest(&self) -> Option<u64> {
        self.slot.lock().unwrap().as_ref().map(|base| base.applied)
    }

    /// Take the opened base. The caller holds it until it adopts it or offers it back.
    pub(crate) fn take(&self) -> Option<Arc<Base>> {
        self.slot.lock().unwrap().take()
    }
}

/// The shared-mode settings of one member.
#[derive(Clone, Debug)]
pub(crate) struct Options {
    /// The quota of a new cluster claim. An existing claim sets the cluster quota.
    pub quota: u64,
    /// The initial voters of a new cluster claim. Every bootstrap member must name the same set.
    pub voters: Vec<u64>,
    /// The member joins after MemberAdd. It skips the voter check of the claim.
    pub join: bool,
    /// The member has no Raft log yet. It may start from an empty base and wait for Raft.
    pub empty_log: bool,
    /// The longest time that a read view may pin its base.
    pub pin_max: Duration,
}

/// The apply side of shared mode on one member.
pub(crate) struct Shared {
    pub(crate) overlay: Overlay,
    pub(crate) base: Arc<Base>,
    opened: Arc<Opened>,
    pub(crate) source: Arc<Source>,
    /// `ViewCheckpoint` entries that this member applied.
    pub(crate) announcements: u64,
    /// The `F` of the current base. Physical Compact replies wait for a covering base.
    based: Arc<tokio::sync::watch::Sender<u64>>,
    pub(crate) pin_max: Duration,
}

impl Shared {
    fn new(base: Arc<Base>, source: Arc<Source>, pin_max: Duration) -> Self {
        Self {
            overlay: Overlay::new(base.applied),
            based: Arc::new(tokio::sync::watch::channel(base.applied).0),
            base,
            opened: Arc::default(),
            source,
            announcements: 0,
            pin_max,
        }
    }

    /// The view at `seq`.
    pub(crate) fn view(&self, seq: u64) -> StateView {
        StateView::Shared(SharedView::new(
            self.base.clone(),
            self.overlay.segments(),
            seq,
        ))
    }

    /// The view that apply reads. It sees every batch that apply adds later.
    pub(crate) fn live(&self) -> StateView {
        self.view(u64::MAX)
    }

    /// The view after the newest batch, which equals the applied state.
    pub(crate) fn pinned(&self) -> StateView {
        self.view(self.overlay.queue().pushed())
    }

    /// Close the open segment and open checkpoint `id` in the background.
    pub(crate) fn announce(&mut self, id: uuid::Uuid) {
        self.announcements += 1;
        self.overlay.close_segment();
        let source = self.source.clone();
        let opened = self.opened.clone();
        tokio::spawn(async move {
            match source.open_base(id).await {
                Ok(base) => opened.offer(base),
                // The member keeps its base and waits for the next checkpoint.
                Err(error) => tracing::warn!(%error, %id, "cannot open a view checkpoint"),
            }
        });
    }

    /// Offer a base that opened elsewhere, such as in a test.
    #[cfg(test)]
    pub(crate) fn offer(&self, base: Arc<Base>) {
        self.opened.offer(base);
    }

    fn set_base(&mut self, base: Arc<Base>) {
        self.based.send_replace(base.applied);
        self.base = base;
    }

    /// Switch to the newest opened base when it covers more than the current one. Return true
    /// after a switch. The base `F` never goes down.
    pub(crate) fn switch(&mut self) -> bool {
        let Some(base) = self.opened.take_through(self.overlay.queue().pushed()) else {
            return false;
        };
        if base.applied <= self.base.applied {
            return false;
        }
        // A paused member switches before the next `ViewCheckpoint` entry applies, so the
        // switch also closes the open segment. A covered open segment is then released.
        self.overlay.close_segment();
        self.overlay.release(base.applied);
        self.set_base(base);
        true
    }

    /// Make `base` the whole state and drop the overlay. Install and rebase use this.
    fn reset(&mut self, base: Arc<Base>) {
        self.overlay.reset(base.applied);
        self.set_base(base);
    }
}

/// The shared-mode handles that the node uses outside apply.
pub(crate) struct Handles {
    pub admin: Arc<slatedb::admin::Admin>,
    pub queue: Arc<super::overlay::Queue>,
    pub source: Arc<Source>,
    /// The `F` of the current base.
    pub based: tokio::sync::watch::Receiver<u64>,
    pub opened: Arc<Opened>,
}

impl StateMachine {
    /// Open the shared state of the cluster at `root` in `store`. See the module comment.
    ///
    /// No member opens the writer here. The leader opens it in `node/shared`.
    pub(crate) async fn open_shared(
        store: Arc<dyn ObjectStore>,
        root: &str,
        cluster: &str,
        cache: StateCache,
        options: &Options,
        backend: transfer::Backend,
    ) -> Result<Self> {
        cache.validate()?;
        let quota = if options.join {
            claimed_quota(store.as_ref(), root, cluster).await?
        } else {
            claim(
                store.as_ref(),
                root,
                cluster,
                options.quota,
                &options.voters,
            )
            .await?
        };
        let location =
            transfer::Location::open(store.clone(), &join(root, STATE), backend, cache).await?;
        let state_root = location.root();
        let source = Arc::new(Source {
            store: store.clone(),
            root: root.to_owned(),
            cache: cache.build(),
            pins: Mutex::default(),
            received: Mutex::default(),
        });
        ensure!(
            state_root == source.state(),
            "shared state has no generations"
        );
        let rows = Arc::new(rows::RowCache::new(cache.row_bytes));
        let admin = Arc::new(slatedb::admin::Admin::builder(state_root.as_str(), store).build());
        // A joining member with no log waits for Raft: entries from index 1 or a snapshot.
        let base = match newest_base(&admin, &source).await? {
            Some(_) if options.join && options.empty_log => Arc::new(Base::empty()),
            Some(base) => {
                // A stop between the first checkpoint and its marker leaves no marker.
                if !source.bootstrapped().await? {
                    source.mark_bootstrapped(base.checkpoint.unwrap()).await?;
                }
                base
            }
            None if source.bootstrapped().await? && !(options.join && options.empty_log) => {
                bail!("the shared state has no view checkpoint after its bootstrap")
            }
            None => Arc::new(Base::empty()),
        };
        let shared = Shared::new(base, source, options.pin_max);
        let reads = shared.live();
        let mut state = Self::load(None, admin, location, rows, reads, Some(shared)).await?;
        state.quota = quota;
        state.plan_compaction();
        Ok(state)
    }

    /// Return true in shared state mode.
    pub(crate) fn is_shared(&self) -> bool {
        self.shared.is_some()
    }

    /// The handles of the node, in shared mode.
    pub(crate) fn shared_handles(&self) -> Option<Handles> {
        let shared = self.shared.as_ref()?;
        Some(Handles {
            admin: self.admin.clone(),
            queue: shared.overlay.queue(),
            source: shared.source.clone(),
            based: shared.based.subscribe(),
            opened: shared.opened.clone(),
        })
    }

    /// Apply a `ViewCheckpoint` entry. It changes no etcd state and writes no batch.
    pub(crate) async fn apply_view_checkpoint(
        &mut self,
        index: u64,
        checkpoint: &str,
    ) -> Result<()> {
        let Some(shared) = &mut self.shared else {
            // A replicated member stores only the apply position.
            self.apply(index, None).await?;
            return Ok(());
        };
        ensure!(
            index == self.applied + 1,
            "non-contiguous state machine apply"
        );
        let id = uuid::Uuid::parse_str(checkpoint).context("invalid view checkpoint")?;
        shared.announce(id);
        self.reads = shared.live();
        self.applied = index;
        self.applied_term = self.entry_term;
        Ok(())
    }

    /// Use a base that finished opening. Apply calls this before each job.
    pub(crate) fn switch_base(&mut self) {
        if let Some(shared) = &mut self.shared {
            if shared.switch() {
                self.reads = shared.live();
            }
        }
    }

    /// Make `base` the state at its index `F` and drop the overlay. The Raft log must already
    /// hold the matching boundary: a received snapshot, or a log that covers `F`.
    ///
    /// `term` is the Raft term at `F` when the caller knows it. A known checkpoint term must
    /// match it.
    pub(crate) async fn adopt_base(&mut self, base: Arc<Base>, term: u64) -> Result<()> {
        let mut shared = self
            .shared
            .take()
            .context("adopt needs shared state mode")?;
        shared.reset(base);
        let reads = shared.live();
        let rows = Arc::new(rows::RowCache::new(self.location.cache.row_bytes));
        let result = Self::load(
            None,
            self.admin.clone(),
            self.location.clone(),
            rows,
            reads,
            Some(shared),
        )
        .await;
        let mut next = result?;
        if next.applied_term != 0 && term != 0 {
            ensure!(
                next.applied_term == term,
                "checkpoint term {} differs from Raft term {term}",
                next.applied_term
            );
        }
        if next.applied_term == 0 {
            next.applied_term = term;
            next.entry_term = term;
        }
        next.metrics = self.metrics.clone();
        next.quota = self.quota;
        next.write_batches = self.write_batches;
        next.row_marker = self.row_marker.take();
        // The new rows reach watchers through storage catch-up.
        next.watch_capture = self.watch_capture.take();
        if let Some(capture) = &mut next.watch_capture {
            capture.invalidate();
        }
        *self = next;
        self.plan_compaction();
        Ok(())
    }

    /// Rebuild the state from the newest checkpoint at or below `limit` after a missing object.
    /// The caller replays the local log from the new `applied` to `limit`.
    pub(crate) async fn rebase(&mut self, limit: u64) -> Result<()> {
        let shared = self
            .shared
            .as_ref()
            .context("rebase needs shared state mode")?;
        let base = newest_base_through(&self.admin, &shared.source, limit)
            .await?
            .context("the shared state has no view checkpoint at or below the rebase index")?;
        tracing::warn!(
            base = base.applied,
            applied = self.applied,
            "rebase on a new checkpoint"
        );
        self.adopt_base(base, 0).await
    }

    /// Adopt the opened base above the applied index. A paused member uses this when its log
    /// commits the base index, so the Raft log needs no change. `term` is the log term there.
    pub(crate) async fn adopt_opened(&mut self, base: Arc<Base>, term: u64) -> Result<()> {
        ensure!(base.applied > self.applied, "the opened base is not ahead");
        self.adopt_base(base, term).await
    }

    /// Install a format 6 snapshot: the checkpoint that Raft sent. The receive handler opened it.
    pub(crate) async fn install_view(
        &mut self,
        descriptor: transfer::ViewDescriptor,
        index: u64,
        term: u64,
    ) -> Result<()> {
        let shared = self
            .shared
            .as_ref()
            .context("a format 6 snapshot needs shared state mode")?;
        let id = descriptor.checkpoint()?;
        // Release the receipt on every path, also for a snapshot that applies nothing.
        let received = shared.source.take_received(id);
        if index <= self.applied {
            return Ok(());
        }
        let base = match received {
            Some(base) => base,
            None => shared.source.open_base(id).await?,
        };
        ensure!(
            base.applied == index,
            "checkpoint index {} differs from snapshot index {index}",
            base.applied
        );
        self.adopt_base(base, term).await
    }

    /// The format 6 snapshot of the current base: its index, data, and ConfState. `None` before
    /// the first checkpoint.
    pub(crate) async fn base_snapshot(
        &self,
    ) -> Result<Option<(u64, Vec<u8>, raft::eraftpb::ConfState)>> {
        let shared = self.shared.as_ref().context("not in shared state mode")?;
        let Some(id) = shared.base.checkpoint else {
            return Ok(None);
        };
        // The ConfState comes from the checkpoint, not from the current membership.
        let members = shared
            .base
            .get(crate::cluster::KEY)
            .await?
            .context("the base checkpoint has no membership")?;
        let members: crate::cluster::Store = serde_json::from_slice(&members)?;
        let data = transfer::ViewDescriptor::new(id).encode()?;
        Ok(Some((shared.base.applied, data, members.conf())))
    }

    /// The base index `F`, the applied announcements, and the overlay bytes, in shared mode.
    pub(crate) fn view_status(&self) -> Option<(u64, u64, u64)> {
        let shared = self.shared.as_ref()?;
        Some((
            shared.base.applied,
            shared.announcements,
            shared.overlay.bytes(),
        ))
    }
}
