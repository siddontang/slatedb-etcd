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

/// The eight-byte logical size total after each shared-mode batch, for older readers. The
/// split by row kind is in the counter row with the apply marker.
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
/// cannot both pass, because only one `PutMode::Create` succeeds. An existing claim is read
/// first, so a member with a read-only credential can start.
pub(crate) async fn claim_mode(
    store: &dyn ObjectStore,
    root: &str,
    cluster: &str,
    mode: StateMode,
) -> Result<()> {
    let path = Path::from(join(root, MODE));
    let check = |data: &[u8]| -> Result<()> {
        let existing: ModeClaim =
            serde_json::from_slice(data).context("invalid cluster mode claim")?;
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
    };
    match store.get(&path).await {
        Ok(result) => return check(&result.bytes().await?),
        Err(slatedb::object_store::Error::NotFound { .. }) => {}
        Err(error) => return Err(error).context("cannot read the cluster mode"),
    }
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
            check(&store.get(&path).await?.bytes().await?)
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
    /// The SSTs whose cache entries a warm loaded. Only SSTs of registered live bases stay here.
    /// The same lock orders base registrations, warm results, and pruning.
    warmed: HashSet<slatedb::manifest::SsTableId>,
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

/// Prunes the warm set when a warm ends or its caller drops it.
struct Prune<'a>(&'a Source);

impl Drop for Prune<'_> {
    fn drop(&mut self) {
        self.0.prune_warmed();
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

    /// Load the cache entries of the SSTs of `base` that no earlier warm loaded. See
    /// `Base::warm_sst`. Only a follower adoption calls this, within `WARM_MAX`. A publication,
    /// a snapshot receipt, or a restart opens a base without it.
    ///
    /// Each SST counts as warm only after its entries loaded, so a failed or stopped warm leaves
    /// it for the next base. Concurrent warms add to one set. When the warm ends or its caller
    /// drops it at the time limit, the set drops SSTs that no live base holds. So the set does
    /// not grow with each flush and compaction, also when every warm times out.
    pub(crate) async fn warm(&self, base: &Base) {
        let _prune = Prune(self);
        let cold: Vec<_> = {
            let pins = self.pins.lock().unwrap();
            base.ssts()
                .iter()
                .filter(|(id, _)| !pins.warmed.contains(id))
                .copied()
                .collect()
        };
        for (id, l0) in cold {
            match base.warm_sst(id, l0).await {
                Ok(()) => {
                    self.pins.lock().unwrap().warmed.insert(id);
                }
                Err(error) => {
                    tracing::warn!(%error, sst = %id.value(), "cannot warm the cache for an SST");
                }
            }
        }
    }

    /// Drop the warm SSTs that no registered live base holds. A warm and a base switch call
    /// this. The base of a running warm is live, because the warm borrows it. One lock covers the
    /// live bases and the set, so a base that registers or a warm that ends at the same time is
    /// ordered before or after this.
    fn prune_warmed(&self) {
        let mut pins = self.pins.lock().unwrap();
        pins.bases.retain(|(_, base)| base.strong_count() > 0);
        // The references end after the lock: the last one may drop a base and its reader.
        let live: Vec<Arc<Base>> = pins.bases.iter().filter_map(|(_, b)| b.upgrade()).collect();
        let ssts: HashSet<_> = live
            .iter()
            .flat_map(|base| base.ssts().iter().map(|(id, _)| *id))
            .collect();
        pins.warmed.retain(|id| ssts.contains(id));
        drop(pins);
        drop(live);
    }

    /// The SSTs whose cache entries a warm loaded, and the SSTs of the registered live bases,
    /// read under one lock.
    #[cfg(test)]
    pub(crate) fn warmed(
        &self,
    ) -> (
        HashSet<slatedb::manifest::SsTableId>,
        HashSet<slatedb::manifest::SsTableId>,
    ) {
        let pins = self.pins.lock().unwrap();
        let live: Vec<Arc<Base>> = pins.bases.iter().filter_map(|(_, b)| b.upgrade()).collect();
        let ssts = live
            .iter()
            .flat_map(|base| base.ssts().iter().map(|(id, _)| *id))
            .collect();
        let warmed = pins.warmed.clone();
        drop(pins);
        drop(live);
        (warmed, ssts)
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
        self.open_writer_with(slatedb_settings()).await
    }

    pub async fn open_writer_with(&self, settings: Settings) -> Result<Db> {
        Ok(Db::builder(self.state(), self.store.clone())
            .with_db_cache(self.cache.clone(), 0)
            .with_settings(settings)
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
    /// Write the bootstrap marker. A pending restore claim gives its snapshot checksum to the
    /// marker, so a repair after a stopped restore keeps the identity of that restore.
    pub async fn mark_bootstrapped(&self, checkpoint: uuid::Uuid) -> Result<()> {
        let restore = match bootstrap_op(self).await? {
            Some(BootstrapOp::Restore { snapshot }) => Some(snapshot),
            _ => None,
        };
        self.mark_bootstrapped_with(checkpoint, restore.as_deref())
            .await
    }

    /// The checksum of the snapshot that the bootstrap marker records, if a restore wrote it.
    pub(crate) async fn restored_snapshot(&self) -> Result<Option<String>> {
        let data = self
            .store
            .get(&Path::from(join(&self.root, BOOTSTRAP)))
            .await?
            .bytes()
            .await?;
        let marker: serde_json::Value = serde_json::from_slice(&data)?;
        Ok(marker["restore"].as_str().map(str::to_owned))
    }

    /// Write the bootstrap marker. A restore records its snapshot checksum in it.
    pub(crate) async fn mark_bootstrapped_with(
        &self,
        checkpoint: uuid::Uuid,
        restore: Option<&str>,
    ) -> Result<()> {
        let data = serde_json::to_vec(&serde_json::json!({
            "first_checkpoint": checkpoint,
            "restore": restore,
        }))?;
        match self
            .store
            .put_opts(
                &Path::from(join(&self.root, BOOTSTRAP)),
                data.into(),
                PutMode::Create.into(),
            )
            .await
        {
            Ok(_) => Ok(()),
            // Another member wrote the marker first. It must name the same bootstrap.
            Err(slatedb::object_store::Error::AlreadyExists { .. }) => {
                let existing = self.restored_snapshot().await?;
                ensure!(
                    existing.as_deref() == restore,
                    "the bootstrap marker records another bootstrap"
                );
                Ok(())
            }
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
    // A Defragment publishes a second checkpoint at the same sequence. Its manifest is newer.
    views.sort_by_key(|(seq, checkpoint)| (*seq, checkpoint.manifest_id));
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

/// Open the newest `view-` checkpoint whose row is in `[low, high]` and that holds the
/// membership. The name gives only a lower bound of the row, so each candidate opens and its own
/// row decides.
///
/// A record above its checkpoint row needs that checkpoint, or one in this range: the leader
/// certified that no entry in `(low, high]` changed state, so each checkpoint of this shared
/// state with its row in the range holds the state at `high`.
async fn newest_base_between(
    admin: &slatedb::admin::Admin,
    source: &Source,
    low: u64,
    high: u64,
) -> Result<Option<Arc<Base>>> {
    for (seq, checkpoint) in view_checkpoints(admin).await?.into_iter().rev() {
        if seq > high {
            continue;
        }
        let base = source.open_base(checkpoint.id).await?;
        if (low..=high).contains(&base.applied) && base.get(crate::cluster::KEY).await?.is_some() {
            return Ok(Some(base));
        }
    }
    Ok(None)
}

/// Open checkpoint `checkpoint` of a base above its row `row` at `index`. If it is gone, take
/// the newest checkpoint with its row in `[row, index]`. A missing row means the record names no
/// range, so only the checkpoint itself can serve.
async fn reopen_above_row(
    admin: &slatedb::admin::Admin,
    source: &Source,
    checkpoint: uuid::Uuid,
    row: Option<u64>,
    index: u64,
) -> Result<Arc<Base>> {
    let error = match source.open_base(checkpoint).await {
        Ok(base) => return Ok(base),
        Err(error) => error,
    };
    let replacement = match row {
        Some(row) => newest_base_between(admin, source, row, index).await?,
        None => None,
    };
    replacement.ok_or(error).with_context(|| {
        format!(
            "cannot open checkpoint {checkpoint}, which holds the state at {index}, and no other \
             view checkpoint has a row from {} to {index}; restore the checkpoint, or give this \
             member an empty data directory so that it receives a snapshot",
            row.map_or("its row".to_owned(), |row| row.to_string()),
        )
    })
}

/// Open the newest `view-` checkpoint that holds the membership. The publisher deletes a
/// checkpoint without it, but a stop can leave one behind.
/// The source of the offline converter. It opens no member state.
pub(crate) fn offline_source(
    store: Arc<dyn ObjectStore>,
    root: &str,
    cache: StateCache,
) -> Result<Arc<Source>> {
    cache.validate()?;
    Ok(Arc::new(Source {
        store,
        root: root.to_owned(),
        cache: cache.build(),
        pins: Mutex::default(),
        received: Mutex::default(),
    }))
}

/// The term at the index of `base`, zero when unknown, and its membership row.
pub(crate) async fn identity_parts(base: &Base) -> Result<(u64, Option<Vec<u8>>)> {
    let term = match base.get(APPLIED_TERM).await? {
        Some(row) => {
            let row: [u8; 16] = row.as_ref().try_into().context("invalid applied term")?;
            let index = u64::from_be_bytes(row[..8].try_into()?);
            let term = u64::from_be_bytes(row[8..].try_into()?);
            if index == base.applied {
                term
            } else {
                0
            }
        }
        None => 0,
    };
    let members = base.get(crate::cluster::KEY).await?.map(|row| row.to_vec());
    Ok((term, members))
}

/// Change the mode claim of a stopped replicated cluster to shared. Only the offline converter
/// does this. A cluster that already has the shared claim keeps it.
pub(crate) async fn convert_mode(store: &dyn ObjectStore, root: &str, cluster: &str) -> Result<()> {
    let path = Path::from(join(root, MODE));
    match store.get(&path).await {
        Ok(result) => {
            let existing: ModeClaim = serde_json::from_slice(&result.bytes().await?)
                .context("invalid cluster mode claim")?;
            ensure!(
                existing.version == 1 && existing.cluster == cluster,
                "the namespace belongs to another cluster"
            );
            if existing.mode == StateMode::Shared {
                return Ok(());
            }
        }
        Err(slatedb::object_store::Error::NotFound { .. }) => {}
        Err(error) => return Err(error.into()),
    }
    let claim = ModeClaim {
        version: 1,
        cluster: cluster.to_owned(),
        mode: StateMode::Shared,
    };
    store.put(&path, serde_json::to_vec(&claim)?.into()).await?;
    Ok(())
}

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

/// Bases that opened in the background, each with the index that its announcement certified.
/// Apply takes the newest one.
pub(crate) struct Opened {
    slot: Mutex<Option<(Arc<Base>, u64)>>,
    /// The coverage of the newest offered base. The node starts a switch when it goes up.
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
    /// A later checkpoint at the same coverage, such as after a Defragment, replaces the waiting
    /// one, so its new layout reaches the member.
    ///
    /// `covers` is the index through which the base holds the whole state, as its announcement
    /// certified. A lower value means the base row `F`. The coverage belongs to this checkpoint
    /// only. Another checkpoint at the same row does not get it.
    pub(crate) fn offer(&self, base: Arc<Base>, covers: u64) {
        let covers = covers.max(base.applied);
        let mut slot = self.slot.lock().unwrap();
        if slot.as_ref().is_some_and(|(old, old_covers)| {
            *old_covers > covers || (*old_covers == covers && old.checkpoint == base.checkpoint)
        }) {
            return;
        }
        *slot = Some((base, covers));
        drop(slot);
        // Each offer starts a switch, also at an unchanged index.
        self.ready.send_modify(|old| *old = (*old).max(covers));
    }

    fn take_through(&self, covered: u64) -> Option<(Arc<Base>, u64)> {
        let mut slot = self.slot.lock().unwrap();
        // A base above the applied state needs an adopt: a snapshot install or a restart.
        if slot
            .as_ref()
            .is_some_and(|(base, _)| base.applied <= covered)
        {
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
        self.slot
            .lock()
            .unwrap()
            .as_ref()
            .map(|(base, _)| base.applied)
    }

    /// Take the opened base and its coverage. The caller holds them until it adopts the base or
    /// offers it back.
    pub(crate) fn take(&self) -> Option<(Arc<Base>, u64)> {
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
    /// Rows of a native snapshot that seed a new cluster.
    pub restore: Option<Restore>,
    /// The checkpoint that the durable Raft snapshot record names, if any.
    pub record: Option<Record>,
}

/// A format 6 Raft snapshot record: a checkpoint, the record index, and the term there. The
/// index can be above the checkpoint row, because a certified announcement extends it. Such a
/// record also holds the row `F`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Record {
    pub checkpoint: uuid::Uuid,
    pub index: u64,
    pub term: u64,
    /// The checkpoint row of a record above it. `None` for a record that does not say.
    pub row: Option<u64>,
}

impl Record {
    /// Read the record of a Raft snapshot. Other formats give `None`.
    pub(crate) fn of(snapshot: &raft::eraftpb::Snapshot) -> Result<Option<Self>> {
        let Some(view) = transfer::ViewDescriptor::decode(snapshot.get_data())? else {
            return Ok(None);
        };
        Ok(Some(Self {
            checkpoint: view.checkpoint()?,
            index: snapshot.get_metadata().index,
            term: snapshot.get_metadata().term,
            row: view.row,
        }))
    }
}

/// The rows of a native snapshot that seed a new shared cluster. The shared database gets them
/// at index 1 and term 1, then the first view checkpoint and the bootstrap marker. Each member
/// then adopts that checkpoint as its base, as at a restart.
#[derive(Clone)]
pub struct Restore {
    rows: Arc<Vec<(Vec<u8>, Vec<u8>)>>,
    /// The snapshot checksum. `shared/restore.json` records it before the first write.
    id: String,
}

impl Restore {
    pub(crate) fn new(rows: Vec<(Vec<u8>, Vec<u8>)>, id: String) -> Self {
        Self {
            rows: Arc::new(rows),
            id,
        }
    }
}

impl std::fmt::Debug for Restore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Restore({} rows, {})", self.rows.len(), self.id)
    }
}

/// The one bootstrap operation of a shared cluster. Ordinary startup and a snapshot restore
/// claim it with a create-only put before any writer opens, so only one of them bootstraps.
const BOOTSTRAP_OP: &str = "shared/bootstrap-op.json";

/// The bootstrap operation of a shared cluster.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum BootstrapOp {
    /// The first leader starts from an empty state.
    Empty,
    /// A member restores the snapshot with this checksum at index 1.
    Restore { snapshot: String },
}

async fn bootstrap_op(source: &Source) -> Result<Option<BootstrapOp>> {
    match source
        .store
        .get(&Path::from(join(&source.root, BOOTSTRAP_OP)))
        .await
    {
        Ok(result) => Ok(Some(
            serde_json::from_slice(&result.bytes().await?).context("invalid bootstrap claim")?,
        )),
        Err(slatedb::object_store::Error::NotFound { .. }) => Ok(None),
        Err(error) => Err(error.into()),
    }
}

/// Claim `op` as the bootstrap operation, or return the operation that another member claimed.
pub(crate) async fn claim_bootstrap(source: &Source, op: &BootstrapOp) -> Result<BootstrapOp> {
    let path = Path::from(join(&source.root, BOOTSTRAP_OP));
    match source
        .store
        .put_opts(
            &path,
            serde_json::to_vec(op)?.into(),
            PutMode::Create.into(),
        )
        .await
    {
        Ok(_) | Err(slatedb::object_store::Error::AlreadyExists { .. }) => {}
        Err(error) => return Err(error.into()),
    }
    bootstrap_op(source)
        .await?
        .context("the bootstrap claim disappeared")
}

/// Resolve the bootstrap operation before an ordinary start selects its base and admits clients.
///
/// A bootstrapped cluster needs nothing. Otherwise the start claims an empty bootstrap. A pending
/// restore claim holds the start: once the restore created its checkpoint, the start completes
/// the marker with the snapshot checksum. Before that the start waits, and then fails, so it
/// never serves an empty base while a restore claim exists.
async fn resolve_bootstrap(source: &Source, admin: &slatedb::admin::Admin) -> Result<()> {
    let deadline = std::time::Instant::now() + RESTORE_WAIT;
    loop {
        if source.bootstrapped().await? {
            return Ok(());
        }
        let BootstrapOp::Restore { snapshot } =
            claim_bootstrap(source, &BootstrapOp::Empty).await?
        else {
            return Ok(());
        };
        if let Some(base) = newest_base(admin, source).await? {
            let checkpoint = base
                .checkpoint
                .context("the restored base has no checkpoint")?;
            return source
                .mark_bootstrapped_with(checkpoint, Some(&snapshot))
                .await;
        }
        ensure!(
            std::time::Instant::now() < deadline,
            "the restore of snapshot {snapshot} into this cluster is pending; start a member \
             with the same --restore-snapshot, or use a new cluster prefix"
        );
        tracing::info!(%snapshot, "wait for the pending snapshot restore");
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// The longest time that an ordinary start waits for a pending restore.
#[cfg(not(test))]
const RESTORE_WAIT: Duration = Duration::from_secs(60);
#[cfg(test)]
const RESTORE_WAIT: Duration = Duration::from_secs(3);

/// The message for a restore into a cluster that this snapshot did not create.
const FRESH_PREFIX: &str = "a snapshot restore needs a new cluster prefix and empty member \
    directories; this shared state is not a restore of this snapshot (see \
    docs/shared-state-guide.md)";

/// Write the restored rows into a new shared database at index 1, create the first view
/// checkpoint, and write the bootstrap marker with the snapshot checksum.
///
/// The restore claims the bootstrap operation with the checksum before its first write. A later
/// or concurrent restore of the same snapshot completes the same steps, also after a failure
/// between the first write and the marker. A restore into a cluster that an ordinary start or
/// another snapshot created fails. An ordinary start waits while a restore claim is pending.
pub(crate) async fn seed(
    source: &Source,
    admin: &slatedb::admin::Admin,
    restore: &Restore,
) -> Result<()> {
    let ours = BootstrapOp::Restore {
        snapshot: restore.id.clone(),
    };
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        if source.bootstrapped().await? {
            ensure!(
                source.restored_snapshot().await?.as_deref() == Some(restore.id.as_str()),
                FRESH_PREFIX
            );
            return Ok(());
        }
        match bootstrap_op(source).await? {
            Some(op) => ensure!(op == ours, FRESH_PREFIX),
            // A state without a claim came from an ordinary start of an older binary or an
            // unknown write.
            None if admin.read_manifest(None).await?.is_some() => bail!(FRESH_PREFIX),
            None => ensure!(claim_bootstrap(source, &ours).await? == ours, FRESH_PREFIX),
        }
        match write_seed(source, admin, restore).await {
            Ok(()) => return Ok(()),
            // Another member can restore the same snapshot at once. Its writer fences this one.
            Err(error) if std::time::Instant::now() < deadline => {
                tracing::warn!(%error, "the shared restore failed; try again")
            }
            Err(error) => {
                return Err(error.context(
                    "the restore of this snapshot did not complete; if no member runs, delete \
                     shared/ under the cluster prefix and restore again (see \
                     docs/shared-state-guide.md)",
                ))
            }
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Claim the restore and write its rows, then stop before the checkpoint and the marker, as a
/// failed restore does.
#[cfg(test)]
pub(crate) async fn seed_rows_for_test(source: &Source, restore: &Restore) -> Result<()> {
    let op = BootstrapOp::Restore {
        snapshot: restore.id.clone(),
    };
    ensure!(claim_bootstrap(source, &op).await? == op, FRESH_PREFIX);
    let db = source.open_writer().await?;
    let mut batch = slatedb::WriteBatch::new();
    for (key, value) in restore.rows.iter() {
        batch.put(key, value);
    }
    batch.put(APPLIED_TERM, applied_term_row(1, 1));
    db.write_with_options(batch, &slatedb::config::WriteOptions { seqnum: 1 })
        .await?;
    db.flush().await?;
    db.close().await?;
    Ok(())
}

/// Claim the restore, write its rows, and create its checkpoint, then stop before the marker.
#[cfg(test)]
pub(crate) async fn seed_checkpoint_for_test(source: &Source, restore: &Restore) -> Result<()> {
    seed_rows_for_test(source, restore).await?;
    let db = source.open_writer().await?;
    let options = slatedb::config::CheckpointOptions {
        name: Some(view_name(1)),
        ..Default::default()
    };
    db.create_checkpoint(slatedb::config::CheckpointScope::Durable, &options)
        .await?;
    db.close().await?;
    Ok(())
}

async fn write_seed(
    source: &Source,
    admin: &slatedb::admin::Admin,
    restore: &Restore,
) -> Result<()> {
    let db = source.open_writer().await?;
    match db.manifest().last_l0_seq() {
        0 => {
            let mut batch = slatedb::WriteBatch::new();
            for (key, value) in restore.rows.iter() {
                batch.put(key, value);
            }
            batch.put(APPLIED_TERM, applied_term_row(1, 1));
            db.write_with_options(batch, &slatedb::config::WriteOptions { seqnum: 1 })
                .await?;
            db.flush().await?;
        }
        // An earlier attempt of this restore wrote the rows. The claim names the snapshot.
        1 => ensure!(
            decode_applied(db.get(APPLIED).await?)? == 1,
            "the partial restore is not at index 1"
        ),
        last => bail!("the shared state is at sequence {last}, not at a partial restore"),
    }
    let existing = view_checkpoints(admin)
        .await?
        .into_iter()
        .find(|(seq, _)| *seq == 1)
        .map(|(_, checkpoint)| checkpoint.id);
    let checkpoint = match existing {
        Some(id) => id,
        None => {
            let options = slatedb::config::CheckpointOptions {
                name: Some(view_name(1)),
                ..Default::default()
            };
            db.create_checkpoint(slatedb::config::CheckpointScope::Durable, &options)
                .await?
                .id
        }
    };
    source
        .mark_bootstrapped_with(checkpoint, Some(&restore.id))
        .await?;
    db.close().await?;
    ensure!(
        source.restored_snapshot().await?.as_deref() == Some(restore.id.as_str()),
        FRESH_PREFIX
    );
    Ok(())
}

/// The apply side of shared mode on one member.
pub(crate) struct Shared {
    pub(crate) overlay: Overlay,
    pub(crate) base: Arc<Base>,
    /// The index through which the base holds the whole state of this member. It is the base
    /// row `F`, or a higher index that an announcement of the same checkpoint certified and that
    /// this member applied. No entry in between changed state. See `docs/shared-state.md`.
    pub(crate) base_index: u64,
    opened: Arc<Opened>,
    pub(crate) source: Arc<Source>,
    /// `ViewCheckpoint` entries that this member applied.
    pub(crate) announcements: u64,
    /// The checkpoint of the newest applied `ViewCheckpoint` entry. A new writer does not
    /// announce it again.
    announced: Arc<tokio::sync::watch::Sender<Option<uuid::Uuid>>>,
    /// The checkpoint of the current base, its row `F`, and its base index.
    base_id: Arc<tokio::sync::watch::Sender<Option<BaseRef>>>,
    /// Background opens of announced checkpoints. DST waits for them to keep schedules exact.
    opening: Vec<tokio::task::JoinHandle<()>>,
    /// Opens and installs that loaded a base at its row and then raised it above. DST counts
    /// them.
    extensions: u64,
    /// A failed open tries again until the base opens or a newer announcement replaces it.
    /// DST turns this off, because a retry would run at a time that the schedule does not fix.
    retry_opens: bool,
    /// The checkpoint that waits for the opener task.
    pending: Arc<Mutex<Pending>>,
    /// The `base_index` of the current base. Physical Compact replies wait for a covering base.
    based: Arc<tokio::sync::watch::Sender<u64>>,
    pub(crate) pin_max: Duration,
}

/// The longest time that a follower warms the cache for a new base before it offers the base.
/// SSTs that a stopped warm did not load stay cold, and the next base warms them. A slow store
/// thus cannot hold back a usable base while the overlay grows.
pub(crate) const WARM_MAX: Duration = Duration::from_millis(500);

/// The first wait before a failed checkpoint open tries again.
const OPEN_RETRY_MIN: Duration = Duration::from_millis(100);
/// The longest wait between two open attempts.
const OPEN_RETRY_MAX: Duration = Duration::from_secs(2);

/// The announced checkpoint that waits for an open, and whether an opener task runs.
#[derive(Default)]
struct Pending {
    /// Only the newest waiting checkpoint, with its certified coverage. A newer announcement
    /// replaces an older one.
    id: Option<(uuid::Uuid, u64)>,
    running: bool,
}

/// Open announced checkpoints one at a time and offer each base for a switch.
///
/// A started open runs to its end, so a steady stream of announcements still advances the
/// base. After each open the task takes the newest waiting checkpoint, if any.
///
/// The announcement is already applied, so the writer does not announce the checkpoint again,
/// and a member that is not paused does not poll. Thus a failed open tries again with a
/// backoff from `OPEN_RETRY_MIN` to `OPEN_RETRY_MAX`, also on an idle member. A newer waiting
/// checkpoint replaces the failed one before the next attempt. The task stops when nothing
/// waits, or when the state closes. A transient error never leaves the member on an old base.
///
/// Each opened base first warms the cache for at most `WARM_MAX`, so reads after the switch
/// find the blocks of the new SSTs.
async fn open_announced(
    source: Arc<Source>,
    opened: Arc<Opened>,
    pending: Arc<Mutex<Pending>>,
    mut announced: tokio::sync::watch::Receiver<Option<uuid::Uuid>>,
) {
    let next = || {
        let mut pending = pending.lock().unwrap();
        let id = pending.id.take();
        pending.running = id.is_some();
        id
    };
    let waiting = || pending.lock().unwrap().id.is_some();
    let mut current = next();
    while let Some((id, covers)) = current {
        let mut backoff = OPEN_RETRY_MIN;
        loop {
            match source.open_base(id).await {
                Ok(base) => {
                    if tokio::time::timeout(WARM_MAX, source.warm(&base))
                        .await
                        .is_err()
                    {
                        tracing::debug!(%id, "the cache warm of a base reached its time limit");
                    }
                    opened.offer(base, covers);
                    break;
                }
                Err(error) => tracing::warn!(%error, %id, "cannot open a view checkpoint"),
            }
            if waiting() {
                break;
            }
            tokio::select! {
                _ = tokio::time::sleep(backoff) => {}
                changed = announced.changed() => {
                    if changed.is_err() {
                        pending.lock().unwrap().running = false;
                        return;
                    }
                }
            }
            if waiting() {
                break;
            }
            backoff = (backoff * 2).min(OPEN_RETRY_MAX);
        }
        current = next();
    }
}

/// Open checkpoint `id` once and offer its base. A manual apply, such as DST, uses this, so
/// each announcement opens its own checkpoint at a fixed point of the schedule. It does not warm
/// the cache, because a timed warm would make the schedule depend on the clock.
async fn open_once(source: Arc<Source>, opened: Arc<Opened>, id: uuid::Uuid, covers: u64) {
    match source.open_base(id).await {
        Ok(base) => opened.offer(base, covers),
        // The member keeps its base and waits for the next checkpoint.
        Err(error) => tracing::warn!(%error, %id, "cannot open a view checkpoint"),
    }
}

impl Shared {
    fn new(base: Arc<Base>, source: Arc<Source>, pin_max: Duration) -> Self {
        Self {
            overlay: Overlay::new(base.applied),
            based: Arc::new(tokio::sync::watch::channel(base.applied).0),
            base_id: Arc::new(tokio::sync::watch::channel(base_ref(&base, base.applied)).0),
            base_index: base.applied,
            base,
            opened: Arc::default(),
            source,
            announcements: 0,
            announced: Arc::new(tokio::sync::watch::channel(None).0),
            opening: vec![],
            extensions: 0,
            retry_opens: true,
            pending: Arc::default(),
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

    /// The view at the covered index, which equals the applied state.
    pub(crate) fn pinned(&self) -> StateView {
        self.view(self.overlay.queue().covered())
    }

    /// Close the open segment and open checkpoint `id` in the background. The announcement
    /// certifies that the checkpoint holds the whole state through `covers`.
    ///
    /// An announcement of the current base only raises its coverage, so it opens nothing.
    pub(crate) fn announce(&mut self, id: uuid::Uuid, covers: u64) {
        self.announcements += 1;
        self.announced.send_replace(Some(id));
        self.overlay.close_segment();
        if self.base.checkpoint == Some(id) {
            self.opened.offer(self.base.clone(), covers);
            return;
        }
        let source = self.source.clone();
        let opened = self.opened.clone();
        self.opening.retain(|task| !task.is_finished());
        if !self.retry_opens {
            self.opening
                .push(tokio::spawn(open_once(source, opened, id, covers)));
            return;
        }
        // The checkpoint waits for the opener. It replaces an older waiting one, but not the
        // open that runs.
        let mut pending = self.pending.lock().unwrap();
        pending.id = Some((id, covers));
        if pending.running {
            return;
        }
        pending.running = true;
        drop(pending);
        let announced = self.announced.subscribe();
        let pending = self.pending.clone();
        self.opening.push(tokio::spawn(open_announced(
            source, opened, pending, announced,
        )));
    }

    /// Offer a base that opened elsewhere, such as in a test.
    #[cfg(test)]
    pub(crate) fn offer(&self, base: Arc<Base>) {
        self.opened.offer(base, 0);
    }

    fn set_base(&mut self, base: Arc<Base>, index: u64) {
        self.based.send_replace(index);
        self.base_id.send_replace(base_ref(&base, index));
        self.base_index = index;
        self.base = base;
    }

    /// Switch to the newest opened base when it covers more than the current one. Return true
    /// after a switch. The base index never goes down.
    ///
    /// A base at row `F` that holds the state through `covers` is usable once this member
    /// covers `F`. It then holds this member's state at each index from `F` through
    /// `min(covers, covered)`, so the overlay drops the batches through that index. Members
    /// group entries in other ways, so a member can store a batch where the leader stored none.
    pub(crate) fn switch(&mut self) -> bool {
        let covered = self.overlay.queue().covered();
        let Some((base, covers)) = self.opened.take_through(covered) else {
            return false;
        };
        let index = covers.min(covered);
        // The base never moves back. A new checkpoint at the same index replaces the base: a
        // Defragment publishes one with a compacted layout.
        if index < self.base_index
            || (index == self.base_index && base.checkpoint == self.base.checkpoint)
        {
            return false;
        }
        // A paused member switches before the next `ViewCheckpoint` entry applies, so the
        // switch also closes the open segment. A covered open segment is then released.
        self.overlay.close_segment();
        self.overlay.release(index);
        self.set_base(base, index);
        true
    }

    /// Make `base` the whole state and drop the overlay. Install and rebase use this.
    fn reset(&mut self, base: Arc<Base>) {
        self.overlay.reset(base.applied);
        let index = base.applied;
        self.set_base(base, index);
    }

    /// Raise the index of a base with an empty overlay to `index`. The caller knows that the
    /// base holds the whole state there.
    fn extend(&mut self, index: u64) {
        self.extensions += 1;
        self.overlay.cover(index);
        self.base_index = index;
        self.based.send_replace(index);
        self.base_id.send_replace(base_ref(&self.base, index));
    }
}

/// The checkpoint of a member's base, with its row `F` and the member's base index. One value
/// holds all three, so a reader never pairs the index of one base with the row of another.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct BaseRef {
    pub checkpoint: uuid::Uuid,
    pub row: u64,
    pub index: u64,
}

fn base_ref(base: &Base, index: u64) -> Option<BaseRef> {
    base.checkpoint.map(|checkpoint| BaseRef {
        checkpoint,
        row: base.applied,
        index,
    })
}

/// The shared-mode handles that the node uses outside apply.
pub(crate) struct Handles {
    pub admin: Arc<slatedb::admin::Admin>,
    pub queue: Arc<super::overlay::Queue>,
    pub source: Arc<Source>,
    /// The index through which the current base holds the whole state.
    pub based: tokio::sync::watch::Receiver<u64>,
    /// The checkpoint of the newest applied `ViewCheckpoint` entry.
    pub announced: tokio::sync::watch::Receiver<Option<uuid::Uuid>>,
    /// The checkpoint of the current base, its row `F`, and its base index.
    pub base_id: tokio::sync::watch::Receiver<Option<BaseRef>>,
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
        match &options.restore {
            Some(restore) => seed(&source, &admin, restore).await?,
            // A joining member finds a bootstrapped cluster. Its Raft log decides its base.
            None if !options.join => resolve_bootstrap(&source, &admin).await?,
            None => {}
        }
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
        // The Raft record can name a checkpoint that holds the state above its own row. The log
        // then starts after the record index, so no checkpoint below that index can start this
        // member. The newest checkpoint can be another one at the same row, such as after a
        // Defragment, so the record checkpoint opens by its ID.
        let record = options.record.filter(|record| base.applied < record.index);
        let base = match record {
            Some(record) if base.checkpoint != Some(record.checkpoint) => {
                reopen_above_row(&admin, &source, record.checkpoint, record.row, record.index)
                    .await?
            }
            _ => base,
        };
        let shared = Shared::new(base, source, options.pin_max);
        let reads = shared.live();
        let mut state = Self::load(None, admin, location, rows, reads, Some(shared)).await?;
        // The state loads at the row, so its counter and term rows match. The record gives the
        // index and the term of the extension.
        if let Some(record) = record {
            ensure!(
                state.applied <= record.index,
                "checkpoint {} is above its Raft snapshot record",
                record.checkpoint
            );
            state.extend_base(record.index, record.term)?;
        }
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
            announced: shared.announced.subscribe(),
            base_id: shared.base_id.subscribe(),
            opened: shared.opened.clone(),
        })
    }

    /// Apply a `ViewCheckpoint` entry. It changes no etcd state and writes no batch.
    ///
    /// `covers` is the index through which the leader certified the checkpoint. An entry of an
    /// earlier binary has none, and zero means the checkpoint row. The leader proposed the entry
    /// after it applied `covers`, so a value at or above `index` is not valid.
    pub(crate) async fn apply_view_checkpoint(
        &mut self,
        index: u64,
        checkpoint: &str,
        covers: u64,
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
        ensure!(covers < index, "a view checkpoint covers its own entry");
        shared.announce(id, covers);
        shared.overlay.cover(index);
        self.reads = shared.live();
        self.applied = index;
        self.applied_term = self.entry_term;
        Ok(())
    }

    /// Set whether a failed open of an announced checkpoint tries again. A manual apply, such
    /// as DST, turns this off to keep its schedule exact.
    pub(crate) fn set_open_retries(&mut self, retry: bool) {
        if let Some(shared) = &mut self.shared {
            shared.retry_opens = retry;
        }
    }

    /// Wait for the background opens of announced checkpoints. Return true if one ran.
    #[cfg(test)]
    pub(crate) async fn settle_opens(&mut self) -> bool {
        let Some(shared) = &mut self.shared else {
            return false;
        };
        let opening = std::mem::take(&mut shared.opening);
        let any = !opening.is_empty();
        for task in opening {
            let _ = task.await;
        }
        any
    }

    /// Use a base that finished opening. Apply calls this before each job.
    pub(crate) fn switch_base(&mut self) {
        if let Some(shared) = &mut self.shared {
            if shared.switch() {
                self.reads = shared.live();
                // The warm of the new base pruned the set while the old base was still live.
                shared.source.prune_warmed();
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
        next.quota_flag = self.quota_flag;
        // A base before the seed has no row. The bootstrap membership then stays in memory.
        if next.members_row.is_none() {
            next.members = self.members.clone();
        }
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
    ///
    /// The log can start above the newest checkpoint row: a member truncates through its base
    /// index. If no newer checkpoint reaches that index, the rebase opens the current base again
    /// and raises it to its index. `term` gives the Raft term at an index of the local log.
    pub(crate) async fn rebase(
        &mut self,
        limit: u64,
        term: impl FnOnce(u64) -> Result<u64>,
    ) -> Result<()> {
        let shared = self
            .shared
            .as_ref()
            .context("rebase needs shared state mode")?;
        let (current, row, index) = (
            shared.base.checkpoint,
            shared.base.applied,
            shared.base_index,
        );
        let newest = newest_base_through(&self.admin, &shared.source, limit).await?;
        let base = match (newest, current) {
            (Some(base), _) if base.applied >= index => base,
            (_, Some(current)) if index > row => {
                reopen_above_row(&self.admin, &shared.source, current, Some(row), index).await?
            }
            (Some(base), _) => base,
            (None, _) => {
                bail!("the shared state has no view checkpoint at or below the rebase index")
            }
        };
        tracing::warn!(
            base = base.applied,
            index,
            applied = self.applied,
            "rebase on a new checkpoint"
        );
        let extend = index > row && (row..index).contains(&base.applied);
        self.adopt_base(base, 0).await?;
        if extend {
            self.extend_base(index, term(index)?)?;
        }
        Ok(())
    }

    /// Store a batch that holds only the apply marker and the counters at the applied index.
    /// The next checkpoint then has its row at that index. An earlier binary installs a view
    /// snapshot only at its checkpoint row, and a leader uses this for such a member. The batch
    /// changes no etcd state, so apply derives nothing from it.
    pub(crate) fn mark_row(&mut self) -> Result<()> {
        let size = self.logical_bytes;
        let index = self.applied;
        let term = self.applied_term;
        let shared = self.shared.as_mut().context("not in shared state mode")?;
        if shared.overlay.queue().pushed() >= index {
            return Ok(());
        }
        let mut batch = WriteBatch::new();
        mark_applied(&mut batch, index, term);
        batch.put(LOGICAL_BYTES, size.total().to_be_bytes());
        batch.put(LOGICAL_SIZE, logical_size_row(index, size));
        shared.overlay.push(index, Arc::new(batch));
        self.write_batches += 1;
        Ok(())
    }

    /// Raise a base that has an empty overlay from its row `F` to `index`. The caller knows that
    /// no entry in `(F, index]` changed state: a certified announcement or a Raft snapshot record
    /// of this checkpoint says so. `term` is the Raft term at `index`. The derived state loaded
    /// at `F`, so its counter rows match the base. Only the position and the term change.
    pub(crate) fn extend_base(&mut self, index: u64, term: u64) -> Result<()> {
        let shared = self.shared.as_mut().context("not in shared state mode")?;
        ensure!(
            self.applied == shared.base.applied && shared.overlay.queue().pushed() == self.applied,
            "only a base without overlay batches can extend"
        );
        if index == self.applied {
            return Ok(());
        }
        ensure!(index > self.applied, "the base cannot extend below its row");
        ensure!(term != 0, "an extended base needs the Raft term at {index}");
        shared.extend(index);
        self.applied = index;
        self.applied_term = term;
        self.entry_term = term;
        self.durable_applied = index;
        Ok(())
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
        // The record index can be above the checkpoint row: the leader certified that no entry
        // in between changed state. The record gives the term at its own index only.
        ensure!(
            base.applied <= index,
            "checkpoint index {} is above snapshot index {index}",
            base.applied
        );
        let row_term = if base.applied == index { term } else { 0 };
        self.adopt_base(base, row_term).await?;
        self.extend_base(index, term)
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
        let descriptor = if shared.base_index > shared.base.applied {
            transfer::ViewDescriptor::above_row(id, shared.base.applied)
        } else {
            transfer::ViewDescriptor::new(id)
        };
        Ok(Some((
            shared.base_index,
            descriptor.encode()?,
            members.conf(),
        )))
    }

    /// Opens and installs that raised a base above its row.
    #[cfg(test)]
    pub(crate) fn extensions(&self) -> u64 {
        self.shared.as_ref().map_or(0, |shared| shared.extensions)
    }

    /// The checkpoint of the base, if any.
    pub(crate) fn base_checkpoint(&self) -> Option<uuid::Uuid> {
        self.shared.as_ref()?.base.checkpoint
    }

    /// The base index, the applied announcements, and the overlay bytes, in shared mode.
    pub(crate) fn view_status(&self) -> Option<(u64, u64, u64)> {
        let shared = self.shared.as_ref()?;
        Some((
            shared.base_index,
            shared.announcements,
            shared.overlay.bytes(),
        ))
    }
}
