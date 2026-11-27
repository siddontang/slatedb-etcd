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

mod adopt;
mod restore;
#[cfg(test)]
pub(crate) mod tests;
pub use restore::Restore;
pub(crate) use restore::{claim_bootstrap, not_restored, BootstrapOp, PROGRESS};
#[cfg(test)]
pub(crate) use restore::{seed, Hooks, Stop};

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

/// The name prefix of the checkpoints that the publisher creates.
const VIEW: &str = "view-";
const CLAIM: &str = "shared/owner.json";
const BOOTSTRAP: &str = "shared/bootstrap.json";
const STATE: &str = "shared/state";
/// A received snapshot that Raft did not install releases its checkpoint after this time.
const RECEIPT_LIFETIME: Duration = Duration::from_secs(60);

/// The cluster claim. The first member creates it, and every member checks it.
#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
struct Claim {
    version: u32,
    cluster: String,
    /// The logical quota of the cluster. Zero disables it. Every member applies this value.
    quota: u64,
    /// The initial voters. Members with another bootstrap set refuse to open the writer.
    voters: Vec<u64>,
    /// The random ID of this shared store. Members send it with their peer traffic, so members
    /// on another bucket or prefix refuse each other.
    store_id: uuid::Uuid,
}

/// The values of a checked cluster claim.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Claimed {
    pub quota: u64,
    pub store_id: uuid::Uuid,
}

/// Parse a cluster claim. A claim without a store ID does not parse.
fn parse_claim(data: &[u8]) -> Result<Claim> {
    let claim: Claim =
        serde_json::from_slice(data).context("invalid cluster claim, or no valid store ID")?;
    ensure!(
        !claim.store_id.is_nil(),
        "the cluster claim has a nil store ID"
    );
    Ok(claim)
}

/// Read and check an existing claim. Return `None` when the store has no claim.
async fn read_claim(store: &dyn ObjectStore, root: &str, cluster: &str) -> Result<Option<Claim>> {
    let data = match store.get(&Path::from(join(root, CLAIM))).await {
        Ok(result) => result.bytes().await?,
        Err(slatedb::object_store::Error::NotFound { .. }) => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let existing = parse_claim(&data)?;
    ensure!(
        existing.version == 1 && existing.cluster == cluster,
        "the shared state belongs to another cluster"
    );
    Ok(Some(existing))
}

/// Read and check an existing claim. Return `None` when the store has no claim.
pub(crate) async fn claimed(
    store: &dyn ObjectStore,
    root: &str,
    cluster: &str,
) -> Result<Option<Claimed>> {
    Ok(read_claim(store, root, cluster)
        .await?
        .map(|existing| Claimed {
            quota: existing.quota,
            store_id: existing.store_id,
        }))
}

/// Read the claim of the store that a data directory is bound to. Refuse a claim with another
/// store ID, and for an initial voter, a claim with other voters.
///
/// This never creates the claim. A claim that disappears during a start stays absent, and the
/// start fails.
pub(crate) async fn bound(
    store: &dyn ObjectStore,
    root: &str,
    cluster: &str,
    store_id: uuid::Uuid,
    voters: Option<&[u64]>,
) -> Result<Claimed> {
    let existing = read_claim(store, root, cluster).await?;
    let found = existing.as_ref().map(|claim| claim.store_id);
    let Some(existing) = existing.filter(|claim| claim.store_id == store_id) else {
        bail!(
            "the data directory belongs to shared store {store_id}, but the store at this \
             location has {}: the store was replaced. Use the original store, or a new data \
             directory",
            found.map_or("no cluster claim".to_owned(), |id| format!("store ID {id}"))
        );
    };
    if let Some(voters) = voters {
        let mut voters = voters.to_vec();
        voters.sort_unstable();
        ensure!(
            existing.voters == voters,
            "the shared state belongs to other initial voters {:?}",
            existing.voters
        );
    }
    Ok(Claimed {
        quota: existing.quota,
        store_id,
    })
}

/// Read the claim for a joining member. It is not one of the initial voters, so it never creates
/// the claim.
pub(crate) async fn joined(store: &dyn ObjectStore, root: &str, cluster: &str) -> Result<Claimed> {
    claimed(store, root, cluster)
        .await?
        .context("a joining member needs an existing shared cluster")
}

/// Create or check the shared state claim under `root`. Return the cluster quota and the store
/// ID.
///
/// The claim holds the initial voters. Two members that each bootstrap a separate one-voter
/// cluster in the same namespace race on one `PutMode::Create`, so only one opens the writer.
/// The member that creates the claim also picks the store ID.
pub(crate) async fn claim(
    store: &dyn ObjectStore,
    root: &str,
    cluster: &str,
    quota: u64,
    voters: &[u64],
) -> Result<Claimed> {
    let path = Path::from(join(root, CLAIM));
    let mut voters = voters.to_vec();
    voters.sort_unstable();
    let claim = Claim {
        version: 1,
        cluster: cluster.to_owned(),
        quota,
        voters,
        store_id: uuid::Uuid::new_v4(),
    };
    match store
        .put_opts(
            &path,
            serde_json::to_vec(&claim)?.into(),
            PutMode::Create.into(),
        )
        .await
    {
        Ok(_) => Ok(Claimed {
            quota,
            store_id: claim.store_id,
        }),
        Err(slatedb::object_store::Error::AlreadyExists { .. }) => {
            let existing = parse_claim(&store.get(&path).await?.bytes().await?)?;
            ensure!(
                existing.version == 1 && existing.cluster == cluster,
                "the shared state belongs to another cluster"
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
            Ok(Claimed {
                quota: existing.quota,
                store_id: existing.store_id,
            })
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
    /// Bases of received snapshots that no install took yet, with their receive time. An
    /// install takes the base and keeps it while it retries. Raft can also drop a snapshot, so a
    /// receipt expires after `RECEIPT_LIFETIME`.
    received: Mutex<std::collections::HashMap<uuid::Uuid, (std::time::Instant, Arc<Base>)>>,
    /// DST: every open fails, as for a checkpoint that cannot open.
    #[cfg(test)]
    pub(crate) fail_opens: std::sync::atomic::AtomicBool,
    /// Tests: the next writer open fails with this error.
    #[cfg(test)]
    pub(crate) fail_writer: Mutex<Option<anyhow::Error>>,
    /// Tests: each writer open fails with the error of this function.
    #[cfg(test)]
    pub(crate) fail_writers: Mutex<Option<fn() -> anyhow::Error>>,
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

/// Checkpoints that retention selected for deletion. Dropping it ends the marks.
pub(crate) struct Retiring<'a> {
    source: &'a Source,
    pub ids: Vec<uuid::Uuid>,
}

impl Drop for Retiring<'_> {
    fn drop(&mut self) {
        let mut pins = self.source.pins.lock().unwrap();
        for id in &self.ids {
            pins.retiring.remove(id);
        }
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
        #[cfg(test)]
        ensure!(
            !self.fail_opens.load(std::sync::atomic::Ordering::Relaxed),
            "an injected open failure"
        );
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
    /// open of a marked checkpoint fails. The marks end when the returned value drops, also when
    /// the caller stops before its deletions end.
    pub(crate) fn begin_retire(
        &self,
        views: &[(u64, uuid::Uuid)],
        reported: &HashSet<uuid::Uuid>,
        keep: usize,
    ) -> Retiring<'_> {
        let mut pins = self.pins.lock().unwrap();
        let pinned = pins.pinned();
        let ids = retired(views, &pinned, reported, keep);
        pins.retiring.extend(ids.iter().copied());
        Retiring { source: self, ids }
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
        self.build_writer(settings, None).await
    }

    /// Open the writer of a leader with `clock` as its SlateDB clock. See
    /// `node::shared::clock::Gate`.
    pub async fn open_writer_on(
        &self,
        clock: Arc<dyn slatedb_common::clock::SystemClock>,
    ) -> Result<Db> {
        self.build_writer(slatedb_settings(), Some(clock)).await
    }

    async fn build_writer(
        &self,
        settings: Settings,
        clock: Option<Arc<dyn slatedb_common::clock::SystemClock>>,
    ) -> Result<Db> {
        #[cfg(test)]
        if let Some(error) = self.fail_writer.lock().unwrap().take() {
            return Err(error);
        }
        #[cfg(test)]
        if let Some(error) = *self.fail_writers.lock().unwrap() {
            return Err(error());
        }
        let mut builder = Db::builder(self.state(), self.store.clone())
            .with_db_cache(self.cache.clone(), 0)
            .with_settings(settings)
            .with_block_cache_policy(block_cache_policy());
        if let Some(clock) = clock {
            builder = builder.with_system_clock(clock);
        }
        Ok(builder.build().await?)
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
    /// A restore claim gives its snapshot digest to the marker, so a publisher of a restored
    /// cluster writes the marker that the restore wrote.
    pub async fn mark_bootstrapped(&self, checkpoint: uuid::Uuid) -> Result<()> {
        let restore = match restore::bootstrap_op(self).await? {
            Some(BootstrapOp::Restore { snapshot }) => Some(snapshot),
            _ => None,
        };
        self.mark_bootstrapped_with(checkpoint, restore.as_deref())
            .await
    }

    /// The digest of the snapshot that the bootstrap marker records, if a restore wrote it.
    pub(crate) async fn restored_snapshot(&self) -> Result<Option<String>> {
        let data = self
            .store
            .get(&Path::from(join(&self.root, BOOTSTRAP)))
            .await?
            .bytes()
            .await?;
        let marker: serde_json::Value = serde_json::from_slice(&data)
            .map_err(|error| super::stored(format!("invalid bootstrap marker: {error}")))?;
        Ok(marker["restore"].as_str().map(str::to_owned))
    }

    /// Write the bootstrap marker. A restore records its snapshot digest in it.
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
                if existing.as_deref() != restore {
                    return Err(super::stored(
                        "the bootstrap marker records another bootstrap",
                    ));
                }
                Ok(())
            }
            Err(error) => Err(error.into()),
        }
    }
}

/// Return true when `error` comes from a writer that a newer writer fenced.
pub(crate) fn fenced(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        cause.downcast_ref::<slatedb::Error>().is_some_and(|error| {
            matches!(
                error.kind(),
                slatedb::ErrorKind::Closed(slatedb::CloseReason::Fenced)
            )
        })
    })
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
    // A stopped writer task drops this future at an await. `retiring` then ends the marks.
    let retiring = source.begin_retire(&views, reported, keep);
    let mut result = Ok(());
    for id in &retiring.ids {
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
    drop(retiring);
    result
}

/// Open the newest `view-` checkpoint at or below index `limit` that holds the membership.
///
/// A checkpoint that retention deleted after the listing is skipped. An older base can then need
/// log entries that the member truncated, and the replay of the rebase fails on them.
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
        let Some(base) = open_present(source, checkpoint.id).await? else {
            continue;
        };
        if base.applied <= limit && base.get(crate::cluster::KEY).await?.is_some() {
            return Ok(Some(base));
        }
    }
    Ok(None)
}

/// Open checkpoint `id`, or return `None` when it is gone: retention deleted it, or deletes it
/// now, or one of its objects is missing. Every other error stops the caller, so a transient
/// error never selects another base.
pub(crate) async fn open_present(source: &Source, id: uuid::Uuid) -> Result<Option<Arc<Base>>> {
    match source.open_base(id).await {
        Ok(base) => Ok(Some(base)),
        Err(error) if gone(&error) => {
            tracing::warn!(%error, %id, "skip a view checkpoint that is gone");
            Ok(None)
        }
        Err(error) => Err(error),
    }
}

/// Return true when `error` says that a checkpoint or one of its objects is gone: the checkpoint
/// left the manifest, its manifest or an SST was deleted, or retention deletes it now.
pub(crate) fn gone(error: &anyhow::Error) -> bool {
    missing_object(error)
        || error.chain().any(|cause| {
            let text = cause.to_string();
            text.contains("checkpoint missing")
                || text.contains("failed to find manifest")
                || text.contains("is being deleted")
        })
}

/// Open the newest `view-` checkpoint whose row is in `[low, high]` and that holds the
/// membership. The name gives only a lower bound of the row, so each candidate opens and its own
/// row decides.
///
/// A record above its checkpoint row needs that checkpoint, or one in this range: the leader
/// certified that no entry in `(low, high]` changed state, so each checkpoint of this shared
/// state with its row in the range holds the state at `high`.
pub(crate) async fn newest_base_between(
    admin: &slatedb::admin::Admin,
    source: &Source,
    low: u64,
    high: u64,
) -> Result<Option<Arc<Base>>> {
    for (seq, checkpoint) in view_checkpoints(admin).await?.into_iter().rev() {
        if seq > high {
            continue;
        }
        let Some(base) = open_present(source, checkpoint.id).await? else {
            continue;
        };
        if (low..=high).contains(&base.applied) && base.get(crate::cluster::KEY).await?.is_some() {
            return Ok(Some(base));
        }
    }
    Ok(None)
}

/// Open checkpoint `checkpoint` of a base above its row `row` at `index`. If it is gone, take
/// the newest checkpoint with its row in `[row, index]`. A missing row means the record names no
/// range, so only the checkpoint itself can serve. Another error stops the open.
async fn reopen_above_row(
    admin: &slatedb::admin::Admin,
    source: &Source,
    checkpoint: uuid::Uuid,
    row: Option<u64>,
    index: u64,
) -> Result<Arc<Base>> {
    let error = match source.open_base(checkpoint).await {
        Ok(base) => return Ok(base),
        Err(error) if gone(&error) => error,
        Err(error) => return Err(error),
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

/// A source that opens no member state.
#[cfg(test)]
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
        #[cfg(test)]
        fail_opens: Default::default(),
        #[cfg(test)]
        fail_writer: Default::default(),
        #[cfg(test)]
        fail_writers: Default::default(),
    }))
}

/// Open the newest `view-` checkpoint that holds the membership. The publisher deletes a
/// checkpoint without it, but a stop can leave one behind.
async fn newest_base(admin: &slatedb::admin::Admin, source: &Source) -> Result<Option<Arc<Base>>> {
    for (_, checkpoint) in view_checkpoints(admin).await?.into_iter().rev() {
        // A checkpoint that retention deleted after the listing is skipped. An older base below
        // the Raft record or the log fails the start later.
        let Some(base) = open_present(source, checkpoint.id).await? else {
            continue;
        };
        if base.get(crate::cluster::KEY).await?.is_some() {
            return Ok(Some(base));
        }
        tracing::warn!(checkpoint = %checkpoint.id, "skip a view checkpoint without membership");
    }
    Ok(None)
}

/// Return true when `error` comes from an object that the store no longer has, such as an SST
/// that GC deleted under an old base. See `storage_fault::classify`. A read error that crossed a
/// `KvError` keeps its class.
pub(crate) fn missing_object(error: &anyhow::Error) -> bool {
    crate::mvcc::storage_fault::classify(error)
        == Some(crate::mvcc::storage_fault::StorageFault::Missing)
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
    /// The budget of released overlay segments that long holders keep. See `overlay::Holders`.
    pub overlay_max_bytes: u64,
    /// A checked native snapshot that seeds a new cluster.
    pub restore: Option<Restore>,
    /// The checkpoint that the durable Raft snapshot record names, if any.
    pub record: Option<Record>,
    /// The store ID that the data directory is bound to. With it, the open only reads the claim
    /// and refuses another ID. A data directory gets it at its first open.
    pub store_id: Option<uuid::Uuid>,
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
    /// The store ID of the cluster claim. Peers of another store refuse this member.
    pub(crate) store_id: uuid::Uuid,
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
    /// The native backups that keep overlay segments of this member.
    pub(crate) holders: Arc<super::overlay::Holders>,
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
    pub(super) fn new(
        base: Arc<Base>,
        source: Arc<Source>,
        store_id: uuid::Uuid,
        pin_max: Duration,
        holders: Arc<super::overlay::Holders>,
    ) -> Self {
        Self {
            store_id,
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
            holders,
        }
    }

    /// The view at `seq`.
    pub(crate) fn view(&self, seq: u64) -> StateView {
        StateView::Shared(self.shared_view(seq))
    }

    fn shared_view(&self, seq: u64) -> SharedView {
        SharedView::new(self.base.clone(), self.overlay.segments(), seq)
    }

    /// The view that apply reads. It sees every batch that apply adds later.
    pub(crate) fn live(&self) -> StateView {
        self.view(u64::MAX)
    }

    /// The view at the covered index, which equals the applied state.
    pub(crate) fn pinned(&self) -> StateView {
        StateView::Shared(self.pinned_view())
    }

    /// The shared view at the covered index. A backup pins it.
    pub(crate) fn pinned_view(&self) -> SharedView {
        self.shared_view(self.overlay.queue().covered())
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
    ) -> Result<Self> {
        cache.validate()?;
        // The state and the peer identity use the store ID of this one read. A bound data
        // directory reads the claim only, before any restore or base selection.
        let voters = (!options.join).then_some(options.voters.as_slice());
        let claimed = match options.store_id {
            Some(id) => bound(store.as_ref(), root, cluster, id, voters).await?,
            None if options.join => joined(store.as_ref(), root, cluster).await?,
            None => {
                claim(
                    store.as_ref(),
                    root,
                    cluster,
                    options.quota,
                    &options.voters,
                )
                .await?
            }
        };
        let location = transfer::Location::new(&join(root, STATE), cache);
        let state_root = location.root();
        let source = Arc::new(Source {
            store: store.clone(),
            root: root.to_owned(),
            cache: cache.build(),
            pins: Mutex::default(),
            received: Mutex::default(),
            #[cfg(test)]
            fail_opens: Default::default(),
            #[cfg(test)]
            fail_writer: Default::default(),
            #[cfg(test)]
            fail_writers: Default::default(),
        });
        ensure!(
            state_root == source.state(),
            "shared state has no generations"
        );
        let rows = Arc::new(rows::RowCache::new(cache.row_bytes));
        let admin = Arc::new(slatedb::admin::Admin::builder(state_root.as_str(), store).build());
        match &options.restore {
            Some(restore) => restore::seed(&source, &admin, restore).await?,
            // A joining member never claims the bootstrap, but it waits for a pending restore.
            None => restore::resolve_bootstrap(&source, &admin, options.join).await?,
        }
        // A joining member with no log waits for Raft: entries from index 1 or a snapshot.
        let base = match newest_base(&admin, &source).await? {
            Some(_) if options.join && options.empty_log => Arc::new(Base::empty()),
            Some(base) => {
                // A stop between the first checkpoint and its marker leaves no marker.
                if !source.bootstrapped().await? {
                    restore::mark_base(&source, &base).await?;
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
        let holders = Arc::new(super::overlay::Holders::new(options.overlay_max_bytes));
        let shared = Shared::new(
            base,
            source,
            claimed.store_id,
            options.pin_max,
            holders.clone(),
        );
        let reads = shared.live();
        let mut state = Self::load(admin, location, rows, reads, shared).await?;
        holders.register(&state.metrics.registry)?;
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
        state.quota = claimed.quota;
        state.plan_compaction();
        Ok(state)
    }

    /// The ID of the shared store.
    pub(crate) fn store_id(&self) -> uuid::Uuid {
        self.shared.store_id
    }

    /// The handles of the node.
    pub(crate) fn shared_handles(&self) -> Handles {
        let shared = &self.shared;
        Handles {
            admin: self.admin.clone(),
            queue: shared.overlay.queue(),
            source: shared.source.clone(),
            based: shared.based.subscribe(),
            announced: shared.announced.subscribe(),
            base_id: shared.base_id.subscribe(),
            opened: shared.opened.clone(),
        }
    }

    /// Apply a `ViewCheckpoint` entry. It changes no etcd state and writes no batch.
    ///
    /// `covers` is the index through which the leader certified the checkpoint. Zero means the
    /// checkpoint row. The leader proposed the entry
    /// after it applied `covers`, so a value at or above `index` is not valid.
    pub(crate) async fn apply_view_checkpoint(
        &mut self,
        index: u64,
        checkpoint: &str,
        covers: u64,
    ) -> Result<()> {
        // The in-memory state of a test stores only the apply position.
        #[cfg(test)]
        if let StateView::Memory(_) = &self.reads {
            self.apply(index, None).await?;
            return Ok(());
        }
        let shared = &mut self.shared;
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
        self.shared.retry_opens = retry;
    }

    /// Wait for the background opens of announced checkpoints. Return true if one ran.
    #[cfg(test)]
    pub(crate) async fn settle_opens(&mut self) -> bool {
        let opening = std::mem::take(&mut self.shared.opening);
        let any = !opening.is_empty();
        for task in opening {
            let _ = task.await;
        }
        any
    }

    /// Use a base that finished opening. Apply calls this before each job.
    pub(crate) fn switch_base(&mut self) {
        let shared = &mut self.shared;
        if shared.switch() {
            self.reads = shared.live();
            // The warm of the new base pruned the set while the old base was still live.
            shared.source.prune_warmed();
        }
    }

    /// Raise a base that has an empty overlay from its row `F` to `index`. The caller knows that
    /// no entry in `(F, index]` changed state: a certified announcement or a Raft snapshot record
    /// of this checkpoint says so. `term` is the Raft term at `index`. The derived state loaded
    /// at `F`, so its counter rows match the base. Only the position and the term change.
    pub(crate) fn extend_base(&mut self, index: u64, term: u64) -> Result<()> {
        let shared = &mut self.shared;
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

    /// The format 6 snapshot of the current base: its index, data, and ConfState. `None` before
    /// the first checkpoint.
    pub(crate) async fn base_snapshot(
        &self,
    ) -> Result<Option<(u64, Vec<u8>, raft::eraftpb::ConfState)>> {
        let shared = &self.shared;
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
        self.shared.extensions
    }

    /// Offer `base` for a switch through `covers`, as if an announcement certified it there. A
    /// test switches a member to a wrong base with this.
    #[cfg(test)]
    pub(crate) fn offer_base(&self, base: Arc<Base>, covers: u64) {
        self.shared.opened.offer(base, covers);
    }

    /// The checkpoint of the base, if any.
    pub(crate) fn base_checkpoint(&self) -> Option<uuid::Uuid> {
        self.shared.base.checkpoint
    }

    /// The base index, the applied announcements, and the overlay bytes.
    pub(crate) fn view_status(&self) -> (u64, u64, u64) {
        let shared = &self.shared;
        (
            shared.base_index,
            shared.announcements,
            shared.overlay.bytes(),
        )
    }
}
