//! Shared state mode: one SlateDB for the whole cluster. See `docs/shared-state.md`.
//!
//! This module owns the apply side: the cluster claim, the overlay, the base checkpoint, and
//! base switches. The leader tasks that write the shared `Db` are in `node/shared`.
//!
//! Phase 1 limits: one voter only, and the writer opens with the state, not at a leader change.
//! Follower bases, `ViewAck`, install, rebase, Raft snapshots of format 6, and fencing come in
//! later phases. Shared mode does not truncate the Raft log yet.

use std::collections::HashSet;
use std::sync::{Arc, Mutex, Weak};

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
    #[cfg(test)]
    pub fn pinned(&self) -> HashSet<uuid::Uuid> {
        self.pins.lock().unwrap().pinned()
    }

    /// Select the checkpoints to delete and mark them, under the lock that opens take. A later
    /// open of a marked checkpoint fails. Call `end_retire` after the deletions.
    pub(crate) fn begin_retire(&self, views: &[(u64, uuid::Uuid)], keep: usize) -> Vec<uuid::Uuid> {
        let mut pins = self.pins.lock().unwrap();
        let pinned = pins.pinned();
        let candidates = retired(views, &pinned, keep);
        pins.retiring.extend(candidates.iter().copied());
        candidates
    }

    pub(crate) fn end_retire(&self, ids: &[uuid::Uuid]) {
        let mut pins = self.pins.lock().unwrap();
        for id in ids {
            pins.retiring.remove(id);
        }
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
/// checkpoint under a live view must survive GC.
pub(crate) fn retired(
    views: &[(u64, uuid::Uuid)],
    pinned: &HashSet<uuid::Uuid>,
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
        .filter(|(seq, _)| *seq < oldest_pin)
        .map(|(_, id)| *id)
        .collect()
}

/// Delete the old `view-` checkpoints that no live base of this member pins.
///
/// Phase 1 has one member, so its own pins are all the pins. Other checkpoints are not ours.
pub(crate) async fn retain(
    admin: &slatedb::admin::Admin,
    source: &Source,
    keep: usize,
) -> Result<()> {
    let views: Vec<_> = view_checkpoints(admin)
        .await?
        .into_iter()
        .map(|(seq, checkpoint)| (seq, checkpoint.id))
        .collect();
    let candidates = source.begin_retire(&views, keep);
    let mut result = Ok(());
    for id in &candidates {
        if let Err(error) = admin.delete_checkpoint(*id).await {
            result = Err(error.into());
            break;
        }
    }
    source.end_retire(&candidates);
    result
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

/// Bases that opened in the background. Apply takes the newest one.
#[derive(Default)]
pub(crate) struct Opened(Mutex<Option<Arc<Base>>>);

impl Opened {
    /// Keep `base` unless an opened base already covers more. Opens can finish out of order.
    pub(crate) fn offer(&self, base: Arc<Base>) {
        let mut slot = self.0.lock().unwrap();
        if slot.as_ref().is_none_or(|old| old.applied < base.applied) {
            *slot = Some(base);
        }
    }

    fn take_through(&self, pushed: u64) -> Option<Arc<Base>> {
        let mut slot = self.0.lock().unwrap();
        // A base above the overlay needs an adopt, which phase 2 adds.
        if slot.as_ref().is_some_and(|base| base.applied <= pushed) {
            return slot.take();
        }
        None
    }
}

/// The apply side of shared mode on one member.
pub(crate) struct Shared {
    pub(crate) overlay: Overlay,
    pub(crate) base: Arc<Base>,
    opened: Arc<Opened>,
    pub(crate) source: Arc<Source>,
    /// `ViewCheckpoint` entries that this member applied.
    pub(crate) announcements: u64,
    /// The writer's sequence when it opened. The materializer continues above it.
    pub(crate) written: u64,
}

impl Shared {
    fn new(base: Arc<Base>, source: Arc<Source>, written: u64) -> Self {
        Self {
            overlay: Overlay::new(base.applied),
            base,
            opened: Arc::default(),
            source,
            announcements: 0,
            written,
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

    /// Switch to the newest opened base when it covers more than the current one. Return true
    /// after a switch. The base `F` never goes down.
    pub(crate) fn switch(&mut self) -> bool {
        let Some(base) = self.opened.take_through(self.overlay.queue().pushed()) else {
            return false;
        };
        if base.applied <= self.base.applied {
            return false;
        }
        self.overlay.release(base.applied);
        self.base = base;
        true
    }
}

/// The handles that the leader writer tasks use.
pub(crate) struct WriterParts {
    pub db: Arc<Db>,
    pub admin: Arc<slatedb::admin::Admin>,
    pub queue: Arc<super::overlay::Queue>,
    pub source: Arc<Source>,
    /// The writer's sequence when it opened.
    pub written: u64,
}

impl StateMachine {
    /// Open the shared state of the cluster at `root` in `store`. See the module comment.
    pub(crate) async fn open_shared(
        store: Arc<dyn ObjectStore>,
        root: &str,
        cluster: &str,
        cache: StateCache,
        quota: u64,
        voters: &[u64],
        backend: transfer::Backend,
    ) -> Result<Self> {
        cache.validate()?;
        // The claim comes before the writer opens. A member that loses it changes nothing.
        let quota = claim(store.as_ref(), root, cluster, quota, voters).await?;
        let location =
            transfer::Location::open(store.clone(), &join(root, STATE), backend, cache).await?;
        let state_root = location.root();
        let source = Arc::new(Source {
            store: store.clone(),
            root: root.to_owned(),
            cache: cache.build(),
            pins: Mutex::default(),
        });
        ensure!(
            state_root == source.state(),
            "shared state has no generations"
        );
        let rows = Arc::new(rows::RowCache::new(cache.row_bytes));
        let admin = slatedb::admin::Admin::builder(state_root.as_str(), store.clone()).build();
        let db = Arc::new(
            Db::builder(state_root.as_str(), store)
                .with_db_cache(source.cache.clone(), 0)
                .with_settings(slatedb_settings())
                .with_block_cache_policy(block_cache_policy())
                .build()
                .await?,
        );
        let result = async {
            let written = db.manifest().last_l0_seq();
            let base = match newest_base(&admin, &source).await? {
                Some(base) => {
                    // A stop between the first checkpoint and its marker leaves no marker.
                    if !source.bootstrapped().await? {
                        source.mark_bootstrapped(base.checkpoint.unwrap()).await?;
                    }
                    base
                }
                None if source.bootstrapped().await? => {
                    bail!("the shared state has no view checkpoint after its bootstrap")
                }
                None => Arc::new(Base::empty()),
            };
            ensure!(
                base.applied <= written,
                "the base checkpoint is ahead of the shared database"
            );
            let shared = Shared::new(base, source, written);
            let reads = shared.live();
            let mut state =
                Self::load(db.clone(), admin, location, rows, reads, Some(shared)).await?;
            state.quota = quota;
            Ok(state)
        }
        .await;
        if result.is_err() {
            let _ = db
                .close_with_options(slatedb::config::CloseOptions { flush_type: None })
                .await;
        }
        let mut state = result?;
        state.plan_compaction();
        Ok(state)
    }

    /// Return true in shared state mode.
    pub(crate) fn is_shared(&self) -> bool {
        self.shared.is_some()
    }

    /// The handles of the leader writer tasks, in shared mode.
    pub(crate) fn writer_parts(&self) -> Option<WriterParts> {
        let shared = self.shared.as_ref()?;
        Some(WriterParts {
            db: self.db.clone(),
            admin: self.admin.clone(),
            queue: shared.overlay.queue(),
            source: shared.source.clone(),
            written: shared.written,
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

    /// The base index `F` and the applied announcements, in shared mode.
    pub(crate) fn view_status(&self) -> Option<(u64, u64, u64)> {
        let shared = self.shared.as_ref()?;
        Some((
            shared.base.applied,
            shared.announcements,
            shared.overlay.bytes(),
        ))
    }
}
