//! Shared state mode: one SlateDB for the whole cluster. See `docs/shared-state.md`.
//!
//! This module owns the apply side: the cluster claim, the overlay, the base checkpoint, and
//! base switches. The leader tasks that write the shared `Db` are in `node/shared`.
//!
//! Phase 1 limits: one voter only, and the writer opens with the state, not at a leader change.
//! Follower bases, `ViewAck`, install, rebase, Raft snapshots of format 6, and fencing come in
//! later phases. Shared mode does not truncate the Raft log yet.

use std::sync::{Arc, Mutex};

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

/// The cluster claim. The first member creates it, and every member checks it.
#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
struct Claim {
    version: u32,
    cluster: String,
    mode: StateMode,
    /// The logical quota of the cluster. Zero disables it. Every member applies this value.
    quota: u64,
}

/// Create or check the claim under `root`. Return the cluster quota.
pub(crate) async fn claim(
    store: &dyn ObjectStore,
    root: &str,
    cluster: &str,
    quota: u64,
) -> Result<u64> {
    let path = Path::from(join(root, CLAIM));
    let claim = Claim {
        version: 1,
        cluster: cluster.to_owned(),
        mode: StateMode::Shared,
        quota,
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

/// Return an error when `root` holds a shared-mode cluster claim.
pub(crate) async fn refuse_claim(store: &dyn ObjectStore, root: &str) -> Result<()> {
    match store.head(&Path::from(join(root, CLAIM))).await {
        Ok(_) => bail!("the cluster uses shared state mode"),
        Err(slatedb::object_store::Error::NotFound { .. }) => Ok(()),
        Err(error) => Err(error.into()),
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
}

impl Source {
    /// The path of the shared SlateDB.
    pub fn state(&self) -> String {
        join(&self.root, STATE)
    }

    pub async fn open_base(&self, id: uuid::Uuid) -> Result<Base> {
        Base::open(self.store.clone(), &self.state(), id, self.cache.clone()).await
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
                Ok(base) => opened.offer(Arc::new(base)),
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
        backend: transfer::Backend,
    ) -> Result<Self> {
        cache.validate()?;
        let quota = claim(store.as_ref(), root, cluster, quota).await?;
        let location =
            transfer::Location::open(store.clone(), &join(root, STATE), backend, cache).await?;
        let state_root = location.root();
        let source = Arc::new(Source {
            store: store.clone(),
            root: root.to_owned(),
            cache: cache.build(),
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
            let base = match view_checkpoints(&admin).await?.pop() {
                Some((_, checkpoint)) => source.open_base(checkpoint.id).await?,
                None if source.bootstrapped().await? => {
                    bail!("the shared state has no view checkpoint after its bootstrap")
                }
                None => Base::empty(),
            };
            ensure!(
                base.applied <= written,
                "the base checkpoint is ahead of the shared database"
            );
            let shared = Shared::new(Arc::new(base), source, written);
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
