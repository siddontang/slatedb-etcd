//! Restore a native backup into a new shared cluster with bounded memory.
//!
//! The restore runs when a member opens, before it admits clients. Each step can run again, on
//! the same member or on another member with the same file:
//!
//! 1. Check. `restore_shared` reads the file once: its digest, its key order, and its row sizes.
//! 2. Claim. `shared/bootstrap-op.json` names the snapshot digest. Only one bootstrap claims it.
//! 3. Input. `shared/restore-input.json` records the digest, the row count, the membership row,
//!    the alarm owner, and the transform rules. Every attempt uses it instead of its own flags.
//! 4. Lease. One member at a time writes. See `Watch`.
//! 5. Rows. The writer stores the copied rows in batches. The batch through copied row `n` has
//!    sequence `n`, and it also stores `PROGRESS`: `n`, the SHA-256 of the file through row `n`,
//!    and the logical size. A flush follows every `FLUSH_BYTES` or `FLUSH_INTERVAL`.
//! 6. Completion. One batch at `N = rows + 1` stores the apply marker, the membership row, and
//!    the logical size, and deletes `PROGRESS`. The writer flushes it and reads it back.
//! 7. Publication. The writer creates the checkpoint `view_name(N)`, then the bootstrap marker.
//!
//! `next` maps what a member observes to its next step. A restore that stopped continues after
//! its progress row, once the file prefix has the recorded digest.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use slatedb::object_store::{ObjectMeta, UpdateVersion};

use super::*;
use crate::state::backup::{self, Checked, ImageFile};

#[cfg(test)]
mod tests;

/// The one bootstrap operation of a shared cluster. Ordinary startup and a snapshot restore
/// claim it with a create-only put before any writer opens, so only one of them bootstraps.
const BOOTSTRAP_OP: &str = "shared/bootstrap-op.json";
/// The restore input. See the module comment.
const INPUT: &str = "shared/restore-input.json";
/// The restore lease. See `Watch`.
const LEASE: &str = "shared/restore-lease.json";
/// The restore progress row. Only a restore that has not completed has it. Backups, hashes,
/// and the logical size leave it out.
pub(crate) const PROGRESS: &[u8] = b"meta/restore-progress";
/// The version of the row transform in `backup::restored`. A restore continues only with the
/// rules of its input.
const TRANSFORM: u32 = 1;
/// The rule for the auth tokens of the snapshot.
const DROP_TOKENS: &str = "drop";
/// The Raft term of the restored index.
const TERM: u64 = 1;

/// The copied row bytes of one write batch.
const BATCH_BYTES: usize = if cfg!(test) {
    256
} else {
    backup::RESTORE_BATCH_BYTES
};
/// The row bytes between flushes.
const FLUSH_BYTES: usize = if cfg!(test) {
    1024
} else {
    backup::RESTORE_FLUSH_BYTES
};
/// The longest time between flushes.
const FLUSH_INTERVAL: Duration = Duration::from_secs(10);
/// The owner renews the lease this often, and only after progress.
const RENEW: Duration = if cfg!(test) {
    Duration::from_millis(100)
} else {
    Duration::from_secs(10)
};
/// A lease that stays unchanged this long is stale. Another restore can then take it, and an
/// ordinary start stops waiting.
const STALE: Duration = if cfg!(test) {
    Duration::from_millis(1500)
} else {
    Duration::from_secs(60)
};
/// The interval of the lease and marker reads while a member waits.
const POLL: Duration = if cfg!(test) {
    Duration::from_millis(50)
} else {
    Duration::from_secs(1)
};
/// The interval of the progress and wait logs.
const LOG_INTERVAL: Duration = Duration::from_secs(10);

/// The message for a restore into a cluster that this snapshot did not create.
const FRESH_PREFIX: &str = "a snapshot restore needs a new cluster prefix and empty member \
    directories; this shared state is not a restore of this snapshot (see \
    docs/shared-state-guide.md)";

/// Return true for the rows of a snapshot that a shared restore does not copy. The completion
/// batch writes the apply marker, the counters, and the membership row.
pub(crate) fn not_restored(key: &[u8]) -> bool {
    backup::replaced(key)
}

/// A checked native snapshot that seeds a new shared cluster.
#[derive(Clone)]
pub struct Restore {
    path: PathBuf,
    /// The input that this member creates if no member did.
    input: Input,
    /// The file size, for the progress log.
    bytes: u64,
    member: u64,
    #[cfg(test)]
    pub(crate) hooks: Arc<Hooks>,
}

impl Restore {
    pub(in crate::state) fn new(
        checked: Checked,
        member: u64,
        members: &crate::cluster::Store,
    ) -> Result<Self> {
        let members_row = serde_json::to_vec(members)?;
        backup::row_size(crate::cluster::KEY, &members_row)?;
        let input = Input {
            digest: hex::encode(checked.digest),
            rows: checked.copied,
            members: String::from_utf8(members_row)?,
            alarm_member: *members
                .members
                .keys()
                .next()
                .context("restore member is missing")?,
            auth_tokens: DROP_TOKENS.to_owned(),
            transform: TRANSFORM,
        };
        Ok(Self {
            bytes: std::fs::metadata(&checked.path)?.len(),
            path: checked.path,
            input,
            member,
            #[cfg(test)]
            hooks: Arc::default(),
        })
    }
}

#[cfg(test)]
impl Restore {
    /// Check the image at `path` for member 1 with peer URL `url`.
    pub(crate) fn for_test(path: &std::path::Path, url: &str, hooks: Hooks) -> Self {
        let members = crate::cluster::Store::initial(&[(1, url.to_owned())].into());
        let checked = backup::check(path, not_restored).unwrap();
        let mut restore = Self::new(checked, 1, &members).unwrap();
        restore.hooks = Arc::new(hooks);
        restore
    }

    /// The snapshot digest that the bootstrap marker records.
    pub(crate) fn digest(&self) -> &str {
        &self.input.digest
    }
}

impl std::fmt::Debug for Restore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Restore({} rows, {})",
            self.input.rows, self.input.digest
        )
    }
}

/// Test points of a restore.
#[cfg(test)]
#[derive(Default)]
pub(crate) struct Hooks {
    /// Stop as a crash does: the writer closes without a flush.
    pub stop: std::sync::Mutex<Option<Stop>>,
    /// Pause before the writer opens: (reached, resume).
    pub before_open: Option<(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>)>,
    /// Hold each batch read while this is true.
    pub stall_reads: std::sync::atomic::AtomicBool,
    /// The time that a resume takes for each row it hashes again.
    pub rehash_delay: Duration,
    /// The time that each write batch takes.
    pub batch_delay: Duration,
    /// The largest file read of the write pass, and the wait before each read.
    pub read_throttle: Option<(usize, Duration)>,
    /// The progress ordinal of the last write start of this restore.
    pub started_after: std::sync::Mutex<Option<u64>>,
}

/// Where `Hooks::stop` stops a restore.
#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Stop {
    /// After the first batch, before any flush.
    BeforeFlush,
    /// After this many flushes of row batches.
    AfterFlush(u32),
    /// After the completion batch is durable.
    AfterCompletion,
    /// After the checkpoint, before the marker.
    AfterCheckpoint,
}

#[cfg(test)]
impl Hooks {
    fn stop_at(&self, stop: Stop) -> Result<()> {
        let mut armed = self.stop.lock().unwrap();
        if *armed == Some(stop) {
            *armed = None;
            bail!("an injected stop at {stop:?}");
        }
        Ok(())
    }
}

/// The bootstrap operation of a shared cluster.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum BootstrapOp {
    /// The first leader starts from an empty state.
    Empty,
    /// A member restores the snapshot with this SHA-256 digest in hexadecimal.
    Restore { snapshot: String },
}

/// The restore input, `INPUT`. See the module comment.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct Input {
    digest: String,
    /// The copied rows. The restored index is one above.
    rows: u64,
    /// The membership row of the restored cluster.
    members: String,
    /// The member that owns the restored alarms.
    alarm_member: u64,
    /// The restore drops the auth tokens of the snapshot.
    auth_tokens: String,
    transform: u32,
}

impl Input {
    fn index(&self) -> u64 {
        self.rows + 1
    }

    /// Return true when this member can continue a restore with input `stored`. The members and
    /// the alarm owner come from `stored`.
    fn matches(&self, stored: &Input) -> bool {
        self.digest == stored.digest
            && self.rows == stored.rows
            && self.auth_tokens == stored.auth_tokens
            && self.transform == stored.transform
    }
}

/// The progress row, `PROGRESS`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct Progress {
    /// The last copied row of the stored batches.
    ordinal: u64,
    /// The SHA-256 of the file through that row, in hexadecimal.
    prefix: String,
    /// The logical size of the stored rows: current, history, journal, and other bytes.
    usage: [u64; 4],
}

impl Progress {
    fn usage(&self) -> crate::batch::Usage {
        let [current, history, journal, other] = self.usage;
        crate::batch::Usage {
            current,
            history,
            journal,
            other,
        }
    }
}

/// The lease of the member that writes, `LEASE`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct LeaseBody {
    member: u64,
    /// One take of the lease. Ownership checks compare it.
    attempt: uuid::Uuid,
    /// The renewals of this attempt. An S3 ETag can derive from the content, so each renewal
    /// changes the content.
    counter: u64,
}

/// What a member observed about the lease.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Lease {
    /// The lease names the attempt of this member.
    Ours,
    /// Another attempt holds the lease. It is stale after `STALE` without a change.
    Other { stale: bool },
    /// No lease exists. For an ordinary start it is stale after `STALE`.
    Absent { stale: bool },
}

/// The stored rows, as the lease owner reads them through its writer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Rows {
    Empty,
    Partial(Progress),
    /// The apply marker of a completed restore.
    Complete(u64),
    /// Rows that no step of a restore writes.
    Other,
}

/// What a member observed about the bootstrap of the shared state.
#[derive(Clone, Debug)]
pub(super) struct Facts {
    /// The bootstrap marker, with the snapshot digest that it records.
    marker: Option<Option<String>>,
    claim: Option<BootstrapOp>,
    /// The shared database has a manifest.
    stored: bool,
    input: Option<Input>,
    lease: Lease,
    /// The checkpoint `view_name(N)` and the index of its base.
    checkpoint: Option<(uuid::Uuid, u64)>,
    /// The stored rows, after this member opened the writer.
    rows: Option<Rows>,
}

impl Facts {
    /// The facts of a bootstrapped cluster. Only the marker, with its digest, is known.
    fn marked(digest: Option<String>) -> Self {
        Self {
            marker: Some(digest),
            claim: None,
            stored: false,
            input: None,
            lease: Lease::Absent { stale: false },
            checkpoint: None,
            rows: None,
        }
    }
}

/// The next step of a member.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Step {
    /// The cluster is bootstrapped, or an ordinary start claimed the empty bootstrap.
    Done,
    /// The shared state belongs to another bootstrap.
    FreshPrefix,
    /// Claim the bootstrap: the restore of this snapshot, or an empty start.
    Claim,
    CreateInput,
    TakeLease,
    /// Another member restores, or the lease owner has not yet opened the writer.
    Wait,
    /// An ordinary start found a restore without progress for `STALE`.
    Pending,
    OpenWriter,
    /// Write the rows after the progress row, if any, then the completion batch.
    Write(Option<Progress>),
    Checkpoint,
    /// Write the bootstrap marker for the completed checkpoint.
    Mark(uuid::Uuid),
}

/// Map the observed facts to the next step. `ours` is the input of a member that restores, and
/// `None` for an ordinary start.
pub(super) fn next(facts: &Facts, ours: Option<&Input>) -> Step {
    if let Some(marker) = &facts.marker {
        return match ours {
            Some(ours) if marker.as_deref() != Some(ours.digest.as_str()) => Step::FreshPrefix,
            _ => Step::Done,
        };
    }
    let digest = match (&facts.claim, ours) {
        (Some(BootstrapOp::Restore { snapshot }), _) => snapshot,
        (Some(BootstrapOp::Empty), None) => return Step::Done,
        (Some(BootstrapOp::Empty), Some(_)) => return Step::FreshPrefix,
        // A state without a claim came from an unknown write.
        (None, Some(_)) if facts.stored => return Step::FreshPrefix,
        (None, _) => return Step::Claim,
    };
    if ours.is_some_and(|ours| ours.digest != *digest) {
        return Step::FreshPrefix;
    }
    let Some(input) = &facts.input else {
        return match ours {
            // The input comes before the writer opens.
            Some(_) if facts.stored => Step::FreshPrefix,
            Some(_) => Step::CreateInput,
            None => waiting(facts.lease),
        };
    };
    if input.digest != *digest || ours.is_some_and(|ours| !ours.matches(input)) {
        return Step::FreshPrefix;
    }
    // Only the completion batch reaches the restored index, so never mark an earlier base.
    if let Some((checkpoint, applied)) = facts.checkpoint {
        return if applied == input.index() {
            Step::Mark(checkpoint)
        } else {
            Step::FreshPrefix
        };
    }
    if ours.is_none() {
        return waiting(facts.lease);
    }
    match facts.lease {
        Lease::Ours => {}
        Lease::Other { stale: false } => return Step::Wait,
        Lease::Other { stale: true } | Lease::Absent { .. } => return Step::TakeLease,
    }
    match &facts.rows {
        None => Step::OpenWriter,
        Some(Rows::Empty) => Step::Write(None),
        Some(Rows::Partial(progress)) if (1..=input.rows).contains(&progress.ordinal) => {
            Step::Write(Some(progress.clone()))
        }
        Some(Rows::Complete(applied)) if *applied == input.index() => Step::Checkpoint,
        Some(_) => Step::FreshPrefix,
    }
}

/// The step of an ordinary start while a restore is pending.
fn waiting(lease: Lease) -> Step {
    match lease {
        Lease::Other { stale: true } | Lease::Absent { stale: true } => Step::Pending,
        _ => Step::Wait,
    }
}

/// Read the JSON object `name` under the cluster root, with its metadata.
async fn get_json<T: serde::de::DeserializeOwned>(
    source: &Source,
    name: &str,
) -> Result<Option<(T, ObjectMeta)>> {
    match source
        .store
        .get(&Path::from(join(&source.root, name)))
        .await
    {
        Ok(result) => {
            let meta = result.meta.clone();
            let value = serde_json::from_slice(&result.bytes().await?)
                .map_err(|error| crate::state::stored(format!("invalid {name}: {error}")))?;
            Ok(Some((value, meta)))
        }
        Err(slatedb::object_store::Error::NotFound { .. }) => Ok(None),
        Err(error) => Err(error.into()),
    }
}

/// Create the JSON object `name` unless it exists.
async fn create_json(source: &Source, name: &str, value: &impl Serialize) -> Result<()> {
    let path = Path::from(join(&source.root, name));
    let data = serde_json::to_vec(value)?;
    match source
        .store
        .put_opts(&path, data.into(), PutMode::Create.into())
        .await
    {
        Ok(_) | Err(slatedb::object_store::Error::AlreadyExists { .. }) => Ok(()),
        Err(error) => Err(error.into()),
    }
}

pub(super) async fn bootstrap_op(source: &Source) -> Result<Option<BootstrapOp>> {
    Ok(get_json(source, BOOTSTRAP_OP).await?.map(|(op, _)| op))
}

/// Claim `op` as the bootstrap operation, or return the operation that another member claimed.
pub(crate) async fn claim_bootstrap(source: &Source, op: &BootstrapOp) -> Result<BootstrapOp> {
    create_json(source, BOOTSTRAP_OP, op).await?;
    bootstrap_op(source)
        .await?
        .context("the bootstrap claim disappeared")
}

/// The lease as one member sees it. The owner renews it after progress, and a contender takes
/// it after `STALE` without a change. Both write with a conditional update, so one wins.
///
/// The owner renews only after it hashed file bytes or made rows durable. A read or a flush
/// that hangs stops the renewals, so another member takes over. Its writer then fences the
/// hung one. The staleness uses the monotonic clock of the observer, so clock skew between
/// members does not matter.
struct Watch {
    member: u64,
    /// The lease of this member while it holds it, with its object version.
    held: Option<(LeaseBody, UpdateVersion)>,
    /// The last lease that this member read, when it first saw it, and its version.
    seen: Option<(Option<LeaseBody>, Instant)>,
    version: Option<UpdateVersion>,
}

fn version(meta: &ObjectMeta) -> UpdateVersion {
    UpdateVersion {
        e_tag: meta.e_tag.clone(),
        version: meta.version.clone(),
    }
}

/// A conditional put that lost to another one.
fn lost_race(error: &slatedb::object_store::Error) -> bool {
    matches!(
        error,
        slatedb::object_store::Error::AlreadyExists { .. }
            | slatedb::object_store::Error::Precondition { .. }
    )
}

impl Watch {
    fn new(member: u64) -> Self {
        Self {
            member,
            held: None,
            seen: None,
            version: None,
        }
    }

    fn path(source: &Source) -> Path {
        Path::from(join(&source.root, LEASE))
    }

    async fn observe(&mut self, source: &Source) -> Result<Lease> {
        let found = get_json::<LeaseBody>(source, LEASE).await?;
        self.version = found.as_ref().map(|(_, meta)| version(meta));
        let body = found.map(|(body, _)| body);
        let now = Instant::now();
        let since = match &self.seen {
            Some((seen, since)) if *seen == body => *since,
            _ => now,
        };
        let stale = now.duration_since(since) >= STALE;
        let ours = match (&body, &self.held) {
            (Some(body), Some((held, _))) => body.attempt == held.attempt,
            _ => false,
        };
        let lease = match &body {
            Some(_) if ours => Lease::Ours,
            Some(_) => Lease::Other { stale },
            None => Lease::Absent { stale },
        };
        if !ours {
            self.held = None;
        }
        self.seen = Some((body, since));
        Ok(lease)
    }

    /// Take the lease that `observe` saw. Return false when another member changed it first.
    async fn take(&mut self, source: &Source) -> Result<bool> {
        let body = LeaseBody {
            member: self.member,
            attempt: uuid::Uuid::new_v4(),
            counter: 0,
        };
        let mode = match &self.version {
            Some(version) => PutMode::Update(version.clone()),
            None => PutMode::Create,
        };
        let data = serde_json::to_vec(&body)?;
        match source
            .store
            .put_opts(&Self::path(source), data.into(), mode.into())
            .await
        {
            Ok(result) => {
                tracing::info!(attempt = %body.attempt, "took the snapshot restore lease");
                let version = UpdateVersion {
                    e_tag: result.e_tag,
                    version: result.version,
                };
                self.held = Some((body, version));
                Ok(true)
            }
            Err(error) if lost_race(&error) => Ok(false),
            Err(error) => Err(error.into()),
        }
    }

    /// Renew the held lease. Fail with `held` cleared when another member took it.
    async fn renew(&mut self, source: &Source) -> Result<()> {
        let (held, current) = self.held.as_ref().context("no restore lease")?;
        let body = LeaseBody {
            counter: held.counter + 1,
            ..held.clone()
        };
        let data = serde_json::to_vec(&body)?;
        let mode = PutMode::Update(current.clone());
        let error = match source
            .store
            .put_opts(&Self::path(source), data.into(), mode.into())
            .await
        {
            Ok(result) => {
                let version = UpdateVersion {
                    e_tag: result.e_tag,
                    version: result.version,
                };
                self.held = Some((body, version));
                return Ok(());
            }
            Err(error) => error,
        };
        if !lost_race(&error) {
            return Err(error.into());
        }
        // A put can succeed with a lost reply, so compare the fields.
        match get_json::<LeaseBody>(source, LEASE).await? {
            Some((found, meta)) if found == body => {
                self.held = Some((body, version(&meta)));
                Ok(())
            }
            _ => {
                self.held = None;
                bail!("another member took the snapshot restore lease")
            }
        }
    }

    /// Fail with `held` cleared unless the lease still names the held attempt.
    async fn check(&mut self, source: &Source) -> Result<()> {
        let (held, _) = self.held.as_ref().context("no restore lease")?;
        let found = get_json::<LeaseBody>(source, LEASE).await?;
        if found.is_some_and(|(body, _)| body.attempt == held.attempt) {
            return Ok(());
        }
        self.held = None;
        bail!("another member took the snapshot restore lease")
    }

    /// Every `RENEW`, renew the lease after progress, or check it without progress. Return only
    /// when the lease is lost.
    async fn keep(&mut self, source: &Source, advance: &Advance) -> anyhow::Error {
        let mut last = advance.total();
        loop {
            tokio::time::sleep(RENEW).await;
            let now = advance.total();
            let result = if now == last {
                self.check(source).await
            } else {
                self.renew(source).await.map(|()| last = now)
            };
            match result {
                Ok(()) => {}
                Err(error) if self.held.is_none() => return error,
                Err(error) => tracing::warn!(%error, "cannot renew the restore lease; retry"),
            }
        }
    }
}

/// The progress of a lease owner: the file bytes that it hashed, and its flushes.
#[derive(Default)]
struct Advance {
    hashed: Arc<AtomicU64>,
    flushes: AtomicU64,
}

impl Advance {
    fn total(&self) -> u64 {
        self.hashed.load(Ordering::Relaxed) + self.flushes.load(Ordering::Relaxed)
    }
}

/// Read the bootstrap marker alone: `None` without a marker, else the digest that it records.
async fn marker(source: &Source) -> Result<Option<Option<String>>> {
    let marker = get_json::<serde_json::Value>(source, BOOTSTRAP).await?;
    Ok(marker.map(|(marker, _)| marker["restore"].as_str().map(str::to_owned)))
}

/// Read the facts. `writer` is the open writer of the lease owner.
///
/// The bootstrap marker decides the outcome alone. With a marker, the function reads nothing
/// else: a restart then needs neither the restore objects nor the restore checkpoint.
async fn observe(
    source: &Source,
    admin: &slatedb::admin::Admin,
    watch: &mut Watch,
    writer: Option<&Db>,
) -> Result<Facts> {
    if let Some(digest) = marker(source).await? {
        return Ok(Facts::marked(digest));
    }
    let claim = bootstrap_op(source).await?;
    let stored = admin.read_manifest(None).await?.is_some();
    let input: Option<Input> = get_json(source, INPUT).await?.map(|(input, _)| input);
    let lease = watch.observe(source).await?;
    let mut checkpoint = None;
    if let Some(input) = &input {
        let name = view_name(input.index());
        let found = view_checkpoints(admin)
            .await?
            .into_iter()
            .find(|(_, checkpoint)| checkpoint.name.as_deref() == Some(name.as_str()));
        if let Some((_, found)) = found {
            if let Some(base) = open_present(source, found.id).await? {
                checkpoint = Some((found.id, base.applied));
            }
        }
    }
    let rows = match writer {
        Some(db) => Some(stored_rows(db).await?),
        None => None,
    };
    Ok(Facts {
        marker: None,
        claim,
        stored,
        input,
        lease,
        checkpoint,
        rows,
    })
}

async fn stored_rows(db: &Db) -> Result<Rows> {
    let progress = db.get(PROGRESS).await?;
    let applied = decode_applied(db.get(APPLIED).await?)?;
    Ok(match (progress, applied) {
        (Some(row), 0) => {
            Rows::Partial(serde_json::from_slice(&row).context("invalid restore progress")?)
        }
        (None, 0) if db.manifest().last_l0_seq() == 0 => Rows::Empty,
        (None, applied) if applied > 0 => Rows::Complete(applied),
        _ => Rows::Other,
    })
}

/// Allows a log message at most every `LOG_INTERVAL`.
struct Every(Option<Instant>);

impl Every {
    fn due(&mut self) -> bool {
        let due = self.0.is_none_or(|last| last.elapsed() >= LOG_INTERVAL);
        if due {
            self.0 = Some(Instant::now());
        }
        due
    }
}

/// Restore `restore` into the new shared state of `source`, or check that it is restored.
///
/// A restore into a cluster that an ordinary start or another snapshot created fails. Members
/// with the same file wait while one of them holds the lease. A member takes over a lease that
/// stays unchanged for `STALE` and continues the restore. To abandon a partial restore, follow
/// "Abandon a partial restore" in `docs/shared-state-guide.md`. It keeps `shared/owner.json`,
/// because the member directories recorded its store ID.
pub(crate) async fn seed(
    source: &Source,
    admin: &slatedb::admin::Admin,
    restore: &Restore,
) -> Result<()> {
    let ours = BootstrapOp::Restore {
        snapshot: restore.input.digest.clone(),
    };
    let mut watch = Watch::new(restore.member);
    let mut log = Every(None);
    loop {
        let facts = observe(source, admin, &mut watch, None).await?;
        match next(&facts, Some(&restore.input)) {
            Step::Done => return Ok(()),
            Step::FreshPrefix => bail!(FRESH_PREFIX),
            Step::Claim => {
                claim_bootstrap(source, &ours).await?;
            }
            Step::CreateInput => create_json(source, INPUT, &restore.input).await?,
            Step::TakeLease => {
                if !watch.take(source).await? {
                    tokio::time::sleep(POLL).await;
                }
            }
            Step::Wait => {
                if log.due() {
                    tracing::info!("wait for the member that holds the snapshot restore lease");
                }
                tokio::time::sleep(POLL).await;
            }
            Step::Mark(checkpoint) => {
                source
                    .mark_bootstrapped_with(checkpoint, Some(&restore.input.digest))
                    .await?
            }
            Step::OpenWriter => {
                let Err(error) = own(source, admin, restore, &mut watch).await else {
                    continue;
                };
                // Decide on the facts first. Another member can publish the restore meanwhile:
                // the marker then decides, also when the obsolete lease cannot be read.
                let facts = observe(source, admin, &mut watch, None).await?;
                match next(&facts, Some(&restore.input)) {
                    Step::Done => return Ok(()),
                    Step::FreshPrefix => bail!(FRESH_PREFIX),
                    // A member that lost the lease while its open was in flight can fence the
                    // owner. The owner then opens again and continues after the durable rows.
                    _ if facts.lease == Lease::Ours && fenced(&error) => {
                        tracing::warn!(%error, "a stale restore writer fenced this one; open again")
                    }
                    _ if facts.lease == Lease::Ours => {
                        return Err(error.context(
                            "the restore of this snapshot did not complete; start it again, or \
                             abandon it as in \"Abandon a partial restore\" in \
                             docs/shared-state-guide.md: stop every member, keep \
                             shared/owner.json, and delete the other shared/ objects",
                        ))
                    }
                    // The writer of a member that took the lease fences this one.
                    _ => tracing::warn!(%error, "another member took over the snapshot restore"),
                }
            }
            step => bail!("unexpected snapshot restore step {step:?} without a writer"),
        }
    }
}

/// Open the writer under the lease and run the steps that need it. Return when the restore is
/// published or the lease is lost.
async fn own(
    source: &Source,
    admin: &slatedb::admin::Admin,
    restore: &Restore,
    watch: &mut Watch,
) -> Result<()> {
    #[cfg(test)]
    if let Some((reached, resume)) = &restore.hooks.before_open {
        reached.notify_one();
        resume.notified().await;
    }
    let db = source.open_writer().await?;
    let result = write_steps(source, admin, restore, watch, &db).await;
    // A failed step leaves unflushed rows behind, as a crash does.
    let closed = db
        .close_with_options(slatedb::config::CloseOptions { flush_type: None })
        .await;
    result?;
    Ok(closed?)
}

async fn write_steps(
    source: &Source,
    admin: &slatedb::admin::Admin,
    restore: &Restore,
    watch: &mut Watch,
    db: &Db,
) -> Result<()> {
    loop {
        // The writer fenced any earlier one. Check the lease and the marker again before a
        // write: a member that paused before its open can find a published restore.
        let facts = observe(source, admin, watch, Some(db)).await?;
        let step = next(&facts, Some(&restore.input));
        let input = facts.input.as_ref();
        match step {
            Step::Write(from) => {
                let input = input.context("a restore write needs its input")?;
                let advance = Advance::default();
                tokio::select! {
                    result = write(db, restore, input, from, &advance) => result?,
                    error = watch.keep(source, &advance) => return Err(error),
                }
            }
            Step::Checkpoint => {
                let input = input.context("a restore checkpoint needs its input")?;
                let options = slatedb::config::CheckpointOptions {
                    name: Some(view_name(input.index())),
                    ..Default::default()
                };
                db.create_checkpoint(slatedb::config::CheckpointScope::Durable, &options)
                    .await?;
                // The restore runs before Raft, so no voter clock check applies. Each leader
                // waits until its clock passes this tick. See "Clocks" in the design.
                let local_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_or(0, |since| since.as_millis() as i64);
                tracing::info!(
                    clock_tick = db.manifest().last_l0_clock_tick(),
                    local_ms,
                    "the restore checkpoint holds the writer clock of this host"
                );
                #[cfg(test)]
                restore.hooks.stop_at(Stop::AfterCheckpoint)?;
            }
            Step::Mark(checkpoint) => {
                source
                    .mark_bootstrapped_with(checkpoint, Some(&restore.input.digest))
                    .await?
            }
            Step::FreshPrefix => bail!(FRESH_PREFIX),
            Step::Done | Step::Wait | Step::TakeLease => return Ok(()),
            step => bail!("unexpected snapshot restore step {step:?} with a writer"),
        }
    }
}

/// Write the copied rows after `from`, then the completion batch.
async fn write(
    db: &Db,
    restore: &Restore,
    input: &Input,
    from: Option<Progress>,
    advance: &Advance,
) -> Result<()> {
    let started = Instant::now();
    #[cfg(test)]
    {
        *restore.hooks.started_after.lock().unwrap() = Some(from.as_ref().map_or(0, |p| p.ordinal));
    }
    let mut ordinal = from.as_ref().map_or(0, |from| from.ordinal);
    let mut usage = from.as_ref().map_or_else(Default::default, Progress::usage);
    let mut reopening = {
        let (restore, counted) = (restore.clone(), advance.hashed.clone());
        tokio::task::spawn_blocking(move || reopen(&restore, from.as_ref(), counted))
    };
    let image = loop {
        tokio::select! {
            image = &mut reopening => break image??,
            () = tokio::time::sleep(LOG_INTERVAL) => tracing::info!(
                bytes = advance.hashed.load(Ordering::Relaxed),
                of_bytes = restore.bytes,
                "verify the prefix of the partial snapshot restore"
            ),
        }
    };
    if ordinal > 0 {
        tracing::info!(rows = ordinal, "continue the partial snapshot restore");
    }
    #[cfg(test)]
    let hooks = restore.hooks.clone();
    let read = move |image| {
        #[cfg(test)]
        let hooks = hooks.clone();
        move || {
            #[cfg(test)]
            while hooks.stall_reads.load(Ordering::Relaxed) {
                std::thread::sleep(Duration::from_millis(10));
            }
            backup::read_batch(image, BATCH_BYTES)
        }
    };
    let mut next = Some(tokio::task::spawn_blocking(read(image)));
    let mut digest = None;
    let mut unflushed = 0;
    let mut flushed = Instant::now();
    #[cfg(test)]
    let mut flushes = 0;
    let mut log = Every(Some(Instant::now()));
    while let Some(batch) = next.take() {
        let backup::Batch { rows, prefix, rest } = batch.await??;
        match rest {
            Ok(image) => next = Some(tokio::task::spawn_blocking(read(image))),
            Err(end) => digest = Some(end),
        }
        if rows.is_empty() {
            continue;
        }
        let mut batch = slatedb::WriteBatch::new();
        for (key, value) in rows {
            let value = backup::restored(&key, value, input.alarm_member)?;
            if key.starts_with(b"etcd/") {
                usage.add_row(&key, value.len())?;
            }
            unflushed += key.len() + value.len();
            batch.put(&key, &value);
            ordinal += 1;
        }
        let row = Progress {
            ordinal,
            prefix: hex::encode(prefix),
            usage: [usage.current, usage.history, usage.journal, usage.other],
        };
        batch.put(PROGRESS, serde_json::to_vec(&row)?);
        db.write_with_options(batch, &slatedb::config::WriteOptions { seqnum: ordinal })
            .await?;
        #[cfg(test)]
        {
            restore.hooks.stop_at(Stop::BeforeFlush)?;
            tokio::time::sleep(restore.hooks.batch_delay).await;
        }
        if unflushed >= FLUSH_BYTES || flushed.elapsed() >= FLUSH_INTERVAL {
            db.flush().await?;
            crate::failpoint::hit("shared-restore-after-flush");
            advance.flushes.fetch_add(1, Ordering::Relaxed);
            unflushed = 0;
            flushed = Instant::now();
            #[cfg(test)]
            {
                flushes += 1;
                restore.hooks.stop_at(Stop::AfterFlush(flushes))?;
            }
        }
        if log.due() {
            tracing::info!(
                rows = ordinal,
                of = input.rows,
                bytes = advance.hashed.load(Ordering::Relaxed),
                of_bytes = restore.bytes,
                "restore snapshot rows"
            );
        }
    }
    // The file can change after its check. Its digest must match the input before completion.
    ensure!(
        digest.map(hex::encode).as_deref() == Some(input.digest.as_str()),
        "snapshot changed during restore"
    );
    ensure!(
        ordinal == input.rows,
        "the snapshot has {ordinal} rows, not the {} of its check",
        input.rows
    );
    complete(db, input, usage).await?;
    tracing::info!(index = input.index(), elapsed = ?started.elapsed(), "restored the snapshot rows");
    #[cfg(test)]
    restore.hooks.stop_at(Stop::AfterCompletion)?;
    Ok(())
}

/// Open the image and read its header. For a partial restore, also read the stored rows again
/// and compare the digest of the file through them. `counted` gets the file bytes read.
fn reopen(
    restore: &Restore,
    from: Option<&Progress>,
    counted: Arc<AtomicU64>,
) -> Result<ImageFile> {
    let mut image = ImageFile::open(&restore.path, not_restored)?;
    image.count_in(counted);
    #[cfg(test)]
    if let Some((bytes, delay)) = restore.hooks.read_throttle {
        image.throttle(bytes, delay);
    }
    image.header()?;
    let Some(from) = from else {
        return Ok(image);
    };
    for _ in 0..from.ordinal {
        image
            .next()?
            .context("the snapshot has fewer rows than the partial restore")?;
        #[cfg(test)]
        std::thread::sleep(restore.hooks.rehash_delay);
    }
    // A file that changed after an earlier attempt wrote its rows fails here.
    ensure!(hex::encode(image.prefix()) == from.prefix, FRESH_PREFIX);
    Ok(image)
}

/// Store the completion batch at the restored index, flush it, and read it back.
async fn complete(db: &Db, input: &Input, mut usage: crate::batch::Usage) -> Result<()> {
    let index = input.index();
    let mut batch = slatedb::WriteBatch::new();
    batch.put(APPLIED, index.to_be_bytes());
    batch.put(APPLIED_TERM, applied_term_row(index, TERM));
    // The first checkpoint must hold the membership. See `newest_base`.
    batch.put(crate::cluster::KEY, input.members.as_bytes());
    usage.add_row(crate::cluster::KEY, input.members.len())?;
    batch.put(LOGICAL_SIZE, logical_size_row(index, usage));
    batch.delete(PROGRESS);
    db.write_with_options(batch, &slatedb::config::WriteOptions { seqnum: index })
        .await?;
    db.flush().await?;
    ensure!(
        stored_rows(db).await? == Rows::Complete(index),
        "the restore completion did not read back"
    );
    Ok(())
}

/// Resolve the bootstrap operation before a start without a snapshot selects its base and admits
/// clients.
///
/// A bootstrapped cluster needs nothing. Otherwise an ordinary start claims an empty bootstrap. A
/// `join` start never claims it. It waits up to `STALE` for the initial voters to claim it, and
/// then fails with a retryable error. A pending restore claim holds either start while the
/// restore makes progress. Once the restore created its checkpoint, the
/// start can complete the marker. A restore without progress for `STALE` fails the start, so it
/// never serves an empty base while a restore claim exists.
pub(super) async fn resolve_bootstrap(
    source: &Source,
    admin: &slatedb::admin::Admin,
    join: bool,
) -> Result<()> {
    let mut watch = Watch::new(0);
    let mut log = Every(None);
    let started = Instant::now();
    loop {
        let facts = observe(source, admin, &mut watch, None).await?;
        let snapshot = match &facts.claim {
            Some(BootstrapOp::Restore { snapshot }) => snapshot.as_str(),
            _ => "",
        };
        match next(&facts, None) {
            Step::Done => return Ok(()),
            // An empty view before the decision could become a restore, so the join waits.
            Step::Claim if join => {
                ensure!(
                    started.elapsed() < STALE,
                    "the cluster has no bootstrap yet; start its initial members, then join again"
                );
                if log.due() {
                    tracing::info!("wait for the initial members to bootstrap the cluster");
                }
                tokio::time::sleep(POLL).await;
            }
            Step::Claim => {
                claim_bootstrap(source, &BootstrapOp::Empty).await?;
            }
            Step::Mark(checkpoint) => {
                source
                    .mark_bootstrapped_with(checkpoint, Some(snapshot))
                    .await?
            }
            Step::Wait => {
                if log.due() {
                    tracing::info!(%snapshot, "wait for the pending snapshot restore");
                }
                tokio::time::sleep(POLL).await;
            }
            Step::Pending => bail!(
                "the restore of snapshot {snapshot} into this cluster is pending; start a member \
                 with the same --restore-snapshot, or use a new cluster prefix"
            ),
            Step::FreshPrefix => bail!(
                "the shared state has a checkpoint that is not the restore of snapshot \
                 {snapshot}; use a new cluster prefix"
            ),
            step => bail!("unexpected bootstrap step {step:?} of an ordinary start"),
        }
    }
}

/// Write the bootstrap marker for `base` after its open found no marker. Under a restore claim
/// only the completed restore can be the base.
pub(super) async fn mark_base(source: &Source, base: &Base) -> Result<()> {
    let checkpoint = base.checkpoint.context("the base has no checkpoint")?;
    let Some(BootstrapOp::Restore { snapshot }) = bootstrap_op(source).await? else {
        return source.mark_bootstrapped_with(checkpoint, None).await;
    };
    let input: Option<Input> = get_json(source, INPUT).await?.map(|(input, _)| input);
    ensure!(
        input.is_some_and(|input| input.index() == base.applied),
        "checkpoint {checkpoint} at {} is not the restore of snapshot {snapshot}",
        base.applied
    );
    source
        .mark_bootstrapped_with(checkpoint, Some(&snapshot))
        .await
}
