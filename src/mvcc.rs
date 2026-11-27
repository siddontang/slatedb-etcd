use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::Pin;

use crate::WriteBatch;
use prost::Message;
use serde::{Deserialize, Serialize};
use tonic::{Code, Status};

use crate::proto::etcdserverpb::request_op::Request as Op;
use crate::proto::etcdserverpb::response_op::Response as Resp;
use crate::proto::etcdserverpb::*;
use crate::proto::mvccpb::KeyValue;
use crate::proto::state::Version;
use crate::state::{StateRead, StateScan};

mod put;
mod range;
pub(crate) mod storage_fault;
#[cfg(test)]
pub(crate) use put::DIRECT_TXNS;
pub(crate) use put::{batchable_txn, prepare_put, prepare_txn_puts, CurrentView};

pub(crate) const CURRENT: &[u8] = b"etcd/current/";
pub(crate) const HISTORY: &[u8] = b"etcd/history/";
pub const REVISION: &[u8] = b"etcd/revision";
pub(crate) const COMPACTED: &str = "mvcc: required revision has been compacted";
pub const COMPACT_REVISION: &[u8] = b"etcd/compact-revision";
type KvResult<T> = Result<T, KvError>;
type KvFuture<'a, T> = Pin<Box<dyn Future<Output = KvResult<T>> + Send + 'a>>;
type Rows = BTreeMap<Vec<u8>, KeyValue>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KvError {
    pub code: i32,
    pub message: String,
    /// The class of a storage error. It does not cross the wire, so a reply keeps only its code.
    #[serde(skip)]
    fault: Option<crate::mvcc::storage_fault::StorageFault>,
}

impl KvError {
    pub(crate) fn new(code: Code, message: &str) -> Self {
        Self {
            code: code as i32,
            message: format!("etcdserver: {message}"),
            fault: None,
        }
    }

    fn invalid(message: &str) -> Self {
        Self::new(Code::InvalidArgument, message)
    }

    /// Classify a storage error of a read. A transient or missing object fails the request with
    /// a retryable status. Stored data that does not decode fails it with `DataLoss`, and the
    /// read counts it. The caller states that `error` comes from a storage call or from stored
    /// data, so an error of no known class is corrupt data.
    pub(crate) fn storage(error: impl Into<anyhow::Error>) -> Self {
        use crate::mvcc::storage_fault::{StorageFault, CORRUPT_READS};
        let error = error.into();
        let fault = crate::mvcc::storage_fault::classify(&error).unwrap_or(StorageFault::Corrupt);
        let (code, message) = match fault {
            StorageFault::Transient => {
                tracing::warn!(error = format!("{error:#}"), "a storage read failed");
                (Code::Unavailable, "storage unavailable; retry".to_owned())
            }
            StorageFault::Missing => (
                Code::Unavailable,
                "a checkpoint object is missing; retry".to_owned(),
            ),
            StorageFault::Corrupt => {
                CORRUPT_READS.inc();
                tracing::error!(
                    error = format!("{error:#}"),
                    "a read met corrupt stored data"
                );
                (Code::DataLoss, format!("corrupt stored data: {error:#}"))
            }
        };
        Self {
            fault: Some(fault),
            ..Self::new(code, &message)
        }
    }

    /// The error of a read outside the KV path, such as a diagnostic. A classified storage
    /// error keeps its class. Another error fails the request with `Unavailable`.
    pub(crate) fn read_failure(error: anyhow::Error) -> Self {
        match crate::mvcc::storage_fault::classify(&error) {
            Some(_) => Self::storage(error),
            None => Self::new(Code::Unavailable, &format!("{error:#}")),
        }
    }

    /// A stored row that does not decode or contradicts another one.
    pub(crate) fn corrupt(message: &str) -> Self {
        Self::storage(crate::mvcc::storage_fault::stored(message))
    }

    /// An error of this member's own invariants, such as a write batch that cannot count its
    /// bytes. It stops the member.
    pub(crate) fn invariant(error: impl std::fmt::Display) -> Self {
        Self::new(Code::Internal, &format!("invariant error: {error}"))
    }

    /// Return the error of this member's own invariants, which stops the member. A storage
    /// error fails only its request, so it returns `None`.
    pub(crate) fn stops_member(&self) -> Option<anyhow::Error> {
        match self.fault {
            Some(_) => None,
            None => self.unapplied(),
        }
    }

    /// Return the error that stops apply instead of becoming the result of an entry: a storage
    /// error, whose class survives, or an invariant error. Each member must reach the same
    /// result, so a read that failed on this member cannot be one.
    fn unapplied(&self) -> Option<anyhow::Error> {
        if let Some(fault) = self.fault {
            return Some(anyhow::Error::new(crate::mvcc::storage_fault::Classified {
                fault,
                message: self.message.clone(),
            }));
        }
        (self.code == Code::Internal as i32).then(|| anyhow::anyhow!("{}", self.message))
    }

    pub fn status(self) -> Status {
        Status::new(Code::from_i32(self.code), self.message)
    }
}

/// The error of an apply entry point. Only a `Request` error is the result of an entry.
#[derive(Debug)]
pub(crate) enum ApplyError {
    /// The result of the entry. Each member reaches the same one.
    Request(KvError),
    /// A storage error of a read, with its class, or an invariant error. The entry stays
    /// unapplied.
    Unapplied(anyhow::Error),
}

impl From<KvError> for ApplyError {
    fn from(error: KvError) -> Self {
        match error.unapplied() {
            Some(error) => Self::Unapplied(error),
            None => Self::Request(error),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Outcome<T = ResponseOp> {
    pub result: KvResult<T>,
}

fn current_key(key: &[u8]) -> Vec<u8> {
    [CURRENT, key].concat()
}

fn current_bounds(key: &[u8], end: &[u8]) -> std::ops::Range<Vec<u8>> {
    let upper = if end == [0] {
        b"etcd/current0".to_vec()
    } else {
        current_key(end)
    };
    current_key(key)..upper
}

fn history_prefix(key: &[u8]) -> Vec<u8> {
    const HEX: &[u8] = b"0123456789abcdef";
    // Reserve the revision suffix for history writes.
    let mut out = Vec::with_capacity(HISTORY.len() + key.len() * 2 + 1 + 8);
    out.extend_from_slice(HISTORY);
    for byte in key {
        out.push(HEX[(byte >> 4) as usize]);
        out.push(HEX[(byte & 15) as usize]);
    }
    out.push(b'/');
    out
}

/// Name the history version of a key at one revision.
pub(crate) fn history_key(key: &[u8], revision: i64) -> Vec<u8> {
    let mut out = history_prefix(key);
    out.extend_from_slice(&revision.to_be_bytes());
    out
}

pub(crate) fn contains(key: &[u8], begin: &[u8], end: &[u8]) -> bool {
    if end.is_empty() {
        key == begin
    } else {
        key >= begin && (end == [0] || key < end)
    }
}

pub(crate) fn header(revision: i64) -> Option<ResponseHeader> {
    Some(ResponseHeader {
        revision,
        ..Default::default()
    })
}

fn response(response: Resp) -> ResponseOp {
    ResponseOp {
        response: Some(response),
    }
}

fn key_check(key: &[u8]) -> KvResult<()> {
    if key.is_empty() {
        Err(KvError::invalid("key is not provided"))
    } else {
        Ok(())
    }
}

/// Require both branches, including nested transactions, to contain only permitted reads.
pub fn read_only(request: &RequestOp, serializable: bool) -> bool {
    match &request.request {
        Some(Op::RequestRange(range)) => !serializable || range.serializable,
        Some(Op::RequestTxn(txn)) => txn
            .success
            .iter()
            .chain(&txn.failure)
            .all(|op| read_only(op, serializable)),
        _ => false,
    }
}

/// Check both transaction branches before evaluating any predicate.
pub fn validate(request: &RequestOp) -> KvResult<()> {
    validate_op(request, 128)?;
    if let Some(Op::RequestTxn(txn)) = &request.request {
        intervals(&txn.success)?;
        intervals(&txn.failure)?;
    }
    Ok(())
}

fn validate_op(request: &RequestOp, budget: usize) -> KvResult<()> {
    match request
        .request
        .as_ref()
        .ok_or_else(|| KvError::invalid("request is not provided"))?
    {
        Op::RequestRange(r) => {
            key_check(&r.key)?;
            if !(0..=2).contains(&r.sort_order) || !(0..=4).contains(&r.sort_target) {
                return Err(KvError::invalid("invalid sort option"));
            }
        }
        Op::RequestPut(r) => {
            key_check(&r.key)?;
            if r.ignore_value && !r.value.is_empty() {
                return Err(KvError::invalid("value is provided"));
            }
            if r.ignore_lease && r.lease != 0 {
                return Err(KvError::invalid("lease is provided"));
            }
        }
        Op::RequestDeleteRange(r) => key_check(&r.key)?,
        Op::RequestTxn(r) => {
            let count = r.compare.len().max(r.success.len()).max(r.failure.len());
            if count > budget {
                return Err(KvError::invalid("too many operations in txn request"));
            }
            for cmp in &r.compare {
                key_check(&cmp.key)?;
                if !(0..=3).contains(&cmp.result) || !(0..=4).contains(&cmp.target) {
                    return Err(KvError::invalid("invalid compare option"));
                }
            }
            for op in r.success.iter().chain(&r.failure) {
                validate_op(op, budget - count)?;
            }
        }
    }
    Ok(())
}

type Intervals = (BTreeSet<Vec<u8>>, Vec<(Vec<u8>, Vec<u8>)>);

// Puts must be disjoint on each possible path. Overlapping deletes are allowed.
fn intervals(ops: &[RequestOp]) -> KvResult<Intervals> {
    let mut puts = BTreeSet::new();
    let mut deletes = vec![];
    let duplicate = || KvError::invalid("duplicate key given in txn request");
    for op in ops {
        if let Some(Op::RequestDeleteRange(d)) = &op.request {
            deletes.push((d.key.clone(), d.range_end.clone()));
        }
    }
    for op in ops {
        if let Some(Op::RequestTxn(t)) = &op.request {
            let (a, da) = intervals(&t.success)?;
            let (b, db) = intervals(&t.failure)?;
            for key in a.union(&b) {
                if puts.contains(key)
                    || deletes.iter().any(|(begin, end)| contains(key, begin, end))
                {
                    return Err(duplicate());
                }
            }
            puts.extend(a);
            puts.extend(b);
            deletes.extend(da);
            deletes.extend(db);
        }
    }
    for op in ops {
        if let Some(Op::RequestPut(p)) = &op.request {
            if !puts.insert(p.key.clone()) || deletes.iter().any(|(a, b)| contains(&p.key, a, b)) {
                return Err(duplicate());
            }
        }
    }
    Ok((puts, deletes))
}

/// A resolved request. Operations borrow the request, so planning copies no keys or values.
enum Plan<'r> {
    Op(std::borrow::Cow<'r, Op>),
    Txn {
        succeeded: bool,
        children: Vec<Plan<'r>>,
    },
}

// Range reads cache the blocks they fetch. SlateDB scans skip the block cache by default,
// so each Range otherwise reads and decodes every touched block from the object store again.
// Moka uses TinyLFU admission, so one large scan does not flush hot point-read blocks.
fn scan_options() -> slatedb::config::ScanOptions {
    slatedb::config::ScanOptions {
        cache_blocks: true,
        ..Default::default()
    }
}

enum ReadDb<'a, R> {
    Live(&'a R),
    /// A pinned view at sequence `seq`. Its point reads share rows through the row cache.
    Pinned {
        db: &'a R,
        rows: &'a crate::state::rows::RowCache,
        seq: u64,
    },
    /// Applied state plus earlier writes of the same apply group.
    Overlay {
        db: &'a R,
        previous: &'a WriteBatch,
        rows: &'a crate::state::current::CurrentRows,
    },
}

impl<R: StateRead> ReadDb<'_, R> {
    async fn get(&self, key: &[u8]) -> Result<Option<bytes::Bytes>, slatedb::Error> {
        match self {
            Self::Live(db) | Self::Pinned { db, .. } => db.get(key).await,
            Self::Overlay { db, previous, rows } => {
                if let Some(value) = previous.get(key) {
                    return Ok(value);
                }
                if let Some(value) = rows.get(key) {
                    return Ok(Some(value));
                }
                db.get(key).await
            }
        }
    }

    /// Read the current row of a user key. Pinned views share rows through the row cache.
    async fn current(&self, key: &[u8]) -> KvResult<Option<KeyValue>> {
        let decode = |row: &[u8]| KeyValue::decode(row).map_err(KvError::storage);
        let Self::Pinned { db, rows, seq } = self else {
            let row = self
                .get(&current_key(key))
                .await
                .map_err(KvError::storage)?;
            return row.as_deref().map(decode).transpose();
        };
        let seq = *seq;
        let meters = &crate::metrics::reads::READS;
        if let Some(kv) = rows.get(key, seq, decode) {
            meters.row.hit.inc();
            return kv.map(Some);
        }
        meters.row.miss.inc();
        let timer = meters.row_read.start_timer();
        let row = db.get(&current_key(key)).await.map_err(KvError::storage)?;
        timer.observe_duration();
        let Some(row) = row else {
            return Ok(None);
        };
        let kv = decode(&row)?;
        rows.fill(key, &row, seq);
        Ok(Some(kv))
    }

    /// Return the group write of a current row, if an earlier group member changed it.
    fn group_row(&self, key: &[u8]) -> Option<Option<bytes::Bytes>> {
        match self {
            Self::Overlay { previous, .. } => previous.get(&current_key(key)),
            _ => None,
        }
    }

    // The overlay merges only point reads. Grouping places scans before earlier group writes.
    fn scan_base(&self) -> Result<&R, slatedb::Error> {
        match self {
            Self::Overlay { db, previous, .. } if previous.is_empty() => Ok(db),
            Self::Overlay { .. } => Err(slatedb::Error::unavailable(
                "scan over an apply group overlay".to_string(),
            )),
            Self::Live(db) | Self::Pinned { db, .. } => Ok(db),
        }
    }

    async fn scan(&self, range: std::ops::Range<Vec<u8>>) -> Result<R::Scan<'_>, slatedb::Error> {
        self.scan_base()?.scan_range(range, &scan_options()).await
    }

    /// Return the last row of `range`, or `None` when the range is empty.
    async fn last(
        &self,
        range: std::ops::Range<Vec<u8>>,
    ) -> Result<Option<crate::state::Row>, slatedb::Error> {
        let options = slatedb::config::ScanOptions {
            order: slatedb::IterationOrder::Descending,
            ..scan_options()
        };
        let mut scan = self.scan_base()?.scan_range(range, &options).await?;
        scan.next().await
    }
}

struct View<'a, R> {
    writes: crate::metrics::Writes,
    db: ReadDb<'a, R>,
    revision: i64,
    compact_revision: i64,
    changes: BTreeMap<Vec<u8>, Option<KeyValue>>,
    events: Vec<crate::proto::mvccpb::Event>,
    /// History revisions of previous values that the Watch journal omits. See `watch::record`.
    prev_revisions: Vec<i64>,
    leases: &'a BTreeMap<i64, crate::lease::LeaseRecord>,
    /// Metadata of the live applied rows. Pinned read views have no index.
    index: Option<&'a crate::state::key_index::KeyIndex>,
    /// Bytes of rows a streaming Range may materialize. `None` means unbounded.
    row_budget: Option<usize>,
    /// Current keys at the pinned snapshot. Live views count rows by scanning.
    keys: Option<&'a crate::state::key_index::KeyPin>,
}

// The keys of a `state.Journal` row. Decoding skips the values and the other fields.
#[derive(prost::Message)]
struct JournalKeys {
    #[prost(message, repeated, tag = "1")]
    events: Vec<EventKey>,
}

// The type and key of a `mvccpb.Event`.
#[derive(prost::Message)]
struct EventKey {
    #[prost(int32, tag = "1")]
    r#type: i32,
    #[prost(message, optional, tag = "2")]
    kv: Option<KeyOnly>,
}

// The key and revisions of a `mvccpb.KeyValue`.
#[derive(prost::Message)]
struct KeyOnly {
    #[prost(bytes = "vec", tag = "1")]
    key: Vec<u8>,
    #[prost(int64, tag = "2")]
    create_revision: i64,
    #[prost(int64, tag = "3")]
    mod_revision: i64,
}

impl KeyOnly {
    /// Whether the key was live just before an event of `kind` wrote this row. A delete removes
    /// a live key. A put of a live key keeps its create revision, and a new key starts at its own.
    fn live_before(&self, kind: i32) -> bool {
        kind == crate::proto::mvccpb::event::EventType::Delete as i32
            || self.create_revision < self.mod_revision
    }
}

#[cfg(test)]
thread_local! {
    /// Versions that journal pages read on this thread.
    pub(crate) static JOURNAL_VERSION_READS: std::cell::Cell<u64> =
        const { std::cell::Cell::new(0) };
}

impl<R: StateRead> View<'_, R> {
    /// Find the base metadata of a key. `None` means that the index is unavailable.
    fn indexed(&self, key: &[u8]) -> Option<Option<KeyValue>> {
        let index = self.index?;
        // Earlier writes of the same apply group are newer than the applied index.
        // A row that does not decode makes the caller read it and report the error.
        if let Some(row) = self.db.group_row(key) {
            return match row {
                None => Some(None),
                Some(row) => crate::state::key_index::KeyMeta::decode(&row)
                    .ok()
                    .map(|meta| Some(meta.stub())),
            };
        }
        Some(index.get(key).map(|meta| meta.stub()))
    }

    async fn point(&self, key: &[u8], overlay: bool) -> KvResult<Option<KeyValue>> {
        if overlay {
            if let Some(value) = self.changes.get(key) {
                return Ok(value.clone());
            }
        }
        self.db.current(key).await
    }

    fn check_revision(&self, request: &RangeRequest, nested: bool) -> KvResult<()> {
        if request.revision < self.compact_revision
            && (request.revision > 0 || (nested && request.revision != 0))
        {
            return Err(KvError::new(Code::OutOfRange, COMPACTED));
        }
        if request.revision > self.revision {
            return Err(KvError::new(
                Code::OutOfRange,
                "mvcc: required revision is a future revision",
            ));
        }
        Ok(())
    }

    async fn latest(&self, key: &[u8], end: &[u8], overlay: bool) -> KvResult<Rows> {
        let mut rows = BTreeMap::new();
        if end.is_empty() {
            if let Some(kv) = self.db.current(key).await? {
                rows.insert(key.to_vec(), kv);
            }
        } else {
            let bounds = current_bounds(key, end);
            if bounds.is_empty() {
                return Ok(rows);
            }
            let mut scan = self.db.scan(bounds).await.map_err(KvError::storage)?;
            while let Some(row) = scan.next().await.map_err(KvError::storage)? {
                let kv = KeyValue::decode(row.value).map_err(KvError::storage)?;
                if contains(&kv.key, key, end) {
                    rows.insert(kv.key.clone(), kv);
                }
            }
        }
        if overlay {
            for (k, value) in &self.changes {
                if contains(k, key, end) {
                    if let Some(kv) = value {
                        rows.insert(k.clone(), kv.clone());
                    } else {
                        rows.remove(k);
                    }
                }
            }
        }
        Ok(rows)
    }

    /// Return the version of `key` at `revision`, or `None` if no version precedes it.
    /// One descending scan reads only that version.
    async fn version_at(&self, key: &[u8], revision: i64) -> KvResult<Option<Version>> {
        let range = history_prefix(key)..history_key(key, revision + 1);
        let row = self.db.last(range).await.map_err(KvError::storage)?;
        row.map(|row| Version::decode(row.value).map_err(KvError::storage))
            .transpose()
    }

    /// Read `[key, end)` at `revision` from the history rows of the interval.
    ///
    /// History rows sort by key and then by revision. One ascending scan reads the versions of
    /// each key up to `revision` by row key and decodes only the selected version. It reads a few
    /// later versions and then seeks past the rest. A key with more than `PROBE_VERSIONS` versions
    /// up to `revision` reads its current row instead. If that row is newer than `revision`, one
    /// descending scan reads the version. The cost is about one seek per key in the interval.
    async fn history_range(
        &self,
        key: &[u8],
        end: &[u8],
        revision: i64,
        out: &mut range::Assembly<'_>,
    ) -> KvResult<()> {
        const PROBE_VERSIONS: usize = 16;
        // A seek costs more than reading a few adjacent rows.
        const READ_AHEAD: usize = 8;
        let lower = history_prefix(key);
        let upper = if end == [0] {
            // The hex key encoding keeps key order, and `0` sorts after the `/` separator.
            [&HISTORY[..HISTORY.len() - 1], b"0"].concat()
        } else {
            history_prefix(end)
        };
        if lower >= upper {
            return Ok(());
        }
        let mut scan = self
            .db
            .scan(lower..upper.clone())
            .await
            .map_err(KvError::storage)?;
        let mut next = scan.next().await.map_err(KvError::storage)?;
        while let Some(mut row) = next.take() {
            // History keys end with the revision. The rest names the user key.
            let Some(prefix_len) = row
                .key
                .len()
                .checked_sub(8)
                .filter(|&len| len > HISTORY.len())
            else {
                return Err(KvError::corrupt("short history key"));
            };
            let prefix = row.key.slice(..prefix_len);
            let row_len = row.key.len();
            let same_key = |candidate: &crate::state::Row| {
                candidate.key.len() == row_len && candidate.key.starts_with(&prefix)
            };
            let row_revision =
                |key: &[u8]| i64::from_be_bytes(key[prefix_len..].try_into().unwrap());
            let mut latest = None;
            let mut probed = 0;
            let mut later = 0;
            // Read versions of this key. `next` gets the first row of the next key.
            let seek = loop {
                if row_revision(&row.key) <= revision {
                    latest = Some(row.value);
                    probed += 1;
                    if probed == PROBE_VERSIONS {
                        break true;
                    }
                } else {
                    later += 1;
                    if later > READ_AHEAD {
                        break true;
                    }
                }
                match scan.next().await.map_err(KvError::storage)? {
                    Some(candidate) if same_key(&candidate) => row = candidate,
                    other => {
                        next = other;
                        break false;
                    }
                }
            };
            // The metadata projection skips the value. Only a kept row decodes it.
            let version = latest
                .map(|row| {
                    let meta = range::VersionMeta::decode(row.clone()).map_err(KvError::storage)?;
                    Ok::<_, KvError>((meta, row))
                })
                .transpose()?;
            let candidate = match version {
                Some((version, _)) if probed == PROBE_VERSIONS => {
                    // A current row that is not newer than `revision` is the version at it.
                    // A point read is cheaper than a descending scan.
                    match self.db.current(&version.key).await? {
                        Some(kv) if kv.mod_revision <= revision => Some(kv),
                        _ => self
                            .version_at(&version.key, revision)
                            .await?
                            .and_then(|version| version.value),
                    }
                    .map(range::Candidate::Decoded)
                }
                version => version.and_then(|(version, row)| version.live(row)),
            };
            if let Some(candidate) = candidate {
                out.offer(candidate)?;
            }
            if seek {
                // All versions of this key sort before this bound. Longer keys start at it.
                let mut after = prefix.to_vec();
                *after
                    .last_mut()
                    .expect("history prefix ends with a separator") = b'0';
                if after >= upper {
                    break;
                }
                scan.seek(&after).await.map_err(KvError::storage)?;
                next = scan.next().await.map_err(KvError::storage)?;
            }
        }
        Ok(())
    }

    /// Read `[key, end)` at `revision` from the current rows and the later Watch journal.
    ///
    /// A key that no journal event after `revision` names has the same row now as at `revision`.
    /// The result merges those current rows with the versions at `revision` of the named keys.
    /// The count is the current count plus a correction for each named key. The first event of a
    /// key after `revision` shows whether the key was live at `revision`, so the count reads no
    /// history. Only named keys that the result holds read their version. The cost is about
    /// `keep` rows plus the journal after `revision`. It does not depend on older history.
    ///
    /// Journal rows share the batch of their revision, and compaction keeps those after its
    /// boundary. `None` means that the view has no key order or that the journal costs more
    /// than reading the history rows of the interval.
    async fn journal_range(
        &self,
        key: &[u8],
        end: &[u8],
        revision: i64,
        keep: usize,
    ) -> KvResult<Option<(Vec<KeyValue>, i64)>> {
        let (Some(keys), ReadDb::Pinned { db, .. }) = (self.keys, &self.db) else {
            return Ok(None);
        };
        let bounds = current_bounds(key, end);
        if bounds.is_empty() {
            return Ok(Some((vec![], 0)));
        }
        let mut count = keys.count(key, end);
        // Each later event costs one key decode. Reading the history rows costs about one seek
        // per key in the interval.
        let budget = usize::try_from(count).unwrap_or(0).max(1024);
        let mut events = 0;
        // Each key that an event after `revision` names, and whether it was live at `revision`.
        let mut named = BTreeMap::<Vec<u8>, bool>::new();
        if revision < self.revision {
            let mut scan = db
                .scan_range(
                    crate::watch::key(revision + 1)..crate::watch::key(self.revision + 1),
                    &scan_options(),
                )
                .await
                .map_err(KvError::storage)?;
            while let Some(row) = scan.next().await.map_err(KvError::storage)? {
                // The key projection skips the values of the events without copying them.
                let journal = JournalKeys::decode(row.value).map_err(KvError::storage)?;
                events += journal.events.len();
                if events > budget {
                    return Ok(None);
                }
                for event in journal.events {
                    let Some(kv) = event.kv.filter(|kv| contains(&kv.key, key, end)) else {
                        continue;
                    };
                    // The state before the first later event of a key is its state at `revision`.
                    let live = kv.live_before(event.r#type);
                    named.entry(kv.key).or_insert(live);
                }
            }
        }
        for (named, &then) in &named {
            let live_now = keys.count(named, &[named.as_slice(), &[0]].concat()) == 1;
            count += i64::from(then) - i64::from(live_now);
        }
        let mut rows = Vec::new();
        if keep == 0 {
            return Ok(Some((rows, count)));
        }
        // Collect first: an iterator adapter held across an await fails the `Send` bound.
        let live: Vec<&[u8]> = named
            .iter()
            .filter(|(_, live)| **live)
            .map(|(key, _)| key.as_slice())
            .collect();
        let mut older = live.into_iter().peekable();
        let mut scan = self.db.scan(bounds).await.map_err(KvError::storage)?;
        let mut current = None;
        while rows.len() < keep {
            if current.is_none() {
                while let Some(row) = scan.next().await.map_err(KvError::storage)? {
                    let kv = KeyValue::decode(row.value).map_err(KvError::storage)?;
                    if !named.contains_key(&kv.key) {
                        current = Some(kv);
                        break;
                    }
                }
            }
            let take_older = match (&current, older.peek()) {
                (Some(now), Some(then)) => *then < now.key.as_slice(),
                (None, Some(_)) => true,
                (_, None) => false,
            };
            match (take_older, current.take()) {
                (true, now) => {
                    current = now;
                    let named = older.next().unwrap();
                    #[cfg(test)]
                    JOURNAL_VERSION_READS.set(JOURNAL_VERSION_READS.get() + 1);
                    // The journal shows the key live at `revision`, so a version holds its row.
                    let then = self
                        .version_at(named, revision)
                        .await?
                        .and_then(|version| version.value)
                        .ok_or_else(|| {
                            KvError::corrupt("history has no live version that the journal names")
                        })?;
                    rows.push(then);
                }
                (false, Some(now)) => rows.push(now),
                (false, None) => break,
            }
        }
        Ok(Some((rows, count)))
    }

    async fn compare(&self, cmp: &Compare) -> KvResult<bool> {
        let rows = match self.indexed(&cmp.key) {
            // A metadata row has no value bytes. Value comparisons read the stored row.
            Some(meta) if cmp.range_end.is_empty() && cmp.target != 3 => {
                meta.map(|kv| (cmp.key.clone(), kv)).into_iter().collect()
            }
            _ => self.latest(&cmp.key, &cmp.range_end, false).await?,
        };
        if rows.is_empty() && cmp.target == 3 {
            return Ok(false);
        }
        let empty = KeyValue::default();
        let values: Vec<_> = if rows.is_empty() {
            vec![&empty]
        } else {
            rows.values().collect()
        };
        use compare::TargetUnion as T;
        for kv in values {
            let order = match cmp.target {
                0 => kv.version.cmp(&match &cmp.target_union {
                    Some(T::Version(v)) => *v,
                    _ => 0,
                }),
                1 => kv.create_revision.cmp(&match &cmp.target_union {
                    Some(T::CreateRevision(v)) => *v,
                    _ => 0,
                }),
                2 => kv.mod_revision.cmp(&match &cmp.target_union {
                    Some(T::ModRevision(v)) => *v,
                    _ => 0,
                }),
                3 => kv.value.as_slice().cmp(match &cmp.target_union {
                    Some(T::Value(v)) => v,
                    _ => &[],
                }),
                4 => kv.lease.cmp(&match &cmp.target_union {
                    Some(T::Lease(v)) => *v,
                    _ => 0,
                }),
                _ => return Err(KvError::invalid("invalid compare option")),
            };
            let matched = match cmp.result {
                0 => order.is_eq(),
                1 => order.is_gt(),
                2 => order.is_lt(),
                3 => !order.is_eq(),
                _ => false,
            };
            if !matched {
                return Ok(false);
            }
        }
        Ok(true)
    }

    // Resolve nested predicates and validate state-dependent errors against the base view.
    fn plan<'a, 'r: 'a>(&'a self, request: &'r RequestOp, nested: bool) -> KvFuture<'a, Plan<'r>> {
        Box::pin(async move {
            let op = request
                .request
                .as_ref()
                .ok_or_else(|| KvError::invalid("request is not provided"))?;
            match op {
                Op::RequestTxn(txn) => {
                    let mut succeeded = true;
                    for cmp in &txn.compare {
                        if !self.compare(cmp).await? {
                            succeeded = false;
                            break;
                        }
                    }
                    let mut children = vec![];
                    for child in if succeeded {
                        &txn.success
                    } else {
                        &txn.failure
                    } {
                        children.push(self.plan(child, true).await?);
                    }
                    Ok(Plan::Txn {
                        succeeded,
                        children,
                    })
                }
                Op::RequestRange(r) => {
                    self.check_revision(r, nested)?;
                    Ok(Plan::Op(std::borrow::Cow::Borrowed(op)))
                }
                Op::RequestPut(p) => {
                    if p.lease != 0 && !self.leases.contains_key(&p.lease) {
                        return Err(KvError::new(Code::NotFound, "requested lease not found"));
                    }
                    // Only a put that keeps the old value or lease needs an existing key.
                    if p.ignore_value || p.ignore_lease {
                        let exists = match self.indexed(&p.key) {
                            Some(meta) => meta.is_some(),
                            None => self.point(&p.key, false).await?.is_some(),
                        };
                        if !exists {
                            return Err(KvError::invalid("key not found"));
                        }
                    }
                    Ok(Plan::Op(std::borrow::Cow::Borrowed(op)))
                }
                _ => Ok(Plan::Op(std::borrow::Cow::Borrowed(op))),
            }
        })
    }

    fn current_revision(&self) -> i64 {
        self.revision + i64::from(!self.changes.is_empty())
    }

    fn execute<'a, 'r: 'a>(&'a mut self, plan: Plan<'r>, nested: bool) -> KvFuture<'a, ResponseOp> {
        Box::pin(async move {
            let op = match plan {
                Plan::Op(op) => op,
                Plan::Txn {
                    succeeded,
                    children,
                } => {
                    let mut responses = Vec::with_capacity(children.len());
                    for child in children {
                        responses.push(self.execute(child, true).await?);
                    }
                    return Ok(response(Resp::ResponseTxn(TxnResponse {
                        header: header(if nested { 0 } else { self.current_revision() }),
                        succeeded,
                        responses,
                    })));
                }
            };
            Ok(match op.as_ref() {
                Op::RequestRange(r) => response(Resp::ResponseRange(self.range(r).await?)),
                Op::RequestPut(p) => {
                    let old = match self.changes.get(&p.key) {
                        Some(value) => value.clone().map(put::Old::Full),
                        None => match self.indexed(&p.key) {
                            Some(meta) if !put::needs_old_value(p) => meta.map(put::Old::Meta),
                            Some(None) => None,
                            _ => self.point(&p.key, false).await?.map(put::Old::Full),
                        },
                    };
                    let revision = self.revision + 1;
                    let kv = put::value(p, old.as_ref().map(put::Old::kv), revision);
                    self.writes.put(&kv.key, &kv.value);
                    let (journal_prev, prev_revision) = put::prev_event(old.as_ref(), revision)?;
                    self.events.push(crate::proto::mvccpb::Event {
                        r#type: 0,
                        kv: Some(kv.clone()),
                        prev_kv: journal_prev,
                    });
                    self.prev_revisions.push(prev_revision);
                    self.changes.insert(p.key.clone(), Some(kv));
                    response(Resp::ResponsePut(PutResponse {
                        header: header(revision),
                        prev_kv: if p.prev_kv {
                            old.and_then(put::Old::into_full)
                        } else {
                            None
                        },
                    }))
                }
                Op::RequestDeleteRange(d) => {
                    let rows = self.latest(&d.key, &d.range_end, true).await?;
                    let deleted = rows.len() as i64;
                    self.writes.deletes += 1;
                    for (key, old) in &rows {
                        self.events.push(crate::proto::mvccpb::Event {
                            r#type: 1,
                            kv: Some(KeyValue {
                                key: key.clone(),
                                mod_revision: self.revision + 1,
                                ..Default::default()
                            }),
                            prev_kv: Some(old.clone()),
                        });
                        self.prev_revisions.push(0);
                        self.changes.insert(key.clone(), None);
                    }
                    response(Resp::ResponseDeleteRange(DeleteRangeResponse {
                        header: header(self.current_revision()),
                        deleted,
                        prev_kvs: if d.prev_kv {
                            rows.into_values().collect()
                        } else {
                            vec![]
                        },
                    }))
                }
                Op::RequestTxn(_) => unreachable!("transactions use a resolved plan"),
            })
        })
    }
}

/// Evaluate a read without constructing a state write batch.
pub async fn read(
    db: &impl StateRead,
    revision: i64,
    compact_revision: i64,
    request: &RequestOp,
    leases: &BTreeMap<i64, crate::lease::LeaseRecord>,
) -> KvResult<ResponseOp> {
    read_from(
        ReadDb::Live(db),
        revision,
        compact_revision,
        request,
        leases,
        None,
        None,
    )
    .await
}

pub(crate) async fn read_snapshot(
    db: &impl StateRead,
    rows: &crate::state::rows::RowCache,
    revision: i64,
    compact_revision: i64,
    request: &RequestOp,
    leases: &BTreeMap<i64, crate::lease::LeaseRecord>,
    keys: &crate::state::key_index::KeyPin,
) -> KvResult<ResponseOp> {
    read_from(
        ReadDb::Pinned {
            db,
            rows,
            seq: keys.seq(),
        },
        revision,
        compact_revision,
        request,
        leases,
        Some(keys),
        None,
    )
    .await
}

/// Return true when a current Range streams rows in key order from one scan, and so can run in
/// a shared scan slot. Every Range keeps at most `limit` rows. Historical pages stay out of the
/// shared slots: a Kubernetes page of large objects passes the row budget and would run twice.
pub(crate) fn streams_range(r: &RangeRequest) -> bool {
    !r.range_end.is_empty() && r.revision <= 0 && key_ordered(r)
}

/// Return true when a Range returns all rows of its interval in key order.
pub(super) fn key_ordered(r: &RangeRequest) -> bool {
    r.sort_target == 0
        && matches!(r.sort_order, 0 | 1)
        && r.min_mod_revision == 0
        && r.max_mod_revision == 0
        && r.min_create_revision == 0
        && r.max_create_revision == 0
}

const OVER_BUDGET: &str = "range rows exceed the shared read budget";

pub(super) fn over_budget() -> KvError {
    KvError::new(Code::ResourceExhausted, OVER_BUDGET)
}

/// Approximate heap and inline bytes of one materialized row.
pub(super) fn row_bytes(kv: &KeyValue) -> usize {
    std::mem::size_of::<KeyValue>() + kv.key.len() + kv.value.len()
}

/// Read a streaming Range from a pinned snapshot within `budget` bytes of rows.
/// `None` means the rows exceed the budget. The caller can repeat the read on the same snapshot
/// without a budget and get the same result.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn read_snapshot_within(
    db: &impl StateRead,
    rows: &crate::state::rows::RowCache,
    revision: i64,
    compact_revision: i64,
    request: &RequestOp,
    leases: &BTreeMap<i64, crate::lease::LeaseRecord>,
    keys: &crate::state::key_index::KeyPin,
    budget: usize,
) -> Option<KvResult<ResponseOp>> {
    let result = read_from(
        ReadDb::Pinned {
            db,
            rows,
            seq: keys.seq(),
        },
        revision,
        compact_revision,
        request,
        leases,
        Some(keys),
        Some(budget),
    )
    .await;
    match result {
        Err(error)
            if error.code == Code::ResourceExhausted as i32
                && error.message == over_budget().message =>
        {
            None
        }
        result => Some(result),
    }
}

async fn read_from<'a, R: StateRead>(
    db: ReadDb<'a, R>,
    revision: i64,
    compact_revision: i64,
    request: &RequestOp,
    leases: &'a BTreeMap<i64, crate::lease::LeaseRecord>,
    keys: Option<&'a crate::state::key_index::KeyPin>,
    row_budget: Option<usize>,
) -> KvResult<ResponseOp> {
    validate(request)?;
    if !read_only(request, false) {
        return Err(KvError::invalid("read request contains a write"));
    }
    let mut view = View {
        writes: Default::default(),
        db,
        revision,
        compact_revision,
        changes: BTreeMap::new(),
        events: vec![],
        prev_revisions: vec![],
        leases,
        index: None,
        row_budget,
        keys,
    };
    if let Some(Op::RequestRange(range)) = &request.request {
        view.check_revision(range, false)?;
        return Ok(response(Resp::ResponseRange(view.range(range).await?)));
    }
    let plan = view.plan(request, false).await?;
    view.execute(plan, false).await
}

/// Evaluate a write on the live applied state. `index` must describe the same applied rows.
pub(crate) async fn prepare_with_counts(
    db: &impl StateRead,
    index: Option<&crate::state::key_index::KeyIndex>,
    revision: i64,
    compact_revision: i64,
    request: &RequestOp,
    leases: &BTreeMap<i64, crate::lease::LeaseRecord>,
) -> Result<(ResponseOp, WriteBatch, i64, crate::metrics::Writes), ApplyError> {
    prepare_view(
        ReadDb::Live(db),
        index,
        revision,
        compact_revision,
        request,
        leases,
    )
    .await
    .map_err(ApplyError::from)
}

/// Evaluate a request after earlier writes of the same apply group.
/// Point reads and index lookups see `previous` first. A scan must come before any group write.
pub(crate) async fn prepare_grouped(
    current: &CurrentView<'_, impl StateRead>,
    previous: &WriteBatch,
    revision: i64,
    compact_revision: i64,
    request: &RequestOp,
    leases: &BTreeMap<i64, crate::lease::LeaseRecord>,
) -> Result<(ResponseOp, WriteBatch, i64, crate::metrics::Writes), ApplyError> {
    let db = ReadDb::Overlay {
        db: current.db,
        previous,
        rows: current.rows,
    };
    prepare_view(
        db,
        Some(current.index),
        revision,
        compact_revision,
        request,
        leases,
    )
    .await
    .map_err(ApplyError::from)
}

async fn prepare_view(
    db: ReadDb<'_, impl StateRead>,
    index: Option<&crate::state::key_index::KeyIndex>,
    revision: i64,
    compact_revision: i64,
    request: &RequestOp,
    leases: &BTreeMap<i64, crate::lease::LeaseRecord>,
) -> KvResult<(ResponseOp, WriteBatch, i64, crate::metrics::Writes)> {
    validate(request)?;
    let mut view = View {
        writes: Default::default(),
        db,
        revision,
        compact_revision,
        changes: BTreeMap::new(),
        events: vec![],
        prev_revisions: vec![],
        leases,
        index,
        row_budget: None,
        keys: None,
    };
    let plan = view.plan(request, false).await?;
    let response = view.execute(plan, false).await?;
    let next = view.current_revision();
    let mut batch = WriteBatch::new();
    put::record_changes(
        &mut batch,
        next,
        view.events,
        view.prev_revisions,
        view.changes,
    );
    if revision > 1 {
        // A revision above one implies a stored revision row.
        batch.put_known(REVISION, next.to_be_bytes().to_vec(), Some(8));
    } else {
        batch.put(REVISION, next.to_be_bytes());
    }
    Ok((response, batch, next, view.writes))
}

/// Report whether a request needs more than point reads of current rows.
/// Range scans, historical reads, and leased puts under Auth read state outside the overlay.
pub(crate) fn needs_scan(request: &RequestOp, secured: bool) -> bool {
    match &request.request {
        Some(Op::RequestRange(r)) => !r.range_end.is_empty() || r.revision > 0,
        Some(Op::RequestPut(p)) => secured && p.lease != 0,
        Some(Op::RequestDeleteRange(d)) => !d.range_end.is_empty(),
        Some(Op::RequestTxn(t)) => {
            t.compare.iter().any(|c| !c.range_end.is_empty())
                || t.success
                    .iter()
                    .chain(&t.failure)
                    .any(|op| needs_scan(op, secured))
        }
        None => false,
    }
}

pub(crate) fn batchable_put(request: &RequestOp) -> bool {
    matches!(&request.request, Some(Op::RequestPut(p))
        if p.lease == 0 && !p.ignore_lease && !p.ignore_value)
}

/// Read adjacent SST blocks together during sequential maintenance scans.
pub(crate) fn maintenance_scan() -> slatedb::config::ScanOptions {
    slatedb::config::ScanOptions {
        read_ahead_bytes: 64 * 1024,
        ..Default::default()
    }
}

/// Validate a compaction boundary. The background pass removes the older versions later.
pub fn check_compaction(
    revision: i64,
    compact_revision: i64,
    request: &CompactionRequest,
) -> KvResult<CompactionResponse> {
    if request.revision <= compact_revision {
        return Err(KvError::new(Code::OutOfRange, COMPACTED));
    }
    if request.revision > revision {
        return Err(KvError::new(
            Code::OutOfRange,
            "mvcc: required revision is a future revision",
        ));
    }
    Ok(CompactionResponse {
        header: header(revision),
    })
}

pub fn response_header(op: &mut ResponseOp) -> Option<&mut ResponseHeader> {
    let header = match op.response.as_mut() {
        Some(Resp::ResponseRange(r)) => &mut r.header,
        Some(Resp::ResponsePut(r)) => &mut r.header,
        Some(Resp::ResponseDeleteRange(r)) => &mut r.header,
        Some(Resp::ResponseTxn(r)) => &mut r.header,
        None => return None,
    };
    header.as_mut()
}

pub fn fill_header(op: &mut ResponseOp, cluster: u64, member: u64, term: u64) {
    if let Some(header) = response_header(op) {
        header.cluster_id = cluster;
        header.member_id = member;
        header.raft_term = term;
    }
}

/// Return the current keys of a lease in key order. Without the key index, scan every current row.
pub(crate) async fn lease_keys(
    db: &impl StateRead,
    index: Option<&crate::state::key_index::KeyIndex>,
    id: i64,
) -> KvResult<Vec<Vec<u8>>> {
    if let Some(index) = index {
        return Ok(index.lease_keys(id));
    }
    scan_lease_keys(db, id).await
}

/// Find the current keys of a lease with a scan of every current row.
pub(crate) async fn scan_lease_keys(db: &impl StateRead, id: i64) -> KvResult<Vec<Vec<u8>>> {
    let mut scan = db
        .scan_under(CURRENT, &Default::default())
        .await
        .map_err(KvError::storage)?;
    let mut keys = vec![];
    while let Some(row) = scan.next().await.map_err(KvError::storage)? {
        let kv = KeyValue::decode(row.value).map_err(KvError::storage)?;
        if kv.lease == id {
            keys.push(kv.key);
        }
    }
    Ok(keys)
}

pub(crate) async fn revoke_keys(
    db: &impl StateRead,
    index: Option<&crate::state::key_index::KeyIndex>,
    revision: i64,
    compact_revision: i64,
    id: i64,
    leases: &BTreeMap<i64, crate::lease::LeaseRecord>,
) -> KvResult<(WriteBatch, i64, u64)> {
    let mut view = View {
        writes: Default::default(),
        db: ReadDb::Live(db),
        revision,
        compact_revision,
        changes: BTreeMap::new(),
        events: vec![],
        prev_revisions: vec![],
        leases,
        index: None,
        row_budget: None,
        keys: None,
    };
    for key in lease_keys(db, index, id).await? {
        view.execute(
            Plan::Op(std::borrow::Cow::Owned(Op::RequestDeleteRange(
                DeleteRangeRequest {
                    key,
                    ..Default::default()
                },
            ))),
            true,
        )
        .await?;
    }
    let next = view.current_revision();
    let mut batch = WriteBatch::new();
    crate::watch::record(
        next,
        view.events,
        view.prev_revisions,
        &view.changes,
        &mut batch,
    );
    for (key, value) in view.changes {
        batch.delete(current_key(&key));
        let mut history_key = history_prefix(&key);
        history_key.extend_from_slice(&next.to_be_bytes());
        batch.put_new(
            history_key,
            Version {
                key,
                revision: next,
                value,
            }
            .encode_to_vec(),
        );
    }
    batch.put(REVISION, next.to_be_bytes());
    Ok((batch, next, view.writes.deletes))
}
