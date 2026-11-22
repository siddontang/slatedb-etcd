use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::Pin;

use crate::WriteBatch;
use prost::Message;
use serde::{Deserialize, Serialize};
use slatedb::Db;
use tonic::{Code, Status};

use crate::proto::etcdserverpb::request_op::Request as Op;
use crate::proto::etcdserverpb::response_op::Response as Resp;
use crate::proto::etcdserverpb::*;
use crate::proto::mvccpb::KeyValue;
use crate::proto::state::Version;

mod put;
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
}

impl KvError {
    pub(crate) fn new(code: Code, message: &str) -> Self {
        Self {
            code: code as i32,
            message: format!("etcdserver: {message}"),
        }
    }

    fn invalid(message: &str) -> Self {
        Self::new(Code::InvalidArgument, message)
    }

    pub(crate) fn storage(error: impl std::fmt::Display) -> Self {
        Self::new(Code::Internal, &format!("storage error: {error}"))
    }

    pub fn status(self) -> Status {
        Status::new(Code::from_i32(self.code), self.message)
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

enum ReadDb<'a> {
    Live(&'a Db),
    Pinned(&'a slatedb::DbSnapshot, &'a crate::state::rows::RowCache),
    /// Applied state plus earlier writes of the same apply group.
    Overlay {
        db: &'a Db,
        previous: &'a WriteBatch,
        rows: &'a crate::state::current::CurrentRows,
    },
}

impl ReadDb<'_> {
    async fn get(&self, key: Vec<u8>) -> Result<Option<bytes::Bytes>, slatedb::Error> {
        match self {
            Self::Live(db) => db.get(key).await,
            Self::Pinned(db, _) => db.get(key).await,
            Self::Overlay { db, previous, rows } => {
                if let Some(value) = previous.get(&key) {
                    return Ok(value);
                }
                if let Some(value) = rows.get(&key) {
                    return Ok(Some(value));
                }
                db.get(key).await
            }
        }
    }

    /// Read the current row of a user key. Pinned views share rows through the row cache.
    async fn current(&self, key: &[u8]) -> KvResult<Option<KeyValue>> {
        let decode = |row: &[u8]| KeyValue::decode(row).map_err(KvError::storage);
        let Self::Pinned(db, rows) = self else {
            let row = self.get(current_key(key)).await.map_err(KvError::storage)?;
            return row.as_deref().map(decode).transpose();
        };
        let seq = db.seq();
        if let Some(kv) = rows.get(key, seq, decode) {
            return kv.map(Some);
        }
        let row = db.get(current_key(key)).await.map_err(KvError::storage)?;
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
    fn scan_base(&self) -> Result<&Db, slatedb::Error> {
        match self {
            Self::Overlay { db, previous, .. } if previous.is_empty() => Ok(db),
            Self::Overlay { .. } => Err(slatedb::Error::unavailable(
                "scan over an apply group overlay".to_string(),
            )),
            Self::Live(db) => Ok(db),
            Self::Pinned(..) => unreachable!("pinned scans use the snapshot"),
        }
    }

    async fn scan(
        &self,
        range: std::ops::Range<Vec<u8>>,
    ) -> Result<slatedb::DbIterator, slatedb::Error> {
        let options = scan_options();
        match self {
            Self::Pinned(db, _) => db.scan_with_options(range, &options).await,
            _ => self.scan_base()?.scan_with_options(range, &options).await,
        }
    }

    async fn scan_prefix(&self, prefix: Vec<u8>) -> Result<slatedb::DbIterator, slatedb::Error> {
        match self {
            Self::Pinned(db, _) => {
                db.scan_prefix_with_options(prefix, .., &scan_options())
                    .await
            }
            _ => {
                self.scan_base()?
                    .scan_prefix_with_options(prefix, .., &scan_options())
                    .await
            }
        }
    }
}

struct View<'a> {
    writes: crate::metrics::Writes,
    db: ReadDb<'a>,
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

impl View<'_> {
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

    async fn historical(&self, key: &[u8], end: &[u8], revision: i64) -> KvResult<Rows> {
        let prefix = if end.is_empty() {
            history_prefix(key)
        } else {
            HISTORY.to_vec()
        };
        let mut versions = BTreeMap::<Vec<u8>, Version>::new();
        let mut scan = self
            .db
            .scan_prefix(prefix)
            .await
            .map_err(KvError::storage)?;
        while let Some(row) = scan.next().await.map_err(KvError::storage)? {
            let v = Version::decode(row.value).map_err(KvError::storage)?;
            if v.revision <= revision
                && contains(&v.key, key, end)
                && versions
                    .get(&v.key)
                    .is_none_or(|old| old.revision < v.revision)
            {
                versions.insert(v.key.clone(), v);
            }
        }
        Ok(versions
            .into_iter()
            .filter_map(|(k, v)| v.value.map(|v| (k, v)))
            .collect())
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

    async fn ordered_range(&self, r: &RangeRequest) -> KvResult<RangeResponse> {
        let mut response = RangeResponse {
            header: header(self.current_revision()),
            ..Default::default()
        };
        let bounds = current_bounds(&r.key, &r.range_end);
        if bounds.is_empty() {
            return Ok(response);
        }
        // The key order of a pinned view holds the count, so the scan stops after `limit + 1`
        // rows. Debug builds scan the whole interval and check the count against the rows.
        let total = self.keys.map(|keys| keys.count(&r.key, &r.range_end));
        let full_scan = total.is_none() || cfg!(debug_assertions);
        if let Some(total) = total.filter(|_| r.count_only && !full_scan) {
            response.count = total;
            return Ok(response);
        }
        let mut budget = self.row_budget;
        let mut scan = self.db.scan(bounds).await.map_err(KvError::storage)?;
        while let Some(row) = scan.next().await.map_err(KvError::storage)? {
            response.count += 1;
            if r.count_only {
                continue;
            }
            if r.limit > 0 && response.count > r.limit {
                response.more = true;
                if !full_scan {
                    break;
                }
                continue;
            }
            let mut kv = KeyValue::decode(row.value).map_err(KvError::storage)?;
            if r.keys_only {
                kv.value.clear();
            }
            if let Some(budget) = &mut budget {
                *budget = budget.checked_sub(row_bytes(&kv)).ok_or_else(over_budget)?;
            }
            response.kvs.push(kv);
        }
        if let Some(total) = total {
            if full_scan {
                debug_assert_eq!(
                    total, response.count,
                    "key order count differs from the rows"
                );
            }
            response.count = total;
        }
        Ok(response)
    }

    async fn range(&self, r: &RangeRequest) -> KvResult<RangeResponse> {
        if self.changes.is_empty() && streams_range(r) {
            return self.ordered_range(r).await;
        }
        let rows: Vec<_> = if r.revision > 0 {
            self.historical(&r.key, &r.range_end, r.revision)
                .await?
                .into_values()
                .collect()
        } else if r.range_end.is_empty() {
            self.point(&r.key, true).await?.into_iter().collect()
        } else {
            self.latest(&r.key, &r.range_end, true)
                .await?
                .into_values()
                .collect()
        };
        let count = rows.len() as i64;
        let mut kvs: Vec<_> = if r.count_only {
            vec![]
        } else {
            rows.into_iter()
                .filter(|kv| {
                    (r.min_mod_revision == 0 || kv.mod_revision >= r.min_mod_revision)
                        && (r.max_mod_revision == 0 || kv.mod_revision <= r.max_mod_revision)
                        && (r.min_create_revision == 0
                            || kv.create_revision >= r.min_create_revision)
                        && (r.max_create_revision == 0
                            || kv.create_revision <= r.max_create_revision)
                })
                .collect()
        };
        kvs.sort_by(|a, b| {
            let order = match r.sort_target {
                1 => a.version.cmp(&b.version),
                2 => a.create_revision.cmp(&b.create_revision),
                3 => a.mod_revision.cmp(&b.mod_revision),
                4 => a.value.cmp(&b.value),
                _ => a.key.cmp(&b.key),
            };
            if r.sort_order == 2 {
                order.reverse()
            } else {
                order
            }
        });
        let more = r.limit > 0 && kvs.len() as i64 > r.limit;
        if more {
            kvs.truncate(r.limit as usize);
        }
        if r.keys_only {
            for kv in &mut kvs {
                kv.value.clear();
            }
        }
        Ok(RangeResponse {
            header: header(self.current_revision()),
            kvs,
            more,
            count,
        })
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
    db: &Db,
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
    db: &slatedb::DbSnapshot,
    rows: &crate::state::rows::RowCache,
    revision: i64,
    compact_revision: i64,
    request: &RequestOp,
    leases: &BTreeMap<i64, crate::lease::LeaseRecord>,
    keys: &crate::state::key_index::KeyPin,
) -> KvResult<ResponseOp> {
    read_from(
        ReadDb::Pinned(db, rows),
        revision,
        compact_revision,
        request,
        leases,
        Some(keys),
        None,
    )
    .await
}

/// Return true when a current Range streams rows in key order from one scan.
/// Such a Range holds at most `limit` rows. Other Ranges collect the interval before they sort or filter it.
pub(crate) fn streams_range(r: &RangeRequest) -> bool {
    !r.range_end.is_empty()
        && r.revision <= 0
        && r.sort_target == 0
        && matches!(r.sort_order, 0 | 1)
        && r.min_mod_revision == 0
        && r.max_mod_revision == 0
        && r.min_create_revision == 0
        && r.max_create_revision == 0
}

const OVER_BUDGET: &str = "range rows exceed the shared read budget";

fn over_budget() -> KvError {
    KvError::new(Code::ResourceExhausted, OVER_BUDGET)
}

/// Approximate heap and inline bytes of one materialized row.
fn row_bytes(kv: &KeyValue) -> usize {
    std::mem::size_of::<KeyValue>() + kv.key.len() + kv.value.len()
}

/// Read a streaming Range from a pinned snapshot within `budget` bytes of rows.
/// `None` means the rows exceed the budget. The caller can repeat the read on the same snapshot
/// without a budget and get the same result.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn read_snapshot_within(
    db: &slatedb::DbSnapshot,
    rows: &crate::state::rows::RowCache,
    revision: i64,
    compact_revision: i64,
    request: &RequestOp,
    leases: &BTreeMap<i64, crate::lease::LeaseRecord>,
    keys: &crate::state::key_index::KeyPin,
    budget: usize,
) -> Option<KvResult<ResponseOp>> {
    let result = read_from(
        ReadDb::Pinned(db, rows),
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

async fn read_from<'a>(
    db: ReadDb<'a>,
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

/// Build one atomic state batch. Semantic errors discard all changes.
pub async fn prepare(
    db: &Db,
    revision: i64,
    compact_revision: i64,
    request: &RequestOp,
    leases: &BTreeMap<i64, crate::lease::LeaseRecord>,
) -> KvResult<(ResponseOp, WriteBatch, i64)> {
    let (response, batch, next, _) =
        prepare_with_counts(db, None, revision, compact_revision, request, leases).await?;
    Ok((response, batch, next))
}

/// Evaluate a write on the live applied state. `index` must describe the same applied rows.
pub(crate) async fn prepare_with_counts(
    db: &Db,
    index: Option<&crate::state::key_index::KeyIndex>,
    revision: i64,
    compact_revision: i64,
    request: &RequestOp,
    leases: &BTreeMap<i64, crate::lease::LeaseRecord>,
) -> KvResult<(ResponseOp, WriteBatch, i64, crate::metrics::Writes)> {
    prepare_view(
        ReadDb::Live(db),
        index,
        revision,
        compact_revision,
        request,
        leases,
    )
    .await
}

/// Evaluate a request after earlier writes of the same apply group.
/// Point reads and index lookups see `previous` first. A scan must come before any group write.
pub(crate) async fn prepare_grouped(
    current: &CurrentView<'_>,
    previous: &WriteBatch,
    revision: i64,
    compact_revision: i64,
    request: &RequestOp,
    leases: &BTreeMap<i64, crate::lease::LeaseRecord>,
) -> KvResult<(ResponseOp, WriteBatch, i64, crate::metrics::Writes)> {
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
}

async fn prepare_view(
    db: ReadDb<'_>,
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

pub(crate) async fn lease_keys(db: &Db, id: i64) -> KvResult<Vec<Vec<u8>>> {
    let mut scan = db
        .scan_prefix(CURRENT, ..)
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
    db: &Db,
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
    for key in lease_keys(db, id).await? {
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
