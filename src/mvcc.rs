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
pub(crate) use put::prepare_put;

const CURRENT: &[u8] = b"etcd/current/";
const HISTORY: &[u8] = b"etcd/history/";
pub const REVISION: &[u8] = b"etcd/revision";
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

#[derive(Debug, Serialize, Deserialize)]
pub struct Outcome<T = ResponseOp> {
    pub result: KvResult<T>,
}

fn current_key(key: &[u8]) -> Vec<u8> {
    [CURRENT, key].concat()
}

fn history_prefix(key: &[u8]) -> Vec<u8> {
    const HEX: &[u8] = b"0123456789abcdef";
    let mut out = Vec::with_capacity(HISTORY.len() + key.len() * 2 + 1);
    out.extend_from_slice(HISTORY);
    for byte in key {
        out.push(HEX[(byte >> 4) as usize]);
        out.push(HEX[(byte & 15) as usize]);
    }
    out.push(b'/');
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

enum Plan {
    Op(Op),
    Txn {
        succeeded: bool,
        children: Vec<Plan>,
    },
}

enum ReadDb<'a> {
    Live(&'a Db),
    Pinned(&'a slatedb::DbSnapshot),
}

impl ReadDb<'_> {
    async fn get(&self, key: Vec<u8>) -> Result<Option<bytes::Bytes>, slatedb::Error> {
        match self {
            Self::Live(db) => db.get(key).await,
            Self::Pinned(db) => db.get(key).await,
        }
    }

    async fn scan(
        &self,
        range: std::ops::Range<Vec<u8>>,
    ) -> Result<slatedb::DbIterator, slatedb::Error> {
        match self {
            Self::Live(db) => db.scan(range).await,
            Self::Pinned(db) => db.scan(range).await,
        }
    }

    async fn scan_prefix(&self, prefix: Vec<u8>) -> Result<slatedb::DbIterator, slatedb::Error> {
        match self {
            Self::Live(db) => db.scan_prefix(prefix, ..).await,
            Self::Pinned(db) => db.scan_prefix(prefix, ..).await,
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
    leases: &'a BTreeMap<i64, crate::lease::LeaseRecord>,
}

impl View<'_> {
    async fn point(&self, key: &[u8], overlay: bool) -> KvResult<Option<KeyValue>> {
        if overlay {
            if let Some(value) = self.changes.get(key) {
                return Ok(value.clone());
            }
        }
        self.db
            .get(current_key(key))
            .await
            .map_err(KvError::storage)?
            .map(|value| KeyValue::decode(value).map_err(KvError::storage))
            .transpose()
    }

    fn check_revision(&self, request: &RangeRequest, nested: bool) -> KvResult<()> {
        if request.revision < self.compact_revision
            && (request.revision > 0 || (nested && request.revision != 0))
        {
            return Err(KvError::new(
                Code::OutOfRange,
                "mvcc: required revision has been compacted",
            ));
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
            if let Some(value) = self
                .db
                .get(current_key(key))
                .await
                .map_err(KvError::storage)?
            {
                rows.insert(
                    key.to_vec(),
                    KeyValue::decode(value).map_err(KvError::storage)?,
                );
            }
        } else {
            let upper = if end == [0] {
                b"etcd/current0".to_vec()
            } else {
                current_key(end)
            };
            if current_key(key) >= upper {
                return Ok(rows);
            }
            let mut scan = self
                .db
                .scan(current_key(key)..upper)
                .await
                .map_err(KvError::storage)?;
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
        let rows = self.latest(&cmp.key, &cmp.range_end, false).await?;
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
    fn plan<'a>(&'a self, request: &'a RequestOp, nested: bool) -> KvFuture<'a, Plan> {
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
                    Ok(Plan::Op(op.clone()))
                }
                Op::RequestPut(p) => {
                    if p.lease != 0 && !self.leases.contains_key(&p.lease) {
                        return Err(KvError::new(Code::NotFound, "requested lease not found"));
                    }
                    if (p.ignore_value || p.ignore_lease)
                        && self.point(&p.key, false).await?.is_none()
                    {
                        return Err(KvError::invalid("key not found"));
                    }
                    Ok(Plan::Op(op.clone()))
                }
                _ => Ok(Plan::Op(op.clone())),
            }
        })
    }

    fn current_revision(&self) -> i64 {
        self.revision + i64::from(!self.changes.is_empty())
    }

    async fn range(&self, r: &RangeRequest) -> KvResult<RangeResponse> {
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

    fn execute<'a>(&'a mut self, plan: Plan, nested: bool) -> KvFuture<'a, ResponseOp> {
        Box::pin(async move {
            Ok(match plan {
                Plan::Txn {
                    succeeded,
                    children,
                } => {
                    let mut responses = vec![];
                    for child in children {
                        responses.push(self.execute(child, true).await?);
                    }
                    response(Resp::ResponseTxn(TxnResponse {
                        header: header(if nested { 0 } else { self.current_revision() }),
                        succeeded,
                        responses,
                    }))
                }
                Plan::Op(Op::RequestRange(r)) => {
                    response(Resp::ResponseRange(self.range(&r).await?))
                }
                Plan::Op(Op::RequestPut(p)) => {
                    let old = self.point(&p.key, true).await?;
                    let revision = self.revision + 1;
                    let kv = put::value(&p, old.as_ref(), revision);
                    self.writes.put(&kv.key, &kv.value);
                    self.events.push(crate::proto::mvccpb::Event {
                        r#type: 0,
                        kv: Some(kv.clone()),
                        prev_kv: old.clone(),
                    });
                    self.changes.insert(p.key, Some(kv));
                    response(Resp::ResponsePut(PutResponse {
                        header: header(revision),
                        prev_kv: if p.prev_kv { old } else { None },
                    }))
                }
                Plan::Op(Op::RequestDeleteRange(d)) => {
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
                Plan::Op(Op::RequestTxn(_)) => unreachable!("transactions use a resolved plan"),
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
    )
    .await
}

pub(crate) async fn read_snapshot(
    db: &slatedb::DbSnapshot,
    revision: i64,
    compact_revision: i64,
    request: &RequestOp,
    leases: &BTreeMap<i64, crate::lease::LeaseRecord>,
) -> KvResult<ResponseOp> {
    read_from(
        ReadDb::Pinned(db),
        revision,
        compact_revision,
        request,
        leases,
    )
    .await
}

async fn read_from(
    db: ReadDb<'_>,
    revision: i64,
    compact_revision: i64,
    request: &RequestOp,
    leases: &BTreeMap<i64, crate::lease::LeaseRecord>,
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
        leases,
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
        prepare_with_counts(db, revision, compact_revision, request, leases).await?;
    Ok((response, batch, next))
}

pub(crate) async fn prepare_with_counts(
    db: &Db,
    revision: i64,
    compact_revision: i64,
    request: &RequestOp,
    leases: &BTreeMap<i64, crate::lease::LeaseRecord>,
) -> KvResult<(ResponseOp, WriteBatch, i64, crate::metrics::Writes)> {
    validate(request)?;
    let mut view = View {
        writes: Default::default(),
        db: ReadDb::Live(db),
        revision,
        compact_revision,
        changes: BTreeMap::new(),
        events: vec![],
        leases,
    };
    let plan = view.plan(request, false).await?;
    let response = view.execute(plan, false).await?;
    let next = view.current_revision();
    let mut batch = WriteBatch::new();
    crate::watch::record(next, view.events, &mut batch);
    for (key, value) in view.changes {
        if let Some(value) = &value {
            batch.put(current_key(&key), value.encode_to_vec());
        } else {
            batch.delete(current_key(&key));
        }
        put::record_version(&mut batch, key, value, next);
    }
    batch.put(REVISION, next.to_be_bytes());
    Ok((response, batch, next, view.writes))
}

pub(crate) fn batchable_put(request: &RequestOp) -> bool {
    matches!(&request.request, Some(Op::RequestPut(p))
        if p.lease == 0 && !p.ignore_lease && !p.ignore_value)
}

/// Remove older versions while retaining each key's value at the compaction boundary.
pub async fn prepare_compaction(
    db: &Db,
    revision: i64,
    compact_revision: i64,
    request: &CompactionRequest,
) -> KvResult<(CompactionResponse, WriteBatch)> {
    if request.revision <= compact_revision {
        return Err(KvError::new(
            Code::OutOfRange,
            "mvcc: required revision has been compacted",
        ));
    }
    if request.revision > revision {
        return Err(KvError::new(
            Code::OutOfRange,
            "mvcc: required revision is a future revision",
        ));
    }
    let mut batch = WriteBatch::new();
    let mut scan = db
        .scan_prefix(HISTORY, ..)
        .await
        .map_err(KvError::storage)?;
    let mut previous: Option<(Vec<u8>, Vec<u8>)> = None;
    while let Some(row) = scan.next().await.map_err(KvError::storage)? {
        let version = Version::decode(row.value).map_err(KvError::storage)?;
        if version.revision <= request.revision {
            // History keys sort by user key, then by increasing revision.
            if let Some((key, history_key)) = previous.take() {
                if key == version.key {
                    batch.delete(history_key);
                }
            }
            previous = Some((version.key, row.key.to_vec()));
        }
    }
    crate::watch::compact(db, request.revision, &mut batch).await?;
    batch.put(COMPACT_REVISION, request.revision.to_be_bytes());
    Ok((
        CompactionResponse {
            header: header(revision),
        },
        batch,
    ))
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
        leases,
    };
    for key in lease_keys(db, id).await? {
        view.execute(
            Plan::Op(Op::RequestDeleteRange(DeleteRangeRequest {
                key,
                ..Default::default()
            })),
            true,
        )
        .await?;
    }
    let next = view.current_revision();
    let mut batch = WriteBatch::new();
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
    crate::watch::record(next, view.events, &mut batch);
    batch.put(REVISION, next.to_be_bytes());
    Ok((batch, next, view.writes.deletes))
}
