use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::Pin;

use prost::Message;
use serde::{Deserialize, Serialize};
use slatedb::{Db, WriteBatch};
use tonic::{Code, Status};

use crate::proto::etcdserverpb::request_op::Request as Op;
use crate::proto::etcdserverpb::response_op::Response as Resp;
use crate::proto::etcdserverpb::*;
use crate::proto::mvccpb::KeyValue;

const CURRENT: &[u8] = b"etcd/current/";
const HISTORY: &[u8] = b"etcd/history/";
pub const REVISION: &[u8] = b"etcd/revision";
type KvResult<T> = Result<T, KvError>;
type KvFuture<'a, T> = Pin<Box<dyn Future<Output = KvResult<T>> + Send + 'a>>;
type Rows = BTreeMap<Vec<u8>, KeyValue>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KvError {
    pub code: i32,
    pub message: String,
}

impl KvError {
    fn new(code: Code, message: &str) -> Self {
        Self {
            code: code as i32,
            message: format!("etcdserver: {message}"),
        }
    }

    fn invalid(message: &str) -> Self {
        Self::new(Code::InvalidArgument, message)
    }

    fn storage(error: impl std::fmt::Display) -> Self {
        Self::new(Code::Internal, &format!("storage error: {error}"))
    }

    pub fn status(self) -> Status {
        Status::new(Code::from_i32(self.code), self.message)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Outcome {
    pub result: KvResult<ResponseOp>,
}

#[derive(Serialize, Deserialize)]
struct Version {
    key: Vec<u8>,
    revision: i64,
    value: Option<KeyValue>,
}

fn current_key(key: &[u8]) -> Vec<u8> {
    [CURRENT, key].concat()
}

fn history_prefix(key: &[u8]) -> Vec<u8> {
    let mut out = HISTORY.to_vec();
    for byte in key {
        out.extend_from_slice(format!("{byte:02x}").as_bytes());
    }
    out.push(b'/');
    out
}

fn contains(key: &[u8], begin: &[u8], end: &[u8]) -> bool {
    if end.is_empty() {
        key == begin
    } else {
        key >= begin && (end == [0] || key < end)
    }
}

fn header(revision: i64) -> Option<ResponseHeader> {
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

struct View<'a> {
    db: &'a Db,
    revision: i64,
    changes: BTreeMap<Vec<u8>, Option<KeyValue>>,
}

impl View<'_> {
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
            .scan_prefix(prefix, ..)
            .await
            .map_err(KvError::storage)?;
        while let Some(row) = scan.next().await.map_err(KvError::storage)? {
            let v: Version = bincode::deserialize(&row.value).map_err(KvError::storage)?;
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
                Op::RequestRange(r) if nested && r.revision < -1 => Err(KvError::new(
                    Code::OutOfRange,
                    "mvcc: required revision has been compacted",
                )),
                Op::RequestRange(r) if r.revision > self.revision => Err(KvError::new(
                    Code::OutOfRange,
                    "mvcc: required revision is a future revision",
                )),
                Op::RequestPut(p) => {
                    if p.lease != 0 {
                        return Err(KvError::new(Code::NotFound, "requested lease not found"));
                    }
                    if (p.ignore_value || p.ignore_lease)
                        && self.latest(&p.key, &[], false).await?.is_empty()
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
        let rows = if r.revision > 0 {
            self.historical(&r.key, &r.range_end, r.revision).await?
        } else {
            self.latest(&r.key, &r.range_end, true).await?
        };
        let count = rows.len() as i64;
        let mut kvs: Vec<_> = if r.count_only {
            vec![]
        } else {
            rows.into_values()
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
                    let old = self.latest(&p.key, &[], true).await?.remove(&p.key);
                    let revision = self.revision + 1;
                    let kv = KeyValue {
                        key: p.key.clone(),
                        value: if p.ignore_value {
                            old.as_ref().unwrap().value.clone()
                        } else {
                            p.value
                        },
                        version: old.as_ref().map_or(1, |v| v.version + 1),
                        create_revision: old.as_ref().map_or(revision, |v| v.create_revision),
                        mod_revision: revision,
                        lease: 0,
                    };
                    self.changes.insert(p.key, Some(kv));
                    response(Resp::ResponsePut(PutResponse {
                        header: header(revision),
                        prev_kv: if p.prev_kv { old } else { None },
                    }))
                }
                Plan::Op(Op::RequestDeleteRange(d)) => {
                    let rows = self.latest(&d.key, &d.range_end, true).await?;
                    let deleted = rows.len() as i64;
                    for key in rows.keys() {
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

/// Build one atomic state batch. Semantic errors discard all changes.
pub async fn prepare(
    db: &Db,
    revision: i64,
    request: &RequestOp,
) -> KvResult<(ResponseOp, WriteBatch, i64)> {
    validate(request)?;
    let mut view = View {
        db,
        revision,
        changes: BTreeMap::new(),
    };
    let plan = view.plan(request, false).await?;
    let response = view.execute(plan, false).await?;
    let next = view.current_revision();
    let mut batch = WriteBatch::new();
    for (key, value) in view.changes {
        if let Some(value) = &value {
            batch.put(current_key(&key), value.encode_to_vec());
        } else {
            batch.delete(current_key(&key));
        }
        let mut history_key = history_prefix(&key);
        history_key.extend_from_slice(&next.to_be_bytes());
        let version = Version {
            key,
            revision: next,
            value,
        };
        batch.put(
            history_key,
            bincode::serialize(&version).map_err(KvError::storage)?,
        );
    }
    batch.put(REVISION, next.to_be_bytes());
    Ok((response, batch, next))
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
