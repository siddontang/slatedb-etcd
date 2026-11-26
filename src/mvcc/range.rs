//! Assemble Range responses from key-ordered row sources under the read budgets.
//!
//! Every source yields candidate rows in key order: one point row, the overlay rows of a
//! write transaction, a current-row scan, or a history scan that keeps the newest version of
//! each key at the requested revision. Key-ordered results stream and keep at most `limit`
//! rows. Other sort orders keep the best `limit` rows in a heap. Only kept rows are decoded
//! in full: counts, filters, and rows past the limit read the revision fields alone.

use std::cmp::Ordering;
use std::collections::BinaryHeap;

use super::*;

/// The error of a read whose kept rows exceed `--max-response-bytes`.
pub(crate) const TOO_LARGE: &str = "etcdserver: range response is too large";

/// Byte budgets of one read. `None` means unbounded. Apply-side views have no budget, so
/// write results never depend on member-local configuration.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Budget {
    /// Bytes of rows a shared streaming scan may materialize before it runs again alone.
    pub rows: Option<usize>,
    /// Encoded bytes of kept rows across the whole response, including transaction children.
    pub response: Option<usize>,
}

impl Budget {
    fn take(&mut self, kv: &KeyValue) -> KvResult<()> {
        if let Some(rows) = &mut self.rows {
            *rows = rows.checked_sub(row_bytes(kv)).ok_or_else(over_budget)?;
        }
        if let Some(response) = &mut self.response {
            *response = response
                .checked_sub(response_bytes(kv))
                .ok_or_else(too_large)?;
        }
        Ok(())
    }

    fn give_back(&mut self, kv: &KeyValue) {
        if let Some(rows) = &mut self.rows {
            *rows += row_bytes(kv);
        }
        if let Some(response) = &mut self.response {
            *response += response_bytes(kv);
        }
    }
}

pub(crate) fn too_large() -> KvError {
    KvError {
        code: Code::ResourceExhausted as i32,
        message: TOO_LARGE.into(),
    }
}

/// Encoded bytes of one `kvs` entry of a RangeResponse: tag, length, and message.
fn response_bytes(kv: &KeyValue) -> usize {
    let len = kv.encoded_len();
    1 + prost::length_delimiter_len(len) + len
}

/// The fields of `mvccpb.KeyValue` without its value bytes.
#[derive(Clone, PartialEq, Message)]
struct RowMeta {
    #[prost(bytes = "vec", tag = "1")]
    key: Vec<u8>,
    #[prost(int64, tag = "2")]
    create_revision: i64,
    #[prost(int64, tag = "3")]
    mod_revision: i64,
    #[prost(int64, tag = "4")]
    version: i64,
    #[prost(int64, tag = "6")]
    lease: i64,
}

impl RowMeta {
    fn stub(self) -> KeyValue {
        KeyValue {
            key: self.key,
            create_revision: self.create_revision,
            mod_revision: self.mod_revision,
            version: self.version,
            lease: self.lease,
            value: vec![],
        }
    }
}

/// `state.Version` with the value fields of `RowMeta`.
#[derive(Clone, PartialEq, Message)]
pub(super) struct VersionMeta {
    #[prost(bytes = "vec", tag = "1")]
    pub key: Vec<u8>,
    #[prost(int64, tag = "2")]
    revision: i64,
    #[prost(message, optional, tag = "3")]
    value: Option<RowMeta>,
}

impl VersionMeta {
    /// The candidate of a live version. A deleted key has a version without a value.
    pub(super) fn live(self, row: bytes::Bytes) -> Option<Candidate> {
        self.value.map(|meta| Candidate::History(row, meta.stub()))
    }
}

/// One row in key order. Encoded rows decode their value only when the response keeps it.
pub(super) enum Candidate {
    Decoded(KeyValue),
    /// An encoded `etcd/current/` row and its metadata, once read.
    Current(bytes::Bytes, Option<KeyValue>),
    /// An encoded live `state.Version` and the metadata of its value.
    History(bytes::Bytes, KeyValue),
}

impl Candidate {
    fn meta(&mut self) -> KvResult<&KeyValue> {
        Ok(match self {
            Self::Decoded(kv) | Self::History(_, kv) => kv,
            Self::Current(row, meta) => match meta {
                Some(meta) => meta,
                None => meta.insert(
                    RowMeta::decode(row.clone())
                        .map_err(KvError::storage)?
                        .stub(),
                ),
            },
        })
    }

    /// Return the row, with its value only when `value` is set.
    fn into_kv(self, value: bool) -> KvResult<KeyValue> {
        match (self, value) {
            (Self::Decoded(kv), true) => Ok(kv),
            (Self::Decoded(mut kv), false) => {
                kv.value.clear();
                Ok(kv)
            }
            (Self::Current(_, Some(meta)), false) | (Self::History(_, meta), false) => Ok(meta),
            (Self::Current(row, None), false) => {
                Ok(RowMeta::decode(row).map_err(KvError::storage)?.stub())
            }
            (Self::Current(row, _), true) => KeyValue::decode(row).map_err(KvError::storage),
            (Self::History(row, _), true) => Version::decode(row)
                .map_err(KvError::storage)?
                .value
                .ok_or_else(|| KvError::storage("history row lost its value")),
        }
    }
}

/// A row ranked by the requested sort order, then by key.
struct Ranked {
    kv: KeyValue,
    target: i32,
    descending: bool,
}

/// The request has a revision filter.
fn has_filters(r: &RangeRequest) -> bool {
    r.min_mod_revision != 0
        || r.max_mod_revision != 0
        || r.min_create_revision != 0
        || r.max_create_revision != 0
}

fn order(a: &KeyValue, b: &KeyValue, target: i32, descending: bool) -> Ordering {
    let order = match target {
        1 => a.version.cmp(&b.version),
        2 => a.create_revision.cmp(&b.create_revision),
        3 => a.mod_revision.cmp(&b.mod_revision),
        4 => a.value.cmp(&b.value),
        _ => a.key.cmp(&b.key),
    };
    // A stable sort of key-ordered rows keeps equal rows in key order.
    if descending { order.reverse() } else { order }.then_with(|| a.key.cmp(&b.key))
}

impl Ord for Ranked {
    fn cmp(&self, other: &Self) -> Ordering {
        order(&self.kv, &other.kv, self.target, self.descending)
    }
}

impl PartialOrd for Ranked {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Ranked {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}

impl Eq for Ranked {}

/// Collect the response rows of one Range from candidates in key order.
pub(super) struct Assembly<'r> {
    r: &'r RangeRequest,
    budget: Budget,
    /// Candidates before filters, as etcd counts them.
    count: i64,
    /// Candidates that pass the revision filters.
    matched: i64,
    /// Key order streams into `kvs`. Other orders keep the best rows in `heap`.
    key_order: bool,
    kvs: Vec<KeyValue>,
    heap: BinaryHeap<Ranked>,
}

impl<'r> Assembly<'r> {
    fn new(r: &'r RangeRequest, budget: Budget) -> Self {
        Self {
            r,
            budget,
            count: 0,
            matched: 0,
            key_order: r.sort_target == 0 && r.sort_order != 2,
            kvs: vec![],
            heap: BinaryHeap::new(),
        }
    }

    /// A limit of `k` keeps `k` rows. Zero keeps every row.
    fn keep(&self) -> Option<usize> {
        (self.r.limit > 0).then(|| usize::try_from(self.r.limit).unwrap_or(usize::MAX))
    }

    /// Later candidates can change only the count.
    fn rows_complete(&self) -> bool {
        self.r.count_only
            || (self.key_order && self.keep().is_some_and(|k| self.matched > k as i64))
    }

    fn filtered(&self, kv: &KeyValue) -> bool {
        let r = self.r;
        (r.min_mod_revision == 0 || kv.mod_revision >= r.min_mod_revision)
            && (r.max_mod_revision == 0 || kv.mod_revision <= r.max_mod_revision)
            && (r.min_create_revision == 0 || kv.create_revision >= r.min_create_revision)
            && (r.max_create_revision == 0 || kv.create_revision <= r.max_create_revision)
    }

    pub(super) fn offer(&mut self, mut candidate: Candidate) -> KvResult<()> {
        self.count += 1;
        if self.rows_complete() {
            return Ok(());
        }
        // Without filters, a kept row decodes once and a skipped row not at all.
        let filters = has_filters(self.r);
        if filters && !self.filtered(candidate.meta()?) {
            return Ok(());
        }
        self.matched += 1;
        let keep = self.keep();
        if self.key_order {
            if keep.is_some_and(|k| self.kvs.len() >= k) {
                return Ok(());
            }
            let kv = candidate.into_kv(!self.r.keys_only)?;
            self.budget.take(&kv)?;
            self.kvs.push(kv);
            return Ok(());
        }
        let (target, descending) = (self.r.sort_target, self.r.sort_order == 2);
        // Value order compares values, so keys-only rows keep them until the response is built.
        let value = !self.r.keys_only || target == 4;
        let full = keep.is_some_and(|k| self.heap.len() >= k);
        if full {
            let worst = &self.heap.peek().unwrap().kv;
            if target != 4 && order(candidate.meta()?, worst, target, descending).is_ge() {
                return Ok(());
            }
        }
        let kv = candidate.into_kv(value)?;
        if full {
            let worst = &self.heap.peek().unwrap().kv;
            if order(&kv, worst, target, descending).is_ge() {
                return Ok(());
            }
            let worst = self.heap.pop().unwrap();
            self.budget.give_back(&worst.kv);
        }
        self.budget.take(&kv)?;
        self.heap.push(Ranked {
            kv,
            target,
            descending,
        });
        Ok(())
    }

    /// Rows past the limit passed the filters.
    fn more(&self) -> bool {
        !self.r.count_only && self.keep().is_some_and(|k| self.matched > k as i64)
    }

    fn finish(self, revision: i64) -> (RangeResponse, Budget) {
        let more = self.more();
        let mut kvs = if self.key_order {
            self.kvs
        } else {
            self.heap
                .into_sorted_vec()
                .into_iter()
                .map(|ranked| ranked.kv)
                .collect()
        };
        if self.r.keys_only {
            for kv in &mut kvs {
                kv.value.clear();
            }
        }
        let response = RangeResponse {
            header: header(revision),
            kvs,
            more,
            count: self.count,
        };
        (response, self.budget)
    }
}

impl<R: StateRead> View<'_, R> {
    pub(super) async fn range(&mut self, r: &RangeRequest) -> KvResult<RangeResponse> {
        let mut out = Assembly::new(r, self.budget);
        let total = self.offer_rows(r, &mut out).await?;
        let (mut response, budget) = out.finish(self.current_revision());
        if let Some(total) = total {
            response.count = total;
        }
        self.budget = budget;
        Ok(response)
    }

    /// Offer the rows of `r` in key order. `Some` is the count of the whole interval when the
    /// source knows it without offering every row.
    async fn offer_rows(&self, r: &RangeRequest, out: &mut Assembly<'_>) -> KvResult<Option<i64>> {
        if r.revision > 0 {
            // Historical rows ignore the overlay: `check_revision` keeps `r.revision` at or
            // below the base revision.
            if r.range_end.is_empty() {
                let kv = self.version_at(&r.key, r.revision).await?;
                if let Some(kv) = kv.and_then(|version| version.value) {
                    out.offer(Candidate::Decoded(kv))?;
                }
                return Ok(None);
            }
            // Key order without filters needs only the first `limit + 1` rows and the count.
            let keep = if r.count_only {
                0
            } else if r.limit > 0 && key_ordered(r) {
                usize::try_from(r.limit)
                    .unwrap_or(usize::MAX)
                    .saturating_add(1)
            } else {
                usize::MAX
            };
            if keep != usize::MAX {
                let journal = self
                    .journal_range(&r.key, &r.range_end, r.revision, keep)
                    .await?;
                if let Some((rows, count)) = journal {
                    for kv in rows {
                        out.offer(Candidate::Decoded(kv))?;
                    }
                    // Debug builds also read the history rows and check the result against them.
                    if cfg!(debug_assertions) {
                        let mut history = Assembly::new(r, Budget::default());
                        self.history_range(&r.key, &r.range_end, r.revision, &mut history)
                            .await?;
                        let journal = (&out.kvs, count, out.more());
                        let history = (&history.kvs, history.count, history.more());
                        debug_assert_eq!(journal, history, "journal range differs from history");
                    }
                    return Ok(Some(count));
                }
            }
            self.history_range(&r.key, &r.range_end, r.revision, out)
                .await?;
            return Ok(None);
        }
        if r.range_end.is_empty() {
            if let Some(kv) = self.point(&r.key, true).await? {
                out.offer(Candidate::Decoded(kv))?;
            }
            return Ok(None);
        }
        if !self.changes.is_empty() {
            for kv in self.latest(&r.key, &r.range_end, true).await?.into_values() {
                out.offer(Candidate::Decoded(kv))?;
            }
            return Ok(None);
        }
        let bounds = current_bounds(&r.key, &r.range_end);
        if bounds.is_empty() {
            return Ok(None);
        }
        // The key order of a pinned view holds the count, so the scan stops once the rows are
        // complete. Debug builds scan the whole interval and check the count against the rows.
        let total = self.keys.map(|keys| keys.count(&r.key, &r.range_end));
        let full_scan = total.is_none() || cfg!(debug_assertions);
        if total.is_some() && r.count_only && !full_scan {
            return Ok(total);
        }
        let mut scan = self.db.scan(bounds).await.map_err(KvError::storage)?;
        while let Some(row) = scan.next().await.map_err(KvError::storage)? {
            if !full_scan && out.rows_complete() {
                break;
            }
            out.offer(Candidate::Current(row.value, None))?;
        }
        if let Some(total) = total.filter(|_| full_scan) {
            debug_assert_eq!(total, out.count, "key order count differs from the rows");
        }
        Ok(total)
    }
}
