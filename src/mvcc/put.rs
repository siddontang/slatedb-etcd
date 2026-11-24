//! Build ordinary Put mutations from one current-value lookup.

use super::*;
use crate::state::key_index::KeyMeta;

pub(super) fn value(p: &PutRequest, old: Option<&KeyValue>, revision: i64) -> KeyValue {
    KeyValue {
        key: p.key.clone(),
        value: if p.ignore_value {
            old.unwrap().value.clone()
        } else {
            p.value.clone()
        },
        version: old.map_or(1, |v| v.version + 1),
        create_revision: old.map_or(revision, |v| v.create_revision),
        mod_revision: revision,
        lease: if p.ignore_lease {
            old.unwrap().lease
        } else {
            p.lease
        },
    }
}

pub(super) fn record_version(
    batch: &mut WriteBatch,
    key: Vec<u8>,
    value: Option<KeyValue>,
    next: i64,
) {
    let history_key = history_key(&key, next);
    let version = Version {
        key,
        revision: next,
        value,
    };
    batch.put_new(history_key, version.encode_to_vec());
}

// Embedded messages and bytes have the same wire type. Keep both KeyValue payloads intact.
#[derive(prost::Message)]
struct PutEvent {
    #[prost(bytes = "bytes", required, tag = "2")]
    kv: bytes::Bytes,
    #[prost(bytes = "bytes", optional, tag = "3")]
    prev_kv: Option<bytes::Bytes>,
}

// The wire form of `state.Journal` with one Put event.
#[derive(prost::Message)]
struct PutJournal {
    #[prost(message, required, tag = "1")]
    event: PutEvent,
    #[prost(int64, repeated, tag = "2")]
    prev_revisions: Vec<i64>,
    #[prost(bool, tag = "3")]
    values_in_history: bool,
}

// The wire form of `mvccpb.Event` with encoded `KeyValue` payloads.
#[derive(prost::Message)]
struct EventRow {
    #[prost(int32, tag = "1")]
    r#type: i32,
    #[prost(bytes = "bytes", required, tag = "2")]
    kv: bytes::Bytes,
    #[prost(bytes = "bytes", optional, tag = "3")]
    prev_kv: Option<bytes::Bytes>,
}

// The wire form of `state.Journal` with encoded events.
#[derive(prost::Message)]
struct JournalRow {
    #[prost(message, repeated, tag = "1")]
    events: Vec<EventRow>,
    #[prost(int64, repeated, tag = "2")]
    prev_revisions: Vec<i64>,
    #[prost(bool, tag = "3")]
    values_in_history: bool,
}

/// Encode `kv` without its value, as a journal event that sets `values_in_history` stores it.
fn without_value(kv: &KeyValue) -> bytes::Bytes {
    KeyValue {
        key: kv.key.clone(),
        value: vec![],
        create_revision: kv.create_revision,
        mod_revision: kv.mod_revision,
        version: kv.version,
        lease: kv.lease,
    }
    .encode_to_vec()
    .into()
}

/// Return true when no two put operations of `ops` name the same key.
fn distinct_keys(ops: &[RequestOp]) -> bool {
    let mut keys: Vec<&[u8]> = ops
        .iter()
        .filter_map(|op| match &op.request {
            Some(Op::RequestPut(p)) => Some(p.key.as_slice()),
            _ => None,
        })
        .collect();
    keys.sort_unstable();
    keys.windows(2).all(|pair| pair[0] != pair[1])
}

fn encode_version(kv: &KeyValue) -> (bytes::Bytes, bytes::Bytes) {
    encode_version_at(kv, kv.mod_revision)
}

/// Encode the history `Version` of `kv` at `revision`. The second value is the `KeyValue` part.
fn encode_version_at(kv: &KeyValue, revision: i64) -> (bytes::Bytes, bytes::Bytes) {
    use prost::encoding::{bytes as field, int64, message};

    let len = field::encoded_len(1, &kv.key)
        + int64::encoded_len(2, &revision)
        + message::encoded_len(3, kv);
    let mut version = Vec::with_capacity(len);
    field::encode(1, &kv.key, &mut version);
    int64::encode(2, &revision, &mut version);
    message::encode(3, kv, &mut version);
    let version = bytes::Bytes::from(version);
    let current = version.slice(version.len() - kv.encoded_len()..);
    (version, current)
}

/// A new value and its encoded `KeyValue` bytes.
type Encoded = (KeyValue, bytes::Bytes);

/// Add the rows of one revision: current, history, HashKV, and Watch journal rows.
///
/// The result matches `watch::record` followed by a current-row write and `record_version` for
/// each change. Each put value is encoded once. The current and history rows share those bytes.
/// The HashKV row holds their digest. When every Put event equals the history version of its key,
/// the journal omits the values and sets `values_in_history`.
pub(super) fn record_changes(
    batch: &mut WriteBatch,
    next: i64,
    events: Vec<crate::proto::mvccpb::Event>,
    mut prev_revisions: Vec<i64>,
    changes: BTreeMap<Vec<u8>, Option<KeyValue>>,
) {
    // Sorted by key, because the map iterates in key order.
    let mut encoded: Vec<(Vec<u8>, Option<Encoded>)> = Vec::with_capacity(changes.len());
    for (key, value) in changes {
        let current = match value {
            Some(kv) => {
                let (version, current) = encode_version_at(&kv, next);
                batch.put_new(history_key(&key, next), version);
                batch.put_bytes(current_key(&key).into(), current.clone());
                Some((kv, current))
            }
            None => {
                batch.delete(current_key(&key));
                record_version(batch, key.clone(), None, next);
                None
            }
        };
        encoded.push((key, current));
    }
    let shared = |kv: &KeyValue| -> Option<bytes::Bytes> {
        let at = encoded
            .binary_search_by(|(key, _)| key.as_slice().cmp(&kv.key))
            .ok()?;
        match &encoded[at].1 {
            Some((stored, current)) if stored == kv => Some(current.clone()),
            _ => None,
        }
    };
    let is_put = |event: &crate::proto::mvccpb::Event| event.r#type == 0;
    // The rule of `watch::record`: each Put event is the history version that this revision writes.
    let values_in_history = events.iter().any(is_put)
        && events.iter().filter(|event| is_put(event)).all(|event| {
            event
                .kv
                .as_ref()
                .is_some_and(|kv| kv.mod_revision == next && shared(kv).is_some())
        });
    let mut rows = Vec::with_capacity(events.len());
    for (sub, event) in events.into_iter().enumerate() {
        let kv = event.kv.unwrap_or_default();
        let deleted = event.r#type == 1;
        let bytes = match shared(&kv) {
            Some(bytes) if !deleted => bytes,
            _ => kv.encode_to_vec().into(),
        };
        let hash = if deleted {
            KeyValue {
                key: kv.key.clone(),
                ..Default::default()
            }
            .encode_to_vec()
        } else {
            crate::state::maintenance::hash_record(&kv.key, &bytes)
        };
        let bytes = if values_in_history && !deleted {
            without_value(&kv)
        } else {
            bytes
        };
        batch.put_new(
            crate::state::maintenance::hash_key(next, sub as i64, deleted),
            hash,
        );
        rows.push(EventRow {
            r#type: event.r#type,
            kv: bytes,
            prev_kv: event.prev_kv.map(|kv| kv.encode_to_vec().into()),
        });
    }
    if !rows.is_empty() {
        if prev_revisions.iter().all(|&revision| revision == 0) {
            prev_revisions.clear();
        }
        batch.put_new(
            crate::watch::key(next),
            JournalRow {
                events: rows,
                prev_revisions,
                values_in_history,
            }
            .encode_to_vec(),
        );
    }
}

/// The applied current state that an ordinary Put reads on the apply worker.
pub(crate) struct CurrentView<'a, R> {
    pub db: &'a R,
    pub rows: &'a crate::state::current::CurrentRows,
    pub index: &'a crate::state::key_index::KeyIndex,
}

impl<R: StateRead> CurrentView<'_, R> {
    async fn value(&self, key: &[u8]) -> KvResult<Option<bytes::Bytes>> {
        match self.rows.get(key) {
            Some(value) => Ok(Some(value)),
            None => self.db.get(key).await.map_err(KvError::storage),
        }
    }
}

/// The old row of a Put. A metadata-only row has no key or value bytes.
pub(super) enum Old {
    Full(KeyValue),
    Meta(KeyValue),
}

impl Old {
    pub(super) fn kv(&self) -> &KeyValue {
        match self {
            Self::Full(kv) | Self::Meta(kv) => kv,
        }
    }

    pub(super) fn into_full(self) -> Option<KeyValue> {
        match self {
            Self::Full(kv) => Some(kv),
            Self::Meta(_) => None,
        }
    }
}

/// Return true when a Put must read the old value bytes.
pub(super) fn needs_old_value(p: &PutRequest) -> bool {
    p.prev_kv || p.ignore_value
}

/// Return true when the Watch journal can name the history version of the previous value.
/// The history version at the old modification revision holds the same `KeyValue`.
/// It remains available whenever Watch can return the previous value.
fn has_history(old_mod_revision: i64, revision: i64) -> bool {
    (1..revision).contains(&old_mod_revision)
}

/// Store a reference to the history version instead of the previous value in the Watch journal.
pub(super) fn prev_event(old: Option<&Old>, revision: i64) -> KvResult<(Option<KeyValue>, i64)> {
    match old {
        None => Ok((None, 0)),
        Some(old) if has_history(old.kv().mod_revision, revision) => {
            Ok((None, old.kv().mod_revision))
        }
        // A change earlier in the same revision has no history version yet.
        Some(Old::Full(kv)) => Ok((Some(kv.clone()), 0)),
        Some(Old::Meta(_)) => Err(KvError::storage("indexed row has no older revision")),
    }
}

/// Add one ordinary Put to `batch` and update the quota total in place.
///
/// The key index supplies the old revision metadata and row length. The old value is read only
/// when the request returns it as prev_kv.
pub(crate) async fn prepare_put(
    view: &CurrentView<'_, impl StateRead>,
    revision: i64,
    request: &RequestOp,
    batch: &mut WriteBatch,
    size: &mut u64,
) -> KvResult<(ResponseOp, i64)> {
    let Some(Op::RequestPut(p)) = &request.request else {
        return Err(KvError::invalid("expected an ordinary put"));
    };
    key_check(&p.key)?;
    let next = revision + 1;
    // One put writes the only history version of its key at `next`.
    let (reply, event, prev_revision) = put_rows(view, next, 0, true, p, batch, size).await?;
    let journal = PutJournal {
        event: PutEvent {
            kv: event.kv,
            prev_kv: event.prev_kv,
        },
        prev_revisions: if prev_revision > 0 {
            vec![prev_revision]
        } else {
            vec![]
        },
        values_in_history: true,
    }
    .encode_to_vec();
    batch
        .put_counted(crate::watch::key(next).into(), journal.into(), None, size)
        .map_err(KvError::storage)?;
    put_revision_row(view, revision, batch, size).await?;
    Ok((response(Resp::ResponsePut(reply)), next))
}

/// Return true when a transaction can use `prepare_txn_puts`: both branches hold only
/// ordinary puts, and each compare reads one key.
pub(crate) fn batchable_txn(request: &RequestOp) -> bool {
    match &request.request {
        Some(Op::RequestTxn(txn)) => {
            txn.compare.iter().all(|cmp| cmp.range_end.is_empty())
                && txn
                    .success
                    .iter()
                    .chain(&txn.failure)
                    .all(super::batchable_put)
        }
        _ => false,
    }
}

#[cfg(test)]
thread_local! {
    /// Transactions that took the direct path on this thread.
    pub(crate) static DIRECT_TXNS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// Add a transaction of ordinary puts to `batch` and update the quota total in place.
///
/// This is the multi-key form of `prepare_put`. All puts share one revision, and each put is
/// the next HashKV sub-revision. The result matches the general evaluator byte for byte.
/// `None` means that the chosen branch is empty. The caller then uses the general evaluator.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn prepare_txn_puts(
    view: &CurrentView<'_, impl StateRead>,
    revision: i64,
    compact_revision: i64,
    request: &RequestOp,
    leases: &BTreeMap<i64, crate::lease::LeaseRecord>,
    batch: &mut WriteBatch,
    size: &mut u64,
    counts: &mut crate::metrics::Writes,
) -> KvResult<Option<(ResponseOp, i64)>> {
    validate(request)?;
    let Some(Op::RequestTxn(txn)) = &request.request else {
        return Err(KvError::invalid("expected a transaction"));
    };
    let succeeded = compares(
        view,
        batch,
        revision,
        compact_revision,
        &txn.compare,
        leases,
    )
    .await?;
    let ops = if succeeded {
        &txn.success
    } else {
        &txn.failure
    };
    if ops.is_empty() {
        return Ok(None);
    }
    let next = revision + 1;
    // The journal omits values only when each event equals the history version at `next`, as in
    // `watch::record`. A repeated key leaves an earlier event that no history row holds.
    let values_in_history = distinct_keys(ops);
    let mut responses = Vec::with_capacity(ops.len());
    let mut events = Vec::with_capacity(ops.len());
    let mut prev_revisions = Vec::with_capacity(ops.len());
    for (sub, op) in ops.iter().enumerate() {
        let Some(Op::RequestPut(p)) = &op.request else {
            return Err(KvError::invalid("expected an ordinary put"));
        };
        let (reply, event, prev_revision) =
            put_rows(view, next, sub, values_in_history, p, batch, size).await?;
        counts.put(&p.key, &p.value);
        responses.push(response(Resp::ResponsePut(reply)));
        events.push(event);
        prev_revisions.push(prev_revision);
    }
    if prev_revisions.iter().all(|&revision| revision == 0) {
        prev_revisions.clear();
    }
    let journal = JournalRow {
        events,
        prev_revisions,
        values_in_history,
    }
    .encode_to_vec();
    batch
        .put_counted(crate::watch::key(next).into(), journal.into(), None, size)
        .map_err(KvError::storage)?;
    put_revision_row(view, revision, batch, size).await?;
    #[cfg(test)]
    DIRECT_TXNS.with(|count| count.set(count.get() + 1));
    Ok(Some((
        response(Resp::ResponseTxn(TxnResponse {
            header: header(next),
            succeeded,
            responses,
        })),
        next,
    )))
}

/// Evaluate transaction compares on the applied state plus the earlier group writes.
async fn compares(
    current: &CurrentView<'_, impl StateRead>,
    previous: &WriteBatch,
    revision: i64,
    compact_revision: i64,
    compares: &[Compare],
    leases: &BTreeMap<i64, crate::lease::LeaseRecord>,
) -> KvResult<bool> {
    if compares.is_empty() {
        return Ok(true);
    }
    let view = View {
        writes: Default::default(),
        db: ReadDb::Overlay {
            db: current.db,
            previous,
            rows: current.rows,
        },
        revision,
        compact_revision,
        changes: BTreeMap::new(),
        events: vec![],
        prev_revisions: vec![],
        leases,
        index: Some(current.index),
        row_budget: None,
        // Compares read single keys from the live overlay, never a pinned key order.
        keys: None,
    };
    for cmp in compares {
        if !view.compare(cmp).await? {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Write the current, HashKV, and history rows of one ordinary put at revision `next`.
/// Return the reply, the Watch event, and the history revision of the previous value, or zero.
///
/// With `values_in_history`, the event omits the value: the caller writes a journal that sets
/// `values_in_history`, so its history row at `next` must be the final version of the key.
#[allow(clippy::too_many_arguments)]
async fn put_rows(
    view: &CurrentView<'_, impl StateRead>,
    next: i64,
    sub: usize,
    values_in_history: bool,
    p: &PutRequest,
    batch: &mut WriteBatch,
    size: &mut u64,
) -> KvResult<(PutResponse, EventRow, i64)> {
    let key = current_key(&p.key);
    // An earlier put in this batch may have replaced the applied row.
    let (meta, raw) = match batch.get(&key) {
        Some(Some(raw)) => {
            let meta = KeyMeta::decode(&raw).map_err(KvError::storage)?;
            (Some(meta), p.prev_kv.then_some(raw))
        }
        Some(None) => (None, None),
        None => match view.index.get(&p.key) {
            None => (None, None),
            Some(&meta) if !p.prev_kv => (Some(meta), None),
            Some(&meta) => {
                let raw = view
                    .value(&key)
                    .await?
                    .ok_or_else(|| KvError::storage("key index has no current row"))?;
                (Some(meta), Some(raw))
            }
        },
    };
    let old_len = meta.map(|m| m.row_len);
    let prev_kv = raw
        .clone()
        .map(KeyValue::decode)
        .transpose()
        .map_err(KvError::storage)?;
    let kv = KeyValue {
        key: p.key.clone(),
        value: p.value.clone(),
        create_revision: meta.map_or(next, |m| m.create_revision),
        mod_revision: next,
        version: meta.map_or(1, |m| m.version + 1),
        lease: 0,
    };
    let (history, current) = encode_version(&kv);
    let (journal_prev, prev_revision) = match meta {
        None => (None, 0),
        Some(m) if has_history(m.mod_revision, next) => (None, m.mod_revision),
        // Earlier group members have lower revisions, so this is not expected.
        Some(_) => match raw {
            Some(raw) => (Some(raw), 0),
            None => return Err(KvError::storage("indexed row has no older revision")),
        },
    };
    // The hash row holds a digest of the KeyValue, not its bytes.
    let hash = crate::state::maintenance::hash_record(&p.key, &current);
    // With values in history, the journal event omits the value. The history row holds it.
    let event_kv = if values_in_history {
        without_value(&kv)
    } else {
        current.clone()
    };
    let rows = [
        (key.into(), current, old_len),
        (
            crate::state::maintenance::hash_key(next, sub as i64, false).into(),
            hash.into(),
            None,
        ),
        (history_key(&p.key, next).into(), history, None),
    ];
    for (key, value, old_len) in rows {
        batch
            .put_counted(key, value, old_len, size)
            .map_err(KvError::storage)?;
    }
    Ok((
        PutResponse {
            header: header(next),
            prev_kv,
        },
        EventRow {
            r#type: 0,
            kv: event_kv,
            prev_kv: journal_prev,
        },
        prev_revision,
    ))
}

/// Count the revision row. It has a fixed width after its first write, so only revision one
/// changes the logical size. The group writes the final value.
async fn put_revision_row(
    view: &CurrentView<'_, impl StateRead>,
    revision: i64,
    batch: &mut WriteBatch,
    size: &mut u64,
) -> KvResult<()> {
    if revision != 1 {
        return Ok(());
    }
    // An earlier group member that wrote no revision may still have stored this row.
    let old_len = match batch.get(REVISION) {
        Some(value) => value.map(|v| v.len()),
        None => view
            .db
            .get(REVISION)
            .await
            .map_err(KvError::storage)?
            .map(|v| v.len()),
    };
    batch
        .put_counted(
            bytes::Bytes::from_static(REVISION),
            (revision + 1).to_be_bytes().to_vec().into(),
            old_len,
            size,
        )
        .map_err(KvError::storage)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn ordinary_put_stores_the_value_only_in_current_and_history() {
        let dir = tempfile::tempdir().unwrap();
        let state = crate::state::StateMachine::open(dir.path()).await.unwrap();
        let request = RequestOp {
            request: Some(Op::RequestPut(PutRequest {
                key: b"key".to_vec(),
                value: vec![b'v'; 4096],
                ..Default::default()
            })),
        };
        let view = CurrentView {
            db: state.db().as_ref(),
            rows: &state.current,
            index: &state.key_index,
        };
        let mut batch = WriteBatch::new();
        let (_, revision) = prepare_put(&view, state.revision, &request, &mut batch, &mut 0)
            .await
            .unwrap();
        let current = batch.get(&current_key(b"key")).unwrap().unwrap();
        let history = batch.get(&history_key(b"key", revision)).unwrap().unwrap();
        assert!(history.as_ptr_range().contains(&current.as_ptr()));
        assert_eq!(KeyValue::decode(current.clone()).unwrap().value.len(), 4096);
        // The hash record gives the CRC of the full KeyValue bytes.
        let hash = batch
            .get(&crate::state::maintenance::hash_key(revision, 0, false))
            .unwrap()
            .unwrap();
        assert!(hash.len() < 64);
        let seed = crc32c::crc32c(b"seed");
        assert_eq!(
            crate::state::maintenance::hash_append(seed, &hash).unwrap(),
            (crc32c::crc32c_append(seed, &current), current.len() as u64)
        );
        // The Watch journal names the history version instead of the value.
        let journal = batch.get(&crate::watch::key(revision)).unwrap().unwrap();
        assert!(journal.len() < 64);
        let journal = crate::proto::state::Journal::decode(journal).unwrap();
        assert!(journal.values_in_history);
        let kv = journal.events[0].kv.as_ref().unwrap();
        assert_eq!(
            (kv.key.as_slice(), kv.mod_revision),
            (&b"key"[..], revision)
        );
        assert!(kv.value.is_empty());
        state.close().await.unwrap();
    }

    /// The writer before `record_changes`: encode each row from the decoded values.
    fn record_separately(
        batch: &mut WriteBatch,
        next: i64,
        events: Vec<crate::proto::mvccpb::Event>,
        prev_revisions: Vec<i64>,
        changes: BTreeMap<Vec<u8>, Option<KeyValue>>,
    ) {
        crate::watch::record(next, events, prev_revisions, &changes, batch);
        for (key, value) in changes {
            if let Some(value) = &value {
                batch.put(current_key(&key), value.encode_to_vec());
            } else {
                batch.delete(current_key(&key));
            }
            record_version(batch, key, value, next);
        }
    }

    #[test]
    fn shared_encoding_matches_separate_encoding() {
        use crate::proto::mvccpb::Event;
        let mut seed = 0x9e37_79b9_7f4a_7c15_u64;
        let mut next_u64 = move || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        for round in 0..500 {
            let next = 1 + (next_u64() % 1000) as i64;
            let mut events = vec![];
            let mut prev_revisions = vec![];
            let mut changes = BTreeMap::new();
            let count = next_u64() % 10;
            for _ in 0..count {
                let key = vec![b'a' + (next_u64() % 6) as u8; 1 + (next_u64() % 3) as usize];
                let old = KeyValue {
                    key: key.clone(),
                    value: vec![7; (next_u64() % 40) as usize],
                    create_revision: 1,
                    mod_revision: (next_u64() % next as u64) as i64,
                    version: 3,
                    lease: (next_u64() % 2) as i64,
                };
                let inline = next_u64() % 2 == 0;
                let prev_kv = (next_u64() % 3 != 0).then(|| old.clone());
                let prev_revision = match &prev_kv {
                    Some(old) if !inline => old.mod_revision,
                    _ => 0,
                };
                match next_u64() % 4 {
                    0 => {
                        events.push(Event {
                            r#type: 1,
                            kv: Some(KeyValue {
                                key: key.clone(),
                                mod_revision: next,
                                ..Default::default()
                            }),
                            prev_kv: Some(old),
                        });
                        prev_revisions.push(0);
                        changes.insert(key, None);
                    }
                    choice => {
                        let kv = KeyValue {
                            key: key.clone(),
                            value: vec![choice as u8; (next_u64() % 300) as usize],
                            create_revision: next - 1,
                            mod_revision: next,
                            version: 1 + (next_u64() % 5) as i64,
                            lease: (next_u64() % 3) as i64,
                        };
                        events.push(Event {
                            r#type: 0,
                            kv: Some(kv.clone()),
                            prev_kv: if prev_revision > 0 { None } else { prev_kv },
                        });
                        prev_revisions.push(prev_revision);
                        // A later change of the same key replaces the stored value.
                        changes.insert(key, Some(kv));
                    }
                }
            }
            let mut shared = WriteBatch::new();
            record_changes(
                &mut shared,
                next,
                events.clone(),
                prev_revisions.clone(),
                changes.clone(),
            );
            let mut separate = WriteBatch::new();
            record_separately(&mut separate, next, events, prev_revisions, changes);
            let shared: Vec<_> = shared.into_rows().collect();
            let separate: Vec<_> = separate.into_rows().collect();
            assert_eq!(shared, separate, "round {round}");
        }
    }
}
