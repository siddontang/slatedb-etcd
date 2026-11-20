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
}

fn encode_version(kv: &KeyValue) -> (bytes::Bytes, bytes::Bytes) {
    use prost::encoding::{bytes as field, int64, message};

    let len = field::encoded_len(1, &kv.key)
        + int64::encoded_len(2, &kv.mod_revision)
        + message::encoded_len(3, kv);
    let mut version = Vec::with_capacity(len);
    field::encode(1, &kv.key, &mut version);
    int64::encode(2, &kv.mod_revision, &mut version);
    message::encode(3, kv, &mut version);
    let version = bytes::Bytes::from(version);
    let current = version.slice(version.len() - kv.encoded_len()..);
    (version, current)
}

/// The applied current state that an ordinary Put reads on the apply worker.
pub(crate) struct CurrentView<'a> {
    pub db: &'a Db,
    pub rows: &'a crate::state::current::CurrentRows,
    pub index: &'a crate::state::key_index::KeyIndex,
}

impl CurrentView<'_> {
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
    view: &CurrentView<'_>,
    revision: i64,
    request: &RequestOp,
    batch: &mut WriteBatch,
    size: &mut u64,
) -> KvResult<(ResponseOp, i64)> {
    let Some(Op::RequestPut(p)) = &request.request else {
        return Err(KvError::invalid("expected an ordinary put"));
    };
    key_check(&p.key)?;
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
    let next = revision + 1;
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
    let (journal_prev, prev_revisions) = match meta {
        None => (None, vec![]),
        Some(m) if has_history(m.mod_revision, next) => (None, vec![m.mod_revision]),
        // Ordinary puts in a batch have distinct revisions, so this is not expected.
        Some(_) => match raw {
            Some(raw) => (Some(raw), vec![]),
            None => return Err(KvError::storage("indexed row has no older revision")),
        },
    };
    let journal = PutJournal {
        event: PutEvent {
            kv: current.clone(),
            prev_kv: journal_prev,
        },
        prev_revisions,
    }
    .encode_to_vec();
    let rows = [
        (key.into(), current.clone(), old_len),
        (
            crate::state::maintenance::hash_key(next, 0, false).into(),
            current,
            None,
        ),
        (history_key(&p.key, next).into(), history, None),
        (crate::watch::key(next).into(), journal.into(), None),
    ];
    for (key, value, old_len) in rows {
        batch
            .put_counted(key, value, old_len, size)
            .map_err(KvError::storage)?;
    }
    // The revision row has a fixed width after its first write.
    if revision == 1 {
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
                next.to_be_bytes().to_vec().into(),
                old_len,
                size,
            )
            .map_err(KvError::storage)?;
    }
    Ok((
        response(Resp::ResponsePut(PutResponse {
            header: header(next),
            prev_kv,
        })),
        next,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn ordinary_put_shares_the_current_and_hash_value() {
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
            db: &state.db,
            rows: &state.current,
            index: &state.key_index,
        };
        let mut batch = WriteBatch::new();
        let (_, revision) = prepare_put(&view, state.revision, &request, &mut batch, &mut 0)
            .await
            .unwrap();
        let current = batch.get(&current_key(b"key")).unwrap().unwrap();
        let hash = batch
            .get(&crate::state::maintenance::hash_key(revision, 0, false))
            .unwrap()
            .unwrap();
        assert_eq!(current.as_ptr(), hash.as_ptr());
        assert_eq!(KeyValue::decode(hash).unwrap().value.len(), 4096);
        state.close().await.unwrap();
    }
}
