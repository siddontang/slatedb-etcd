//! Build ordinary Put mutations from one current-value lookup.

use super::*;

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
    let mut history_key = history_prefix(&key);
    history_key.extend_from_slice(&next.to_be_bytes());
    let version = Version {
        key,
        revision: next,
        value,
    };
    batch.put_new(history_key, version.encode_to_vec());
}

// Ordinary puts need only these two fields from the previous KeyValue.
#[derive(prost::Message)]
struct Prior {
    #[prost(int64, tag = "2")]
    create_revision: i64,
    #[prost(int64, tag = "4")]
    version: i64,
}

// Embedded messages and bytes have the same wire type. Keep both KeyValue payloads intact.
#[derive(prost::Message)]
struct PutEvent {
    #[prost(bytes = "bytes", required, tag = "2")]
    kv: bytes::Bytes,
    #[prost(bytes = "bytes", optional, tag = "3")]
    prev_kv: Option<bytes::Bytes>,
}

#[derive(prost::Message)]
struct PutJournal {
    #[prost(message, required, tag = "1")]
    event: PutEvent,
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

pub(crate) async fn prepare_put(
    db: &Db,
    revision: i64,
    request: &RequestOp,
    batch: &mut WriteBatch,
    size: &mut u64,
    current: &crate::state::current::CurrentRows,
) -> KvResult<(ResponseOp, i64)> {
    let Some(Op::RequestPut(p)) = &request.request else {
        return Err(KvError::invalid("expected an ordinary put"));
    };
    key_check(&p.key)?;
    let key = current_key(&p.key);
    let raw = match batch.get(&key) {
        Some(value) => value,
        None => match current.get(&key) {
            Some(value) => Some(value),
            None => db.get(&key).await.map_err(KvError::storage)?,
        },
    };
    let old_len = raw.as_ref().map(|v| v.len());
    let next = revision + 1;
    let (create_revision, version, prev_kv) = match &raw {
        None => (next, 1, None),
        Some(bytes) if p.prev_kv => {
            let old = KeyValue::decode(bytes.clone()).map_err(KvError::storage)?;
            (old.create_revision, old.version + 1, Some(old))
        }
        Some(bytes) => {
            let old = Prior::decode(bytes.clone()).map_err(KvError::storage)?;
            (old.create_revision, old.version + 1, None)
        }
    };
    let kv = KeyValue {
        key: p.key.clone(),
        value: p.value.clone(),
        create_revision,
        mod_revision: next,
        version,
        lease: 0,
    };
    let (history, current) = encode_version(&kv);
    let journal = PutJournal {
        event: PutEvent {
            kv: current.clone(),
            prev_kv: raw,
        },
    }
    .encode_to_vec();
    let mut history_key = history_prefix(&p.key);
    history_key.extend_from_slice(&next.to_be_bytes());
    let rows = [
        (key.into(), current.clone(), old_len),
        (
            crate::state::maintenance::hash_key(next, 0, false).into(),
            current,
            None,
        ),
        (history_key.into(), history, None),
        (crate::watch::key(next).into(), journal.into(), None),
    ];
    for (key, value, old_len) in rows {
        batch
            .put_counted(key, value, old_len, size)
            .map_err(KvError::storage)?;
    }
    // The revision row has a fixed width after its first write.
    if revision == 1 {
        let old_len = db
            .get(REVISION)
            .await
            .map_err(KvError::storage)?
            .map(|v| v.len());
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
        let mut batch = WriteBatch::new();
        let (_, revision) = prepare_put(
            &state.db,
            state.revision,
            &request,
            &mut batch,
            &mut 0,
            &state.current,
        )
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
