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

pub(crate) async fn prepare_put(
    db: &Db,
    revision: i64,
    request: &RequestOp,
    previous: &WriteBatch,
    current: &crate::state::current::CurrentRows,
) -> KvResult<(ResponseOp, WriteBatch, i64)> {
    let Some(Op::RequestPut(p)) = &request.request else {
        return Err(KvError::invalid("expected an ordinary put"));
    };
    key_check(&p.key)?;
    let key = current_key(&p.key);
    let raw = match previous.get(&key) {
        Some(value) => value,
        None => match current.get(&key) {
            Some(value) => Some(value),
            None => db.get(&key).await.map_err(KvError::storage)?,
        },
    };
    let old_len = raw.as_ref().map(|v| v.len());
    let old = raw
        .map(KeyValue::decode)
        .transpose()
        .map_err(KvError::storage)?;
    let next = revision + 1;
    let kv = value(p, old.as_ref(), next);
    let mut batch = WriteBatch::new();
    let encoded = bytes::Bytes::from(kv.encode_to_vec());
    batch.put_known(key, encoded.clone(), old_len);
    batch.put_new(crate::state::maintenance::hash_key(next, 0, false), encoded);
    let prev_kv = if p.prev_kv { old.clone() } else { None };
    crate::watch::record_journal(
        next,
        vec![crate::proto::mvccpb::Event {
            r#type: 0,
            kv: Some(kv.clone()),
            prev_kv: old,
        }],
        &mut batch,
    );
    record_version(&mut batch, p.key.clone(), Some(kv), next);
    if revision > 1 {
        batch.put_known(REVISION, next.to_be_bytes().to_vec(), Some(8));
    } else {
        batch.put(REVISION, next.to_be_bytes());
    }
    Ok((
        response(Resp::ResponsePut(PutResponse {
            header: header(next),
            prev_kv,
        })),
        batch,
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
        let (_, batch, revision) = prepare_put(
            &state.db,
            state.revision,
            &request,
            &WriteBatch::new(),
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
