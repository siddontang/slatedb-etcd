use serde::{Deserialize, Serialize};
use slatedb::{Db, WriteBatch};

use crate::mvcc::{self, KvError};
use crate::proto::etcdserverpb::{ResponseHeader, WatchCreateRequest, WatchResponse};
use crate::proto::mvccpb::Event;

const EVENTS: &[u8] = b"etcd/events/";
const FLOOR: &[u8] = b"etcd/watch-floor";

fn key(revision: i64) -> Vec<u8> {
    [EVENTS, &revision.to_be_bytes()].concat()
}

pub async fn floor(db: &Db, revision: i64) -> Result<i64, KvError> {
    match db.get(FLOOR).await.map_err(KvError::storage)? {
        Some(value) => Ok(i64::from_be_bytes(
            value.as_ref().try_into().map_err(KvError::storage)?,
        )),
        // Older binaries did not record event order or previous values.
        None => Ok(if revision > 1 { revision + 1 } else { 1 }),
    }
}

pub async fn record(
    db: &Db,
    previous: i64,
    revision: i64,
    events: &[Event],
    batch: &mut WriteBatch,
) -> Result<(), KvError> {
    if db.get(FLOOR).await.map_err(KvError::storage)?.is_none() {
        batch.put(FLOOR, floor(db, previous).await?.to_be_bytes());
    }
    if !events.is_empty() {
        batch.put(
            key(revision),
            bincode::serialize(events).map_err(KvError::storage)?,
        );
    }
    Ok(())
}

pub async fn compact(db: &Db, revision: i64, batch: &mut WriteBatch) -> Result<(), KvError> {
    let mut scan = db
        .scan(EVENTS.to_vec()..key(revision))
        .await
        .map_err(KvError::storage)?;
    while let Some(row) = scan.next().await.map_err(KvError::storage)? {
        batch.delete(row.key);
    }
    Ok(())
}

#[derive(Debug, Serialize, Deserialize)]
pub struct WatchPage {
    pub header: Option<ResponseHeader>,
    pub compact_revision: i64,
    pub next_revision: i64,
    pub responses: Vec<WatchResponse>,
}

pub async fn read(
    db: &Db,
    current: i64,
    compact_revision: i64,
    request: &WatchCreateRequest,
) -> Result<WatchPage, KvError> {
    let mut page = WatchPage {
        header: mvcc::header(current),
        compact_revision: 0,
        next_revision: current + 1,
        responses: vec![],
    };
    if request.start_revision <= 0 {
        return Ok(page);
    }
    let floor = floor(db, current).await?.max(compact_revision);
    if request.start_revision < floor {
        page.compact_revision = floor;
        return Ok(page);
    }
    if request.start_revision > current {
        page.next_revision = request.start_revision;
        return Ok(page);
    }
    let mut scan = db
        .scan(key(request.start_revision)..key(current + 1))
        .await
        .map_err(KvError::storage)?;
    let mut revisions = 0;
    while let Some(row) = scan.next().await.map_err(KvError::storage)? {
        let revision = i64::from_be_bytes(
            row.key[EVENTS.len()..]
                .try_into()
                .map_err(KvError::storage)?,
        );
        let events: Vec<Event> = bincode::deserialize(&row.value).map_err(KvError::storage)?;
        let events: Vec<_> = events
            .into_iter()
            .filter_map(|mut event| {
                let kv = event.kv.as_ref()?;
                if !mvcc::contains(&kv.key, &request.key, &request.range_end)
                    || request.filters.contains(&event.r#type)
                {
                    return None;
                }
                if !request.prev_kv || revision - 1 < compact_revision {
                    event.prev_kv = None;
                }
                Some(event)
            })
            .collect();
        if !events.is_empty() {
            page.responses.push(WatchResponse {
                header: mvcc::header(revision),
                watch_id: request.watch_id,
                events,
                ..Default::default()
            });
        }
        revisions += 1;
        if revisions >= 32 {
            page.next_revision = revision + 1;
            break;
        }
    }
    Ok(page)
}
