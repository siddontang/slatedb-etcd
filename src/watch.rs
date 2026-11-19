use crate::WriteBatch;
use prost::Message;
use serde::{Deserialize, Serialize};
use slatedb::Db;

use crate::mvcc::{self, KvError};
use crate::proto::etcdserverpb::{ResponseHeader, WatchCreateRequest, WatchResponse};
use crate::proto::mvccpb::Event;

const EVENTS: &[u8] = b"etcd/events/";

pub(crate) fn key(revision: i64) -> Vec<u8> {
    [EVENTS, &revision.to_be_bytes()].concat()
}

pub fn record(revision: i64, events: Vec<Event>, batch: &mut WriteBatch) {
    crate::state::maintenance::record_hash(revision, &events, batch);
    record_journal(revision, events, batch);
}

pub(crate) fn record_journal(revision: i64, events: Vec<Event>, batch: &mut WriteBatch) {
    if !events.is_empty() {
        batch.put_new(
            key(revision),
            crate::proto::state::Journal { events }.encode_to_vec(),
        );
    }
}

pub async fn compact(db: &Db, revision: i64, batch: &mut WriteBatch) -> Result<(), KvError> {
    crate::state::maintenance::compact_hash(db, revision, batch).await?;
    let mut scan = db
        .scan_with_options(EVENTS.to_vec()..key(revision), &mvcc::maintenance_scan())
        .await
        .map_err(KvError::storage)?;
    while let Some(row) = scan.next().await.map_err(KvError::storage)? {
        batch.delete_known(row.key, row.value.len());
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
    if request.start_revision < compact_revision {
        page.compact_revision = compact_revision;
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
        let events = crate::proto::state::Journal::decode(row.value)
            .map_err(KvError::storage)?
            .events;
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
