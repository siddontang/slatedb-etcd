use crate::WriteBatch;
use prost::Message;
use slatedb::Db;

use crate::mvcc::{self, KvError};
#[cfg(test)]
use crate::proto::etcdserverpb::WatchResponse;
use crate::proto::etcdserverpb::{ResponseHeader, WatchCreateRequest};
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

/// Capacity of the in-memory event feed, in apply batches.
pub(crate) const FEED_BATCHES: usize = 1024;
/// Journal bytes above this limit make a batch incomplete. Watchers then read storage.
pub(crate) const FEED_BATCH_BYTES: usize = 128 * 1024;
/// Maximum journal revisions in one storage catch-up page.
pub(crate) const PAGE_REVISIONS: usize = 32;

type Journals = Vec<(i64, Vec<Event>)>;

/// Collect journal rows that one apply job writes.
#[derive(Debug)]
pub(crate) struct Capture {
    rows: Vec<(i64, bytes::Bytes)>,
    bytes: usize,
    complete: bool,
}

impl Default for Capture {
    fn default() -> Self {
        Self {
            rows: vec![],
            bytes: 0,
            complete: true,
        }
    }
}

impl Capture {
    pub(crate) fn record(&mut self, key: &[u8], value: Option<&bytes::Bytes>) {
        let (Some(value), Some(revision)) = (value, key.strip_prefix(EVENTS)) else {
            return;
        };
        let Ok(revision) = <[u8; 8]>::try_from(revision).map(i64::from_be_bytes) else {
            return self.invalidate();
        };
        if !self.complete {
            return;
        }
        self.bytes += value.len();
        if self.bytes > FEED_BATCH_BYTES {
            return self.invalidate();
        }
        self.rows.push((revision, value.clone()));
    }

    /// Make the batch incomplete, for example after snapshot installation.
    pub(crate) fn invalidate(&mut self) {
        self.rows = vec![];
        self.complete = false;
    }

    pub(crate) fn take(&mut self, previous: i64, revision: i64) -> Option<Batch> {
        let capture = std::mem::take(self);
        if capture.complete && capture.rows.is_empty() && previous == revision {
            return None;
        }
        Some(Batch {
            previous,
            revision,
            rows: capture.complete.then_some(capture.rows),
            decoded: std::sync::OnceLock::new(),
        })
    }
}

/// Watch events from one apply job. They cover revisions in `(previous, revision]`.
#[derive(Debug)]
pub(crate) struct Batch {
    pub previous: i64,
    pub revision: i64,
    rows: Option<Vec<(i64, bytes::Bytes)>>,
    decoded: std::sync::OnceLock<Option<Journals>>,
}

impl Batch {
    /// Decode once on the first reader. `None` means that watchers must read storage.
    pub(crate) fn journals(&self) -> Option<&[(i64, Vec<Event>)]> {
        self.decoded
            .get_or_init(|| {
                self.rows
                    .as_ref()?
                    .iter()
                    .map(|(revision, value)| {
                        let journal = crate::proto::state::Journal::decode(value.clone()).ok()?;
                        Some((*revision, journal.events))
                    })
                    .collect()
            })
            .as_deref()
    }
}

/// Applied state that watch streams share. The node publishes it before each event batch.
#[derive(Clone)]
pub(crate) struct FeedState {
    pub view: std::sync::Arc<crate::state::ReadView>,
    /// Cluster, member, and term fields for response headers.
    pub header: ResponseHeader,
    pub leader: u64,
}

impl FeedState {
    pub(crate) fn header(&self, revision: i64) -> Option<ResponseHeader> {
        Some(ResponseHeader {
            revision,
            ..self.header
        })
    }
}

/// One stream's view of the event feed.
pub struct Subscription {
    pub(crate) events: tokio::sync::broadcast::Receiver<std::sync::Arc<Batch>>,
    pub(crate) state: tokio::sync::watch::Receiver<FeedState>,
}

/// Select the events of one revision that a watch request accepts.
pub(crate) fn select(
    request: &WatchCreateRequest,
    revision: i64,
    compact_revision: i64,
    events: &[Event],
) -> Vec<Event> {
    let keep_prev = request.prev_kv && revision > compact_revision;
    events
        .iter()
        .filter(|event| {
            event.kv.as_ref().is_some_and(|kv| {
                mvcc::contains(&kv.key, &request.key, &request.range_end)
                    && !request.filters.contains(&event.r#type)
            })
        })
        .map(|event| Event {
            r#type: event.r#type,
            kv: event.kv.clone(),
            prev_kv: if keep_prev {
                event.prev_kv.clone()
            } else {
                None
            },
        })
        .collect()
}

/// Read at most `limit` journal revisions in `[from, through]` from a pinned view.
/// Returns the rows and the last revision that the page covers.
pub(crate) async fn scan(
    db: &slatedb::DbSnapshot,
    from: i64,
    through: i64,
    limit: usize,
) -> Result<(Journals, i64), KvError> {
    let mut rows = vec![];
    if from > through {
        return Ok((rows, through));
    }
    let mut scan = db
        .scan(key(from)..key(through + 1))
        .await
        .map_err(KvError::storage)?;
    while let Some(row) = scan.next().await.map_err(KvError::storage)? {
        let revision = i64::from_be_bytes(
            row.key[EVENTS.len()..]
                .try_into()
                .map_err(KvError::storage)?,
        );
        let events = crate::proto::state::Journal::decode(row.value)
            .map_err(KvError::storage)?
            .events;
        rows.push((revision, events));
        if rows.len() >= limit {
            return Ok((rows, revision));
        }
    }
    Ok((rows, through))
}

#[cfg(test)]
#[derive(Debug)]
pub struct WatchPage {
    pub header: Option<ResponseHeader>,
    pub compact_revision: i64,
    pub next_revision: i64,
    pub responses: Vec<WatchResponse>,
}

#[cfg(test)]
pub(crate) async fn read(
    db: &slatedb::DbSnapshot,
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
    let (rows, through) = scan(db, request.start_revision, current, PAGE_REVISIONS).await?;
    page.next_revision = through + 1;
    for (revision, events) in rows {
        let events = select(request, revision, compact_revision, &events);
        if !events.is_empty() {
            page.responses.push(WatchResponse {
                header: mvcc::header(revision),
                watch_id: request.watch_id,
                events,
                ..Default::default()
            });
        }
    }
    Ok(page)
}
