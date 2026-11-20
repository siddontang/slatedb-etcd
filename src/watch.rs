use crate::WriteBatch;
use prost::Message;

use crate::mvcc::{self, KvError};
#[cfg(test)]
use crate::proto::etcdserverpb::WatchResponse;
use crate::proto::etcdserverpb::{ResponseHeader, WatchCreateRequest};
use crate::proto::mvccpb::{Event, KeyValue};
use crate::proto::state::Journal;

pub(crate) const EVENTS: &[u8] = b"etcd/events/";

pub(crate) fn key(revision: i64) -> Vec<u8> {
    [EVENTS, &revision.to_be_bytes()].concat()
}

/// Record HashKV and Watch rows. `prev_revisions` follows `record_journal`.
pub fn record(revision: i64, events: Vec<Event>, prev_revisions: Vec<i64>, batch: &mut WriteBatch) {
    crate::state::maintenance::record_hash(revision, &events, batch);
    record_journal(revision, events, prev_revisions, batch);
}

/// Store the events of one revision. A positive `prev_revisions` entry replaces the inline
/// previous value with the history revision of that value.
pub(crate) fn record_journal(
    revision: i64,
    events: Vec<Event>,
    mut prev_revisions: Vec<i64>,
    batch: &mut WriteBatch,
) {
    if !events.is_empty() {
        if prev_revisions.iter().all(|&revision| revision == 0) {
            prev_revisions.clear();
        }
        batch.put_new(
            key(revision),
            Journal {
                events,
                prev_revisions,
            }
            .encode_to_vec(),
        );
    }
}

/// Load a previous value from the history version that the journal names.
///
/// etcd fetches prev_kv with a Range at `mod_revision - 1` and omits it when the Range finds
/// nothing. A missing or deleted version therefore yields `None`. Storage errors still fail.
async fn previous_value(
    db: &slatedb::DbSnapshot,
    key: &[u8],
    revision: i64,
) -> Result<Option<KeyValue>, KvError> {
    let Some(row) = db
        .get(mvcc::history_key(key, revision))
        .await
        .map_err(KvError::storage)?
    else {
        tracing::warn!(
            revision,
            "Watch previous value is missing; omitting prev_kv"
        );
        return Ok(None);
    };
    let value = crate::proto::state::Version::decode(row)
        .map_err(KvError::storage)?
        .value;
    if value.is_none() {
        tracing::warn!(
            revision,
            "Watch previous value is a deletion; omitting prev_kv"
        );
    }
    Ok(value)
}

/// Capacity of the in-memory event feed, in apply batches.
pub(crate) const FEED_BATCHES: usize = 1024;
/// Journal bytes above this limit make a batch incomplete. Watchers then read storage.
pub(crate) const FEED_BATCH_BYTES: usize = 128 * 1024;
/// Maximum journal revisions in one storage catch-up page.
pub(crate) const PAGE_REVISIONS: usize = 32;

type Journals = Vec<(i64, Journal)>;

/// Decode one journal row and check that `prev_revisions` matches the events.
fn decode(value: bytes::Bytes) -> Result<Journal, KvError> {
    let journal = Journal::decode(value).map_err(KvError::storage)?;
    if !journal.prev_revisions.is_empty() && journal.prev_revisions.len() != journal.events.len() {
        return Err(KvError::storage("invalid Watch journal"));
    }
    Ok(journal)
}

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
    pub(crate) fn journals(&self) -> Option<&[(i64, Journal)]> {
        self.decoded
            .get_or_init(|| {
                self.rows
                    .as_ref()?
                    .iter()
                    .map(|(revision, value)| Some((*revision, decode(value.clone()).ok()?)))
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
///
/// A journal can name the history revision of a previous value instead of storing it inline.
/// `db` must be a view that includes `revision`.
pub(crate) async fn select(
    db: &slatedb::DbSnapshot,
    request: &WatchCreateRequest,
    revision: i64,
    compact_revision: i64,
    journal: &Journal,
) -> Result<Vec<Event>, KvError> {
    let keep_prev = request.prev_kv && revision > compact_revision;
    let mut events = vec![];
    for (position, event) in journal.events.iter().enumerate() {
        let Some(kv) = event.kv.as_ref() else {
            continue;
        };
        if !mvcc::contains(&kv.key, &request.key, &request.range_end)
            || request.filters.contains(&event.r#type)
        {
            continue;
        }
        let prev_kv = if !keep_prev {
            None
        } else if let Some(&prev) = journal.prev_revisions.get(position).filter(|&&r| r > 0) {
            previous_value(db, &kv.key, prev).await?
        } else {
            event.prev_kv.clone()
        };
        events.push(Event {
            r#type: event.r#type,
            kv: event.kv.clone(),
            prev_kv,
        });
    }
    Ok(events)
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
        rows.push((revision, decode(row.value)?));
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
    for (revision, journal) in rows {
        let events = select(db, request, revision, compact_revision, &journal).await?;
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
