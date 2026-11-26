use crate::WriteBatch;
use prost::Message;

use crate::mvcc::{self, KvError};
#[cfg(test)]
use crate::proto::etcdserverpb::WatchResponse;
use crate::proto::etcdserverpb::{ResponseHeader, WatchCreateRequest};
use crate::proto::mvccpb::{Event, KeyValue};
use crate::proto::state::Journal;
use crate::state::{StateRead, StateScan};

pub(crate) const EVENTS: &[u8] = b"etcd/events/";

pub(crate) fn key(revision: i64) -> Vec<u8> {
    [EVENTS, &revision.to_be_bytes()].concat()
}

/// Record HashKV and Watch rows. `prev_revisions` follows `record_journal`.
///
/// `history` holds the history version that this revision writes for each changed key. When every
/// Put event equals that version, the journal omits the values and names the versions instead.
pub fn record(
    revision: i64,
    events: Vec<Event>,
    prev_revisions: Vec<i64>,
    history: &std::collections::BTreeMap<Vec<u8>, Option<KeyValue>>,
    batch: &mut WriteBatch,
) {
    crate::state::maintenance::record_hash(revision, &events, batch);
    let puts = || events.iter().filter(|event| is_put(event));
    let values_in_history = puts().next().is_some()
        && puts().all(|event| {
            event.kv.as_ref().is_some_and(|kv| {
                kv.mod_revision == revision
                    && matches!(history.get(&kv.key), Some(Some(version)) if version == kv)
            })
        });
    record_journal(revision, events, prev_revisions, values_in_history, batch);
}

/// Store the events of one revision. A positive `prev_revisions` entry replaces the inline
/// previous value with the history revision of that value.
///
/// With `values_in_history`, Put events omit `kv.value`. The caller must write the history
/// version of each Put event key at `revision` with the same `KeyValue`.
pub(crate) fn record_journal(
    revision: i64,
    mut events: Vec<Event>,
    mut prev_revisions: Vec<i64>,
    values_in_history: bool,
    batch: &mut WriteBatch,
) {
    if !events.is_empty() {
        if prev_revisions.iter().all(|&revision| revision == 0) {
            prev_revisions.clear();
        }
        if values_in_history {
            for event in events.iter_mut().filter(|event| is_put(event)) {
                if let Some(kv) = event.kv.as_mut() {
                    kv.value = vec![];
                }
            }
        }
        batch.put_new(
            key(revision),
            Journal {
                events,
                prev_revisions,
                values_in_history,
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
    db: &impl StateRead,
    key: &[u8],
    revision: i64,
) -> Result<Option<KeyValue>, KvError> {
    let Some(row) = db
        .get(&mvcc::history_key(key, revision))
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

/// Return the full `KeyValue` of a Put event whose journal omits the value.
///
/// `row` is the history version of `kv.key` at `kv.mod_revision`. It must be a live version of the
/// same key and revision.
fn stored_value(kv: &KeyValue, row: Option<bytes::Bytes>) -> Result<KeyValue, KvError> {
    let row = row.ok_or_else(|| KvError::corrupt("Watch value has no history version"))?;
    let version = crate::proto::state::Version::decode(row).map_err(KvError::storage)?;
    match version.value {
        Some(value) if value.key == kv.key && value.mod_revision == kv.mod_revision => Ok(value),
        _ => Err(KvError::corrupt(
            "Watch value does not match its history version",
        )),
    }
}

/// Return true when the event of `journal` stores its value in a history version.
fn value_in_history(journal: &Journal, event: &Event) -> bool {
    journal.values_in_history && is_put(event)
}

fn is_put(event: &Event) -> bool {
    event.r#type == crate::proto::mvccpb::event::EventType::Put as i32
}

/// Decode one journal row and check that `prev_revisions` matches the events.
fn decode(value: bytes::Bytes) -> Result<Journal, KvError> {
    let journal = Journal::decode(value).map_err(KvError::storage)?;
    if !journal.prev_revisions.is_empty() && journal.prev_revisions.len() != journal.events.len() {
        return Err(KvError::corrupt("invalid Watch journal"));
    }
    Ok(journal)
}

/// Collect journal rows that one apply job writes.
#[derive(Debug)]
pub(crate) struct Capture {
    rows: Vec<(i64, bytes::Bytes)>,
    /// History versions of the job. They hold the values that journal rows omit.
    history: Vec<(bytes::Bytes, bytes::Bytes)>,
    bytes: usize,
    complete: bool,
}

impl Default for Capture {
    fn default() -> Self {
        Self {
            rows: vec![],
            history: vec![],
            bytes: 0,
            complete: true,
        }
    }
}

impl Capture {
    pub(crate) fn record(&mut self, key: &bytes::Bytes, value: Option<&bytes::Bytes>) {
        if let (true, Some(value)) = (key.starts_with(mvcc::HISTORY), value) {
            if self.complete {
                self.bytes += value.len();
                if self.bytes > FEED_BATCH_BYTES {
                    return self.invalidate();
                }
                self.history.push((key.clone(), value.clone()));
            }
            return;
        }
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
        self.history = vec![];
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
            history: capture.history,
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
    history: Vec<(bytes::Bytes, bytes::Bytes)>,
    decoded: std::sync::OnceLock<Option<Journals>>,
}

impl Batch {
    /// Decode once on the first reader. `None` means that watchers must read storage.
    /// Journals that omit values get them from the history versions of the same job.
    pub(crate) fn journals(&self) -> Option<&[(i64, Journal)]> {
        self.decoded
            .get_or_init(|| {
                let history: std::collections::HashMap<&[u8], &bytes::Bytes> = self
                    .history
                    .iter()
                    .map(|(key, value)| (key.as_ref(), value))
                    .collect();
                self.rows
                    .as_ref()?
                    .iter()
                    .map(|(revision, value)| {
                        let mut journal = decode(value.clone()).ok()?;
                        if journal.values_in_history {
                            for event in &mut journal.events {
                                if !is_put(event) {
                                    continue;
                                }
                                let kv = event.kv.as_ref()?;
                                let row = history
                                    .get(mvcc::history_key(&kv.key, kv.mod_revision).as_slice())
                                    .map(|&row| row.clone());
                                event.kv = Some(stored_value(kv, row).ok()?);
                            }
                            journal.values_in_history = false;
                        }
                        Some((*revision, journal))
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
///
/// A journal can name the history revision of a previous value instead of storing it inline.
/// `db` must be a view that includes `revision`.
pub(crate) async fn select(
    db: &impl StateRead,
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
        let kv = if value_in_history(journal, event) {
            let row = db
                .get(&mvcc::history_key(&kv.key, kv.mod_revision))
                .await
                .map_err(KvError::storage)?;
            stored_value(kv, row)?
        } else {
            kv.clone()
        };
        let prev_kv = if !keep_prev {
            None
        } else if let Some(&prev) = journal.prev_revisions.get(position).filter(|&&r| r > 0) {
            previous_value(db, &kv.key, prev).await?
        } else {
            event.prev_kv.clone()
        };
        events.push(Event {
            r#type: event.r#type,
            kv: Some(kv),
            prev_kv,
        });
    }
    Ok(events)
}

/// Read at most `limit` journal revisions in `[from, through]` from a pinned view.
/// Returns the rows and the last revision that the page covers.
pub(crate) async fn scan(
    db: &impl StateRead,
    from: i64,
    through: i64,
    limit: usize,
) -> Result<(Journals, i64), KvError> {
    let mut rows = vec![];
    if from > through {
        return Ok((rows, through));
    }
    let mut scan = db
        .scan_range(key(from)..key(through + 1), &Default::default())
        .await
        .map_err(KvError::storage)?;
    loop {
        let row = match scan.next().await {
            Ok(Some(row)) => row,
            Ok(None) => break,
            // A page that held its view too long, or that the store failed, keeps the revisions
            // that it read. So a retry on a new view makes progress.
            Err(error)
                if !rows.is_empty()
                    && crate::mvcc::storage_fault::of_slatedb(&error)
                        != crate::mvcc::storage_fault::StorageFault::Corrupt =>
            {
                let covered = rows.last().map(|(revision, _)| *revision).unwrap();
                return Ok((rows, covered));
            }
            Err(error) => return Err(KvError::storage(error)),
        };
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
    db: &impl StateRead,
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
