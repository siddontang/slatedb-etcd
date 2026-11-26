use super::*;
use crate::watch::{Batch, FeedState, PAGE_REVISIONS};
use std::collections::{BTreeSet, HashMap, VecDeque};
use std::sync::Arc;
use tokio::sync::{broadcast, watch};
use tokio_stream::wrappers::ReceiverStream;
use watch_request::RequestUnion;
use watch_server::Watch;

/// Longest interval between token expiry checks. Events do not use it.
const CHECK_INTERVAL: Duration = Duration::from_secs(1);
const MAX_WATCHERS: usize = 1024;
/// Creates that wait behind a progress reply. One more ends the stream with a retryable error,
/// and the client resumes each watch on a new stream.
const MAX_WAITING_CREATES: usize = 64;

struct Watcher {
    _active: Option<crate::metrics::Active>,
    /// The create request. `Stream` keeps its start revision in `start`.
    request: WatchCreateRequest,
    /// The start revision that the client asked for. Zero or less means the next revision.
    requested: i64,
    /// The next revision that this watcher can receive.
    /// `None` means `position + 1` of the stream: the watcher follows the feed.
    start: Option<i64>,
    progress: tokio::time::Instant,
}

type Output = mpsc::Sender<Result<WatchResponse, Status>>;

/// A create request with its decision. An empty `reason` creates the watcher.
struct Create {
    request: WatchCreateRequest,
    reason: &'static str,
}

/// A request that waits behind a progress reply. A Cancel does not wait. See `Stream::cancel`.
enum Waiting {
    Create(Create),
    Progress,
}

/// The state of one watch stream.
///
/// The stream has received every feed batch through `position`.
/// A watcher with a start revision above `position` is synced and takes events from the feed.
/// Other watchers read the journal from a pinned view until they pass that view.
/// Each journal revision reaches a watcher once because delivery advances its start revision.
///
/// Most synced watchers follow the feed: their start revision is `position + 1`. They do not
/// store it, so a feed batch does not visit them one by one. Only watchers in `pinned` store a
/// start revision.
struct Stream {
    tx: Output,
    require_leader: bool,
    credentials: crate::auth::Credentials,
    meters: Option<Arc<crate::metrics::WatchMeters>>,
    state: watch::Receiver<FeedState>,
    progress_interval: Duration,
    checked_auth: Arc<crate::auth::Store>,
    position: i64,
    watchers: BTreeMap<i64, Watcher>,
    /// Single-key watchers by key. A batch visits only the watchers that its events can match.
    keys: HashMap<Vec<u8>, BTreeSet<i64>>,
    /// Watchers of a key range.
    ranges: BTreeSet<i64>,
    /// Watchers that store their start revision.
    pinned: BTreeSet<i64>,
    next_id: i64,
    /// Storage reads that stopped a page in a row. See `Stream::storage_stopped`.
    storage_stops: u32,
    /// The next storage catch-up waits until this time.
    retry_at: tokio::time::Instant,
    /// The goal revision of the progress request that the stream serves. It stays fixed until
    /// the reply.
    progress: Option<i64>,
    /// Creates and progress requests that arrived while a progress reply waited, in request
    /// order. See `Stream::hold_create`.
    waiting: VecDeque<Waiting>,
    /// Runs once after the next journal scan, before the responses take a new view.
    #[cfg(test)]
    after_scan: Option<Box<dyn FnOnce() + Send>>,
}

/// The first wait after storage reads stopped two pages in a row. It doubles to `RETRY_CAP`.
const RETRY_FIRST: Duration = Duration::from_millis(100);
const RETRY_CAP: Duration = Duration::from_secs(5);

/// Return true when a Watch read can succeed on a new view: the store failed, an object is
/// gone, or the pin expired. Corrupt stored data ends the stream with `DataLoss`.
fn retryable(status: &Status) -> bool {
    status.code() == tonic::Code::Unavailable
}

fn unavailable() -> Status {
    Status::unavailable("etcdserver: node unavailable")
}

fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

// A stream error ends the stream, so its size does not matter.
#[allow(clippy::result_large_err)]
impl Stream {
    fn new(
        tx: Output,
        require_leader: bool,
        credentials: crate::auth::Credentials,
        meters: Option<Arc<crate::metrics::WatchMeters>>,
        state: watch::Receiver<FeedState>,
        progress_interval: Duration,
    ) -> Self {
        let (position, checked_auth) = {
            let current = state.borrow();
            (current.view.revision, current.view.auth.clone())
        };
        Self {
            tx,
            require_leader,
            credentials,
            meters,
            state,
            progress_interval,
            checked_auth,
            position,
            watchers: BTreeMap::new(),
            keys: HashMap::new(),
            ranges: BTreeSet::new(),
            pinned: BTreeSet::new(),
            next_id: 0,
            storage_stops: 0,
            retry_at: tokio::time::Instant::now(),
            progress: None,
            waiting: VecDeque::new(),
            #[cfg(test)]
            after_scan: None,
        }
    }

    /// A storage read stopped a page, and the watchers resume on a new view. The first retry
    /// runs at once, as for an expired pin. Each later stop in a row doubles the wait from
    /// `RETRY_FIRST` to `RETRY_CAP`, so a failing store gets no busy loop.
    fn storage_stopped(&mut self) {
        let wait = match self.storage_stops {
            0 => Duration::ZERO,
            stops => RETRY_FIRST
                .saturating_mul(1 << (stops - 1).min(16))
                .min(RETRY_CAP),
        };
        self.storage_stops += 1;
        self.retry_at = tokio::time::Instant::now() + wait;
    }

    /// Return the newest state. A leader-required stream fails without a leader.
    /// Background checks pass `idle` and fail only while the stream has watchers.
    fn current(&self, idle: bool) -> Result<FeedState, Status> {
        let state = self.state.borrow().clone();
        if self.require_leader && state.leader == 0 && !(idle && self.watchers.is_empty()) {
            return Err(Status::unavailable("etcdserver: no leader"));
        }
        Ok(state)
    }

    fn credentials(&self) -> crate::auth::Credentials {
        crate::auth::Credentials {
            time: now(),
            ..self.credentials.clone()
        }
    }

    /// Return the next revision that `watcher` can receive.
    fn start(&self, watcher: &Watcher) -> i64 {
        watcher.start.unwrap_or(self.position + 1)
    }

    fn unsynced(&self) -> bool {
        // A following watcher starts after `position`, so only pinned watchers can be unsynced.
        let bound = self.position.min(self.state.borrow().view.revision);
        self.pinned
            .iter()
            .any(|id| self.watchers[id].start.is_some_and(|start| start <= bound))
    }

    /// Store `position + 1` in every following watcher. Call it before `position` moves past
    /// revisions that the following watchers did not receive.
    fn pin_all(&mut self) {
        let start = self.position + 1;
        for (&id, watcher) in &mut self.watchers {
            if watcher.start.is_none() {
                watcher.start = Some(start);
                self.pinned.insert(id);
            }
        }
    }

    /// Let pinned watchers with start revision `position + 1` follow the feed.
    fn unpin(&mut self) {
        let next = self.position + 1;
        let watchers = &mut self.watchers;
        self.pinned.retain(|id| {
            let watcher = watchers.get_mut(id).expect("pinned watcher");
            if watcher.start == Some(next) {
                watcher.start = None;
                false
            } else {
                true
            }
        });
    }

    /// Add a watcher that starts at `start`.
    fn insert(&mut self, mut watcher: Watcher, start: i64) {
        let id = watcher.request.watch_id;
        if start == self.position + 1 {
            watcher.start = None;
        } else {
            watcher.start = Some(start);
            self.pinned.insert(id);
        }
        if watcher.request.range_end.is_empty() {
            self.keys
                .entry(watcher.request.key.clone())
                .or_default()
                .insert(id);
        } else {
            self.ranges.insert(id);
        }
        self.watchers.insert(id, watcher);
    }

    fn remove(&mut self, id: i64) -> Option<Watcher> {
        let watcher = self.watchers.remove(&id)?;
        self.pinned.remove(&id);
        if watcher.request.range_end.is_empty() {
            if let Some(ids) = self.keys.get_mut(&watcher.request.key) {
                ids.remove(&id);
                if ids.is_empty() {
                    self.keys.remove(&watcher.request.key);
                }
            }
        } else {
            self.ranges.remove(&id);
        }
        Some(watcher)
    }

    /// Put the IDs of the watchers that can match an event of `journal` in `ids`, in ID order.
    /// The caller still filters by revision, key range, and event type.
    fn candidates(&self, journal: &crate::proto::state::Journal, ids: &mut Vec<i64>) {
        ids.clear();
        ids.extend(self.ranges.iter().copied());
        for event in &journal.events {
            if let Some(found) = event.kv.as_ref().and_then(|kv| self.keys.get(&kv.key)) {
                ids.extend(found.iter().copied());
            }
        }
        ids.sort_unstable();
        ids.dedup();
    }

    /// Recheck identity and range permission for every watcher.
    fn check_all(&mut self, state: &FeedState) -> Result<(), Status> {
        let auth = &state.view.auth;
        self.checked_auth = auth.clone();
        if !auth.enabled {
            return Ok(());
        }
        let credentials = self.credentials();
        for watcher in self.watchers.values() {
            permit(auth, &credentials, &watcher.request)?;
        }
        Ok(())
    }

    /// Check the leader and any auth change after the node publishes a new state.
    fn state_changed(&mut self) -> Result<(), Status> {
        let state = self.current(true)?;
        if !Arc::ptr_eq(&state.view.auth, &self.checked_auth) {
            self.check_all(&state)?;
        }
        Ok(())
    }

    /// Deliver one feed batch to the synced watchers that it covers.
    async fn batch(&mut self, batch: &Batch) -> Result<(), Status> {
        let state = self.current(true)?;
        if !Arc::ptr_eq(&state.view.auth, &self.checked_auth) {
            self.check_all(&state)?;
        }
        let journals = batch.journals();
        // Following watchers start at `position + 1`. If this batch does not deliver that
        // revision, they must keep it as a pinned start and catch up from storage.
        let follows = batch.revision <= self.position
            || (journals.is_some() && batch.previous <= self.position);
        if !follows {
            self.pin_all();
        }
        // A missing or incomplete batch leaves the covered watchers for storage catch-up.
        if let Some(journals) = journals {
            let credentials = self.credentials();
            let reads = state.view.read_state();
            // Read every response of the batch on the view first.
            let mut ids = vec![];
            let mut prepared = vec![];
            let mut bytes = 0;
            let mut stopped = None;
            let mut storage = false;
            'rows: for (revision, journal) in journals {
                self.candidates(journal, &mut ids);
                let mut sent = vec![];
                for id in &ids {
                    let watcher = &self.watchers[id];
                    let start = self.start(watcher);
                    if start > batch.previous && start <= *revision {
                        let response =
                            prepare(&state, &reads, &credentials, watcher, *revision, journal)
                                .await;
                        match response {
                            Ok(Some(response)) => {
                                // A full batch stops before this response, and the watcher resumes
                                // at this revision on a new view. The first response always fits,
                                // so the watchers make progress.
                                let size = response.response.encoded_len();
                                if !prepared.is_empty() && bytes + size > PREPARED_BYTES {
                                    stopped = Some((*revision, sent));
                                    break 'rows;
                                }
                                bytes += size;
                                prepared.push((*id, response));
                                sent.push(*id);
                            }
                            Ok(None) => sent.push(*id),
                            Err(status) if retryable(&status) => {
                                storage = true;
                                stopped = Some((*revision, sent));
                                break 'rows;
                            }
                            Err(status) => return Err(status),
                        }
                    }
                }
            }
            // Release the view before any send. A slow client cannot hold the pin.
            drop((reads, state));
            if storage {
                self.storage_stopped();
            }
            record_batch(bytes, prepared.len());
            for (id, response) in prepared {
                let watcher = self.watchers.get_mut(&id).expect("prepared watcher");
                send_prepared(&self.tx, &self.meters, watcher, response).await?;
            }
            if let Some((revision, sent)) = stopped {
                // Each watcher that this batch covers resumes from its own next revision on a
                // new view, through storage catch-up.
                let next = self.position + 1;
                for (&id, watcher) in &mut self.watchers {
                    let start = watcher.start.unwrap_or(next);
                    if start > batch.previous && start <= revision {
                        let resume = if sent.contains(&id) {
                            revision + 1
                        } else {
                            revision
                        };
                        watcher.start = Some(resume);
                        self.pinned.insert(id);
                    }
                }
                self.position = self.position.max(batch.revision);
                self.unpin();
                return Ok(());
            }
            // Pinned watchers that this batch covers start after it.
            for id in &self.pinned {
                let start = self.watchers.get_mut(id).expect("pinned watcher");
                let start = start.start.as_mut().expect("pinned start");
                if *start > batch.previous && *start <= batch.revision {
                    *start = batch.revision + 1;
                }
            }
        }
        self.position = self.position.max(batch.revision);
        self.unpin();
        Ok(())
    }

    async fn receive(
        &mut self,
        received: Result<Arc<Batch>, broadcast::error::RecvError>,
    ) -> Result<(), Status> {
        match received {
            Ok(batch) => self.batch(&batch).await,
            // The next batch starts after a gap. Watchers before it read storage.
            Err(broadcast::error::RecvError::Lagged(_)) => Ok(()),
            Err(broadcast::error::RecvError::Closed) => Err(unavailable()),
        }
    }

    /// Consume the batches that the node has already published.
    async fn drain(&mut self, events: &mut broadcast::Receiver<Arc<Batch>>) -> Result<(), Status> {
        loop {
            let received = match events.try_recv() {
                Ok(batch) => Ok(batch),
                Err(broadcast::error::TryRecvError::Empty) => return Ok(()),
                Err(broadcast::error::TryRecvError::Lagged(n)) => {
                    Err(broadcast::error::RecvError::Lagged(n))
                }
                Err(broadcast::error::TryRecvError::Closed) => {
                    Err(broadcast::error::RecvError::Closed)
                }
            };
            self.receive(received).await?;
        }
    }

    /// Read one journal page from the newest pinned view.
    /// `None` serves unsynced watchers. `Some(goal)` serves every watcher at or below `goal`.
    /// Returns true while those watchers still need more pages.
    async fn catch_up(&mut self, through: Option<i64>) -> Result<bool, Status> {
        let more = self.catch_up_page(through).await;
        self.unpin();
        more
    }

    async fn catch_up_page(&mut self, through: Option<i64>) -> Result<bool, Status> {
        let state = self.current(through.is_none())?;
        let view = state.view.clone();
        let limit = through.map_or(view.revision, |t| t.min(view.revision));
        let bound = match through {
            Some(_) => limit,
            None => self.position.min(limit),
        };
        // A progress request can serve revisions that the feed has not delivered yet.
        // The following watchers then need them too, so they store their start revision.
        if bound > self.position {
            self.pin_all();
        }
        let mut compacted = vec![];
        let mut pending = vec![];
        // Following watchers start after `position`, which is now at least `bound`.
        for &id in &self.pinned {
            let start = self.watchers[&id].start.expect("pinned start");
            if start > bound {
                continue;
            }
            if start < view.compact_revision {
                compacted.push(id);
            } else {
                pending.push((id, start));
            }
        }
        if !compacted.is_empty() {
            // Build the cancellations, release the view, then send. The next page takes a new
            // view for the other watchers.
            let mut responses = vec![];
            for id in compacted {
                self.remove(id);
                responses.push(WatchResponse {
                    header: state.header(view.revision),
                    watch_id: id,
                    canceled: true,
                    compact_revision: view.compact_revision,
                    ..Default::default()
                });
            }
            drop((state, view));
            for response in responses {
                send(&self.tx, response).await?;
            }
            return Ok(true);
        }
        let Some(from) = pending.iter().map(|&(_, start)| start).min() else {
            return Ok(false);
        };
        let credentials = self.credentials();
        for (id, _) in &pending {
            permit(&view.auth, &credentials, &self.watchers[id].request)?;
        }
        // Each page is one read with its own pin deadline. An expired page resumes on a new
        // view after the last revision that each watcher received. A failed page delivers
        // nothing, so the watchers resume at their own starts and skip no revision.
        let reads = view.read_state();
        let (rows, covered) = match crate::watch::scan(&reads, from, limit, PAGE_REVISIONS).await {
            Ok(page) => page,
            Err(error) => {
                let status = error.status();
                if !retryable(&status) {
                    return Err(status);
                }
                drop((reads, view, state));
                self.storage_stopped();
                return Ok(true);
            }
        };
        // The scan used part of the pin. The responses read prev_kv history on a fresh view
        // with its own deadline, so a page that read its journals always delivers some of them.
        // Journal and history rows at these revisions do not change. The fresh view's compact
        // revision decides whether an event keeps its prev_kv, as etcd does.
        drop((reads, view, state));
        #[cfg(test)]
        if let Some(hook) = self.after_scan.take() {
            hook();
        }
        let state = self.current(through.is_none())?;
        // A compaction between the scan and this view can remove the history of a pending
        // watcher. The next page then cancels that watcher with the compact revision, as etcd
        // does, and serves the others. No history read runs below the compact revision.
        if pending
            .iter()
            .any(|&(_, start)| start < state.view.compact_revision)
        {
            return Ok(true);
        }
        let reads = state.view.read_state();
        // Read every response of the page on the view first.
        let mut ids = vec![];
        let mut prepared = vec![];
        let mut bytes = 0;
        let mut stopped = None;
        let mut storage = false;
        'rows: for (revision, journal) in &rows {
            self.candidates(journal, &mut ids);
            let mut sent = vec![];
            for id in &ids {
                // Starts do not change in this loop. A pending watcher has `start <= bound` and
                // is not compacted, so this condition selects the pending watchers.
                let watcher = &self.watchers[id];
                let start = watcher.start.unwrap_or(i64::MAX);
                if start <= bound && start <= *revision {
                    let response =
                        prepare(&state, &reads, &credentials, watcher, *revision, journal).await;
                    match response {
                        Ok(Some(response)) => {
                            // A full batch stops before this response, and the watcher resumes
                            // at this revision on a new view. The first response always fits,
                            // so the watchers make progress.
                            let size = response.response.encoded_len();
                            if !prepared.is_empty() && bytes + size > PREPARED_BYTES {
                                stopped = Some((*revision, sent));
                                break 'rows;
                            }
                            bytes += size;
                            prepared.push((*id, response));
                            sent.push(*id);
                        }
                        Ok(None) => sent.push(*id),
                        Err(status) if retryable(&status) => {
                            storage = true;
                            stopped = Some((*revision, sent));
                            break 'rows;
                        }
                        Err(status) => return Err(status),
                    }
                }
            }
        }
        // Release the view before any send. A slow client cannot hold the pin.
        drop((reads, rows, state));
        if storage {
            self.storage_stopped();
        } else {
            self.storage_stops = 0;
        }
        record_batch(bytes, prepared.len());
        for (id, response) in prepared {
            let watcher = self.watchers.get_mut(&id).expect("prepared watcher");
            send_prepared(&self.tx, &self.meters, watcher, response).await?;
        }
        if let Some((revision, sent)) = stopped {
            // Watchers that received this revision resume after it. The others resume at it.
            for (id, _) in &pending {
                let watcher = self.watchers.get_mut(id).expect("pending watcher");
                let start = watcher.start.as_mut().expect("pending start");
                let resume = if sent.contains(id) {
                    revision + 1
                } else {
                    revision
                };
                *start = (*start).max(resume);
            }
            return Ok(true);
        }
        for (id, _) in &pending {
            let watcher = self.watchers.get_mut(id).expect("pending watcher");
            let start = watcher.start.as_mut().expect("pending start");
            *start = (*start).max(covered + 1);
        }
        Ok(covered < limit)
    }

    /// Run storage catch-up, the waiting progress requests, and the requests behind them to
    /// their end, as the stream loop does.
    #[cfg(test)]
    async fn settle(&mut self) -> Result<(), Status> {
        loop {
            match self.next_waiting() {
                Some(Waiting::Create(create)) => {
                    self.acknowledge(create).await?;
                    continue;
                }
                Some(Waiting::Progress) => {
                    self.progress()?;
                    continue;
                }
                None => {}
            }
            if !self.unsynced() && self.progress.is_none() {
                return Ok(());
            }
            tokio::time::sleep_until(self.retry_at).await;
            match self.progress.is_none() {
                true => self.catch_up(None).await.map(drop)?,
                false => self.progress_page().await?,
            }
        }
    }

    /// Take a create request as the stream loop does: create it at once, or queue it.
    #[cfg(test)]
    async fn request_create(&mut self, request: WatchCreateRequest) -> Result<(), Status> {
        match self.hold_create(request)? {
            Some(create) => self.acknowledge(create).await,
            None => Ok(()),
        }
    }

    /// Take a progress request as the stream loop does: serve it at once, or queue it.
    #[cfg(test)]
    fn request_progress(&mut self) -> Result<(), Status> {
        match self.hold_progress() {
            true => Ok(()),
            false => self.progress(),
        }
    }

    /// Decide a create request against the stream as the request finds it: its range, a
    /// duplicate ID, the watcher limit, and an ID for a request without one. A create that
    /// waits keeps this decision, also when a later Cancel ends the watcher with its ID.
    fn decide(&mut self, mut request: WatchCreateRequest) -> Create {
        if request.key.is_empty() {
            request.key = vec![0];
        }
        let queued: Vec<i64> = self
            .waiting
            .iter()
            .filter_map(|waiting| match waiting {
                Waiting::Create(create) if create.reason.is_empty() => {
                    Some(create.request.watch_id)
                }
                _ => None,
            })
            .collect();
        let taken = |id: i64| self.watchers.contains_key(&id) || queued.contains(&id);
        let reason = if !request.range_end.is_empty()
            && request.range_end != [0]
            && request.key >= request.range_end
        {
            "mvcc: watcher range is empty"
        } else if request.watch_id != 0 && taken(request.watch_id) {
            "mvcc: duplicate watch ID provided on the WatchStream"
        } else if self.watchers.len() + queued.len() >= MAX_WATCHERS {
            "watch limit exceeded"
        } else {
            ""
        };
        if reason.is_empty() && request.watch_id == 0 {
            while taken(self.next_id) {
                self.next_id += 1;
            }
            request.watch_id = self.next_id;
            self.next_id += 1;
        }
        Create { request, reason }
    }

    /// Decide a create, and return it when it can run now. Otherwise queue it behind a waiting
    /// progress reply. The client applies a stream-wide progress reply to each watch that it
    /// knows. So no watch can be acknowledged before an earlier progress reply, which may be
    /// older than its start. A full queue ends the stream with a retryable error.
    #[allow(clippy::result_large_err)]
    fn hold_create(&mut self, request: WatchCreateRequest) -> Result<Option<Create>, Status> {
        let create = self.decide(request);
        if self.progress.is_none() && self.waiting.is_empty() {
            return Ok(Some(create));
        }
        let creates = self
            .waiting
            .iter()
            .filter(|waiting| matches!(waiting, Waiting::Create(_)))
            .count();
        if creates >= MAX_WAITING_CREATES {
            return Err(Status::unavailable(
                "etcdserver: too many watch creates wait for a progress reply; retry",
            ));
        }
        self.waiting.push_back(Waiting::Create(create));
        Ok(None)
    }

    /// Queue a progress request behind a waiting progress reply, and return true. Return false
    /// when it can run now. A request right behind another waiting request folds into it: its
    /// goal is the revision when it runs.
    fn hold_progress(&mut self) -> bool {
        if self.progress.is_none() && self.waiting.is_empty() {
            return false;
        }
        if !matches!(self.waiting.back(), Some(Waiting::Progress)) {
            self.waiting.push_back(Waiting::Progress);
        }
        true
    }

    /// The next waiting request, once no progress reply waits.
    fn next_waiting(&mut self) -> Option<Waiting> {
        match self.progress {
            Some(_) => None,
            None => self.waiting.pop_front(),
        }
    }

    /// Send automatic progress and recheck expiring tokens.
    async fn check(&mut self) -> Result<(), Status> {
        let state = self.current(true)?;
        self.check_all(&state)?;
        let header = state.header(self.position);
        // Release the state before any send. A slow client cannot hold the view.
        drop(state);
        let next = self.position + 1;
        let due: Vec<_> = self
            .watchers
            .iter()
            .filter(|(_, watcher)| {
                watcher.request.progress_notify
                    && watcher.start.unwrap_or(next) == next
                    && watcher.progress.elapsed() >= self.progress_interval
            })
            .map(|(&id, _)| id)
            .collect();
        for id in due {
            let watcher = self.watchers.get_mut(&id).expect("due watcher");
            send(
                &self.tx,
                WatchResponse {
                    header,
                    watch_id: watcher.request.watch_id,
                    ..Default::default()
                },
            )
            .await?;
            watcher.progress = tokio::time::Instant::now();
        }
        Ok(())
    }

    /// Decide a create and reply to it at once, for tests that drive no progress request.
    #[cfg(test)]
    async fn create(&mut self, request: WatchCreateRequest) -> Result<(), Status> {
        let create = self.decide(request);
        self.acknowledge(create).await
    }

    /// Reply to a decided create, and add its watcher. See `Stream::decide`.
    async fn acknowledge(&mut self, create: Create) -> Result<(), Status> {
        let Create {
            mut request,
            reason,
        } = create;
        let state = self.current(false)?;
        state
            .view
            .auth
            .authenticated(&self.credentials())
            .map_err(|e| e.status())?;
        // A create runs only when no progress request waits, so the stream follows the feed.
        let created = self.position;
        let header = state.header(created);
        let auth = state.view.auth.clone();
        // Release the state before the send. The Auth store does not pin the view.
        drop(state);
        send(
            &self.tx,
            WatchResponse {
                header,
                watch_id: if reason.is_empty() {
                    request.watch_id
                } else {
                    -1
                },
                created: true,
                canceled: !reason.is_empty(),
                cancel_reason: reason.into(),
                ..Default::default()
            },
        )
        .await?;
        if !reason.is_empty() {
            return Ok(());
        }
        let requested = request.start_revision;
        if request.start_revision <= 0 {
            request.start_revision = created + 1;
        }
        permit(&auth, &self.credentials(), &request)?;
        let watcher = Watcher {
            _active: self
                .meters
                .as_ref()
                .map(|m| crate::metrics::Active::new(&m.watchers)),
            start: None,
            request,
            requested,
            progress: tokio::time::Instant::now(),
        };
        // A historical watcher is unsynced. The stream loop reads its pages one at a time.
        let start = watcher.request.start_revision;
        self.insert(watcher, start);
        Ok(())
    }

    async fn cancel(&mut self, id: i64) -> Result<(), Status> {
        // A Cancel takes effect when it arrives, also while a progress reply waits, and its
        // reply goes out at once. A client matches a cancel reply by its ID. The live watcher
        // with the ID goes first. Otherwise a waiting create that took the ID ends.
        let canceled = match self.watchers.contains_key(&id) {
            true => self.remove(id).is_some(),
            false => {
                let queued = self.waiting.iter().position(|waiting| {
                    matches!(waiting, Waiting::Create(create)
                        if create.reason.is_empty() && create.request.watch_id == id)
                });
                queued.and_then(|at| self.waiting.remove(at)).is_some()
            }
        };
        if canceled {
            let header = self.current(false)?.header(self.position);
            send(
                &self.tx,
                WatchResponse {
                    header,
                    watch_id: id,
                    canceled: true,
                    ..Default::default()
                },
            )
            .await?;
        }
        Ok(())
    }

    /// Report progress after all events through the current applied revision.
    ///
    /// etcd drops a progress request when a watcher of the stream asked for a start after the
    /// current revision, so this does too. A watcher whose events exist but whose catch-up
    /// waits on a failing store does not drop it. The reply then follows those events, unlike
    /// etcd, which drops such a request too.
    ///
    /// The stream loop reads the pages of the request one at a time. So a failing store does
    /// not keep the loop from a Cancel or a closed client. The goal stays fixed until the reply.
    /// Later requests wait in request order. See `Stream::hold_progress`.
    fn progress(&mut self) -> Result<(), Status> {
        debug_assert!(self.progress.is_none(), "a progress reply waits");
        let goal = self.current(false)?.view.revision;
        if self
            .watchers
            .values()
            .any(|watcher| watcher.requested > goal)
        {
            return Ok(());
        }
        self.progress = Some(goal);
        Ok(())
    }

    /// Read one page for the served progress request. Answer it once the watchers have every
    /// event through its goal. No watcher is created while it waits. See `Stream::hold_create`.
    async fn progress_page(&mut self) -> Result<(), Status> {
        let Some(goal) = self.progress else {
            return Ok(());
        };
        if self.catch_up(Some(goal)).await? {
            return Ok(());
        }
        self.progress = None;
        let header = self.current(false)?.header(goal);
        let watchers: Vec<_> = self
            .watchers
            .values()
            .map(|watcher| self.start(watcher))
            .collect();
        if !watchers.is_empty() && watchers.iter().all(|start| *start <= goal + 1) {
            send(
                &self.tx,
                WatchResponse {
                    header,
                    watch_id: -1,
                    ..Default::default()
                },
            )
            .await?;
        }
        Ok(())
    }
}

#[allow(clippy::result_large_err)]
fn permit(
    auth: &crate::auth::Store,
    credentials: &crate::auth::Credentials,
    request: &WatchCreateRequest,
) -> Result<(), Status> {
    auth.range(credentials, &request.key, &request.range_end, false)
        .map_err(|e| e.status())
}

/// The bytes of the responses that one page or batch holds before it sends them. A full batch
/// sends what it has and resumes each watcher on a new view.
#[cfg(not(test))]
const PREPARED_BYTES: usize = 4 << 20;
#[cfg(test)]
pub(crate) const PREPARED_BYTES: usize = 256 << 10;

/// The largest prepared batch with more than one response, for tests. One response that is
/// larger than the budget is sent alone.
#[cfg(test)]
pub(crate) static LARGEST_BATCH: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

fn record_batch(_bytes: usize, _responses: usize) {
    #[cfg(test)]
    if _responses > 1 {
        LARGEST_BATCH.fetch_max(_bytes, std::sync::atomic::Ordering::Relaxed);
    }
}

/// One response that a page or a batch read on its view. The send runs after the view is gone.
struct Prepared {
    response: WatchResponse,
    events: usize,
}

/// Read the events of one revision for one watcher. `None` means that no event matches.
async fn prepare(
    state: &FeedState,
    reads: &crate::state::view::StateView,
    credentials: &crate::auth::Credentials,
    watcher: &Watcher,
    revision: i64,
    journal: &crate::proto::state::Journal,
) -> Result<Option<Prepared>, Status> {
    let events = crate::watch::select(
        reads,
        &watcher.request,
        revision,
        state.view.compact_revision,
        journal,
    )
    .await
    .map_err(crate::mvcc::KvError::status)?;
    if events.is_empty() {
        return Ok(None);
    }
    permit(&state.view.auth, credentials, &watcher.request)?;
    Ok(Some(Prepared {
        events: events.len(),
        response: WatchResponse {
            header: state.header(revision),
            watch_id: watcher.request.watch_id,
            events,
            ..Default::default()
        },
    }))
}

async fn send_prepared(
    tx: &Output,
    meters: &Option<Arc<crate::metrics::WatchMeters>>,
    watcher: &mut Watcher,
    prepared: Prepared,
) -> Result<(), Status> {
    send_events(tx, prepared.response, watcher.request.fragment).await?;
    if let Some(metrics) = meters {
        metrics.events.inc_by(prepared.events as u64);
    }
    watcher.progress = tokio::time::Instant::now();
    Ok(())
}

impl Service {
    /// Wait until this member applied every Put that it already answered. Its batches are
    /// then on the feed.
    async fn apply_barrier(&self) -> Result<(), Status> {
        let (response, applied) = oneshot::channel();
        self.events
            .send(Event::WatchBarrier(response))
            .await
            .map_err(|_| unavailable())?;
        applied.await.map_err(|_| unavailable())
    }

    async fn watch_stream(
        &self,
        mut input: tonic::Streaming<WatchRequest>,
        tx: &Output,
        require_leader: bool,
        credentials: crate::auth::Credentials,
        meters: Option<Arc<crate::metrics::WatchMeters>>,
    ) -> Result<(), Status> {
        let _stream = meters
            .as_ref()
            .map(|m| crate::metrics::Active::new(&m.streams));
        let (subscribe, receiver) = oneshot::channel();
        self.events
            .send(Event::SubscribeWatch(subscribe))
            .await
            .map_err(|_| unavailable())?;
        let crate::watch::Subscription {
            mut events,
            state: mut changes,
        } = receiver.await.map_err(|_| unavailable())?;
        let mut stream = Stream::new(
            tx.clone(),
            require_leader,
            credentials,
            meters,
            changes.clone(),
            self.watch_progress_interval,
        );
        // Automatic progress uses the configured interval. Token expiry needs at least 1 s.
        let mut checks = tokio::time::interval(self.watch_progress_interval.min(CHECK_INTERVAL));
        checks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // A create waits here for the apply barrier, including a slot in the node queue. The
        // stream keeps serving its watchers, leader loss and checks, and reads the next request
        // only after the create. Creates and progress requests behind a progress reply wait in
        // `Stream::waiting`, and run in request order after the reply. The stream still reads
        // every request then, so a Cancel takes effect at once.
        type Barrier<'a> =
            std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), Status>> + Send + 'a>>;
        let mut pending: Option<(Create, Barrier<'_>)> = None;
        loop {
            while pending.is_none() {
                match stream.next_waiting() {
                    Some(Waiting::Create(create)) => {
                        pending = Some((create, Box::pin(self.apply_barrier())));
                    }
                    Some(Waiting::Progress) => {
                        stream.drain(&mut events).await?;
                        stream.progress()?;
                    }
                    None => break,
                }
            }
            let unsynced = stream.unsynced();
            let waiting = pending.is_some();
            if waiting || !stream.waiting.is_empty() {
                // The create fails without a leader. The stream may have no watcher yet.
                stream.current(false)?;
            }
            // A waiting progress request holds the feed, so no later event goes before its
            // answer. Requests then observe no new batch either.
            let progress = stream.progress.is_some();
            tokio::select! {
                _ = tx.closed() => return Ok(()),
                received = events.recv(), if !progress => stream.receive(received).await?,
                changed = changes.changed() => {
                    changed.map_err(|_| unavailable())?;
                    stream.state_changed()?;
                }
                _ = checks.tick() => stream.check().await?,
                _ = tokio::time::sleep_until(stream.retry_at), if progress => {
                    stream.progress_page().await?;
                }
                _ = tokio::time::sleep_until(stream.retry_at), if unsynced && !progress => {
                    stream.catch_up(None).await?;
                }
                applied = async { pending.as_mut().expect("waiting").1.as_mut().await }, if waiting => {
                    applied?;
                    let (create, _) = pending.take().expect("waiting");
                    // The create observes every batch through the replied Puts. No progress
                    // request waits, because the stream reads no request until the create.
                    stream.drain(&mut events).await?;
                    stream.acknowledge(create).await?;
                }
                message = input.message(), if !waiting => {
                    let Some(message) = message? else { return Ok(()); };
                    // Requests observe all batches that the node published before them.
                    match message.request_union {
                        Some(RequestUnion::CreateRequest(request)) => {
                            if let Some(create) = stream.hold_create(request)? {
                                pending = Some((create, Box::pin(self.apply_barrier())));
                            }
                        }
                        Some(RequestUnion::CancelRequest(request)) => {
                            if !progress {
                                stream.drain(&mut events).await?;
                            }
                            stream.cancel(request.watch_id).await?
                        }
                        Some(RequestUnion::ProgressRequest(_)) => {
                            if !stream.hold_progress() {
                                stream.drain(&mut events).await?;
                                stream.progress()?
                            }
                        }
                        None => continue,
                    }
                }
            }
        }
    }
}

async fn send(tx: &Output, response: WatchResponse) -> Result<(), Status> {
    if !response.events.is_empty() {
        crate::failpoint::hit("watch-before-send");
    }
    tx.send(Ok(response))
        .await
        .map_err(|_| Status::cancelled("watch stream closed"))
}

async fn send_events(
    tx: &Output,
    mut response: WatchResponse,
    fragment: bool,
) -> Result<(), Status> {
    if !fragment || response.encoded_len() <= 1024 * 1024 {
        return send(tx, response).await;
    }
    let events = std::mem::take(&mut response.events);
    for event in events {
        if !response.events.is_empty() && response.encoded_len() + event.encoded_len() > 1024 * 1024
        {
            response.fragment = true;
            send(tx, response.clone()).await?;
            response.events.clear();
        }
        response.events.push(event);
    }
    response.fragment = false;
    send(tx, response).await
}

#[tonic::async_trait]
impl Watch for Service {
    type WatchStream = ReceiverStream<Result<WatchResponse, Status>>;
    async fn watch(
        &self,
        request: Request<tonic::Streaming<WatchRequest>>,
    ) -> Result<Response<Self::WatchStream>, Status> {
        let meters = request
            .extensions()
            .get::<Arc<crate::metrics::WatchMeters>>()
            .cloned();
        let credentials = credentials(&request);
        let require_leader = request.metadata().contains_key("hasleader");
        let service = self.clone();
        let (tx, rx) = mpsc::channel(32);
        tokio::spawn(async move {
            if let Err(error) = service
                .watch_stream(
                    request.into_inner(),
                    &tx,
                    require_leader,
                    credentials,
                    meters,
                )
                .await
            {
                let _ = tx.send(Err(error)).await;
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::StateMachine;

    struct Fixture {
        _dir: tempfile::TempDir,
        state: StateMachine,
        feed: Arc<watch::Sender<FeedState>>,
        output: mpsc::Receiver<Result<WatchResponse, Status>>,
        stream: Stream,
    }

    fn feed_state(state: &StateMachine) -> FeedState {
        FeedState {
            view: state.read_view().unwrap(),
            header: ResponseHeader {
                member_id: 9,
                ..Default::default()
            },
            leader: 1,
        }
    }

    impl Fixture {
        async fn new() -> Self {
            let dir = tempfile::tempdir().unwrap();
            let mut state = StateMachine::open(dir.path()).await.unwrap();
            state.watch_capture = Some(Default::default());
            let (feed, changes) = watch::channel(feed_state(&state));
            let feed = Arc::new(feed);
            let (tx, output) = mpsc::channel(1024);
            let stream = Stream::new(
                tx,
                false,
                Default::default(),
                None,
                changes,
                DEFAULT_WATCH_PROGRESS_INTERVAL,
            );
            Self {
                _dir: dir,
                state,
                feed,
                output,
                stream,
            }
        }

        /// Apply one request and publish its view. The caller delivers the batch.
        async fn apply(&mut self, request: Op) -> Arc<Batch> {
            let previous = self.state.revision;
            let index = self.state.applied + 1;
            self.state
                .apply_etcd(
                    index,
                    &RequestOp {
                        request: Some(request),
                    },
                )
                .await
                .unwrap()
                .result
                .unwrap();
            self.feed.send_replace(feed_state(&self.state));
            Arc::new(self.state.take_watch_batch(previous).unwrap())
        }

        async fn put(&mut self, key: &str, value: &str) -> Arc<Batch> {
            self.apply(Op::RequestPut(PutRequest {
                key: key.into(),
                value: value.into(),
                ..Default::default()
            }))
            .await
        }

        async fn delete(&mut self, key: &str) -> Arc<Batch> {
            self.apply(Op::RequestDeleteRange(DeleteRangeRequest {
                key: key.into(),
                ..Default::default()
            }))
            .await
        }

        async fn watch(&mut self, request: WatchCreateRequest) {
            self.stream.create(request).await.unwrap();
            self.stream.settle().await.unwrap();
            let created = self.output.recv().await.unwrap().unwrap();
            assert!(created.created && !created.canceled);
        }

        fn responses(&mut self) -> Vec<WatchResponse> {
            let mut responses = vec![];
            while let Ok(response) = self.output.try_recv() {
                responses.push(response.unwrap());
            }
            responses
        }

        /// Return `(watch_id, revision, event type)` for every delivered event.
        fn events(&mut self) -> Vec<(i64, i64, i32)> {
            self.responses()
                .into_iter()
                .flat_map(|r| {
                    let revision = r.header.unwrap().revision;
                    r.events
                        .into_iter()
                        .map(move |e| (r.watch_id, revision, e.r#type))
                })
                .collect()
        }

        async fn close(self) {
            drop(self.stream);
            self.state.close().await.unwrap();
        }
    }

    fn request(id: i64, key: &str, start: i64) -> WatchCreateRequest {
        WatchCreateRequest {
            watch_id: id,
            key: key.into(),
            start_revision: start,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn catch_up_hands_off_to_the_feed_without_gaps_or_duplicates() {
        let mut f = Fixture::new().await;
        f.put("k", "1").await;
        f.put("k", "2").await;
        // The stream subscribes at revision 3. Two later batches wait in its queue.
        f.stream = Stream::new(
            f.stream.tx.clone(),
            false,
            Default::default(),
            None,
            f.feed.subscribe(),
            DEFAULT_WATCH_PROGRESS_INTERVAL,
        );
        let queued = [f.put("k", "3").await, f.put("k", "4").await];
        f.watch(request(1, "k", 2)).await;
        for batch in &queued {
            f.stream.batch(batch).await.unwrap();
        }
        let live = f.put("k", "5").await;
        f.stream.batch(&live).await.unwrap();
        let revisions: Vec<_> = f.events().into_iter().map(|(_, r, _)| r).collect();
        assert_eq!(revisions, [2, 3, 4, 5, 6]);
        assert!(!f.stream.unsynced());
        f.close().await;
    }

    #[tokio::test]
    async fn key_index_routes_events_and_followers_track_the_feed() {
        let mut f = Fixture::new().await;
        f.watch(request(1, "a", 0)).await;
        f.watch(request(2, "b", 0)).await;
        f.watch(WatchCreateRequest {
            watch_id: 3,
            key: "a".into(),
            range_end: "c".into(),
            ..Default::default()
        })
        .await;
        // Watcher 4 starts in the future, so it stores its start revision.
        f.watch(request(4, "a", 4)).await;
        assert!(
            f.stream
                .watchers
                .values()
                .filter(|w| w.start.is_none())
                .count()
                == 3
        );
        assert_eq!(f.stream.pinned.iter().copied().collect::<Vec<_>>(), [4]);
        let batch = f.put("a", "1").await;
        f.stream.batch(&batch).await.unwrap();
        let batch = f.put("b", "1").await;
        f.stream.batch(&batch).await.unwrap();
        assert_eq!(f.events(), [(1, 2, 0), (3, 2, 0), (2, 3, 0), (3, 3, 0)]);
        // The feed reached revision 3, so watcher 4 now follows it.
        assert!(f.stream.pinned.is_empty());
        f.stream.cancel(1).await.unwrap();
        assert!(f.responses()[0].canceled);
        assert_eq!(f.stream.keys[b"a".as_slice()], BTreeSet::from([4]));
        let batch = f.put("a", "2").await;
        f.stream.batch(&batch).await.unwrap();
        assert_eq!(f.events(), [(3, 4, 0), (4, 4, 0)]);
        // An incomplete batch leaves the followers at their start revision for catch-up.
        let large = "x".repeat(crate::watch::FEED_BATCH_BYTES + 1);
        let gap = f.put("b", &large).await;
        assert!(gap.journals().is_none());
        f.stream.batch(&gap).await.unwrap();
        assert_eq!(f.stream.pinned.len(), 3);
        assert!(f.stream.unsynced());
        f.stream.settle().await.unwrap();
        assert_eq!(f.events(), [(2, 5, 0), (3, 5, 0)]);
        assert!(f.stream.pinned.is_empty() && !f.stream.unsynced());
        f.close().await;
    }

    #[tokio::test]
    async fn progress_request_serves_following_watchers_before_the_feed() {
        let mut f = Fixture::new().await;
        f.watch(request(1, "k", 0)).await;
        f.watch(request(2, "other", 0)).await;
        // The view has revision 2, but the stream has not received its batch yet.
        let late = f.put("k", "1").await;
        f.stream.request_progress().unwrap();
        f.stream.settle().await.unwrap();
        let responses = f.responses();
        assert_eq!(responses.len(), 2);
        assert_eq!((responses[0].watch_id, responses[0].events.len()), (1, 1));
        assert_eq!(
            (responses[1].watch_id, responses[1].header.unwrap().revision),
            (-1, 2)
        );
        // The late batch must not deliver revision 2 again.
        f.stream.batch(&late).await.unwrap();
        assert!(f.events().is_empty());
        assert!(f.stream.pinned.is_empty());
        let next = f.put("k", "2").await;
        f.stream.batch(&next).await.unwrap();
        assert_eq!(f.events(), [(1, 3, 0)]);
        f.close().await;
    }

    #[tokio::test]
    async fn lagged_feed_falls_back_to_storage() {
        let mut f = Fixture::new().await;
        f.watch(request(1, "k", 0)).await;
        let (sender, mut receiver) = broadcast::channel(2);
        for n in 0..4 {
            let batch = f.put("k", &n.to_string()).await;
            sender.send(batch).unwrap();
        }
        let lagged = receiver.recv().await;
        assert!(matches!(
            lagged,
            Err(broadcast::error::RecvError::Lagged(2))
        ));
        f.stream.receive(lagged).await.unwrap();
        for _ in 0..2 {
            let received = receiver.recv().await;
            f.stream.receive(received).await.unwrap();
        }
        assert!(
            f.events().is_empty(),
            "a batch after a gap reached the watcher"
        );
        assert!(f.stream.unsynced());
        while f.stream.unsynced() {
            f.stream.catch_up(None).await.unwrap();
        }
        let batch = f.put("k", "after").await;
        f.stream.batch(&batch).await.unwrap();
        let revisions: Vec<_> = f.events().into_iter().map(|(_, r, _)| r).collect();
        assert_eq!(revisions, [2, 3, 4, 5, 6]);
        f.close().await;
    }

    /// Eight following and eight historical watchers receive large values. Each revision has
    /// more response bytes than the budget, so the feed and catch-up stop inside a revision.
    #[tokio::test]
    async fn prepared_batches_stay_in_the_byte_budget_and_deliver_each_revision_once() {
        let mut f = Fixture::new().await;
        for id in 1..=8 {
            f.watch(request(id, "k", 0)).await;
        }
        let value = "x".repeat(40 << 10);
        let mut revisions = vec![];
        for _ in 0..6 {
            let batch = f.put("k", &value).await;
            assert!(batch.journals().is_some());
            revisions.push(batch.revision);
            f.stream.batch(&batch).await.unwrap();
            // The feed stops inside the revision. The other watchers read storage.
            assert!(f.stream.unsynced());
            f.stream.settle().await.unwrap();
            assert!(!f.stream.unsynced());
        }
        let mut events = f.events();
        // A historical watcher catches up when the stream creates it.
        for id in 9..=16 {
            f.stream
                .create(request(id, "k", revisions[0]))
                .await
                .unwrap();
            f.stream.settle().await.unwrap();
        }
        events.extend(f.events());
        let largest = LARGEST_BATCH.load(std::sync::atomic::Ordering::Relaxed);
        assert!(largest <= PREPARED_BYTES, "{largest} bytes in one batch");
        let mut seen: HashMap<i64, Vec<i64>> = HashMap::new();
        for (id, revision, _) in events {
            seen.entry(id).or_default().push(revision);
        }
        for id in 1..=16 {
            assert_eq!(seen.get(&id), Some(&revisions), "watcher {id}");
        }
        f.close().await;
    }

    #[tokio::test]
    async fn incomplete_batch_uses_storage_catch_up() {
        let mut f = Fixture::new().await;
        f.watch(request(1, "k", 0)).await;
        let large = "x".repeat(crate::watch::FEED_BATCH_BYTES + 1);
        let batch = f.put("k", &large).await;
        assert!(batch.journals().is_none());
        f.stream.batch(&batch).await.unwrap();
        assert!(f.events().is_empty());
        f.stream.settle().await.unwrap();
        assert_eq!(f.events(), [(1, 2, 0)]);
        f.close().await;
    }

    #[tokio::test]
    async fn feed_and_catch_up_apply_filters_and_previous_values() {
        let mut f = Fixture::new().await;
        let first = f.put("a", "1").await;
        f.stream.batch(&first).await.unwrap();
        let no_put = WatchCreateRequest {
            filters: vec![0],
            prev_kv: true,
            ..request(1, "a", 0)
        };
        let plain = request(2, "a", 0);
        let range = WatchCreateRequest {
            range_end: b"c".to_vec(),
            ..request(3, "a", 0)
        };
        for request in [no_put, plain, range] {
            f.watch(request).await;
        }
        for batch in [
            f.put("a", "2").await,
            f.put("b", "1").await,
            f.put("c", "1").await,
            f.delete("a").await,
        ] {
            f.stream.batch(&batch).await.unwrap();
        }
        let responses = f.responses();
        let mut seen = vec![];
        for response in &responses {
            for event in &response.events {
                let kv = event.kv.as_ref().unwrap();
                seen.push((response.watch_id, kv.key.clone(), event.r#type));
                assert_eq!(response.header.as_ref().unwrap().member_id, 9);
                if response.watch_id == 1 {
                    assert_eq!(event.prev_kv.as_ref().unwrap().value, b"2");
                } else {
                    assert!(event.prev_kv.is_none());
                }
            }
        }
        seen.sort();
        assert_eq!(
            seen,
            [
                (1, b"a".to_vec(), 1),
                (2, b"a".to_vec(), 0),
                (2, b"a".to_vec(), 1),
                (3, b"a".to_vec(), 0),
                (3, b"a".to_vec(), 1),
                (3, b"b".to_vec(), 0),
            ]
        );
        // Storage catch-up applies the same rules.
        let history = WatchCreateRequest {
            filters: vec![1],
            prev_kv: true,
            ..request(4, "a", 2)
        };
        f.stream.create(history).await.unwrap();
        f.stream.settle().await.unwrap();
        let responses = f.responses();
        assert!(responses[0].created);
        let events: Vec<_> = responses[1..].iter().flat_map(|r| &r.events).collect();
        assert_eq!(events.len(), 2);
        assert!(events[0].prev_kv.is_none());
        assert_eq!(events[1].prev_kv.as_ref().unwrap().value, b"1");
        assert!(events.iter().all(|e| e.r#type == 0));
        f.close().await;
    }

    #[tokio::test]
    async fn compaction_cancels_historical_watchers() {
        let mut f = Fixture::new().await;
        for n in 0..3 {
            let batch = f.put("k", &n.to_string()).await;
            f.stream.batch(&batch).await.unwrap();
        }
        let index = f.state.applied + 1;
        f.state
            .apply_compaction(
                index,
                &CompactionRequest {
                    revision: 3,
                    physical: false,
                },
            )
            .await
            .unwrap()
            .result
            .unwrap();
        f.feed.send_replace(feed_state(&f.state));
        f.stream.create(request(1, "k", 2)).await.unwrap();
        f.stream.settle().await.unwrap();
        let responses = f.responses();
        assert!(responses[0].created);
        assert!(responses[1].canceled);
        assert_eq!(responses[1].compact_revision, 3);
        assert!(f.stream.watchers.is_empty());
        f.close().await;
    }

    /// A physical compaction lands between the journal scan and the history reads. The
    /// historical watcher is cancelled with the compact revision. The stream stays open, and a
    /// current watcher on it receives its next event.
    #[tokio::test]
    async fn a_compaction_after_the_scan_cancels_only_the_compacted_watcher() {
        let mut f = Fixture::new().await;
        f.watch(request(2, "other", 0)).await;
        let mut revisions = vec![];
        for n in 0..3 {
            let batch = f.put("k", &n.to_string()).await;
            revisions.push(batch.revision);
            f.stream.batch(&batch).await.unwrap();
        }
        // The feed keeps the view before the compaction until the scan ends.
        let index = f.state.applied + 1;
        f.state
            .apply_compaction(
                index,
                &CompactionRequest {
                    revision: revisions[2],
                    physical: true,
                },
            )
            .await
            .unwrap()
            .result
            .unwrap();
        while f.state.compaction_pending() {
            let index = f.state.applied + 1;
            f.state.apply_compaction_step(index).await.unwrap();
        }
        let compacted = feed_state(&f.state);
        let feed = f.feed.clone();
        f.stream.after_scan = Some(Box::new(move || {
            feed.send_replace(compacted);
        }));
        f.stream
            .create(request(1, "k", revisions[0]))
            .await
            .unwrap();
        f.stream.settle().await.unwrap();
        assert!(f.stream.after_scan.is_none(), "the scan ran");
        let responses = f.responses();
        assert_eq!(responses.len(), 2);
        assert!(responses[0].created);
        assert!(responses[1].canceled);
        assert_eq!(
            (responses[1].watch_id, responses[1].compact_revision),
            (1, revisions[2])
        );
        assert_eq!(f.stream.watchers.keys().copied().collect::<Vec<_>>(), [2]);
        let next = f.put("other", "1").await;
        f.stream.batch(&next).await.unwrap();
        assert_eq!(f.events(), [(2, next.revision, 0)]);
        f.close().await;
    }

    #[tokio::test]
    async fn progress_follows_history_and_a_future_watch_drops_it() {
        let mut f = Fixture::new().await;
        for n in 0..3 {
            let batch = f.put("k", &n.to_string()).await;
            f.stream.batch(&batch).await.unwrap();
        }
        f.watch(request(1, "k", 0)).await;
        f.stream.request_progress().unwrap();
        f.stream.settle().await.unwrap();
        let progress = f.responses();
        assert_eq!(progress.len(), 1);
        assert_eq!(progress[0].watch_id, -1);
        assert_eq!(progress[0].header.as_ref().unwrap().revision, 4);
        f.watch(request(2, "k", 6)).await;
        f.stream.request_progress().unwrap();
        f.stream.settle().await.unwrap();
        assert!(f.responses().is_empty());
        f.close().await;
    }

    #[tokio::test]
    async fn leader_loss_fails_a_leader_required_stream() {
        let mut f = Fixture::new().await;
        f.stream.require_leader = true;
        let mut state = feed_state(&f.state);
        state.leader = 0;
        f.feed.send_replace(state);
        // An idle stream has no watcher to fail.
        f.stream.state_changed().unwrap();
        let error = f.stream.create(request(1, "k", 0)).await.unwrap_err();
        assert!(error.message().contains("no leader"));
        f.close().await;
    }

    /// A member whose node answers Watch subscriptions from the fixture feed and hands each apply
    /// barrier to the test, which releases it once the replied Put is applied.
    struct EarlyReply {
        batches: broadcast::Sender<Arc<Batch>>,
        barriers: mpsc::Receiver<oneshot::Sender<()>>,
        node: mpsc::Sender<Event>,
        client: crate::proto::etcdserverpb::watch_client::WatchClient<tonic::transport::Channel>,
    }

    impl EarlyReply {
        async fn start(f: &Fixture) -> Self {
            let (events, mut node) = mpsc::channel(16);
            let (batches, _) = broadcast::channel(16);
            let (barrier, barriers) = mpsc::channel(16);
            let (feed, published) = (f.feed.clone(), batches.clone());
            tokio::spawn(async move {
                while let Some(event) = node.recv().await {
                    match event {
                        Event::SubscribeWatch(response) => {
                            let _ = response.send(crate::watch::Subscription {
                                events: published.subscribe(),
                                state: feed.subscribe(),
                            });
                        }
                        Event::WatchBarrier(response) => barrier.send(response).await.unwrap(),
                        _ => panic!("unexpected event"),
                    }
                }
            });
            let node_events = events.clone();
            let router = crate::grpc::Servers::new(
                1,
                events,
                &Default::default(),
                crate::auth::TokenIssuer::parse("simple").unwrap(),
                Default::default(),
                DEFAULT_WATCH_PROGRESS_INTERVAL,
            )
            .unwrap()
            .router();
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("http://{}", listener.local_addr().unwrap());
            tokio::spawn(crate::tls::serve(listener, router, None));
            let client = crate::proto::etcdserverpb::watch_client::WatchClient::connect(url)
                .await
                .unwrap();
            Self {
                batches,
                barriers,
                node: node_events,
                client,
            }
        }

        async fn open(&mut self) -> (mpsc::Sender<WatchRequest>, tonic::Streaming<WatchResponse>) {
            self.open_with(false).await
        }

        async fn open_with(
            &mut self,
            require_leader: bool,
        ) -> (mpsc::Sender<WatchRequest>, tonic::Streaming<WatchResponse>) {
            let (requests, input) = mpsc::channel(16);
            let mut request = Request::new(ReceiverStream::new(input));
            if require_leader {
                request
                    .metadata_mut()
                    .insert("hasleader", "true".parse().unwrap());
            }
            let output = self.client.watch(request).await.unwrap().into_inner();
            (requests, output)
        }

        async fn barrier(&mut self) -> oneshot::Sender<()> {
            tokio::time::timeout(std::time::Duration::from_secs(5), self.barriers.recv())
                .await
                .expect("a create waits for the apply barrier")
                .unwrap()
        }
    }

    fn create(request: WatchCreateRequest) -> WatchRequest {
        WatchRequest {
            request_union: Some(RequestUnion::CreateRequest(request)),
        }
    }

    async fn next(output: &mut tonic::Streaming<WatchResponse>) -> WatchResponse {
        tokio::time::timeout(std::time::Duration::from_secs(5), output.message())
            .await
            .unwrap()
            .unwrap()
            .unwrap()
    }

    /// The member answered a Put at R before it applied R. A watcher created then on an open
    /// stream starts after R, and the stream's earlier watcher still receives R.
    #[tokio::test]
    async fn a_create_on_an_open_stream_starts_after_an_early_put_reply() {
        let mut f = Fixture::new().await;
        let mut member = EarlyReply::start(&f).await;
        let (requests, mut output) = member.open().await;
        requests.send(create(request(1, "k", 0))).await.unwrap();
        member.barrier().await.send(()).unwrap();
        let created = next(&mut output).await;
        assert!(created.created);
        let before = created.header.unwrap().revision;
        // The reply for R is out. The second create waits until R is applied.
        requests.send(create(request(2, "k", 0))).await.unwrap();
        let barrier = member.barrier().await;
        let applied = f.put("k", "1").await;
        assert_eq!(applied.revision, before + 1);
        member.batches.send(applied).unwrap();
        barrier.send(()).unwrap();
        let first = next(&mut output).await;
        assert_eq!((first.watch_id, first.events.len()), (1, 1));
        assert_eq!(first.header.unwrap().revision, before + 1);
        let created = next(&mut output).await;
        assert!(created.created);
        assert_eq!(created.watch_id, 2);
        assert_eq!(created.header.unwrap().revision, before + 1);
        member.batches.send(f.put("k", "2").await).unwrap();
        let mut watchers = vec![];
        for _ in 0..2 {
            let response = next(&mut output).await;
            assert_eq!(response.header.unwrap().revision, before + 2);
            assert_eq!(response.events.len(), 1);
            watchers.push(response.watch_id);
        }
        watchers.sort();
        assert_eq!(watchers, [1, 2]);
        f.close().await;
    }

    /// A create that waits for the apply barrier does not hide leader loss from the stream.
    #[tokio::test]
    async fn leader_loss_ends_a_stream_while_a_create_waits_for_the_apply_barrier() {
        let f = Fixture::new().await;
        let mut member = EarlyReply::start(&f).await;
        let (requests, mut output) = member.open_with(true).await;
        requests.send(create(request(1, "k", 0))).await.unwrap();
        member.barrier().await.send(()).unwrap();
        assert!(next(&mut output).await.created);
        requests.send(create(request(2, "k", 0))).await.unwrap();
        let _held = member.barrier().await;
        let mut state = feed_state(&f.state);
        state.leader = 0;
        f.feed.send_replace(state);
        let error = tokio::time::timeout(std::time::Duration::from_secs(5), output.message())
            .await
            .expect("leader loss ends the stream while the barrier is held")
            .unwrap_err();
        assert!(error.message().contains("no leader"));
        f.close().await;
    }

    /// The first create on a stream has no watcher yet, and leader loss still fails it while
    /// it waits for the apply barrier.
    #[tokio::test]
    async fn leader_loss_fails_the_first_create_while_it_waits_for_the_apply_barrier() {
        let f = Fixture::new().await;
        let mut member = EarlyReply::start(&f).await;
        let (requests, mut output) = member.open_with(true).await;
        requests.send(create(request(1, "k", 0))).await.unwrap();
        let _held = member.barrier().await;
        let mut state = feed_state(&f.state);
        state.leader = 0;
        f.feed.send_replace(state);
        let error = tokio::time::timeout(std::time::Duration::from_secs(5), output.message())
            .await
            .expect("leader loss ends the stream while the first barrier is held")
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::Unavailable);
        assert!(error.message().contains("no leader"));
        f.close().await;
    }

    /// A create that waits for a slot in a full node queue does not pause delivery to the
    /// stream's existing watchers.
    #[tokio::test]
    async fn a_full_node_queue_does_not_pause_delivery_to_existing_watchers() {
        let mut f = Fixture::new().await;
        let mut member = EarlyReply::start(&f).await;
        let (requests, mut output) = member.open().await;
        requests.send(create(request(1, "k", 0))).await.unwrap();
        member.barrier().await.send(()).unwrap();
        assert!(next(&mut output).await.created);
        // 16 barriers fill the test's barrier queue, the node holds one, and 16 fill its queue.
        let mut held = vec![];
        for _ in 0..33 {
            let (response, applied) = oneshot::channel();
            held.push(applied);
            member
                .node
                .send(Event::WatchBarrier(response))
                .await
                .unwrap();
        }
        requests.send(create(request(2, "k", 0))).await.unwrap();
        member.batches.send(f.put("k", "1").await).unwrap();
        let event = next(&mut output).await;
        assert_eq!((event.watch_id, event.events.len()), (1, 1));
        f.close().await;
    }

    /// A create with an explicit start also waits for an early Put reply: its header covers
    /// the replied Put, and it still receives events from the requested revision.
    #[tokio::test]
    async fn an_explicit_start_create_waits_for_an_early_put_reply() {
        let mut f = Fixture::new().await;
        let mut member = EarlyReply::start(&f).await;
        let (requests, mut output) = member.open().await;
        requests.send(create(request(1, "k", 0))).await.unwrap();
        member.barrier().await.send(()).unwrap();
        let before = next(&mut output).await.header.unwrap().revision;
        requests
            .send(create(request(2, "k", before + 1)))
            .await
            .unwrap();
        let barrier = member.barrier().await;
        let applied = f.put("k", "1").await;
        assert_eq!(applied.revision, before + 1);
        member.batches.send(applied).unwrap();
        barrier.send(()).unwrap();
        let first = next(&mut output).await;
        assert_eq!((first.watch_id, first.events.len()), (1, 1));
        let created = next(&mut output).await;
        assert!(created.created);
        assert_eq!(created.watch_id, 2);
        assert_eq!(created.header.unwrap().revision, before + 1);
        let event = next(&mut output).await;
        assert_eq!((event.watch_id, event.events.len()), (2, 1));
        assert_eq!(
            event.events[0].kv.as_ref().unwrap().mod_revision,
            before + 1
        );
        f.close().await;
    }

    /// A client that leaves while its create waits for the apply barrier ends its stream task.
    #[tokio::test]
    async fn a_closed_stream_stops_waiting_for_the_apply_barrier() {
        let f = Fixture::new().await;
        let mut member = EarlyReply::start(&f).await;
        let (requests, output) = member.open().await;
        requests.send(create(request(1, "k", 0))).await.unwrap();
        let barrier = member.barrier().await;
        assert_eq!(member.batches.receiver_count(), 1);
        drop((requests, output));
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while member.batches.receiver_count() > 0 || !barrier.is_closed() {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the stream task ends while the apply barrier waits");
        f.close().await;
    }

    /// A Put publishes revision 3 while a progress request at revision 2 holds the feed. A
    /// create then waits until the progress reply went out, so the reply is not older than any
    /// watch that the client knows. Each event reaches each watcher once.
    #[tokio::test]
    async fn a_create_during_a_progress_request_waits_for_its_reply() {
        let mut f = Fixture::new().await;
        f.watch(request(1, "k", 0)).await;
        let two = f.put("k", "2").await;
        f.stream.batch(&two).await.unwrap();
        assert_eq!(f.events(), [(1, 2, 0)]);
        f.stream.request_progress().unwrap();
        let three = f.put("k", "3").await;
        f.stream.request_create(request(2, "k", 0)).await.unwrap();
        assert!(f.responses().is_empty(), "the create did not wait");
        f.stream.settle().await.unwrap();
        let responses = f.responses();
        assert_eq!(responses.len(), 2, "{responses:?}");
        assert_eq!(responses[0].watch_id, -1);
        assert_eq!(responses[0].header.unwrap().revision, 2);
        assert!(responses[1].created && responses[1].watch_id == 2);
        f.stream.batch(&three).await.unwrap();
        let four = f.put("k", "4").await;
        f.stream.batch(&four).await.unwrap();
        assert_eq!(f.events(), [(1, 3, 0), (2, 3, 0), (1, 4, 0), (2, 4, 0)]);
        f.close().await;
    }

    /// A historical watcher created while a progress request holds the feed gets each of its
    /// revisions once, and its create follows the progress reply.
    #[tokio::test]
    async fn a_historical_watch_during_a_progress_request_gets_each_revision_once() {
        let mut f = Fixture::new().await;
        f.watch(request(1, "other", 0)).await;
        let two = f.put("k", "2").await;
        f.stream.batch(&two).await.unwrap();
        f.stream.request_progress().unwrap();
        let three = f.put("k", "3").await;
        f.stream.request_create(request(2, "k", 2)).await.unwrap();
        f.stream.settle().await.unwrap();
        f.stream.batch(&three).await.unwrap();
        f.stream.settle().await.unwrap();
        let responses = f.responses();
        assert_eq!(responses[0].watch_id, -1);
        assert_eq!(responses[0].header.unwrap().revision, 2);
        assert!(responses[1].created);
        let events: Vec<_> = responses
            .iter()
            .filter(|r| r.watch_id == 2)
            .flat_map(|r| r.events.iter().map(|e| e.kv.as_ref().unwrap().mod_revision))
            .collect();
        assert_eq!(events, [2, 3]);
        f.close().await;
    }

    /// A Cancel removes a create that waits behind a progress reply and gets its reply. The
    /// create never runs.
    #[tokio::test]
    async fn a_cancel_removes_a_waiting_create() {
        let mut f = Fixture::new().await;
        f.watch(request(1, "k", 0)).await;
        f.put("k", "2").await;
        f.stream.request_progress().unwrap();
        f.stream.request_create(request(7, "k", 0)).await.unwrap();
        f.stream.cancel(7).await.unwrap();
        let canceled = f.responses();
        assert_eq!(canceled.len(), 1);
        assert!(canceled[0].canceled && canceled[0].watch_id == 7);
        f.stream.settle().await.unwrap();
        let responses = f.responses();
        assert!(responses.iter().all(|r| !r.created), "{responses:?}");
        assert!(!f.stream.watchers.contains_key(&7));
        f.close().await;
    }

    /// As in etcd, a progress request is dropped while a watcher asked for a start after the
    /// current revision: it gets no reply and holds nothing. Once the store reaches that start,
    /// a request gets its reply.
    #[tokio::test]
    async fn a_future_watch_drops_a_progress_request() {
        let mut f = Fixture::new().await;
        f.watch(request(1, "k", 0)).await;
        f.watch(request(2, "k", 50)).await;
        f.stream.request_progress().unwrap();
        assert_eq!(f.stream.progress, None, "the request waits");
        f.stream.settle().await.unwrap();
        assert!(f.responses().is_empty());
        for n in 2..=49 {
            let batch = f.put("other", &n.to_string()).await;
            f.stream.batch(&batch).await.unwrap();
        }
        f.stream.request_progress().unwrap();
        f.stream.settle().await.unwrap();
        assert!(
            f.responses().is_empty(),
            "a reply at 49 before the start 50"
        );
        let batch = f.put("other", "50").await;
        f.stream.batch(&batch).await.unwrap();
        f.stream.request_progress().unwrap();
        f.stream.settle().await.unwrap();
        let progress = f.responses();
        assert_eq!(progress.len(), 1, "{progress:?}");
        assert_eq!(progress[0].watch_id, -1);
        assert_eq!(progress[0].header.unwrap().revision, 50);
        f.close().await;
    }

    /// As in etcd 3.6.5: at revision 2, a watch from revision 3 drops a progress request. After
    /// a Put at 3 it gets the event, and a new request gets its reply at 3. A watch from the
    /// next revision with start 0 does not drop a request at 2.
    #[tokio::test]
    async fn an_explicit_start_after_the_revision_drops_a_progress_request() {
        let mut f = Fixture::new().await;
        let two = f.put("k", "2").await;
        f.stream.batch(&two).await.unwrap();
        f.watch(request(1, "k", 0)).await;
        f.stream.progress().unwrap();
        f.stream.settle().await.unwrap();
        let progress = f.responses();
        assert_eq!(progress.len(), 1, "{progress:?}");
        assert_eq!(progress[0].header.unwrap().revision, 2);
        f.stream.cancel(1).await.unwrap();
        f.responses();
        f.watch(request(2, "k", 3)).await;
        f.stream.progress().unwrap();
        f.stream.settle().await.unwrap();
        assert!(f.responses().is_empty(), "a reply at 2 before the start 3");
        let three = f.put("k", "3").await;
        f.stream.batch(&three).await.unwrap();
        assert_eq!(f.events(), [(2, 3, 0)]);
        f.stream.progress().unwrap();
        f.stream.settle().await.unwrap();
        let progress = f.responses();
        assert_eq!(progress.len(), 1, "{progress:?}");
        assert_eq!(progress[0].header.unwrap().revision, 3);
        f.close().await;
    }

    /// Watcher 5 is live when a progress request waits. A create for ID 5 on another key comes
    /// next, then a Cancel of ID 5. The create was a duplicate when it came, so it fails after
    /// the progress reply. The Cancel ends watcher 5, and no event reaches ID 5 again.
    #[tokio::test]
    async fn a_waiting_create_keeps_its_duplicate_decision() {
        let mut f = Fixture::new().await;
        f.watch(request(1, "k", 0)).await;
        f.watch(request(5, "k", 0)).await;
        f.put("k", "2").await;
        f.stream.progress().unwrap();
        f.stream
            .request_create(request(5, "other", 0))
            .await
            .unwrap();
        f.stream.cancel(5).await.unwrap();
        let canceled = f.responses();
        assert_eq!(canceled.len(), 1, "{canceled:?}");
        assert!(canceled[0].canceled && canceled[0].watch_id == 5);
        f.stream.settle().await.unwrap();
        let responses = f.responses();
        let created: Vec<_> = responses.iter().filter(|r| r.created).collect();
        assert_eq!(created.len(), 1, "{responses:?}");
        assert!(
            created[0].canceled && created[0].watch_id == -1,
            "{:?}",
            created[0]
        );
        assert_eq!(
            created[0].cancel_reason,
            "mvcc: duplicate watch ID provided on the WatchStream"
        );
        assert!(!f.stream.watchers.contains_key(&5));
        let batch = f.put("other", "3").await;
        f.stream.batch(&batch).await.unwrap();
        assert!(f.events().iter().all(|(id, _, _)| *id != 5));
        f.close().await;
    }

    /// Progress requests while a reply waits fold into one, so the queue stays bounded. A
    /// Cancel still gets its reply.
    #[tokio::test]
    async fn waiting_progress_requests_stay_bounded() {
        let mut f = Fixture::new().await;
        f.watch(request(1, "k", 0)).await;
        for _ in 0..1000 {
            f.stream.request_progress().unwrap();
            f.put("k", "v").await;
        }
        assert!(f.stream.progress.is_some());
        assert_eq!(f.stream.waiting.len(), 1);
        f.stream.cancel(1).await.unwrap();
        assert!(f.responses().iter().any(|r| r.canceled && r.watch_id == 1));
        f.close().await;
    }
}
