use super::*;
use crate::watch::{Batch, FeedState, PAGE_REVISIONS};
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use tokio::sync::{broadcast, watch};
use tokio_stream::wrappers::ReceiverStream;
use watch_request::RequestUnion;
use watch_server::Watch;

/// Longest interval between token expiry checks. Events do not use it.
const CHECK_INTERVAL: Duration = Duration::from_secs(1);
const MAX_WATCHERS: usize = 1024;

struct Watcher {
    _active: Option<crate::metrics::Active>,
    /// The create request. `Stream` keeps its start revision in `start`.
    request: WatchCreateRequest,
    /// The next revision that this watcher can receive.
    /// `None` means `position + 1` of the stream: the watcher follows the feed.
    start: Option<i64>,
    progress: tokio::time::Instant,
}

type Output = mpsc::Sender<Result<WatchResponse, Status>>;

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
        }
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
            let mut ids = vec![];
            for (revision, journal) in journals {
                self.candidates(journal, &mut ids);
                for id in &ids {
                    let start = self.start(&self.watchers[id]);
                    let watcher = self.watchers.get_mut(id).expect("indexed watcher");
                    if start > batch.previous && start <= *revision {
                        deliver(
                            &self.tx,
                            &self.meters,
                            &state,
                            &credentials,
                            watcher,
                            *revision,
                            journal,
                        )
                        .await?;
                    }
                }
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
        for id in compacted {
            self.remove(id);
            send(
                &self.tx,
                WatchResponse {
                    header: state.header(view.revision),
                    watch_id: id,
                    canceled: true,
                    compact_revision: view.compact_revision,
                    ..Default::default()
                },
            )
            .await?;
        }
        let Some(from) = pending.iter().map(|&(_, start)| start).min() else {
            return Ok(false);
        };
        let credentials = self.credentials();
        for (id, _) in &pending {
            permit(&view.auth, &credentials, &self.watchers[id].request)?;
        }
        let (rows, covered) = crate::watch::scan(view.state(), from, limit, PAGE_REVISIONS)
            .await
            .map_err(|e| Status::unavailable(format!("etcdserver: {}", e.message)))?;
        let mut ids = vec![];
        for (revision, journal) in &rows {
            self.candidates(journal, &mut ids);
            for id in &ids {
                // Starts do not change in this loop. A pending watcher has `start <= bound` and
                // is not compacted, so this condition selects the pending watchers.
                let watcher = self.watchers.get_mut(id).expect("indexed watcher");
                let start = watcher.start.unwrap_or(i64::MAX);
                if start <= bound && start <= *revision {
                    deliver(
                        &self.tx,
                        &self.meters,
                        &state,
                        &credentials,
                        watcher,
                        *revision,
                        journal,
                    )
                    .await?;
                }
            }
        }
        for (id, _) in &pending {
            let watcher = self.watchers.get_mut(id).expect("pending watcher");
            let start = watcher.start.as_mut().expect("pending start");
            *start = (*start).max(covered + 1);
        }
        Ok(covered < limit)
    }

    async fn catch_up_all(&mut self, through: Option<i64>) -> Result<(), Status> {
        while self.catch_up(through).await? {}
        Ok(())
    }

    /// Send automatic progress and recheck expiring tokens.
    async fn check(&mut self) -> Result<(), Status> {
        let state = self.current(true)?;
        self.check_all(&state)?;
        let header = state.header(self.position);
        let next = self.position + 1;
        for watcher in self.watchers.values_mut() {
            if watcher.request.progress_notify
                && watcher.start.unwrap_or(next) == next
                && watcher.progress.elapsed() >= self.progress_interval
            {
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
        }
        Ok(())
    }

    async fn create(&mut self, mut request: WatchCreateRequest) -> Result<(), Status> {
        let state = self.current(false)?;
        state
            .view
            .auth
            .authenticated(&self.credentials())
            .map_err(|e| e.status())?;
        if request.key.is_empty() {
            request.key = vec![0];
        }
        let reason = if !request.range_end.is_empty()
            && request.range_end != [0]
            && request.key >= request.range_end
        {
            "mvcc: watcher range is empty"
        } else if request.watch_id != 0 && self.watchers.contains_key(&request.watch_id) {
            "mvcc: duplicate watch ID provided on the WatchStream"
        } else if self.watchers.len() >= MAX_WATCHERS {
            "watch limit exceeded"
        } else {
            ""
        };
        if reason.is_empty() && request.watch_id == 0 {
            while self.watchers.contains_key(&self.next_id) {
                self.next_id += 1;
            }
            request.watch_id = self.next_id;
            self.next_id += 1;
        }
        send(
            &self.tx,
            WatchResponse {
                header: state.header(self.position),
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
        if request.start_revision <= 0 {
            request.start_revision = self.position + 1;
        }
        permit(&state.view.auth, &self.credentials(), &request)?;
        let watcher = Watcher {
            _active: self
                .meters
                .as_ref()
                .map(|m| crate::metrics::Active::new(&m.watchers)),
            start: None,
            request,
            progress: tokio::time::Instant::now(),
        };
        let start = watcher.request.start_revision;
        let historical = start <= self.position;
        self.insert(watcher, start);
        if historical {
            self.catch_up_all(None).await?;
        }
        Ok(())
    }

    async fn cancel(&mut self, id: i64) -> Result<(), Status> {
        if self.remove(id).is_some() {
            let state = self.current(false)?;
            send(
                &self.tx,
                WatchResponse {
                    header: state.header(self.position),
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
    async fn progress(&mut self) -> Result<(), Status> {
        let goal = self.current(false)?.view.revision;
        self.catch_up_all(Some(goal)).await?;
        let state = self.current(false)?;
        if !self.watchers.is_empty() && self.watchers.values().all(|w| self.start(w) <= goal + 1) {
            send(
                &self.tx,
                WatchResponse {
                    header: state.header(goal),
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

async fn deliver(
    tx: &Output,
    meters: &Option<Arc<crate::metrics::WatchMeters>>,
    state: &FeedState,
    credentials: &crate::auth::Credentials,
    watcher: &mut Watcher,
    revision: i64,
    journal: &crate::proto::state::Journal,
) -> Result<(), Status> {
    let events = crate::watch::select(
        state.view.state(),
        &watcher.request,
        revision,
        state.view.compact_revision,
        journal,
    )
    .await
    .map_err(|e| Status::unavailable(format!("etcdserver: {}", e.message)))?;
    if events.is_empty() {
        return Ok(());
    }
    permit(&state.view.auth, credentials, &watcher.request)?;
    let sent = events.len();
    send_events(
        tx,
        WatchResponse {
            header: state.header(revision),
            watch_id: watcher.request.watch_id,
            events,
            ..Default::default()
        },
        watcher.request.fragment,
    )
    .await?;
    if let Some(metrics) = meters {
        metrics.events.inc_by(sent as u64);
    }
    watcher.progress = tokio::time::Instant::now();
    Ok(())
}

impl Service {
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
        loop {
            let unsynced = stream.unsynced();
            tokio::select! {
                _ = tx.closed() => return Ok(()),
                received = events.recv() => stream.receive(received).await?,
                changed = changes.changed() => {
                    changed.map_err(|_| unavailable())?;
                    stream.state_changed()?;
                }
                _ = checks.tick() => stream.check().await?,
                _ = std::future::ready(()), if unsynced => {
                    stream.catch_up(None).await?;
                }
                message = input.message() => {
                    let Some(message) = message? else { return Ok(()); };
                    // Requests observe all batches that the node published before them.
                    stream.drain(&mut events).await?;
                    match message.request_union {
                        Some(RequestUnion::CreateRequest(request)) => stream.create(request).await?,
                        Some(RequestUnion::CancelRequest(request)) => {
                            stream.cancel(request.watch_id).await?
                        }
                        Some(RequestUnion::ProgressRequest(_)) => stream.progress().await?,
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
        feed: watch::Sender<FeedState>,
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
        f.stream.catch_up_all(None).await.unwrap();
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
        f.stream.progress().await.unwrap();
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

    #[tokio::test]
    async fn incomplete_batch_uses_storage_catch_up() {
        let mut f = Fixture::new().await;
        f.watch(request(1, "k", 0)).await;
        let large = "x".repeat(crate::watch::FEED_BATCH_BYTES + 1);
        let batch = f.put("k", &large).await;
        assert!(batch.journals().is_none());
        f.stream.batch(&batch).await.unwrap();
        assert!(f.events().is_empty());
        f.stream.catch_up_all(None).await.unwrap();
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
        let responses = f.responses();
        assert!(responses[0].created);
        assert!(responses[1].canceled);
        assert_eq!(responses[1].compact_revision, 3);
        assert!(f.stream.watchers.is_empty());
        f.close().await;
    }

    #[tokio::test]
    async fn progress_follows_history_and_future_watch_delays_it() {
        let mut f = Fixture::new().await;
        for n in 0..3 {
            let batch = f.put("k", &n.to_string()).await;
            f.stream.batch(&batch).await.unwrap();
        }
        f.watch(request(1, "k", 0)).await;
        f.stream.progress().await.unwrap();
        let progress = f.responses();
        assert_eq!(progress.len(), 1);
        assert_eq!(progress[0].watch_id, -1);
        assert_eq!(progress[0].header.as_ref().unwrap().revision, 4);
        f.watch(request(2, "k", 6)).await;
        f.stream.progress().await.unwrap();
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
}
