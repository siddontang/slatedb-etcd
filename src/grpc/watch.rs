use super::*;
use crate::watch::WatchPage;
use tokio_stream::wrappers::ReceiverStream;
use watch_request::RequestUnion;
use watch_server::Watch;

struct Watcher {
    request: WatchCreateRequest,
    progress: tokio::time::Instant,
    credentials: crate::auth::Credentials,
}

type Output = mpsc::Sender<Result<WatchResponse, Status>>;

impl Service {
    async fn watch_page(
        &self,
        request: WatchCreateRequest,
        require_leader: bool,
        through: Option<i64>,
        mut credentials: crate::auth::Credentials,
    ) -> Result<WatchPage, Status> {
        credentials.time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let (tx, rx) = oneshot::channel();
        self.events
            .send(Event::Authenticated(
                credentials,
                Box::new(Event::Watch(request, require_leader, through, tx)),
            ))
            .await
            .map_err(|_| Status::unavailable("etcdserver: node unavailable"))?;
        let value = tokio::time::timeout(Duration::from_secs(10), rx)
            .await
            .map_err(|_| Status::deadline_exceeded("etcdserver: watch request timed out"))?
            .map_err(|_| Status::unavailable("etcdserver: node unavailable"))?
            .map_err(|e| Status::unavailable(format!("etcdserver: {}", e.error)))?;
        serde_json::from_value::<Outcome<WatchPage>>(value)
            .map_err(|e| Status::internal(e.to_string()))?
            .result
            .map_err(|e| e.status())
    }

    async fn poll_watch(
        &self,
        watcher: &mut Watcher,
        tx: &Output,
        require_leader: bool,
        mut through: Option<i64>,
    ) -> Result<bool, Status> {
        loop {
            let page = self
                .watch_page(
                    watcher.request.clone(),
                    require_leader,
                    through,
                    watcher.credentials.clone(),
                )
                .await?;
            if page.compact_revision != 0 {
                send(
                    tx,
                    WatchResponse {
                        header: page.header,
                        watch_id: watcher.request.watch_id,
                        canceled: true,
                        compact_revision: page.compact_revision,
                        ..Default::default()
                    },
                )
                .await?;
                return Ok(false);
            }
            through = Some(page.header.as_ref().unwrap().revision);
            let caught_up = page.next_revision > page.header.as_ref().unwrap().revision;
            let had_events = !page.responses.is_empty();
            for response in page.responses {
                send_events(tx, response, watcher.request.fragment).await?;
            }
            watcher.request.start_revision = page.next_revision;
            if had_events {
                watcher.progress = tokio::time::Instant::now();
            }
            if !caught_up {
                continue;
            }
            if !had_events
                && watcher.request.start_revision == page.header.as_ref().unwrap().revision + 1
                && watcher.request.progress_notify
                && watcher.progress.elapsed() >= Duration::from_secs(10)
            {
                send(
                    tx,
                    WatchResponse {
                        header: page.header,
                        watch_id: watcher.request.watch_id,
                        ..Default::default()
                    },
                )
                .await?;
                watcher.progress = tokio::time::Instant::now();
            }
            return Ok(true);
        }
    }

    async fn watch_stream(
        &self,
        mut input: tonic::Streaming<WatchRequest>,
        tx: &Output,
        require_leader: bool,
        credentials: crate::auth::Credentials,
    ) -> Result<(), Status> {
        let (subscribe, receiver) = oneshot::channel();
        self.events
            .send(Event::SubscribeApply(subscribe))
            .await
            .map_err(|_| Status::unavailable("etcdserver: node unavailable"))?;
        let mut applied = receiver
            .await
            .map_err(|_| Status::unavailable("etcdserver: node unavailable"))?;
        let mut watchers = BTreeMap::<i64, Watcher>::new();
        let mut next_id = 0;
        let mut tick = tokio::time::interval(Duration::from_millis(50));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = tx.closed() => return Ok(()),
                changed = async {
                    tokio::select! {
                        _ = tick.tick() => Ok(()),
                        changed = applied.changed() => changed,
                    }
                } => {
                    changed.map_err(|_| Status::unavailable("etcdserver: node unavailable"))?;
                    let mut canceled = vec![];
                    for (&id, watcher) in &mut watchers {
                        if !self.poll_watch(watcher, tx, require_leader, None).await? {
                            canceled.push(id);
                        }
                    }
                    for id in canceled { watchers.remove(&id); }
                }
                message = input.message() => {
                    let Some(message) = message? else { return Ok(()); };
                    match message.request_union {
                        Some(RequestUnion::CreateRequest(mut request)) => {
                            if request.key.is_empty() { request.key = vec![0]; }
                            let reason = if !request.range_end.is_empty()
                                && request.range_end != [0] && request.key >= request.range_end {
                                "mvcc: watcher range is empty"
                            } else if request.watch_id != 0 && watchers.contains_key(&request.watch_id) {
                                "mvcc: duplicate watch ID provided on the WatchStream"
                            } else if watchers.len() >= 1024 {
                                "watch limit exceeded"
                            } else { "" };
                            if reason.is_empty() && request.watch_id == 0 {
                                while watchers.contains_key(&next_id) { next_id += 1; }
                                request.watch_id = next_id;
                                next_id += 1;
                            }
                            let page = self.watch_page(WatchCreateRequest::default(), require_leader, None, credentials.clone()).await?;
                            send(tx, WatchResponse { header: page.header,
                                watch_id: if reason.is_empty() { request.watch_id } else { -1 },
                                created: true, canceled: !reason.is_empty(), cancel_reason: reason.into(),
                                ..Default::default() }).await?;
                            if !reason.is_empty() { continue; }
                            if request.start_revision <= 0 { request.start_revision = page.next_revision; }
                            let id = request.watch_id;
                            let mut watcher = Watcher { request, progress: tokio::time::Instant::now(), credentials: credentials.clone() };
                            if self.poll_watch(&mut watcher, tx, require_leader, None).await? {
                                watchers.insert(id, watcher);
                            }
                        }
                        Some(RequestUnion::CancelRequest(request)) => {
                            if watchers.remove(&request.watch_id).is_some() {
                                let page = self.watch_page(WatchCreateRequest::default(), require_leader, None, credentials.clone()).await?;
                                send(tx, WatchResponse { header: page.header, watch_id: request.watch_id,
                                    canceled: true, ..Default::default() }).await?;
                            }
                        }
                        Some(RequestUnion::ProgressRequest(_)) => {
                            let page = self.watch_page(WatchCreateRequest::default(), require_leader, None, credentials.clone()).await?;
                            let goal = page.header.as_ref().unwrap().revision;
                            let mut canceled = vec![];
                            for (&id, watcher) in &mut watchers {
                                if !self.poll_watch(watcher, tx, require_leader, Some(goal)).await? {
                                    canceled.push(id);
                                }
                            }
                            for id in canceled { watchers.remove(&id); }
                            if !watchers.is_empty() && watchers.values().all(|w|
                                w.request.start_revision <= goal + 1) {
                                send(tx, WatchResponse { header: page.header, watch_id: -1,
                                    ..Default::default() }).await?;
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
        let credentials = credentials(&request);
        let require_leader = request.metadata().contains_key("hasleader");
        let service = self.clone();
        let (tx, rx) = mpsc::channel(32);
        tokio::spawn(async move {
            if let Err(error) = service
                .watch_stream(request.into_inner(), &tx, require_leader, credentials)
                .await
            {
                let _ = tx.send(Err(error)).await;
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}
