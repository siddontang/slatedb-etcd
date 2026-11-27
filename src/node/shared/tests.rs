//! Writer task, publisher, and member tests of shared mode.

use super::*;

/// The shared-mode options of one test voter.
pub(super) fn voter_options(empty_log: bool) -> crate::state::shared::Options {
    crate::state::shared::Options {
        quota: 0,
        voters: vec![1],
        join: false,
        empty_log,
        pin_max: Duration::from_secs(300),
        overlay_max_bytes: 1 << 30,
        restore: None,
        record: None,
        store_id: None,
    }
}
use crate::state::overlay::Overlay;
use slatedb::object_store::memory::InMemory;
use slatedb::object_store::ObjectStore;

fn batch(seq: u64, rows: &[(&str, &str)]) -> Arc<crate::WriteBatch> {
    let mut batch = crate::WriteBatch::new();
    for (key, value) in rows {
        batch.put(key, value);
    }
    batch.put(b"meta/applied", seq.to_be_bytes());
    Arc::new(batch)
}

async fn value(db: &slatedb::Db, key: &str) -> Option<bytes::Bytes> {
    db.get(key.as_bytes()).await.unwrap()
}

#[tokio::test]
async fn the_materializer_writes_whole_batches_with_their_sequences() {
    let db = Arc::new(
        slatedb::Db::builder("state", Arc::new(InMemory::new()))
            .with_settings(crate::state::slatedb_settings())
            .build()
            .await
            .unwrap(),
    );
    // Entries 1 and 2 reached L0 before a restart.
    let mut first = Overlay::new(0);
    first.push(1, batch(1, &[("a", "1")]));
    first.push(2, batch(2, &[("b", "2")]));
    for (seq, batch) in first.queue().after(0, 10) {
        db.write_with_options(batch.to_storage(), &WriteOptions { seqnum: seq })
            .await
            .unwrap();
    }
    db.flush().await.unwrap();
    assert_eq!(db.manifest().last_l0_seq(), 2);

    // The replay grouped entries 2..=4 into one batch. It spans `last_l0_seq` and holds
    // final values, so the materializer writes it whole at sequence 4.
    let mut replay = Overlay::new(1);
    replay.push(4, batch(4, &[("b", "2"), ("c", "3"), ("d", "4")]));
    let progress = Arc::new(Progress::new(db.manifest().last_l0_seq()));
    let task = tokio::spawn(materialize(
        db.clone(),
        replay.queue(),
        progress.clone(),
        clock::Gate::unchecked(),
    ));
    progress.wait_written(4).await.unwrap();
    assert_eq!(db.snapshot().unwrap().seq(), 4);
    assert_eq!(value(&db, "c").await, Some("3".into()));
    assert_eq!(
        value(&db, "meta/applied").await.unwrap().as_ref(),
        4_u64.to_be_bytes()
    );
    // A later batch keeps its own sequence.
    replay.push(7, batch(7, &[("a", "7")]));
    progress.wait_written(7).await.unwrap();
    assert_eq!(db.snapshot().unwrap().seq(), 7);
    assert_eq!(value(&db, "a").await, Some("7".into()));
    assert_eq!(value(&db, "b").await, Some("2".into()));
    assert!(progress.deadline(Duration::from_secs(1)).is_some());
    assert!(!task.is_finished());
    task.abort();
    db.close().await.unwrap();
}

#[test]
fn flushes_follow_the_oldest_write_and_publication_requests() {
    let progress = Progress::new(0);
    let long = Duration::from_secs(60);
    // An idle writer has no deadline and no flush.
    assert_eq!(progress.deadline(long), None);
    assert_eq!(progress.take_flush(Duration::ZERO), (false, None));
    progress.wrote(1);
    let deadline = progress.deadline(long).unwrap();
    progress.wrote(2);
    // A later write keeps the deadline of the oldest unflushed write.
    assert_eq!(progress.deadline(long), Some(deadline));
    assert_eq!(progress.take_flush(long), (false, None));
    assert_eq!(progress.take_flush(Duration::ZERO), (true, None));
    assert_eq!(progress.take_flush(Duration::ZERO), (false, None));
    assert_eq!(progress.deadline(long), None);
    // A request waits until the materializer writes its sequence.
    progress.request(3);
    assert_eq!(progress.take_flush(long), (false, None));
    progress.wrote(3);
    assert_eq!(progress.take_flush(long), (true, Some(3)));
    assert_eq!(progress.take_flush(long), (false, None));
    // A request that the written sequence already covers is due at once.
    progress.request(2);
    assert_eq!(progress.take_flush(long), (true, Some(2)));
}

/// Read `/status` from the client listener.
pub(super) async fn status(server: &crate::server::Server) -> Value {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut stream = tokio::net::TcpStream::connect(server.client_addr())
        .await
        .unwrap();
    let request = "GET /status HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await.unwrap();
    let response = String::from_utf8(response).unwrap();
    let body = response.split_once("\r\n\r\n").unwrap().1;
    let body = &body[body.find('{').unwrap()..=body.rfind('}').unwrap()];
    serde_json::from_str(body).unwrap()
}

pub(super) async fn eventually(what: &str, mut check: impl std::ops::AsyncFnMut() -> bool) {
    for _ in 0..600 {
        if check().await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("timed out waiting for {what}");
}

/// Stall S3 on one voter until apply pauses, then recover S3 and send no new request. The
/// leader still publishes a checkpoint at or below its applied index, and the paused member
/// switches to it out of band. With `early`, the stall starts before the first announcement,
/// so the switch must release the open segment.
async fn stall_and_recover(early: bool) {
    use crate::object::faults::{FaultStore, STALL_PUT};
    use crate::proto::etcdserverpb::{kv_client::KvClient, PutRequest};
    let limit = 64 * 1024;
    let faults = Arc::new(FaultStore::new(Arc::new(InMemory::new())));
    let dir = tempfile::tempdir().unwrap();
    let mut config = crate::server::ServerConfig::single_node(dir.path());
    config.state_mode = crate::state::StateMode::Shared;
    config.object_store = crate::object::Config::Test(faults.clone(), "root".into());
    config.view_max_age = Duration::from_millis(100);
    config.overlay_max_bytes = limit;
    let server = crate::server::Server::start(config).await.unwrap();
    if early {
        faults.set(STALL_PUT);
    }
    server.wait_ready(Duration::from_secs(15)).await.unwrap();
    let shared = |status: &Value, field: &str| status["shared_state"][field].clone();
    if !early {
        eventually("the first announcement", async || {
            shared(&status(&server).await, "applied_view_checkpoints") == 1
        })
        .await;
        faults.set(STALL_PUT);
    }
    let channel = tonic::transport::Endpoint::from_shared(server.client_url())
        .unwrap()
        .connect()
        .await
        .unwrap();
    // Each write alone applies above twice the soft limit, so the pause does not depend on
    // how many writes the admission check lets in. No checkpoint can appear.
    let puts = (0..8_u8).map(|n| {
        let mut kv = KvClient::new(channel.clone());
        async move {
            kv.put(PutRequest {
                key: vec![b'k', n],
                value: vec![n; 96 * 1024],
                ..Default::default()
            })
            .await
        }
    });
    let written = futures::future::join_all(puts).await;
    assert!(written.iter().any(Result::is_ok));
    eventually("the apply pause", async || {
        status(&server).await["shared_state"]["writer"]["paused"] == true
    })
    .await;
    let paused = status(&server).await;
    assert!(shared(&paused, "overlay_bytes").as_u64().unwrap() > 2 * limit);
    // S3 recovers. No client sends a request.
    faults.set(0);
    eventually("the resumed apply", async || {
        let status = status(&server).await;
        status["shared_state"]["writer"]["paused"] == false
            && shared(&status, "overlay_bytes").as_u64().unwrap() < limit
            && status["applied_index"] == status["commit_index"]
    })
    .await;
    let mut kv = KvClient::new(channel);
    let reply = kv
        .range(crate::proto::etcdserverpb::RangeRequest {
            key: vec![b'k'],
            range_end: vec![b'l'],
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    // The admitted writes are readable. The admission check rejected the others.
    assert!(!reply.kvs.is_empty());
    server.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stalled_store_pauses_apply_until_an_out_of_band_switch() {
    stall_and_recover(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stall_before_the_first_announcement_releases_the_open_segment() {
    stall_and_recover(true).await;
}

/// Status answers while the first leader cannot create the shared manifest, and after.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn status_answers_before_the_writer_creates_the_manifest() {
    use crate::object::faults::{FaultStore, STALL_STATE_PUT};
    use crate::proto::etcdserverpb::{
        kv_client::KvClient, maintenance_client::MaintenanceClient, PutRequest, StatusRequest,
    };
    let faults = Arc::new(FaultStore::new(Arc::new(InMemory::new())));
    // The claims succeed. The writer cannot create the shared manifest.
    faults.set(STALL_STATE_PUT);
    let dir = tempfile::tempdir().unwrap();
    let mut config = crate::server::ServerConfig::single_node(dir.path());
    config.state_mode = crate::state::StateMode::Shared;
    config.object_store = crate::object::Config::Test(faults.clone(), "root".into());
    let server = crate::server::Server::start(config).await.unwrap();
    server.wait_ready(Duration::from_secs(15)).await.unwrap();
    let channel = tonic::transport::Endpoint::from_shared(server.client_url())
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut maintenance = MaintenanceClient::new(channel.clone());
    for _ in 0..3 {
        // No SST exists yet, so the size is the logical size of the overlay.
        let reply = maintenance
            .status(StatusRequest {})
            .await
            .unwrap()
            .into_inner();
        assert_eq!(reply.db_size, reply.db_size_in_use);
    }
    faults.set(0);
    let mut kv = KvClient::new(channel);
    kv.put(PutRequest {
        key: b"key".to_vec(),
        value: b"value".to_vec(),
        ..Default::default()
    })
    .await
    .unwrap();
    eventually("the first checkpoint", async || {
        status(&server).await["shared_state"]["applied_view_checkpoints"]
            .as_u64()
            .unwrap()
            > 0
    })
    .await;
    maintenance.status(StatusRequest {}).await.unwrap();
    server.shutdown().await.unwrap();
}

/// A restart whose base and snapshot record share an index must agree on its term.
#[tokio::test]
async fn a_restart_refuses_a_base_with_another_term_at_the_record() {
    let dir = tempfile::tempdir().unwrap();
    let mut storage = RaftStorage::open(dir.path(), 1, "test", vec![1]).unwrap();
    let mut snapshot = raft::eraftpb::Snapshot::default();
    snapshot.mut_metadata().index = 1;
    snapshot.mut_metadata().term = 3;
    snapshot.data = crate::state::transfer::ViewDescriptor::new(uuid::Uuid::new_v4())
        .encode()
        .unwrap();
    storage.adopt(&snapshot).unwrap();
    let options = voter_options(false);
    let mut state = StateMachine::open_shared(
        Arc::new(InMemory::new()),
        "root",
        "test",
        crate::state::StateCache::default(),
        &options,
        crate::state::transfer::Backend::Local,
    )
    .await
    .unwrap();
    state.entry_term = 2;
    state.apply(1, None).await.unwrap();
    let error = Node::align_log(&mut storage, &state).unwrap_err();
    assert!(error.to_string().contains("differs"), "{error}");
    state.entry_term = 3;
    state.applied_term = 3;
    Node::align_log(&mut storage, &state).unwrap();
    state.close().await.unwrap();
}

/// One key changes 16 times. A Watch with prev_kv catches up from the base with a short pin
/// and slow object reads. It receives every revision once, each with its previous value.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_slow_prev_kv_catch_up_makes_progress() {
    use crate::object::faults::{FaultStore, DELAY_SST_GET};
    use crate::proto::etcdserverpb::{
        kv_client::KvClient, watch_client::WatchClient, watch_request, PutRequest,
        WatchCreateRequest, WatchRequest,
    };
    let faults = Arc::new(FaultStore::new(Arc::new(InMemory::new())));
    let dir = tempfile::tempdir().unwrap();
    let mut config = crate::server::ServerConfig::single_node(dir.path());
    config.state_mode = crate::state::StateMode::Shared;
    config.object_store = crate::object::Config::Test(faults.clone(), "root".into());
    config.view_max_age = Duration::from_millis(100);
    config.view_pin_max = Duration::from_millis(24);
    // A tiny cache, so history reads go to the store.
    config.state_cache = crate::state::StateCache {
        block_bytes: 1,
        meta_bytes: 1,
        row_bytes: 0,
    };
    let server = crate::server::Server::start(config).await.unwrap();
    server.wait_ready(Duration::from_secs(15)).await.unwrap();
    let channel = tonic::transport::Endpoint::from_shared(server.client_url())
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut kv = KvClient::new(channel.clone());
    let mut revisions = vec![];
    for n in 0..16 {
        let reply = kv
            .put(PutRequest {
                key: b"key".to_vec(),
                value: format!("v{n}").into_bytes(),
                ..Default::default()
            })
            .await
            .unwrap();
        revisions.push(reply.into_inner().header.unwrap().revision);
    }
    // The base covers every write when the overlay holds no batch. The applied index can be
    // higher: a later `ViewCheckpoint` entry changes no state, so no base reaches it.
    eventually("a base that covers the writes", async || {
        let status = status(&server).await;
        status["shared_state"]["overlay_bytes"].as_u64() == Some(0)
            && status["shared_state"]["base_applied_index"].as_u64() > Some(0)
    })
    .await;
    faults.set_delay(Duration::from_millis(4));
    faults.set(DELAY_SST_GET);
    let (send, receive) = mpsc::channel(1);
    send.send(WatchRequest {
        request_union: Some(watch_request::RequestUnion::CreateRequest(
            WatchCreateRequest {
                key: b"key".to_vec(),
                start_revision: revisions[0],
                prev_kv: true,
                ..Default::default()
            },
        )),
    })
    .await
    .unwrap();
    let mut stream = WatchClient::new(channel)
        .watch(tokio_stream::wrappers::ReceiverStream::new(receive))
        .await
        .unwrap()
        .into_inner();
    let mut events = vec![];
    while events.len() < revisions.len() {
        let message = tokio::time::timeout(Duration::from_secs(20), stream.message())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        events.extend(message.events);
    }
    let seen: Vec<_> = events
        .iter()
        .map(|event| event.kv.as_ref().unwrap().mod_revision)
        .collect();
    assert_eq!(seen, revisions);
    for (n, event) in events.iter().enumerate().skip(1) {
        let previous = event.prev_kv.as_ref().expect("prev_kv");
        assert_eq!(previous.value, format!("v{}", n - 1).into_bytes());
    }
    faults.set(0);
    drop(send);
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_new_writer_waits_for_its_clock_and_refuses_a_large_skew() {
    use std::sync::atomic::Ordering;
    let clock = Clock::system(Duration::from_secs(5), None);
    // The manifest clock is 300 ms ahead. The writer waits until the local clock passes it.
    let started = Instant::now();
    assert!(clock.pass((clock.now_ms)() + 300).await);
    assert!(started.elapsed() >= Duration::from_millis(250));
    assert!(!clock.alarm.load(Ordering::Relaxed));
    // A clock 10 s behind does not start the writer, and it raises the alarm.
    let started = Instant::now();
    assert!(!clock.pass((clock.now_ms)() + 10_000).await);
    assert!(started.elapsed() < Duration::from_secs(1));
    assert!(clock.alarm.load(Ordering::Relaxed));
    assert!(clock.skew.load(Ordering::Relaxed) >= 9_000);
    // A later check within the limit clears the alarm.
    assert!(clock.pass((clock.now_ms)() - 1).await);
    assert!(!clock.alarm.load(Ordering::Relaxed));
}

/// The open of an older term finishes after the current leader's open and fences it. The
/// current leader opens its writer again, and its overlay batches still reach a checkpoint.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fenced_leader_opens_its_writer_again() {
    let options = voter_options(false);
    let mut state = StateMachine::open_shared(
        Arc::new(InMemory::new()),
        "root",
        "test",
        crate::state::StateCache::default(),
        &options,
        crate::state::transfer::Backend::Local,
    )
    .await
    .unwrap();
    state.members = Arc::new(crate::cluster::Store::initial(
        &[(1, "127.0.0.1:1".to_owned())].into(),
    ));
    state.seed_for_test(1).await.unwrap();
    let handles = state.shared_handles().unwrap();
    let (events, mut announced) = mpsc::channel(64);
    let clock = Clock::system(Duration::from_secs(5), None);
    let mut writer = Writer::new(&handles, Duration::from_millis(50), clock);
    writer.sync(true, 2, &events);
    writer.progress.wait_written(1).await.unwrap();
    // The leader of term 1 opens late. Its open fences the writer of term 2. The current
    // leader can open again before this open ends, which fails the late open instead.
    let old = handles.source.open_writer().await.ok();
    let put = |key: &str| crate::proto::etcdserverpb::RequestOp {
        request: Some(crate::proto::etcdserverpb::request_op::Request::RequestPut(
            crate::proto::etcdserverpb::PutRequest {
                key: key.as_bytes().to_vec(),
                value: b"value".to_vec(),
                ..Default::default()
            },
        )),
    };
    state.apply_etcd(2, &put("after-fence")).await.unwrap();
    // A checkpoint above the fence appears. The writer task did not stop the member.
    let covered = tokio::time::timeout(Duration::from_secs(20), async {
        while let Some(event) = announced.recv().await {
            if let Event::ViewCheckpoint(id, _, reply) = event {
                if let Some(reply) = reply {
                    let _ = reply.send(true);
                }
                let base = handles.source.open_base(id).await.unwrap();
                if base.applied >= 2 {
                    return base;
                }
            }
        }
        panic!("the writer stopped");
    })
    .await
    .unwrap();
    let value = covered.get(b"etcd/current/after-fence").await.unwrap();
    assert!(value.is_some());
    assert!(writer.tasks.try_join_next().is_none());
    // The current leader saw the fence and opened its writer again.
    assert!(writer.reopens.load(std::sync::atomic::Ordering::Relaxed) >= 1);
    // The writer of term 1 is fenced now. Its next flush cannot commit a manifest.
    if let Some(old) = old {
        let flushed = async {
            old.put(b"orphan", b"value").await?;
            old.flush().await
        };
        let error = flushed.await.unwrap_err();
        assert!(fenced(&anyhow::Error::from(error)));
    }
    writer.sync(false, 2, &events);
    writer.stop().await;
    state.close().await.unwrap();
}

/// The announcement events of a publisher. The test answers for the node.
pub(super) struct Announcements(pub(super) mpsc::Receiver<Event>);

impl Announcements {
    /// Wait for the next announcement and reply whether Raft took it.
    pub(super) async fn next(&mut self, wait: Duration, accepted: bool) -> uuid::Uuid {
        tokio::time::timeout(wait, async {
            loop {
                if let Some(Event::ViewCheckpoint(id, _, reply)) = self.0.recv().await {
                    if let Some(reply) = reply {
                        let _ = reply.send(accepted);
                    }
                    return id;
                }
            }
        })
        .await
        .expect("an announcement")
    }

    /// Wait `wait` and fail on any announcement in it.
    async fn none(&mut self, wait: Duration) {
        let next = tokio::time::timeout(wait, self.0.recv()).await;
        assert!(
            !matches!(next, Ok(Some(Event::ViewCheckpoint(..)))),
            "an unexpected announcement"
        );
    }
}

/// Open a one-voter shared state. A first writer publishes a checkpoint at index 1, and its
/// announcement never reaches Raft. Return the state, its handles, the event sender, the
/// announcements, and the stranded checkpoint.
async fn stranded_checkpoint() -> (
    StateMachine,
    Handles,
    mpsc::Sender<Event>,
    Announcements,
    uuid::Uuid,
) {
    stranded_checkpoint_on(Arc::new(InMemory::new())).await
}

/// `stranded_checkpoint` over `store`.
pub(super) async fn stranded_checkpoint_on(
    store: Arc<dyn ObjectStore>,
) -> (
    StateMachine,
    Handles,
    mpsc::Sender<Event>,
    Announcements,
    uuid::Uuid,
) {
    let options = voter_options(false);
    let mut state = StateMachine::open_shared(
        store,
        "root",
        "test",
        crate::state::StateCache::default(),
        &options,
        crate::state::transfer::Backend::Local,
    )
    .await
    .unwrap();
    state.members = Arc::new(crate::cluster::Store::initial(
        &[(1, "127.0.0.1:1".to_owned())].into(),
    ));
    state.seed_for_test(1).await.unwrap();
    let handles = state.shared_handles().unwrap();
    let (events, received) = mpsc::channel(64);
    let mut announcements = Announcements(received);
    let mut first = Writer::new(&handles, STRANDED_AGE, clock());
    first.sync(true, 2, &events);
    let lost = announcements.next(Duration::from_secs(20), true).await;
    first.sync(false, 2, &events);
    first.stop().await;
    (state, handles, events, announcements, lost)
}

pub(super) const STRANDED_AGE: Duration = Duration::from_millis(50);

fn clock() -> Clock {
    Clock::system(Duration::from_secs(5), None)
}

/// The next writer has no writes. It announces the stranded checkpoint, and Raft drops the
/// proposal, as during a leader transfer. On the idle cluster the publisher announces it
/// again within the backoff limit. The base advances and a physical compaction waiter ends.
/// The adopted checkpoint causes no further announcement, also from a later writer.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_new_writer_announces_a_stranded_checkpoint() {
    let (mut state, handles, events, mut announcements, lost) = stranded_checkpoint().await;
    // A physical compaction waits for a base that covers index 1.
    let mut based = handles.based.clone();
    assert_eq!(*based.borrow(), 0);
    let waiter = tokio::spawn(async move { based.wait_for(|base| *base >= 1).await.is_ok() });
    let mut second = Writer::new(&handles, STRANDED_AGE, clock());
    second.sync(true, 3, &events);
    assert_eq!(
        announcements.next(Duration::from_secs(2), false).await,
        lost
    );
    // No write and no request follows. The retry timer sends it again.
    let limit = RETRY_MAX + Duration::from_millis(500);
    assert_eq!(announcements.next(limit, true).await, lost);
    state
        .apply_view_checkpoint(2, &lost.to_string(), 0)
        .await
        .unwrap();
    state.settle_opens().await;
    state.switch_base();
    assert!(tokio::time::timeout(Duration::from_secs(5), waiter)
        .await
        .unwrap()
        .unwrap());
    // Requests and retries over several flush ages do not announce it again.
    for _ in 0..4 {
        second.request_publication(1);
        announcements.none(STRANDED_AGE).await;
    }
    announcements.none(limit).await;
    second.sync(false, 3, &events);
    second.stop().await;
    // A later writer does not announce it either.
    let mut third = Writer::new(&handles, STRANDED_AGE, clock());
    third.sync(true, 4, &events);
    third.progress.wait_written(1).await.unwrap();
    third.request_publication(1);
    announcements.none(limit).await;
    assert!(third.tasks.try_join_next().is_none());
    third.sync(false, 4, &events);
    third.stop().await;
    state.close().await.unwrap();
}

/// A storage error in the stranded check does not drop it. The publisher checks again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_stranded_check_is_retried() {
    let (state, handles, events, mut announcements, lost) = stranded_checkpoint().await;
    let mut second = Writer::new(&handles, STRANDED_AGE, clock());
    let failures = second.applied.failures.clone();
    failures.store(3, std::sync::atomic::Ordering::Relaxed);
    second.sync(true, 3, &events);
    let limit = RETRY_MAX + Duration::from_secs(1);
    assert_eq!(announcements.next(limit, true).await, lost);
    assert_eq!(failures.load(std::sync::atomic::Ordering::Relaxed), 0);
    assert!(second.tasks.try_join_next().is_none());
    second.sync(false, 3, &events);
    second.stop().await;
    state.close().await.unwrap();
}

/// The announcement of a checkpoint applies, and the member cannot read the checkpoint.
/// After the store recovers, the member opens it and switches its base. No client write
/// and no new Raft entry is needed, and a physical compaction waiter ends.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_base_open_is_retried_on_an_idle_member() {
    use crate::object::faults::{FaultStore, FAIL_GET};
    let faults = Arc::new(FaultStore::new(Arc::new(InMemory::new())));
    let options = voter_options(false);
    // A tiny cache, so the open reads the store.
    let cache = crate::state::StateCache {
        block_bytes: 1,
        meta_bytes: 1,
        row_bytes: 0,
    };
    let mut state = StateMachine::open_shared(
        faults.clone(),
        "root",
        "test",
        cache,
        &options,
        crate::state::transfer::Backend::Local,
    )
    .await
    .unwrap();
    state.members = Arc::new(crate::cluster::Store::initial(
        &[(1, "127.0.0.1:1".to_owned())].into(),
    ));
    state.seed_for_test(1).await.unwrap();
    let handles = state.shared_handles().unwrap();
    let (events, received) = mpsc::channel(64);
    let mut announcements = Announcements(received);
    let mut writer = Writer::new(&handles, STRANDED_AGE, clock());
    writer.sync(true, 2, &events);
    let id = announcements.next(Duration::from_secs(20), true).await;
    writer.sync(false, 2, &events);
    writer.stop().await;
    let mut based = handles.based.clone();
    let waiter = tokio::spawn(async move { based.wait_for(|base| *base >= 1).await.is_ok() });
    // The announcement applies while every read fails.
    faults.set(FAIL_GET);
    state
        .apply_view_checkpoint(2, &id.to_string(), 0)
        .await
        .unwrap();
    let applied = state.applied;
    faults.entered.acquire().await.unwrap().forget();
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(handles.opened.newest(), None);
    assert_eq!(*handles.based.borrow(), 0);
    // The store recovers. Only the retry of the open runs.
    faults.set(0);
    let mut ready = handles.opened.subscribe();
    tokio::time::timeout(
        RETRY_MAX + Duration::from_secs(2),
        ready.wait_for(|ready| *ready >= 1),
    )
    .await
    .unwrap()
    .unwrap();
    state.switch_base();
    assert!(tokio::time::timeout(Duration::from_secs(5), waiter)
        .await
        .unwrap()
        .unwrap());
    assert_eq!(state.applied, applied);
    state.close().await.unwrap();
}

/// Announcements arrive faster than one open takes. A started open finishes, so the base
/// advances while announcements still arrive, and it reaches the newest checkpoint.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_base_advances_under_a_stream_of_announcements() {
    use crate::object::faults::{FaultStore, DELAY_SST_GET};
    let faults = Arc::new(FaultStore::new(Arc::new(InMemory::new())));
    let options = voter_options(false);
    // A tiny cache, so each open reads the store.
    let cache = crate::state::StateCache {
        block_bytes: 1,
        meta_bytes: 1,
        row_bytes: 0,
    };
    let mut state = StateMachine::open_shared(
        faults.clone(),
        "root",
        "test",
        cache,
        &options,
        crate::state::transfer::Backend::Local,
    )
    .await
    .unwrap();
    state.members = Arc::new(crate::cluster::Store::initial(
        &[(1, "127.0.0.1:1".to_owned())].into(),
    ));
    state.seed_for_test(1).await.unwrap();
    let handles = state.shared_handles().unwrap();
    let (events, received) = mpsc::channel(64);
    let mut announcements = Announcements(received);
    let mut writer = Writer::new(&handles, STRANDED_AGE, clock());
    writer.sync(true, 2, &events);
    // Sixteen checkpoints, each after one more write. Raft takes none of them yet.
    let mut checkpoints = vec![announcements.next(Duration::from_secs(20), true).await];
    for n in 0..15 {
        let put = crate::proto::etcdserverpb::RequestOp {
            request: Some(crate::proto::etcdserverpb::request_op::Request::RequestPut(
                crate::proto::etcdserverpb::PutRequest {
                    key: format!("key-{n}").into_bytes(),
                    value: b"value".to_vec(),
                    ..Default::default()
                },
            )),
        };
        let index = state.applied + 1;
        state.apply_etcd(index, &put).await.unwrap();
        loop {
            let id = announcements.next(Duration::from_secs(20), true).await;
            let base = handles.source.open_base(id).await.unwrap();
            if base.applied == index {
                checkpoints.push(id);
                break;
            }
        }
    }
    writer.sync(false, 2, &events);
    writer.stop().await;
    let newest = state.applied;
    // Each object read takes 70 ms. The announcements come 40 ms apart.
    faults.set_delay(Duration::from_millis(70));
    faults.set(DELAY_SST_GET);
    let mut advanced = 0;
    for id in &checkpoints {
        let index = state.applied + 1;
        state
            .apply_view_checkpoint(index, &id.to_string(), 0)
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(40)).await;
        state.switch_base();
        advanced = advanced.max(*handles.based.borrow());
    }
    assert!(
        advanced >= 1,
        "the base did not advance during the announcements"
    );
    let mut ready = handles.opened.subscribe();
    tokio::time::timeout(
        Duration::from_secs(20),
        ready.wait_for(|ready| *ready >= newest),
    )
    .await
    .unwrap()
    .unwrap();
    state.switch_base();
    assert_eq!(*handles.based.borrow(), newest);
    faults.set(0);
    state.close().await.unwrap();
}

/// A manual checkpoint arrives after its entry commits and before the entry applies. The
/// publication request waits for apply, so it covers the batch of that entry.
#[tokio::test]
async fn a_manual_checkpoint_requests_its_publication_after_apply() {
    let (mut state, handles, _events, _announcements, _) = stranded_checkpoint().await;
    let mut writer = Writer::new(&handles, Duration::from_secs(60), clock());
    let index = state.applied + 1;
    // Committed at `index`, applied below it: no request yet.
    assert!(!writer.request_checkpoint(index, index - 1));
    let put = crate::proto::etcdserverpb::RequestOp {
        request: Some(crate::proto::etcdserverpb::request_op::Request::RequestPut(
            crate::proto::etcdserverpb::PutRequest {
                key: b"key".to_vec(),
                value: b"value".to_vec(),
                ..Default::default()
            },
        )),
    };
    state.apply_etcd(index, &put).await.unwrap();
    assert!(writer.request_checkpoint(index, index));
    assert_eq!(
        writer.progress.inner.lock().unwrap().publish_through,
        Some(index)
    );
    assert!(!writer.request_checkpoint(index, index));
    state.close().await.unwrap();
}

/// Apply announcement `id` at the next index and switch to its base.
pub(super) async fn adopt_announced(state: &mut StateMachine, id: uuid::Uuid) {
    let index = state.applied + 1;
    state
        .apply_view_checkpoint(index, &id.to_string(), 0)
        .await
        .unwrap();
    state.settle_opens().await;
    state.switch_base();
    assert_eq!(state.base_checkpoint(), Some(id));
}

/// A Defragment publishes a newer checkpoint at the base index, and the leader changes
/// before its announcement applies. The next writer announces it, because the identity of
/// the newest checkpoint differs from the base, and the members adopt it with no write.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_new_writer_announces_a_same_index_checkpoint() {
    let (mut state, handles, events, mut announcements, first) = stranded_checkpoint().await;
    adopt_announced(&mut state, first).await;
    let mut second = Writer::new(&handles, STRANDED_AGE, clock());
    second.sync(true, 3, &events);
    second.progress.wait_written(1).await.unwrap();
    announcements.none(Duration::from_millis(300)).await;
    // The Defragment republish. Its announcement does not apply before the change.
    second.progress.republish();
    let compacted = announcements.next(Duration::from_secs(5), true).await;
    assert_ne!(compacted, first);
    let base = handles.source.open_base(compacted).await.unwrap();
    assert_eq!(base.applied, state.shared.as_ref().unwrap().base.applied);
    drop(base);
    second.sync(false, 3, &events);
    second.stop().await;
    let mut third = Writer::new(&handles, STRANDED_AGE, clock());
    third.sync(true, 4, &events);
    assert_eq!(
        announcements.next(Duration::from_secs(2), true).await,
        compacted
    );
    adopt_announced(&mut state, compacted).await;
    announcements
        .none(RETRY_MAX + Duration::from_millis(500))
        .await;
    third.sync(false, 4, &events);
    third.stop().await;
    state.close().await.unwrap();
}

/// A manual checkpoint requests its publication after apply and before the newly elected
/// writer opens. The open keeps the request, so the writer publishes and announces the
/// covering checkpoint at once, not after the flush age.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_checkpoint_request_survives_the_writer_open() {
    let (mut state, handles, events, mut announcements, first) = stranded_checkpoint().await;
    adopt_announced(&mut state, first).await;
    let put = crate::proto::etcdserverpb::RequestOp {
        request: Some(crate::proto::etcdserverpb::request_op::Request::RequestPut(
            crate::proto::etcdserverpb::PutRequest {
                key: b"key".to_vec(),
                value: b"value".to_vec(),
                ..Default::default()
            },
        )),
    };
    let index = state.applied + 1;
    state.apply_etcd(index, &put).await.unwrap();
    let mut writer = Writer::new(&handles, Duration::from_secs(90), clock());
    assert!(writer.request_checkpoint(index, index));
    writer.sync(true, 3, &events);
    let id = announcements.next(Duration::from_secs(2), true).await;
    assert_eq!(handles.source.open_base(id).await.unwrap().applied, index);
    writer.sync(false, 3, &events);
    writer.stop().await;
    state.close().await.unwrap();
}

/// A one-voter shared state on `store`, opened without a restore, with its handles.
async fn ordinary(store: Arc<dyn ObjectStore>) -> (StateMachine, Handles) {
    let options = voter_options(true);
    let mut state = StateMachine::open_shared(
        store,
        "root",
        "test",
        crate::state::StateCache::default(),
        &options,
        crate::state::transfer::Backend::Local,
    )
    .await
    .unwrap();
    state.members = Arc::new(crate::cluster::Store::initial(
        &[(1, "127.0.0.1:1".to_owned())].into(),
    ));
    let handles = state.shared_handles().unwrap();
    (state, handles)
}

/// A checked image of one row with value `id`, for a restore by member 1.
fn snapshot(dir: &std::path::Path, id: &str) -> crate::state::shared::Restore {
    snapshot_with(dir, id, crate::state::shared::Hooks::default())
}

fn snapshot_with(
    dir: &std::path::Path,
    id: &str,
    hooks: crate::state::shared::Hooks,
) -> crate::state::shared::Restore {
    let path = dir.join(id);
    let rows = vec![(b"etcd/restored".to_vec(), id.as_bytes().to_vec())];
    crate::state::backup::write_image(&path, rows);
    crate::state::shared::Restore::for_test(&path, "127.0.0.1:1", hooks)
}

/// An ordinary leader bootstraps first. A later restore into that cluster fails, and tells
/// the operator that a restore needs a new cluster prefix.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restore_after_an_ordinary_bootstrap_fails() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (mut state, handles) = ordinary(store.clone()).await;
    state.seed_for_test(1).await.unwrap();
    let (events, received) = mpsc::channel(64);
    let mut announcements = Announcements(received);
    let mut writer = Writer::new(&handles, STRANDED_AGE, clock());
    writer.sync(true, 2, &events);
    announcements.next(Duration::from_secs(20), true).await;
    assert!(handles.source.bootstrapped().await.unwrap());
    let admin =
        slatedb::admin::Admin::builder(handles.source.state().as_str(), store.clone()).build();
    let dir = tempfile::tempdir().unwrap();
    let error = crate::state::shared::seed(&handles.source, &admin, &snapshot(dir.path(), "a"))
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("new cluster prefix"),
        "{error:#}"
    );
    writer.sync(false, 2, &events);
    writer.stop().await;
    state.close().await.unwrap();
}

/// A restore stops after its completion batch, or after its checkpoint, before its marker. An
/// ordinary server start never serves the empty base while the restore claim is pending. After
/// the completion batch it fails once the lease is stale. After the checkpoint it completes the
/// marker with the snapshot digest and starts. A retry of the same snapshot then succeeds, and
/// an ordinary start follows the restore.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_ordinary_start_resolves_a_stopped_restore() {
    use crate::state::shared::{Hooks, Stop};
    for checkpointed in [false, true] {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let source = crate::state::shared::offline_source(
            store.clone(),
            "root",
            crate::state::StateCache::default(),
        )
        .unwrap();
        let admin = slatedb::admin::Admin::builder(source.state().as_str(), store.clone()).build();
        let files = tempfile::tempdir().unwrap();
        let stop = if checkpointed {
            Stop::AfterCheckpoint
        } else {
            Stop::AfterCompletion
        };
        let hooks = Hooks {
            stop: std::sync::Mutex::new(Some(stop)),
            ..Default::default()
        };
        let restore = snapshot_with(files.path(), "a", hooks);
        crate::state::shared::seed(&source, &admin, &restore)
            .await
            .unwrap_err();
        let dir = tempfile::tempdir().unwrap();
        let mut config = crate::server::ServerConfig::single_node(dir.path());
        config.state_mode = crate::state::StateMode::Shared;
        config.object_store = crate::object::Config::Test(store.clone(), "root".into());
        let started = tokio::time::timeout(
            Duration::from_secs(30),
            crate::server::Server::start(config.clone()),
        )
        .await
        .unwrap();
        let digest = restore.digest().to_owned();
        if checkpointed {
            let server = started.unwrap();
            server.wait_ready(Duration::from_secs(15)).await.unwrap();
            server.shutdown().await.unwrap();
            assert_eq!(
                source.restored_snapshot().await.unwrap(),
                Some(digest.clone())
            );
        } else {
            let error = started.err().unwrap();
            assert!(format!("{error:#}").contains("pending"), "{error:#}");
            assert!(!source.bootstrapped().await.unwrap());
        }
        // The same snapshot restores with success at either boundary.
        crate::state::shared::seed(&source, &admin, &snapshot(files.path(), "a"))
            .await
            .unwrap();
        assert_eq!(source.restored_snapshot().await.unwrap(), Some(digest));
        let dir = tempfile::tempdir().unwrap();
        let mut config = crate::server::ServerConfig::single_node(dir.path());
        config.state_mode = crate::state::StateMode::Shared;
        config.object_store = crate::object::Config::Test(store.clone(), "root".into());
        let server = crate::server::Server::start(config).await.unwrap();
        server.wait_ready(Duration::from_secs(15)).await.unwrap();
        server.shutdown().await.unwrap();
    }
}

/// A MoveLeader requests its publication while the new writer is still opening, after
/// apply passed the last published index. The open keeps the request, so the writer
/// publishes through the requested index at once, well before the 3 s transfer fallback.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_transfer_request_during_the_writer_open_is_published() {
    let (mut state, handles, events, mut announcements, first) = stranded_checkpoint().await;
    adopt_announced(&mut state, first).await;
    for n in 0..5 {
        let put = crate::proto::etcdserverpb::RequestOp {
            request: Some(crate::proto::etcdserverpb::request_op::Request::RequestPut(
                crate::proto::etcdserverpb::PutRequest {
                    key: format!("key-{n}").into_bytes(),
                    value: b"value".to_vec(),
                    ..Default::default()
                },
            )),
        };
        let index = state.applied + 1;
        state.apply_etcd(index, &put).await.unwrap();
    }
    let applied = state.applied;
    let mut writer = Writer::new(&handles, Duration::from_secs(90), clock());
    writer.sync(true, 3, &events);
    // MoveLeader calls this with the applied index. The writer has not opened yet.
    writer.request_publication(applied);
    let started = Instant::now();
    let id = announcements.next(Duration::from_secs(2), true).await;
    assert!(started.elapsed() < Duration::from_secs(3));
    assert_eq!(handles.source.open_base(id).await.unwrap().applied, applied);
    writer.sync(false, 3, &events);
    writer.stop().await;
    state.close().await.unwrap();
}

/// The leader changes after the Defragment compaction and before the republish. The call
/// returns a retryable error. A retry on the new leader publishes the compacted checkpoint,
/// returns after its announcement applies, and the base then has one run and no L0 SST.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_defragment_that_loses_the_lead_returns_a_retryable_error() {
    let (mut state, handles, events, mut announcements, first) = stranded_checkpoint().await;
    adopt_announced(&mut state, first).await;
    let mut writer = Writer::new(&handles, STRANDED_AGE, clock());
    writer.sync(true, 3, &events);
    writer.progress.wait_written(1).await.unwrap();
    for n in 0..5 {
        let put = crate::proto::etcdserverpb::RequestOp {
            request: Some(crate::proto::etcdserverpb::request_op::Request::RequestPut(
                crate::proto::etcdserverpb::PutRequest {
                    key: format!("key-{n}").into_bytes(),
                    value: b"value".to_vec(),
                    ..Default::default()
                },
            )),
        };
        let index = state.applied + 1;
        state.apply_etcd(index, &put).await.unwrap();
    }
    // The aged flush publishes a checkpoint with an L0 SST. The members adopt it.
    let id = announcements.next(Duration::from_secs(10), true).await;
    adopt_announced(&mut state, id).await;
    let db = writer.open.lock().unwrap().clone().unwrap().db;
    let reached = Arc::new(tokio::sync::Notify::new());
    let resume = Arc::new(tokio::sync::Notify::new());
    let defragment = Defragment {
        db,
        admin: handles.admin.clone(),
        owner: None,
        progress: writer.progress.clone(),
        announced: handles.announced.clone(),
        leadership: writer.leadership.subscribe(),
        term: 3,
        pause: Some((DefragmentStep::Publish, reached.clone(), resume.clone())),
    };
    let running = tokio::spawn(defragment.run());
    reached.notified().await;
    writer.sync(false, 3, &events);
    writer.stop().await;
    resume.notify_one();
    let error = running.await.unwrap().unwrap_err();
    assert_eq!(error.code, tonic::Code::Unavailable as i32, "{error:?}");
    // The retry on the next leader.
    let mut writer = Writer::new(&handles, STRANDED_AGE, clock());
    writer.sync(true, 4, &events);
    let db = loop {
        if let Some(open) = writer.open.lock().unwrap().clone() {
            break open.db;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    let defragment = Defragment {
        db,
        admin: handles.admin.clone(),
        owner: None,
        progress: writer.progress.clone(),
        announced: handles.announced.clone(),
        leadership: writer.leadership.subscribe(),
        term: 4,
        pause: None,
    };
    let running = tokio::spawn(defragment.run());
    let compacted = announcements.next(Duration::from_secs(20), true).await;
    adopt_announced(&mut state, compacted).await;
    running.await.unwrap().unwrap();
    let checkpoint = handles
        .admin
        .list_checkpoints(None)
        .await
        .unwrap()
        .into_iter()
        .find(|checkpoint| Some(checkpoint.id) == state.base_checkpoint())
        .unwrap();
    let manifest = handles
        .admin
        .read_manifest(Some(checkpoint.manifest_id))
        .await
        .unwrap()
        .unwrap();
    assert!(manifest.l0().is_empty());
    assert_eq!(manifest.compacted().len(), 1);
    writer.sync(false, 4, &events);
    writer.stop().await;
    state.close().await.unwrap();
}

/// A leader writer at term 3 with five more writes after the adopted first checkpoint.
async fn defragment_setup() -> (
    StateMachine,
    Handles,
    mpsc::Sender<Event>,
    Announcements,
    Writer,
) {
    let (mut state, handles, events, mut announcements, first) = stranded_checkpoint().await;
    adopt_announced(&mut state, first).await;
    let mut writer = Writer::new(&handles, STRANDED_AGE, clock());
    writer.sync(true, 3, &events);
    writer.progress.wait_written(1).await.unwrap();
    for n in 0..5 {
        let put = crate::proto::etcdserverpb::RequestOp {
            request: Some(crate::proto::etcdserverpb::request_op::Request::RequestPut(
                crate::proto::etcdserverpb::PutRequest {
                    key: format!("key-{n}").into_bytes(),
                    value: b"value".to_vec(),
                    ..Default::default()
                },
            )),
        };
        let index = state.applied + 1;
        state.apply_etcd(index, &put).await.unwrap();
    }
    let _ = &mut announcements;
    (state, handles, events, announcements, writer)
}

fn defragment(
    handles: &Handles,
    writer: &Writer,
    term: u64,
    pause: Option<(
        DefragmentStep,
        Arc<tokio::sync::Notify>,
        Arc<tokio::sync::Notify>,
    )>,
) -> Defragment {
    Defragment {
        db: writer.open.lock().unwrap().clone().unwrap().db,
        admin: handles.admin.clone(),
        owner: None,
        progress: writer.progress.clone(),
        announced: handles.announced.clone(),
        leadership: writer.leadership.subscribe(),
        term,
        pause,
    }
}

/// The announcement of a checkpoint from before the Defragment waits in Raft. It applies
/// after the compaction. Defragment does not count it, because its layout still holds the
/// input SSTs. It returns only after the compacted checkpoint applies.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_defragment_ignores_an_older_checkpoint_that_applies_late() {
    check_delayed_checkpoint(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_defragment_ignores_a_checkpoint_from_before_a_prior_compaction() {
    check_delayed_checkpoint(true).await;
}

async fn check_delayed_checkpoint(compact_before_defragment: bool) {
    let (mut state, handles, events, mut announcements, mut writer) = defragment_setup().await;
    // Hold the announcement of the aged flush: no reply, no apply.
    let (old, reply) = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Some(Event::ViewCheckpoint(id, _, reply)) = announcements.0.recv().await {
                return (id, reply.unwrap());
            }
        }
    })
    .await
    .unwrap();
    if compact_before_defragment {
        // Rewrite the SSTs without advancing the applied announcement. The delayed
        // checkpoint now shares no input SSTs with the upcoming Defragment.
        crate::state::maintenance::compact_all(&handles.admin, &|| Ok(()))
            .await
            .unwrap();
    }
    let reached = Arc::new(tokio::sync::Notify::new());
    let resume = Arc::new(tokio::sync::Notify::new());
    let running = tokio::spawn(
        defragment(
            &handles,
            &writer,
            3,
            Some((DefragmentStep::Publish, reached.clone(), resume.clone())),
        )
        .run(),
    );
    reached.notified().await;
    resume.notify_one();
    let _ = reply.send(true);
    adopt_announced(&mut state, old).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        !running.is_finished(),
        "Defragment counted the older checkpoint"
    );
    let compacted = announcements.next(Duration::from_secs(10), true).await;
    assert_ne!(compacted, old);
    adopt_announced(&mut state, compacted).await;
    running.await.unwrap().unwrap();
    let checkpoint = handles
        .admin
        .list_checkpoints(None)
        .await
        .unwrap()
        .into_iter()
        .find(|checkpoint| checkpoint.id == compacted)
        .unwrap();
    let manifest = handles
        .admin
        .read_manifest(Some(checkpoint.manifest_id))
        .await
        .unwrap()
        .unwrap();
    assert!(manifest.l0().is_empty());
    assert_eq!(manifest.compacted().len(), 1);
    writer.sync(false, 3, &events);
    writer.stop().await;
    state.close().await.unwrap();
}

/// A step down during the flush or the compaction closes the writer. The storage error
/// then becomes the retryable Unavailable error, not an internal error.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_step_down_during_defragment_storage_work_is_retryable() {
    for step in [DefragmentStep::Flush, DefragmentStep::Compact] {
        let (state, handles, events, mut announcements, mut writer) = defragment_setup().await;
        // The aged flush leaves an L0 SST for the compaction.
        announcements.next(Duration::from_secs(10), true).await;
        let reached = Arc::new(tokio::sync::Notify::new());
        let resume = Arc::new(tokio::sync::Notify::new());
        let running = tokio::spawn(
            defragment(
                &handles,
                &writer,
                3,
                Some((step, reached.clone(), resume.clone())),
            )
            .run(),
        );
        reached.notified().await;
        writer.sync(false, 3, &events);
        writer.stop().await;
        resume.notify_one();
        let error = running.await.unwrap().unwrap_err();
        assert_eq!(
            error.code,
            tonic::Code::Unavailable as i32,
            "{step:?}: {error:?}"
        );
        state.close().await.unwrap();
    }
}

/// An adopt across membership changes leaves Raft with the checkpoint's ConfState, also
/// after an election.
#[tokio::test]
async fn an_adopt_takes_the_checkpoint_membership() {
    let dir = tempfile::tempdir().unwrap();
    let mut node = super::super::tests::leader_node(dir.path()).await;
    // The skipped entries added learner 4, promoted it, added learner 5, and removed 3.
    let target = raft::eraftpb::ConfState::from((vec![1, 2, 4], vec![5]));
    reconcile_conf(&mut node.raw, &target).unwrap();
    let conf = |node: &Node| {
        let mut conf = node.raw.raft.prs().conf().to_conf_state();
        conf.voters.sort_unstable();
        conf.learners.sort_unstable();
        conf
    };
    assert_eq!(conf(&node), target);
    // A new election asks the new voters, not the removed one or the learner.
    let term = node.raw.raft.term;
    node.raw.raft.become_follower(term + 1, raft::INVALID_ID);
    node.raw.raft.msgs.clear();
    node.raw.campaign().unwrap();
    assert_eq!(conf(&node), target);
    let mut asked: Vec<_> = node
        .raw
        .raft
        .msgs
        .iter()
        .map(|message| message.to)
        .collect();
    asked.sort_unstable();
    asked.dedup();
    assert_eq!(asked, [2, 4]);
    // A second reconcile changes nothing.
    reconcile_conf(&mut node.raw, &target).unwrap();
    assert_eq!(conf(&node), target);
}

/// A member steps down and leads again before it collects the old task. The old term's
/// normal end is not a failure of the new term.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_old_term_task_end_is_not_a_failure() {
    let options = voter_options(false);
    let mut state = StateMachine::open_shared(
        Arc::new(InMemory::new()),
        "root",
        "test",
        crate::state::StateCache::default(),
        &options,
        crate::state::transfer::Backend::Local,
    )
    .await
    .unwrap();
    let handles = state.shared_handles().unwrap();
    let (events, _announced) = mpsc::channel(64);
    let clock = Clock::system(Duration::from_secs(5), None);
    let mut writer = Some(Writer::new(&handles, Duration::from_millis(50), clock));
    let leader = writer.as_mut().unwrap();
    leader.sync(true, 2, &events);
    leader.sync(false, 2, &events);
    leader.sync(true, 3, &events);
    let failure = tokio::time::timeout(Duration::from_millis(500), async {
        loop {
            if let WriterEvent::Failed(error) = next(&mut writer).await {
                return error;
            }
        }
    })
    .await;
    assert!(failure.is_err(), "{failure:?}");
    let mut leader = writer.unwrap();
    leader.sync(false, 3, &events);
    leader.stop().await;
    state.apply(1, None).await.unwrap();
    state.close().await.unwrap();
}

/// The count of peer requests that `server` refused for their shared store.
async fn refused_stores(server: &crate::server::Server) -> u64 {
    let url = format!("http://{}/metrics", server.client_addr());
    let text = reqwest::get(url).await.unwrap().text().await.unwrap();
    text.lines()
        .find_map(|line| line.strip_prefix("slatedb_etcd_peer_store_refusals_total "))
        .unwrap()
        .parse()
        .unwrap()
}

/// Two voters with one cluster ID and one voter list, but on different prefixes, each create
/// their own store ID. Each voter refuses the other, so no leader is elected and a write fails.
/// The same voters on one prefix elect a leader.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn voters_on_different_shared_stores_form_no_quorum() {
    use crate::proto::etcdserverpb::{kv_client::KvClient, PutRequest};
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    for roots in [["root", "root"], ["root-1", "root-2"]] {
        let dir = tempfile::tempdir().unwrap();
        let peers: std::collections::BTreeMap<u64, String> =
            (1..=2).map(|id| (id, crate::server::test_port())).collect();
        let mut servers = vec![];
        for (id, root) in (1..=2).zip(roots) {
            let mut config =
                crate::server::ServerConfig::single_node(dir.path().join(id.to_string()));
            config.state_mode = crate::state::StateMode::Shared;
            config.object_store = crate::object::Config::Test(store.clone(), root.into());
            config.id = id;
            config.listen = peers[&id].parse().unwrap();
            config.peers = peers.clone();
            config.client_peers = peers.clone();
            servers.push(crate::server::Server::start(config).await.unwrap());
        }
        if roots[0] == roots[1] {
            for server in &servers {
                server.wait_ready(Duration::from_secs(15)).await.unwrap();
            }
            let mut ids = vec![];
            for server in &servers {
                eventually("the shared state status", async || {
                    status(server).await["shared_state"]["store_id"].is_string()
                })
                .await;
                ids.push(status(server).await["shared_state"]["store_id"].clone());
                assert_eq!(refused_stores(server).await, 0);
            }
            assert_eq!(ids[0], ids[1]);
        } else {
            // Without the check, a leader is elected within two election timeouts.
            let ready = servers[0].wait_ready(Duration::from_secs(4)).await;
            assert!(ready.is_err(), "the voters formed a quorum");
            let channel = tonic::transport::Endpoint::from_shared(servers[1].client_url())
                .unwrap()
                .connect()
                .await
                .unwrap();
            let put = PutRequest {
                key: b"key".to_vec(),
                value: b"value".to_vec(),
                ..Default::default()
            };
            let written =
                tokio::time::timeout(Duration::from_secs(2), KvClient::new(channel).put(put)).await;
            assert!(!matches!(written, Ok(Ok(_))), "{written:?}");
            for server in &servers {
                assert_eq!(status(server).await["leader_id"], 0);
                eventually("a refused peer store", async || {
                    refused_stores(server).await > 0
                })
                .await;
            }
        }
        for server in servers {
            server.shutdown().await.unwrap();
        }
    }
}

/// A paused member opens the newest checkpoint at each poll, so an adoption can drop the same
/// base again and again. `/status` reports the base with its failure count, and only the first
/// failure logs. The end of the pause ends the report, after an adoption or an install.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dropped_base_is_reported_until_the_pause_ends() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = tempfile::tempdir().unwrap();
    let mut config = super::super::tests::options(dir.path());
    config.state_mode = crate::state::StateMode::Shared;
    config.object_store = crate::object::Config::Test(store, "root".into());
    let (events, _) = mpsc::channel(256);
    let mut node = Node::open_with_transport(config, events, Some(BTreeMap::new()))
        .await
        .unwrap();
    let id = uuid::Uuid::new_v4();
    let rejected = apply::RejectedBase {
        checkpoint: Some(id),
        index: 9,
        fault: crate::mvcc::storage_fault::StorageFault::Corrupt,
        reason: "invalid members row".into(),
    };
    for _ in 0..3 {
        node.finish_output(Output::RejectedBase(rejected.clone()))
            .unwrap();
    }
    let report =
        |node: &Node| node.status().unwrap()["shared_state"]["writer"]["rejected_base"].clone();
    let reported = report(&node);
    assert_eq!(reported["checkpoint"], id.to_string(), "{reported}");
    assert_eq!(reported["class"], "corrupt");
    assert_eq!(reported["index"], 9);
    assert_eq!(reported["failures"], 3);
    // Another checkpoint starts a new report.
    let other = apply::RejectedBase {
        checkpoint: Some(uuid::Uuid::new_v4()),
        ..rejected
    };
    node.finish_output(Output::RejectedBase(other)).unwrap();
    assert_eq!(report(&node)["failures"], 1);
    // The overlay stays above the soft limit: the member still waits, and the report stays.
    let limit = node.options.overlay_max_bytes;
    node.writer.as_mut().unwrap().paused = true;
    node.state.view = Some((1, 1, limit + 1));
    assert!(node.apply_paused());
    assert_eq!(report(&node)["failures"], 1);
    // An install or an adoption frees the overlay. The pause ends, and so does the report.
    node.state.view = Some((1, 1, 0));
    assert!(!node.apply_paused());
    assert_eq!(report(&node), Value::Null);
}
