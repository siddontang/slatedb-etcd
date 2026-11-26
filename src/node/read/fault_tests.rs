//! Storage errors on client reads. A failed read fails only its own request, and the member
//! stays in operation. See `storage_fault`.

use super::*;
use crate::mvcc::storage_fault::CORRUPT_READS;
use crate::object::faults::{FaultStore, FAIL_SST_GET};
use crate::proto::etcdserverpb::response_op::Response as Resp;
use crate::proto::etcdserverpb::{
    kv_client::KvClient, maintenance_client::MaintenanceClient, DefragmentRequest, PutRequest,
    RangeRequest,
};
use crate::server::{Server, ServerConfig};
use slatedb::object_store::memory::InMemory;
use std::sync::Arc;
use tokio_stream::StreamExt;

/// The tests that count corrupt reads run one at a time, so each sees only its own count.
pub(crate) static COUNTED: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// A replicated member on `faults` with no block, metadata, or row cache. Each read of a
/// flushed row then reads the store.
fn config(dir: &std::path::Path, faults: &Arc<FaultStore>) -> ServerConfig {
    let mut config = ServerConfig::single_node(dir);
    config.object_store = crate::object::Config::Test(faults.clone(), "root".into());
    config.state_cache = crate::state::StateCache {
        block_bytes: 1,
        meta_bytes: 1,
        row_bytes: 0,
    };
    config
}

/// Start a member with keys `k0` to `k9` and the diagnostic value `d` in SSTs, then start it
/// again with empty caches. Each value fills its own SST block, so a read of a key that no
/// earlier read touched loads its block from the store.
async fn flushed(dir: &std::path::Path, faults: &Arc<FaultStore>) -> (Server, KvClient<Channel>) {
    let server = Server::start(config(dir, faults)).await.unwrap();
    server.wait_ready(Duration::from_secs(15)).await.unwrap();
    let body = r#"{"operations":[{"op":"put","key":"d","value":"diagnostic"}]}"#;
    let (code, reply) = http(&server, "POST", "/kv", body).await;
    assert_eq!(code, 200, "{reply}");
    let channel = connect(&server).await;
    let mut kv = KvClient::new(channel.clone());
    for key in 0..10 {
        kv.put(PutRequest {
            key: format!("k{key}").into_bytes(),
            value: vec![b'v'; 8 << 10],
            ..Default::default()
        })
        .await
        .unwrap();
    }
    // Defragment flushes the memtable and compacts it into SSTs.
    MaintenanceClient::new(channel)
        .defragment(DefragmentRequest {})
        .await
        .unwrap();
    server.shutdown().await.unwrap();
    let server = Server::start(config(dir, faults)).await.unwrap();
    server.wait_ready(Duration::from_secs(15)).await.unwrap();
    let kv = KvClient::new(connect(&server).await);
    (server, kv)
}

async fn connect(server: &Server) -> Channel {
    tonic::transport::Endpoint::from_shared(server.client_url())
        .unwrap()
        .connect()
        .await
        .unwrap()
}

type Channel = tonic::transport::Channel;

fn range(key: &str, range_end: &str, serializable: bool) -> RangeRequest {
    RangeRequest {
        key: key.as_bytes().to_vec(),
        range_end: range_end.as_bytes().to_vec(),
        serializable,
        ..Default::default()
    }
}

/// Send one HTTP request to the client listener. Return the status and the body.
async fn http(server: &Server, method: &str, path: &str, body: &str) -> (u16, String) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut stream = tokio::net::TcpStream::connect(server.client_addr())
        .await
        .unwrap();
    let request = format!(
        "{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\
         Content-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await.unwrap();
    let response = String::from_utf8(response).unwrap();
    let code = response.split(' ').nth(1).unwrap().parse().unwrap();
    (code, response.split_once("\r\n\r\n").unwrap().1.to_owned())
}

/// One failed object GET fails each read kind with a retryable status: a point Range and a
/// wide Range in the owner, and a serializable point Range on the local fast path. The member
/// stays up, and the same reads succeed after the store recovers.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_object_read_fails_only_its_request() {
    let faults = Arc::new(FaultStore::new(Arc::new(InMemory::new())));
    let dir = tempfile::tempdir().unwrap();
    let (server, mut kv) = flushed(dir.path(), &faults).await;
    let reads = [
        range("k1", "", false),
        range("k", "l", false),
        range("k2", "", true),
    ];
    faults.set(FAIL_SST_GET);
    for request in reads.clone() {
        let error = kv.range(request.clone()).await.unwrap_err();
        assert_eq!(
            error.code(),
            tonic::Code::Unavailable,
            "{request:?}: {error}"
        );
        assert!(error.message().contains("storage unavailable"), "{error}");
    }
    faults.set(0);
    for request in reads {
        let reply = kv.range(request.clone()).await.unwrap().into_inner();
        assert!(!reply.kvs.is_empty(), "{request:?}");
    }
    server.shutdown().await.unwrap();
}

/// `/debug/value` reads in the apply worker. A failed GET fails only the diagnostic request.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_diagnostic_read_fails_only_its_request() {
    let faults = Arc::new(FaultStore::new(Arc::new(InMemory::new())));
    let dir = tempfile::tempdir().unwrap();
    let (server, mut kv) = flushed(dir.path(), &faults).await;
    faults.set(FAIL_SST_GET);
    let (code, body) = http(&server, "GET", "/debug/value?key=d", "").await;
    assert_eq!(code, 503, "{body}");
    assert!(body.contains("storage unavailable"), "{body}");
    faults.set(0);
    let (code, body) = http(&server, "GET", "/debug/value?key=d", "").await;
    assert_eq!(code, 200, "{body}");
    assert!(body.contains("diagnostic"), "{body}");
    kv.range(range("k1", "", false)).await.unwrap();
    server.shutdown().await.unwrap();
}

/// A linearizable Range over `key` and the keys below it, which runs in a wide slot.
fn linear(key: &[u8], id: u64) -> Query {
    Query::Linear(
        RequestOp {
            request: Some(
                crate::proto::etcdserverpb::request_op::Request::RequestRange(RangeRequest {
                    key: key.to_vec(),
                    range_end: [key, b"\0"].concat(),
                    ..Default::default()
                }),
            ),
        },
        None,
        id,
    )
}

/// A current row that does not decode fails a coalesced read with `DataLoss`. The shared
/// result counts once, and the executor keeps running.
#[tokio::test]
async fn a_corrupt_row_fails_coalesced_reads_once() {
    let _counted = COUNTED.lock().await;
    let dir = tempfile::tempdir().unwrap();
    let mut state = StateMachine::open(dir.path()).await.unwrap();
    state
        .db()
        .put(b"etcd/current/bad", b"\xff\xff")
        .await
        .unwrap();
    // The direct write took one sequence number. A view pins the state at the applied index.
    state.applied += 1;
    let view = state.read_view().unwrap();
    let header = Header {
        cluster: 1,
        member: 1,
        term: 1,
    };
    let mut executor = executor::Executor::new(true);
    let before = CORRUPT_READS.get();
    executor.submit(
        view.clone(),
        vec![linear(b"bad", 1), linear(b"bad", 2)],
        header,
    );
    let Some(Output::SharedLinear(ids, outcome)) = executor.step().await.unwrap() else {
        panic!("expected one shared result");
    };
    assert_eq!(ids, [1, 2]);
    let error = outcome.result.as_ref().unwrap_err();
    assert_eq!(error.code, tonic::Code::DataLoss as i32, "{error:?}");
    assert_eq!(CORRUPT_READS.get(), before + 1);
    executor.submit(view, vec![linear(b"good", 3)], header);
    let Some(Output::Reads(outputs)) = executor.step().await.unwrap() else {
        panic!("expected a read");
    };
    let [Output::Linear { outcome, .. }] = &outputs[..] else {
        panic!("expected one linear read");
    };
    assert!(outcome.result.is_ok(), "{outcome:?}");
    state.close().await.unwrap();
}

/// A hash record with a short key fails HashKV with `DataLoss`. The member does not panic,
/// and a Range on the same state still succeeds.
#[tokio::test]
async fn a_short_hash_record_fails_only_hash_kv() {
    let short = [crate::maintenance::HASH, &[0; 3]].concat();
    a_bad_hash_record_fails_only_hash_kv(short, b"record".to_vec()).await;
}

/// A record key with only its main revision fails HashKV with `DataLoss`, even with a valid
/// record value. The key must have the whole form of `hash_key`.
#[tokio::test]
async fn a_hash_record_key_without_its_subrevision_fails_only_hash_kv() {
    use prost::Message;
    let key = [crate::maintenance::HASH, &2i64.to_be_bytes()].concat();
    let record = crate::proto::mvccpb::KeyValue {
        key: b"good".to_vec(),
        value: b"value".to_vec(),
        create_revision: 2,
        mod_revision: 2,
        version: 1,
        ..Default::default()
    };
    a_bad_hash_record_fails_only_hash_kv(key, record.encode_to_vec()).await;
}

async fn a_bad_hash_record_fails_only_hash_kv(key: Vec<u8>, value: Vec<u8>) {
    let _counted = COUNTED.lock().await;
    let dir = tempfile::tempdir().unwrap();
    let mut state = StateMachine::open(dir.path()).await.unwrap();
    let put = RequestOp {
        request: Some(crate::proto::etcdserverpb::request_op::Request::RequestPut(
            PutRequest {
                key: b"good".to_vec(),
                value: b"value".to_vec(),
                ..Default::default()
            },
        )),
    };
    state.apply_etcd(1, &put).await.unwrap();
    state.db().put(&key, &value).await.unwrap();
    state.applied += 1;
    let before = CORRUPT_READS.get();
    let outcome = state
        .maintenance(
            &crate::maintenance::Command::Hash(0),
            &Default::default(),
            Default::default(),
        )
        .await
        .unwrap();
    let error = outcome.result.unwrap_err();
    assert_eq!(error.code, tonic::Code::DataLoss as i32, "{error:?}");
    assert_eq!(CORRUPT_READS.get(), before + 1);
    let Query::Linear(request, ..) = linear(b"good", 1) else {
        unreachable!()
    };
    let outcome = state.read_view().unwrap().read(&request, None).await;
    let Some(Resp::ResponseRange(reply)) = outcome.unwrap().result.unwrap().response else {
        panic!("expected a Range reply");
    };
    assert_eq!(reply.kvs[0].value, b"value");
    state.close().await.unwrap();
}

/// A diagnostic value that is not UTF-8 is corrupt stored data. `/debug/value` fails with
/// `DataLoss` and counts it.
#[tokio::test]
async fn a_corrupt_diagnostic_value_fails_with_data_loss() {
    let _counted = COUNTED.lock().await;
    let dir = tempfile::tempdir().unwrap();
    let state = StateMachine::open(dir.path()).await.unwrap();
    state.db().put(b"data/bad", b"\xff\xfe").await.unwrap();
    let before = CORRUPT_READS.get();
    let error = state.local_value("bad").await.unwrap_err();
    let error = crate::mvcc::KvError::read_failure(error);
    assert_eq!(error.code, tonic::Code::DataLoss as i32, "{error:?}");
    assert_eq!(CORRUPT_READS.get(), before + 1);
    state.close().await.unwrap();
}

/// A storage error on a Watch catch-up page ends no stream. The page runs again on a new view,
/// and after the store recovers the watcher gets each revision once and in order.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_watch_page_runs_again_without_a_gap() {
    use crate::proto::etcdserverpb::{
        watch_client::WatchClient, watch_request::RequestUnion, WatchCreateRequest, WatchRequest,
    };
    let faults = Arc::new(FaultStore::new(Arc::new(InMemory::new())));
    let dir = tempfile::tempdir().unwrap();
    let (server, _) = flushed(dir.path(), &faults).await;
    faults.set(FAIL_SST_GET);
    let recover = {
        let faults = faults.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(300)).await;
            faults.set(0);
        })
    };
    let create = WatchRequest {
        request_union: Some(RequestUnion::CreateRequest(WatchCreateRequest {
            key: b"k".to_vec(),
            range_end: b"l".to_vec(),
            start_revision: 1,
            ..Default::default()
        })),
    };
    let mut watch = WatchClient::new(connect(&server).await);
    let mut stream = watch
        .watch(tokio_stream::iter([create]).chain(tokio_stream::pending()))
        .await
        .unwrap()
        .into_inner();
    let mut revisions = vec![];
    while revisions.len() < 10 {
        let reply = tokio::time::timeout(Duration::from_secs(10), stream.message())
            .await
            .expect("the watch timed out")
            .expect("the watch failed")
            .expect("the watch ended");
        revisions.extend(
            reply
                .events
                .iter()
                .map(|event| event.kv.as_ref().unwrap().mod_revision),
        );
    }
    recover.await.unwrap();
    assert!(faults.entered.available_permits() > 0, "no read failed");
    let first = revisions[0];
    assert_eq!(revisions, (first..first + 10).collect::<Vec<_>>());
    server.shutdown().await.unwrap();
}

/// A Watch stream on a member whose store fails, with requests from `requests`.
async fn watch_stream(
    server: &Server,
    requests: tokio::sync::mpsc::Receiver<crate::proto::etcdserverpb::WatchRequest>,
) -> tonic::Streaming<crate::proto::etcdserverpb::WatchResponse> {
    use crate::proto::etcdserverpb::watch_client::WatchClient;
    WatchClient::new(connect(server).await)
        .watch(tokio_stream::wrappers::ReceiverStream::new(requests))
        .await
        .unwrap()
        .into_inner()
}

/// A Watch request for `k0` to `k9` from revision 1.
fn historical() -> crate::proto::etcdserverpb::WatchRequest {
    use crate::proto::etcdserverpb::{watch_request::RequestUnion, WatchCreateRequest};
    crate::proto::etcdserverpb::WatchRequest {
        request_union: Some(RequestUnion::CreateRequest(WatchCreateRequest {
            key: b"k".to_vec(),
            range_end: b"l".to_vec(),
            start_revision: 1,
            ..Default::default()
        })),
    }
}

async fn reply(
    stream: &mut tonic::Streaming<crate::proto::etcdserverpb::WatchResponse>,
) -> crate::proto::etcdserverpb::WatchResponse {
    tokio::time::timeout(Duration::from_secs(2), stream.message())
        .await
        .expect("the watch did not reply")
        .expect("the watch failed")
        .expect("the watch ended")
}

/// Wait until the store fails one more read.
async fn failed_read(faults: &FaultStore) {
    tokio::time::timeout(Duration::from_secs(2), faults.entered.acquire())
        .await
        .expect("no read failed")
        .unwrap()
        .forget();
}

/// The active Watch streams of the member.
async fn streams(server: &Server) -> String {
    let (_, metrics) = http(server, "GET", "/metrics", "").await;
    metrics
        .lines()
        .find(|line| line.starts_with("etcd_debugging_mvcc_watch_stream_total "))
        .unwrap()
        .to_owned()
}

/// A historical catch-up whose reads keep failing does not hold the stream. A Cancel gets its
/// reply, and a stream whose client leaves ends.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failing_catch_up_still_serves_cancel_and_disconnect() {
    use crate::proto::etcdserverpb::{watch_request::RequestUnion, WatchCancelRequest};
    let faults = Arc::new(FaultStore::new(Arc::new(InMemory::new())));
    let dir = tempfile::tempdir().unwrap();
    let (server, _) = flushed(dir.path(), &faults).await;
    faults.set(FAIL_SST_GET);
    let (requests, input) = tokio::sync::mpsc::channel(4);
    let mut stream = watch_stream(&server, input).await;
    requests.send(historical()).await.unwrap();
    let created = reply(&mut stream).await;
    assert!(created.created);
    failed_read(&faults).await;
    let cancel = RequestUnion::CancelRequest(WatchCancelRequest {
        watch_id: created.watch_id,
    });
    requests
        .send(crate::proto::etcdserverpb::WatchRequest {
            request_union: Some(cancel),
        })
        .await
        .unwrap();
    assert!(reply(&mut stream).await.canceled);
    drop((requests, stream));
    // A second stream leaves while its catch-up fails. Its task ends.
    let (requests, input) = tokio::sync::mpsc::channel(4);
    let mut stream = watch_stream(&server, input).await;
    requests.send(historical()).await.unwrap();
    assert!(reply(&mut stream).await.created);
    failed_read(&faults).await;
    drop((requests, stream));
    let ended = "etcd_debugging_mvcc_watch_stream_total 0";
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while streams(&server).await != ended {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the stream task did not end"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    faults.set(0);
    server.shutdown().await.unwrap();
}

/// A progress request during a failing catch-up waits. Its reply follows every earlier event.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_progress_reply_follows_the_events_of_a_failing_catch_up() {
    use crate::proto::etcdserverpb::{watch_request::RequestUnion, WatchProgressRequest};
    let faults = Arc::new(FaultStore::new(Arc::new(InMemory::new())));
    let dir = tempfile::tempdir().unwrap();
    let (server, _) = flushed(dir.path(), &faults).await;
    faults.set(FAIL_SST_GET);
    let (requests, input) = tokio::sync::mpsc::channel(4);
    let mut stream = watch_stream(&server, input).await;
    requests.send(historical()).await.unwrap();
    assert!(reply(&mut stream).await.created);
    failed_read(&faults).await;
    let progress = RequestUnion::ProgressRequest(WatchProgressRequest {});
    requests
        .send(crate::proto::etcdserverpb::WatchRequest {
            request_union: Some(progress),
        })
        .await
        .unwrap();
    let early = tokio::time::timeout(Duration::from_millis(500), stream.message()).await;
    assert!(early.is_err(), "a reply came while the store failed");
    faults.set(0);
    let mut revisions = vec![];
    let progress = loop {
        let reply = reply(&mut stream).await;
        if reply.watch_id == -1 {
            break reply;
        }
        revisions.extend(
            reply
                .events
                .iter()
                .map(|e| e.kv.as_ref().unwrap().mod_revision),
        );
    };
    let first = revisions[0];
    assert_eq!(revisions, (first..first + 10).collect::<Vec<_>>());
    assert!(progress.header.unwrap().revision >= revisions[9]);
    server.shutdown().await.unwrap();
}

/// The create that `a_create_waits_for_an_earlier_progress_reply` sends.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Late {
    /// A watch from revision 50, above the current revision.
    Future,
    /// A watch from the current revision.
    Now,
    /// A watch that takes ID 5 of a watch that the stream canceled meanwhile.
    Reused,
}

fn watch_request(
    request: crate::proto::etcdserverpb::watch_request::RequestUnion,
) -> crate::proto::etcdserverpb::WatchRequest {
    crate::proto::etcdserverpb::WatchRequest {
        request_union: Some(request),
    }
}

fn late_create(id: i64, start: i64) -> crate::proto::etcdserverpb::WatchRequest {
    use crate::proto::etcdserverpb::{watch_request::RequestUnion, WatchCreateRequest};
    watch_request(RequestUnion::CreateRequest(WatchCreateRequest {
        key: b"late".to_vec(),
        watch_id: id,
        start_revision: start,
        ..Default::default()
    }))
}

/// A progress request waits while a catch-up fails. A create that comes after it gets its
/// reply only after the progress reply. A client applies a stream-wide progress reply to each
/// watch that it knows, so an earlier reply must not reach a watch that starts after it.
async fn a_create_waits_for_an_earlier_progress_reply(late: Late) {
    use crate::proto::etcdserverpb::{
        watch_request::RequestUnion, WatchCancelRequest, WatchProgressRequest,
    };
    let faults = Arc::new(FaultStore::new(Arc::new(InMemory::new())));
    let dir = tempfile::tempdir().unwrap();
    let (server, _) = flushed(dir.path(), &faults).await;
    faults.set(FAIL_SST_GET);
    let (requests, input) = tokio::sync::mpsc::channel(4);
    let mut stream = watch_stream(&server, input).await;
    requests.send(historical()).await.unwrap();
    assert!(reply(&mut stream).await.created);
    if late == Late::Reused {
        requests.send(late_create(5, 0)).await.unwrap();
        let created = reply(&mut stream).await;
        assert!(created.created && created.watch_id == 5);
    }
    failed_read(&faults).await;
    let progress = RequestUnion::ProgressRequest(WatchProgressRequest {});
    requests.send(watch_request(progress)).await.unwrap();
    let (id, start) = match late {
        Late::Future => (0, 50),
        Late::Now => (0, 0),
        Late::Reused => {
            let cancel = RequestUnion::CancelRequest(WatchCancelRequest { watch_id: 5 });
            requests.send(watch_request(cancel)).await.unwrap();
            let canceled = reply(&mut stream).await;
            assert!(canceled.canceled && canceled.watch_id == 5);
            (5, 0)
        }
    };
    requests.send(late_create(id, start)).await.unwrap();
    let early = tokio::time::timeout(Duration::from_millis(500), stream.message()).await;
    assert!(
        early.is_err(),
        "a reply came while the store failed: {early:?}"
    );
    faults.set(0);
    let mut revisions = vec![];
    let progress = loop {
        let reply = reply(&mut stream).await;
        assert!(!reply.created, "the create went before the progress reply");
        if reply.watch_id == -1 {
            break reply;
        }
        revisions.extend(
            reply
                .events
                .iter()
                .map(|e| e.kv.as_ref().unwrap().mod_revision),
        );
    };
    assert_eq!(revisions.len(), 10);
    let created = reply(&mut stream).await;
    assert!(created.created && !created.canceled, "{created:?}");
    if late == Late::Reused {
        assert_eq!(created.watch_id, 5);
    }
    let goal = progress.header.unwrap().revision;
    assert!(created.header.unwrap().revision >= goal);
    server.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_future_create_waits_for_an_earlier_progress_reply() {
    a_create_waits_for_an_earlier_progress_reply(Late::Future).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_current_create_waits_for_an_earlier_progress_reply() {
    a_create_waits_for_an_earlier_progress_reply(Late::Now).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_reused_id_waits_for_an_earlier_progress_reply() {
    a_create_waits_for_an_earlier_progress_reply(Late::Reused).await;
}

/// A create waits behind a progress request while the store fails. Its Cancel still gets a
/// reply at once, and the create never runs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cancel_of_a_waiting_create_replies_during_a_store_fault() {
    use crate::proto::etcdserverpb::{
        watch_request::RequestUnion, WatchCancelRequest, WatchProgressRequest,
    };
    let faults = Arc::new(FaultStore::new(Arc::new(InMemory::new())));
    let dir = tempfile::tempdir().unwrap();
    let (server, _) = flushed(dir.path(), &faults).await;
    faults.set(FAIL_SST_GET);
    let (requests, input) = tokio::sync::mpsc::channel(4);
    let mut stream = watch_stream(&server, input).await;
    requests.send(historical()).await.unwrap();
    assert!(reply(&mut stream).await.created);
    failed_read(&faults).await;
    let progress = RequestUnion::ProgressRequest(WatchProgressRequest {});
    requests.send(watch_request(progress)).await.unwrap();
    requests.send(late_create(7, 0)).await.unwrap();
    let cancel = RequestUnion::CancelRequest(WatchCancelRequest { watch_id: 7 });
    requests.send(watch_request(cancel)).await.unwrap();
    let canceled = reply(&mut stream).await;
    assert!(
        canceled.canceled && !canceled.created && canceled.watch_id == 7,
        "{canceled:?}"
    );
    faults.set(0);
    loop {
        let reply = reply(&mut stream).await;
        assert!(!reply.created, "the canceled create ran");
        if reply.watch_id == -1 {
            break;
        }
    }
    let after = tokio::time::timeout(Duration::from_millis(300), stream.message()).await;
    assert!(
        after.is_err(),
        "a reply came after the progress reply: {after:?}"
    );
    server.shutdown().await.unwrap();
}
