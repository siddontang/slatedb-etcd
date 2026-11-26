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
    let short = [crate::maintenance::HASH, &[0; 3]].concat();
    state.db().put(&short, b"record").await.unwrap();
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
