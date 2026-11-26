//! Shared-mode backups: the image of a pinned view, its pin, and the member budget.

use super::view::{Backups, IDLE_MAX, MAX_STREAMS};
use super::{backup_error, Stream, MAX_ROW_BYTES};
use crate::proto::etcdserverpb::{response_op, RangeRequest, RequestOp, ResponseOp};
use crate::proto::mvccpb::KeyValue;
use crate::state::shared_tests::{checkpoint, delete, open_shared, put, writer};
use crate::state::*;
use futures::StreamExt;
use slatedb::object_store::memory::InMemory;
use slatedb::object_store::{ObjectStore, ObjectStoreExt};
use std::sync::Arc;
use std::time::Duration;

/// The value size of the base rows. 200 rows make an image of about 3 MiB, far above the 1 MiB
/// that a stream buffers ahead of its client.
const VALUE_BYTES: usize = 16 * 1024;

/// A shared state, its writer, and the store that holds both.
struct Fixture {
    state: StateMachine,
    writer: slatedb::Db,
    written: u64,
    store: Arc<InMemory>,
}

impl Fixture {
    /// A shared state whose base holds `rows` rows of `VALUE_BYTES`.
    async fn new(rows: usize) -> Self {
        let store = Arc::new(InMemory::new());
        let state = open_shared(store.clone()).await;
        let writer = writer(&state).await;
        let mut fixture = Self {
            state,
            writer,
            written: 0,
            store,
        };
        for n in 0..rows {
            fixture
                .put(&format!("large/{n:04}"), &"v".repeat(VALUE_BYTES))
                .await;
        }
        fixture.switch().await;
        fixture
    }

    async fn put(&mut self, key: &str, value: &str) {
        let index = self.state.applied + 1;
        let outcome = self.state.apply_etcd(index, &put(key, value)).await;
        outcome.unwrap().result.unwrap();
    }

    async fn delete(&mut self, key: &str) {
        let index = self.state.applied + 1;
        let outcome = self.state.apply_etcd(index, &delete(key, "")).await;
        outcome.unwrap().result.unwrap();
    }

    /// Store a raw row, as a converted state or many auth commands can leave it.
    async fn put_row(&mut self, key: &[u8], value: Vec<u8>) {
        let index = self.state.applied + 1;
        let mut batch = WriteBatch::new();
        batch.put(key, value);
        self.state.mark_applied(&mut batch, index);
        self.state.write_batch(batch, index).await.unwrap();
        self.state.applied = index;
    }

    /// Publish the overlay in a checkpoint and switch to it. Return the checkpoint.
    async fn switch(&mut self) -> uuid::Uuid {
        let base = checkpoint(&self.state, &self.writer, &mut self.written).await;
        let id = base.checkpoint.unwrap();
        self.state.shared.as_ref().unwrap().offer(base);
        self.state.switch_base();
        assert_eq!(self.state.base_checkpoint(), Some(id));
        id
    }

    fn backups(&self) -> Arc<Backups> {
        self.state.backups().unwrap()
    }

    fn pins(&self, id: uuid::Uuid) -> bool {
        let source = &self.state.shared.as_ref().unwrap().source;
        source.pinned().contains(&id)
    }

    /// Wait until `streams` backups stream and the member no longer pins checkpoint `id`.
    async fn released(&self, id: uuid::Uuid, streams: usize) {
        for _ in 0..500 {
            if self.backups().status().0 == streams && !self.pins(id) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("the backup still pins its view");
    }

    async fn close(self) {
        self.writer.close().await.unwrap();
        self.state.close().await.unwrap();
    }
}

/// Collect a whole stream and check its chunk sizes against `remaining_bytes`.
async fn collect(mut stream: Stream) -> Vec<u8> {
    let mut data = vec![];
    let mut last = None;
    while let Some(chunk) = stream.recv().await {
        let chunk = chunk.unwrap();
        if let Some(last) = last {
            assert_eq!(last, chunk.remaining + chunk.blob.len() as u64);
        }
        last = Some(chunk.remaining);
        data.extend_from_slice(&chunk.blob);
    }
    assert_eq!(last, Some(0));
    data
}

/// Read the stream until its error. Fail if it ends without one.
async fn failure(stream: &mut Stream) -> crate::mvcc::KvError {
    loop {
        match stream.recv().await {
            Some(Ok(_)) => continue,
            Some(Err(error)) => return backup_error(error),
            None => panic!("a failed backup ended like a complete image"),
        }
    }
}

/// Wait until the stream task fills the buffer ahead of the client.
async fn fill() {
    tokio::time::sleep(Duration::from_millis(200)).await;
}

fn decode(image: &[u8]) -> SnapshotData {
    crate::codec::decode(&image[..image.len() - 32]).unwrap()
}

fn current(key: &str) -> Vec<u8> {
    [crate::mvcc::CURRENT, key.as_bytes()].concat()
}

/// The current version of `key` and the response revision.
async fn read(state: &StateMachine, key: &str) -> (Option<KeyValue>, i64) {
    let request = RequestOp {
        request: Some(
            crate::proto::etcdserverpb::request_op::Request::RequestRange(RangeRequest {
                key: key.as_bytes().to_vec(),
                ..Default::default()
            }),
        ),
    };
    let outcome = state.read_etcd(&request).await.unwrap();
    let Ok(ResponseOp {
        response: Some(response_op::Response::ResponseRange(reply)),
    }) = outcome.result
    else {
        panic!("unexpected response {:?}", outcome.result);
    };
    (reply.kvs.first().cloned(), reply.header.unwrap().revision)
}

/// A shared backup streams the bytes of the in-memory view image and its digest. The image
/// merges the base and the overlay, and it leaves out the cluster quota row.
#[tokio::test]
async fn a_shared_backup_streams_the_view_image() {
    let mut fixture = Fixture::new(8).await;
    fixture.put("gone", "base").await;
    fixture.put("kept", "base").await;
    fixture.switch().await;
    fixture.delete("gone").await;
    fixture.put("kept", "overlay").await;
    fixture.put("new", "overlay").await;
    // Shared mode ignores a quota row from a converted state.
    fixture
        .put_row(quota::KEY, 7_u64.to_be_bytes().to_vec())
        .await;
    let state = &fixture.state;
    let expected = state.image_job(true).unwrap().await.unwrap();
    let streamed = collect(state.backup().await.unwrap()).await;
    assert_eq!(streamed, crate::state::tests::backup_bytes(expected));
    assert_eq!(
        streamed.len() % 512,
        32,
        "etcdctl snapshot save checks this"
    );
    let rows = decode(&streamed).rows;
    let value = |key: &str| {
        let key = current(key);
        rows.iter()
            .find(|(row, _)| *row == key)
            .map(|row| row.1.clone())
    };
    assert!(value("gone").is_none());
    assert!(value("kept").is_some() && value("new").is_some());
    assert!(rows.iter().all(|(key, _)| key != quota::KEY));
    assert_eq!(rows.last().unwrap().0, crate::cluster::KEY);
    assert_eq!(fixture.backups().status(), (0, 0));
    fixture.close().await;
}

/// A paused backup keeps the boundary of its pin. Later writes and a base switch release the
/// overlay segments of the boundary, but the image and its restore still hold that state.
#[tokio::test]
async fn a_shared_backup_keeps_its_boundary_across_a_base_switch() {
    let mut fixture = Fixture::new(200).await;
    fixture.put("key", "base").await;
    fixture.put("gone", "base").await;
    let first = fixture.switch().await;
    fixture.put("key", "boundary").await;
    fixture.put("only", "overlay").await;
    fixture.delete("gone").await;
    let (key, revision) = read(&fixture.state, "key").await;
    let key = key.unwrap();
    let only = read(&fixture.state, "only").await.0.unwrap();

    let mut stream = fixture.state.backup().await.unwrap();
    assert_eq!(stream.header.as_ref().unwrap().revision, revision);
    let first_chunk = stream.recv().await.unwrap().unwrap();
    fill().await;
    fixture.put("key", "after").await;
    fixture.delete("only").await;
    fixture.put("later", "after").await;
    fixture.switch().await;
    assert!(fixture.pins(first), "the backup keeps its base");
    let mut data = first_chunk.blob;
    data.extend_from_slice(&collect(stream).await);
    fixture.released(first, 0).await;

    let dir = tempfile::tempdir().unwrap();
    let restored = crate::state::tests::restore(dir.path(), data).await;
    assert_eq!(restored.revision, revision);
    let (restored_key, restored_revision) = read(&restored, "key").await;
    assert_eq!(restored_revision, revision);
    let restored_key = restored_key.unwrap();
    assert_eq!(restored_key.value, b"boundary");
    assert_eq!(
        (restored_key.mod_revision, restored_key.create_revision),
        (key.mod_revision, key.create_revision)
    );
    assert_eq!(read(&restored, "only").await.0.unwrap(), only);
    assert!(read(&restored, "gone").await.0.is_none());
    assert!(read(&restored, "later").await.0.is_none());
    restored.close().await.unwrap();
    fixture.close().await;
}

/// A row that a restore cannot decode fails the backup before its first chunk. The auth row can
/// grow beyond one Raft entry, so it has the same check.
#[tokio::test]
async fn a_shared_backup_rejects_a_large_row_before_its_first_chunk() {
    let mut fixture = Fixture::new(1).await;
    fixture
        .put_row(crate::auth::KEY, vec![b' '; MAX_ROW_BYTES])
        .await;
    let mut stream = fixture.state.backup().await.unwrap();
    let error = match stream.recv().await.unwrap() {
        Ok(_) => panic!("the backup sent a chunk before the row check"),
        Err(error) => backup_error(error),
    };
    assert_eq!(
        error.code,
        tonic::Code::ResourceExhausted as i32,
        "{error:?}"
    );
    assert!(stream.recv().await.is_none());
    assert_eq!(fixture.backups().status(), (0, 0));
    fixture.close().await;
}

/// A client that goes away ends the stream and releases the pinned base and segments.
#[tokio::test]
async fn a_shared_backup_releases_its_pin_when_the_client_goes_away() {
    let mut fixture = Fixture::new(200).await;
    let first = fixture.state.base_checkpoint().unwrap();
    fixture.put("key", "overlay").await;
    let mut stream = fixture.state.backup().await.unwrap();
    stream.recv().await.unwrap().unwrap();
    fill().await;
    fixture.switch().await;
    let (streams, bytes) = fixture.backups().status();
    assert!(streams == 1 && bytes > 0, "{streams} {bytes}");
    assert!(fixture.pins(first));
    drop(stream);
    fixture.released(first, 0).await;
    fixture.close().await;
}

/// An SST that goes away during the stream fails it with Unavailable. The stream never ends like
/// a complete image.
#[tokio::test]
async fn a_shared_backup_fails_when_an_sst_goes_away() {
    let fixture = Fixture::new(200).await;
    let mut stream = fixture.state.backup().await.unwrap();
    stream.recv().await.unwrap().unwrap();
    fill().await;
    let ssts: Vec<_> = fixture
        .store
        .list(None)
        .filter_map(|meta| async move { meta.ok() })
        .filter(|meta| std::future::ready(meta.location.as_ref().ends_with(".sst")))
        .collect()
        .await;
    assert!(!ssts.is_empty());
    for meta in ssts {
        fixture.store.delete(&meta.location).await.unwrap();
    }
    let error = tokio::time::timeout(Duration::from_secs(60), failure(&mut stream))
        .await
        .expect("the stream kept retrying");
    assert_eq!(error.code, tonic::Code::Unavailable as i32, "{error:?}");
    assert_eq!(fixture.backups().status(), (0, 0));
    fixture.close().await;
}

/// The count pass runs after the backup returns its stream, so apply continues during a long
/// count. A client that leaves before the first chunk releases the pin.
#[tokio::test]
async fn a_long_count_pass_does_not_hold_apply() {
    let mut fixture = Fixture::new(4).await;
    let first = fixture.state.base_checkpoint().unwrap();
    let hold = Arc::new(tokio::sync::Semaphore::new(0));
    *fixture.backups().hold.lock().unwrap() = Some(hold.clone());
    let started = std::time::Instant::now();
    let mut stream = fixture.state.backup().await.unwrap();
    assert!(started.elapsed() < Duration::from_secs(1));
    fixture.put("during", "count").await;
    fixture.switch().await;
    // The Maintenance handler waits 10 s for the stream. The count takes longer.
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(11)).await;
    fixture.put("still", "applies").await;
    let pending = tokio::time::timeout(Duration::from_millis(100), stream.recv()).await;
    assert!(pending.is_err(), "the stream sent data during the count");
    tokio::time::resume();
    assert!(fixture.pins(first));
    drop(stream);
    fixture.released(first, 0).await;
    fixture.close().await;
}

/// A client that takes no chunk for `IDLE_MAX` ends the stream with Unavailable and frees its
/// pin. The time of the count pass does not count.
#[tokio::test]
async fn an_idle_client_ends_a_shared_backup() {
    let mut fixture = Fixture::new(200).await;
    let first = fixture.state.base_checkpoint().unwrap();
    let hold = Arc::new(tokio::sync::Semaphore::new(0));
    *fixture.backups().hold.lock().unwrap() = Some(hold.clone());
    let mut stream = fixture.state.backup().await.unwrap();
    fixture.switch().await;
    tokio::time::pause();
    tokio::time::advance(IDLE_MAX + Duration::from_secs(30)).await;
    hold.add_permits(1);
    stream
        .recv()
        .await
        .unwrap()
        .expect("the count pass counted as idle");
    // The task fills the buffer and waits for the client.
    tokio::time::sleep(Duration::from_millis(100)).await;
    tokio::time::advance(IDLE_MAX - Duration::from_secs(1)).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(fixture.backups().status().0, 1, "the stream ended early");
    tokio::time::advance(Duration::from_secs(2)).await;
    fixture.released(first, 0).await;
    // The error comes before the buffered chunks.
    let Some(Err(error)) = stream.recv().await else {
        panic!("the ended backup sent a buffered chunk");
    };
    let error = backup_error(error);
    tokio::time::resume();
    assert_eq!(error.code, tonic::Code::Unavailable as i32, "{error:?}");
    assert!(error.message.contains("took no chunk"), "{error:?}");
    fixture.close().await;
}

/// Put rows of 10 KiB under `prefix` until the overlay holds `bytes`.
async fn fill_overlay(fixture: &mut Fixture, prefix: &str, bytes: u64) {
    let value = "w".repeat(10 * 1024);
    let mut n = 0;
    while fixture.state.view_status().unwrap().2 < bytes {
        fixture.put(&format!("{prefix}/{n:03}"), &value).await;
        n += 1;
    }
}

/// A slow backup keeps the segments that base switches release. While the leader keeps writing,
/// the pinned segments pass the budget, and the oldest backup ends. It ends at once, though its
/// client still has a full buffer to read, and the client then reads the error first. A new
/// backup above the budget fails with ResourceExhausted, and it succeeds later.
#[tokio::test]
async fn the_budget_ends_the_oldest_backup_with_a_full_buffer() {
    const KIB: u64 = 1024;
    let mut fixture = Fixture::new(200).await;
    let first = fixture.state.base_checkpoint().unwrap();
    let limit = 1024 * KIB;
    let registry = prometheus::Registry::new();
    fixture.backups().configure(limit, &registry).unwrap();

    let mut oldest = fixture.state.backup().await.unwrap();
    oldest.recv().await.unwrap().unwrap();
    fill().await;
    // The oldest backup pins the open segment, which takes these writes.
    fill_overlay(&mut fixture, "first", 600 * KIB).await;
    fixture.switch().await;
    let newer = fixture.state.backup().await.unwrap();
    let (streams, bytes) = fixture.backups().status();
    assert!(streams == 2 && bytes >= 600 * KIB && bytes <= limit);
    // The newer backup pins the next segment. Together they pass the budget.
    fill_overlay(&mut fixture, "second", 600 * KIB).await;
    fixture.switch().await;
    for _ in 0..200 {
        if fixture.backups().status().0 == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let (streams, bytes) = fixture.backups().status();
    assert!(streams == 1 && bytes <= limit, "{streams} {bytes}");
    let text = prometheus::TextEncoder::new()
        .encode_to_string(&registry.gather())
        .unwrap();
    assert!(
        text.contains("slatedb_etcd_shared_backup_streams 1\n"),
        "{text}"
    );
    let retained = format!("slatedb_etcd_shared_backup_retained_bytes {bytes}\n");
    assert!(text.contains(&retained), "{text}");
    fixture.released(first, 1).await;
    // The error comes before the buffered chunks.
    let Some(Err(error)) = oldest.recv().await else {
        panic!("the ended backup sent a buffered chunk");
    };
    let error = backup_error(error);
    assert_eq!(error.code, tonic::Code::Unavailable as i32, "{error:?}");
    assert!(error.message.contains("backup too slow"), "{error:?}");
    assert!(oldest.recv().await.is_none());

    // With the newer backup, a view with a large open segment passes the budget.
    fill_overlay(&mut fixture, "third", 600 * KIB).await;
    let error = backup_error(fixture.state.backup().await.err().unwrap());
    assert_eq!(
        error.code,
        tonic::Code::ResourceExhausted as i32,
        "{error:?}"
    );
    // The newer backup completes at its boundary.
    let image = decode(&collect(newer).await);
    assert!(image
        .rows
        .iter()
        .any(|(key, _)| *key == current("first/000")));
    assert!(!image
        .rows
        .iter()
        .any(|(key, _)| *key == current("second/000")));
    // The rejected backup succeeds on a retry.
    collect(fixture.state.backup().await.unwrap()).await;
    fixture.close().await;
}

/// A member streams at most `MAX_STREAMS` backups. Another one fails with ResourceExhausted
/// until a stream ends.
#[tokio::test]
async fn a_member_streams_a_bounded_number_of_backups() {
    let fixture = Fixture::new(1).await;
    let hold = Arc::new(tokio::sync::Semaphore::new(0));
    *fixture.backups().hold.lock().unwrap() = Some(hold.clone());
    let mut streams = vec![];
    for _ in 0..MAX_STREAMS {
        streams.push(fixture.state.backup().await.unwrap());
    }
    let error = backup_error(fixture.state.backup().await.err().unwrap());
    assert_eq!(
        error.code,
        tonic::Code::ResourceExhausted as i32,
        "{error:?}"
    );
    drop(streams.pop());
    for _ in 0..200 {
        if fixture.backups().status().0 < MAX_STREAMS {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    streams.push(fixture.state.backup().await.unwrap());
    hold.add_permits(1);
    for stream in streams {
        collect(stream).await;
    }
    fixture.close().await;
}
