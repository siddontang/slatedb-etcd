//! The materializer of a leader whose base released a batch above the written sequence.

use super::*;
use crate::state::overlay::Overlay;
use slatedb::object_store::memory::InMemory;

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

/// A member grouped entries 1 and 2 into a batch at 2. A base with row 1 certified through 2
/// released it. As leader, the writer opens at sequence 1, and the batch is gone. The
/// materializer must wait for a later batch: no busy polls, the physical sequence stays at
/// 1, and the stop that `lead` selects ends it.
#[tokio::test]
async fn an_idle_materializer_waits_past_a_released_batch() {
    use std::future::Future;
    let db = Arc::new(
        slatedb::Db::builder("state", Arc::new(InMemory::new()))
            .with_settings(crate::state::slatedb_settings())
            .build()
            .await
            .unwrap(),
    );
    db.write_with_options(
        batch(1, &[("a", "1")]).to_storage(),
        &WriteOptions { seqnum: 1 },
    )
    .await
    .unwrap();
    let mut overlay = Overlay::new(0);
    overlay.push(2, batch(2, &[("a", "1")]));
    overlay.close_segment();
    overlay.release(2);
    let queue = overlay.queue();
    assert_eq!((queue.pushed(), queue.len(), queue.released()), (2, 0, 2));
    let progress = Arc::new(Progress::new(1));
    let mut task = Box::pin(materialize(
        db.clone(),
        queue,
        progress.clone(),
        clock::Gate::unchecked(),
        0,
    ));
    let mut polls = 0;
    tokio::select! {
        biased;
        _ = tokio::time::sleep(Duration::from_millis(30)) => {}
        result = std::future::poll_fn(|cx| {
            polls += 1;
            task.as_mut().poll(cx)
        }) => panic!("the idle materializer returned: {result:?}"),
    }
    assert!((1..=4).contains(&polls), "polled {polls} times");
    assert_eq!(progress.written(), 1);
    // A later batch goes to the database at its own sequence.
    overlay.push(3, batch(3, &[("b", "3")]));
    let (stop, mut stopped) = tokio::sync::watch::channel(false);
    let run = tokio::spawn(async move {
        tokio::select! {
            result = task => result,
            _ = stop_requested(&mut stopped) => Ok(()),
        }
    });
    progress.wait_written(3).await.unwrap();
    assert_eq!(value(&db, "b").await, Some("3".into()));
    stop.send_replace(true);
    tokio::time::timeout(Duration::from_secs(1), run)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    db.close().await.unwrap();
}
