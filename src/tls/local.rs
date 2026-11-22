//! Run the HTTP/2 streams of one connection inside that connection's task.
//!
//! hyper spawns one task for each HTTP/2 stream. All streams of a connection share one h2
//! state mutex. When runtime workers poll these tasks in parallel, they wait on that mutex.
//! In a macOS profile of one-member Range load at concurrency 32, 68% of busy thread samples
//! waited on or released that mutex. The etcd client uses one connection for each endpoint.
//!
//! `Local` gives hyper an executor that queues each stream future for the connection task.
//! That task polls the connection and its streams in turn, so the mutex has no contention.
//! Ready streams run before the connection is polled again, so their replies share a flush.
//! Request work that waits for Raft or a read job still runs in other tasks.

use std::future::Future;
use std::pin::Pin;

use futures::stream::{FuturesUnordered, StreamExt};
use tokio::sync::mpsc;

type Task = Pin<Box<dyn Future<Output = ()> + Send>>;

/// An executor that sends stream futures to their connection task.
#[derive(Clone)]
pub(crate) struct Local {
    tasks: mpsc::UnboundedSender<Task>,
}

impl<F> hyper::rt::Executor<F> for Local
where
    F: Future<Output = ()> + Send + 'static,
{
    fn execute(&self, future: F) {
        // A failed send means that the connection task stopped. The stream stops with it.
        let _ = self.tasks.send(Box::pin(future));
    }
}

/// Create an executor and the receiver that `drive` polls.
pub(crate) fn executor() -> (Local, mpsc::UnboundedReceiver<Task>) {
    let (tasks, receiver) = mpsc::unbounded_channel();
    (Local { tasks }, receiver)
}

/// Poll `connection` and the streams that it starts until all of them complete.
pub(crate) async fn drive<C: Future>(
    connection: C,
    mut receiver: mpsc::UnboundedReceiver<Task>,
) -> C::Output {
    let mut streams = FuturesUnordered::new();
    tokio::pin!(connection);
    let output = loop {
        tokio::select! {
            biased;
            Some(task) = receiver.recv() => streams.push(task),
            Some(()) = streams.next(), if !streams.is_empty() => {}
            output = &mut connection => break output,
        }
    };
    // hyper can still own a stream after the connection future returns.
    receiver.close();
    while let Some(task) = receiver.recv().await {
        streams.push(task);
    }
    while streams.next().await.is_some() {}
    output
}

#[cfg(test)]
mod tests {
    use super::*;
    use hyper::rt::Executor;

    #[tokio::test]
    async fn streams_finish_before_and_after_the_connection() {
        let (local, receiver) = executor();
        let (early_tx, early_rx) = tokio::sync::oneshot::channel();
        let (late_tx, late_rx) = tokio::sync::oneshot::channel::<()>();
        let (done_tx, done_rx) = tokio::sync::oneshot::channel();
        local.execute(async move {
            let _ = early_tx.send(());
        });
        local.execute(async move {
            let _ = late_rx.await;
            let _ = done_tx.send(());
        });
        let connection = async move {
            early_rx.await.unwrap();
            drop(local);
            7
        };
        let driver = tokio::spawn(drive(connection, receiver));
        tokio::task::yield_now().await;
        assert!(!driver.is_finished());
        late_tx.send(()).unwrap();
        assert_eq!(driver.await.unwrap(), 7);
        done_rx.await.unwrap();
    }
}
