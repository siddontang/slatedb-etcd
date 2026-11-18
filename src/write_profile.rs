//! Count work on the Raft owner, WAL thread, and apply worker in diagnostic builds.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::future::{poll_fn, Future};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::task::{Context, Wake, Waker};

use serde_json::{json, Value};

#[derive(Clone, Copy)]
pub(crate) enum Stage {
    Raft,
    Wal,
    Apply,
}

use Stage::*;

#[derive(Default)]
struct Counts {
    batches: AtomicU64,
    entries: AtomicU64,
    allocations: AtomicU64,
    bytes: AtomicU64,
    wakes_from: [AtomicU64; 3],
}

static COUNTS: [Counts; 3] = [const {
    Counts {
        batches: AtomicU64::new(0),
        entries: AtomicU64::new(0),
        allocations: AtomicU64::new(0),
        bytes: AtomicU64::new(0),
        wakes_from: [const { AtomicU64::new(0) }; 3],
    }
}; 3];

thread_local! { static ACTIVE: Cell<Option<Stage>> = const { Cell::new(None) }; }

pub(crate) struct Scope(Option<Stage>);

impl Drop for Scope {
    fn drop(&mut self) {
        ACTIVE.set(self.0);
    }
}

impl Stage {
    pub(crate) fn enter(self) -> Scope {
        Scope(ACTIVE.replace(Some(self)))
    }

    pub(crate) fn batch(self, entries: usize) {
        let counts = &COUNTS[self as usize];
        counts.batches.fetch_add(1, Relaxed);
        counts.entries.fetch_add(entries as u64, Relaxed);
    }
}

/// Instrument the system allocator in the standalone profiling binary.
pub struct StageAllocator;

fn allocation(size: usize) {
    if let Some(stage) = ACTIVE.get() {
        let counts = &COUNTS[stage as usize];
        counts.allocations.fetch_add(1, Relaxed);
        counts.bytes.fetch_add(size as u64, Relaxed);
    }
}

// The wrapper forwards the original pointer and layout to the system allocator.
unsafe impl GlobalAlloc for StageAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        allocation(layout.size());
        System.alloc(layout)
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        allocation(layout.size());
        System.alloc_zeroed(layout)
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        allocation(size);
        System.realloc(ptr, layout, size)
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout)
    }
}

struct Target {
    stage: Stage,
    parent: Mutex<Option<Waker>>,
}

impl Wake for Target {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        if let Some(source) = ACTIVE.get() {
            COUNTS[self.stage as usize].wakes_from[source as usize].fetch_add(1, Relaxed);
        }
        let parent = self.parent.lock().unwrap().clone();
        if let Some(parent) = parent {
            parent.wake();
        }
    }
}

pub(crate) struct Probe {
    target: Arc<Target>,
    waker: Waker,
}

impl Probe {
    pub(crate) fn new(stage: Stage) -> Self {
        let target = Arc::new(Target {
            stage,
            parent: Mutex::new(None),
        });
        Self {
            waker: target.clone().into(),
            target,
        }
    }

    pub(crate) async fn run<F: Future>(&self, future: F) -> F::Output {
        let mut future = std::pin::pin!(future);
        poll_fn(|cx| {
            self.target
                .parent
                .lock()
                .unwrap()
                .replace(cx.waker().clone());
            let _stage = self.target.stage.enter();
            future.as_mut().poll(&mut Context::from_waker(&self.waker))
        })
        .await
    }
}

pub(crate) fn snapshot() -> Value {
    let mut result = json!({});
    for (name, stage) in [("raft", Raft), ("wal", Wal), ("apply", Apply)] {
        let counts = &COUNTS[stage as usize];
        result[name] = json!({
            "batches": counts.batches.load(Relaxed), "entries": counts.entries.load(Relaxed),
            "allocation_calls": counts.allocations.load(Relaxed), "requested_bytes": counts.bytes.load(Relaxed),
            "wakes_from_raft": counts.wakes_from[Raft as usize].load(Relaxed),
            "wakes_from_wal": counts.wakes_from[Wal as usize].load(Relaxed),
            "wakes_from_apply": counts.wakes_from[Apply as usize].load(Relaxed),
        });
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::Future;
    use std::task::Poll;

    #[tokio::test]
    async fn pending_receiver_forwards_its_wake_and_restores_the_callers_stage() {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let probe = Probe::new(Apply);
        let mut wait = std::pin::pin!(probe.run(rx));
        let before = COUNTS[Apply as usize].wakes_from[Wal as usize].load(Relaxed);
        poll_fn(|cx| {
            assert!(wait.as_mut().poll(cx).is_pending());
            assert!(ACTIVE.get().is_none());
            Poll::Ready(())
        })
        .await;
        {
            let _source = Wal.enter();
            tx.send(17).unwrap();
        }
        assert_eq!(wait.await.unwrap(), 17);
        assert!(COUNTS[Apply as usize].wakes_from[Wal as usize].load(Relaxed) > before);
    }
}
