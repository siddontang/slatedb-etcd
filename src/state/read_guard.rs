//! Keep compacted SSTs on disk while a read may still use them.
//!
//! A SlateDB read takes the database's in-memory SST list when it starts: a point read uses it
//! until it returns, and an iterator uses it until it is dropped. When the compactor replaces
//! SSTs, it writes a checkpoint that keeps them for `CompactorOptions::checkpoint_lifetime`, and
//! garbage collection deletes them after that. SlateDB does not know which reads are still
//! running, so a read that outlives the lifetime fails with `NotFound`.
//!
//! On a member-local directory, this module lets GC delete a replaced SST only when no running
//! read can use it:
//!
//! - Each read of the member's database holds a [`ReadGuard`] from before it takes the SST list
//!   until it ends. Entering is two atomic operations on a striped counter.
//! - [`LiveSstFilter`], a SlateDB `GcFilter`, keeps every candidate SST that the database's
//!   in-memory manifest still lists. A read that starts later cannot get any other candidate.
//! - Then it waits for a grace period, as RCU does: it starts a new epoch and waits until every
//!   read of the previous epoch has ended. If a long read is still running, it deletes nothing
//!   now and tries again at the next GC run.
//!
//! The compactor checkpoint stays as a second line of defense for reads that hold no guard.

use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
use std::sync::{Arc, OnceLock, Weak};
use std::time::{Duration, Instant};

use slatedb::Db;
use slatedb_common::ObjectMetadata;

/// Counters of running reads. Reads on different threads use different stripes, so they rarely
/// share a cache line.
const STRIPES: usize = 16;

/// How long one GC run waits for the reads of the previous epoch. Point reads and paged scans
/// end in milliseconds. A longer read makes GC skip this run.
const GRACE_WAIT: Duration = Duration::from_secs(2);

#[repr(align(128))]
struct Stripe([AtomicUsize; 2]);

struct Readers {
    /// The current epoch. A read counts in the half `epoch % 2` of its stripe.
    epoch: AtomicUsize,
    stripes: [Stripe; STRIPES],
    /// One grace period at a time. Two GC tasks of different databases may call the filter.
    grace: tokio::sync::Mutex<()>,
}

static READERS: Readers = Readers::new();

/// A running read. Take it before the read takes its SST list, and drop it after the read's last
/// use of an SST.
#[must_use]
pub(crate) struct ReadGuard {
    readers: &'static Readers,
    stripe: usize,
    half: usize,
}

/// Start a read.
pub(crate) fn enter() -> ReadGuard {
    READERS.enter()
}

impl Drop for ReadGuard {
    fn drop(&mut self) {
        self.readers.stripes[self.stripe].0[self.half].fetch_sub(1, SeqCst);
    }
}

fn stripe() -> usize {
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    thread_local! {
        static STRIPE: usize = NEXT.fetch_add(1, SeqCst) % STRIPES;
    }
    STRIPE.with(|stripe| *stripe)
}

impl Readers {
    const fn new() -> Self {
        Self {
            epoch: AtomicUsize::new(0),
            stripes: [const { Stripe([AtomicUsize::new(0), AtomicUsize::new(0)]) }; STRIPES],
            grace: tokio::sync::Mutex::const_new(()),
        }
    }

    fn enter(&'static self) -> ReadGuard {
        let stripe = stripe();
        let counters = &self.stripes[stripe].0;
        loop {
            let epoch = self.epoch.load(SeqCst);
            let half = epoch % 2;
            counters[half].fetch_add(1, SeqCst);
            // If GC started a new epoch in between, it may have missed this count. Count again
            // in the new epoch, which GC has not waited for yet.
            if self.epoch.load(SeqCst) == epoch {
                return ReadGuard {
                    readers: self,
                    stripe,
                    half,
                };
            }
            counters[half].fetch_sub(1, SeqCst);
        }
    }

    fn drained(&self, half: usize) -> bool {
        self.stripes
            .iter()
            .all(|stripe| stripe.0[half].load(SeqCst) == 0)
    }

    /// Wait until every read that started before this call has ended, for at most `wait`.
    /// Return false if a read may still be running.
    async fn grace_period(&self, wait: Duration) -> bool {
        let _one = self.grace.lock().await;
        let epoch = self.epoch.load(SeqCst);
        // Reads of the epoch before `epoch` must end before their half is reused.
        if !self.drained((epoch + 1) % 2) {
            return false;
        }
        self.epoch.store(epoch + 1, SeqCst);
        let deadline = Instant::now() + wait;
        loop {
            if self.drained(epoch % 2) {
                return true;
            }
            if Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }
}

/// The GC filter of a member-local database. It passes every object other than compacted SSTs.
#[derive(Default)]
pub(crate) struct LiveSstFilter {
    db: OnceLock<Weak<Db>>,
}

impl LiveSstFilter {
    /// Bind the filter to its database. Until then, it keeps every compacted SST.
    pub(crate) fn bind(&self, db: &Arc<Db>) {
        let _ = self.db.set(Arc::downgrade(db));
    }

    async fn allowed(&self, candidates: HashSet<ObjectMetadata>) -> HashSet<ObjectMetadata> {
        let (ssts, others): (HashSet<_>, HashSet<_>) = candidates
            .into_iter()
            .partition(|object| compacted_sst(object).is_some());
        if ssts.is_empty() {
            return others;
        }
        let Some(db) = self.db.get().and_then(Weak::upgrade) else {
            return others;
        };
        // Reads that start after this point take this SST list or a later one.
        let listed: HashSet<String> = db
            .manifest()
            .all_sst_views()
            .map(|view| view.sst.id.value().to_string())
            .collect();
        drop(db);
        let unlisted: HashSet<_> = ssts
            .into_iter()
            .filter(|object| compacted_sst(object).is_some_and(|id| !listed.contains(id)))
            .collect();
        if unlisted.is_empty() || !READERS.grace_period(GRACE_WAIT).await {
            if !unlisted.is_empty() {
                tracing::debug!(
                    count = unlisted.len(),
                    "a running read delays the deletion of replaced SSTs"
                );
            }
            return others;
        }
        others.into_iter().chain(unlisted).collect()
    }
}

#[async_trait::async_trait]
impl slatedb::GcFilter for LiveSstFilter {
    async fn filter(&self, candidates: HashSet<ObjectMetadata>) -> HashSet<ObjectMetadata> {
        self.allowed(candidates).await
    }
}

/// The SST ID of `object` if it is a compacted SST, `<root>/compacted/<id>.sst`.
fn compacted_sst(object: &ObjectMetadata) -> Option<&str> {
    let mut parts = object.location.as_ref().rsplit('/');
    let name = parts.next()?;
    (parts.next()? == "compacted")
        .then(|| name.strip_suffix(".sst"))
        .flatten()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn readers() -> &'static Readers {
        Box::leak(Box::new(Readers::new()))
    }

    const NO_WAIT: Duration = Duration::from_millis(20);

    /// A grace period waits for reads that started before it, not for later ones.
    #[tokio::test]
    async fn grace_period_waits_only_for_earlier_reads() {
        let readers = readers();
        assert!(readers.grace_period(NO_WAIT).await);

        let early = readers.enter();
        assert!(!readers.grace_period(NO_WAIT).await);
        // The early read still blocks the next grace periods, and a later read does not
        // unblock them.
        let later = readers.enter();
        assert!(!readers.grace_period(NO_WAIT).await);
        drop(early);
        let after = readers.enter();
        assert!(
            !readers.grace_period(NO_WAIT).await,
            "`later` started before"
        );
        drop(later);
        // `after` started before this grace period too.
        assert!(!readers.grace_period(NO_WAIT).await);
        drop(after);
        assert!(readers.grace_period(NO_WAIT).await);
    }

    /// A read that ends during the wait lets the grace period finish.
    #[tokio::test]
    async fn grace_period_ends_when_the_read_ends() {
        let readers = readers();
        let read = readers.enter();
        let end = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            drop(read);
        });
        assert!(readers.grace_period(Duration::from_secs(10)).await);
        end.await.unwrap();
    }

    /// Model of GC against concurrent reads. The SST list has a version; SST `k` is listed
    /// only in version `k`. GC unlists SST `k` (the version moves to `k + 1`), waits for a grace
    /// period, and frees SST `k`. A read takes the version under its guard and checks, while it
    /// holds the guard, that its SST is never freed.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_freed_sst_is_never_in_use() {
        use std::sync::atomic::{AtomicBool, AtomicI64};
        let readers = readers();
        let listed = Arc::new(AtomicI64::new(0));
        let freed = Arc::new(AtomicI64::new(-1));
        let stop = Arc::new(AtomicBool::new(false));
        let reads = Arc::new(AtomicI64::new(0));
        let mut tasks = vec![];
        for _ in 0..4 {
            let (listed, freed, stop, reads) =
                (listed.clone(), freed.clone(), stop.clone(), reads.clone());
            tasks.push(tokio::spawn(async move {
                while !stop.load(SeqCst) {
                    let _read = readers.enter();
                    let sst = listed.load(SeqCst);
                    for _ in 0..3 {
                        assert!(freed.load(SeqCst) < sst, "SST {sst} freed during a read");
                        tokio::task::yield_now().await;
                    }
                    reads.fetch_add(1, SeqCst);
                }
            }));
        }
        let mut freed_count = 0;
        let deadline = Instant::now() + Duration::from_secs(20);
        while freed_count < 300 || reads.load(SeqCst) < 10_000 {
            assert!(
                Instant::now() < deadline,
                "freed {freed_count}, reads {reads:?}"
            );
            let sst = listed.fetch_add(1, SeqCst);
            if readers.grace_period(Duration::from_millis(100)).await {
                freed.fetch_max(sst, SeqCst);
                freed_count += 1;
            }
        }
        stop.store(true, SeqCst);
        for task in tasks {
            task.await.unwrap();
        }
    }

    fn object(location: &str) -> ObjectMetadata {
        ObjectMetadata {
            location: location.into(),
            last_modified: chrono::DateTime::<chrono::Utc>::UNIX_EPOCH,
            size: 1,
            e_tag: None,
            version: None,
        }
    }

    #[test]
    fn only_compacted_ssts_are_filtered() {
        let id = "01KB0000000000000000000000";
        assert_eq!(
            compacted_sst(&object(&format!("db/compacted/{id}.sst"))),
            Some(id)
        );
        assert_eq!(
            compacted_sst(&object(&format!("compacted/{id}.sst"))),
            Some(id)
        );
        assert_eq!(
            compacted_sst(&object("db/wal/00000000000000000001.sst")),
            None
        );
        assert_eq!(
            compacted_sst(&object("db/manifest/00000000000000000001.manifest")),
            None
        );
        assert_eq!(compacted_sst(&object(&format!("{id}.sst"))), None);
    }
}
