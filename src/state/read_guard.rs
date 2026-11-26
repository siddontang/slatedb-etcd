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
//! - Then it waits for a grace period, as RCU does. The filter gives each such SST the current
//!   epoch as its ticket, and GC deletes the SST after every read of that epoch has ended. A
//!   ticket stays across GC runs, and each run moves the epochs forward when the older reads have
//!   ended. Reads that start later do not delay a ticket, so GC makes progress while bounded
//!   reads overlap without a break. A read that runs for minutes delays only the deletion.
//!
//! The compactor checkpoint stays as a second line of defense for reads that hold no guard.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::{Duration, Instant};

use slatedb::Db;
use slatedb_common::ObjectMetadata;

/// Counters of running reads. Reads on different threads use different stripes, so they rarely
/// share a cache line.
const STRIPES: usize = 16;

/// How long one GC run waits for reads to end. Point reads and paged scans end in milliseconds.
/// SSTs that wait for a longer read stay for a later GC run.
const GRACE_WAIT: Duration = Duration::from_secs(2);

#[repr(align(128))]
struct Stripe([AtomicUsize; 2]);

struct Readers {
    /// The current epoch. A read counts in the half `epoch % 2` of its stripe.
    epoch: AtomicUsize,
    /// Every read that started in an epoch below this value has ended.
    completed: AtomicUsize,
    stripes: [Stripe; STRIPES],
    /// Serializes `advance`. The GC tasks of two databases may call it at the same time.
    advancing: Mutex<()>,
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
            completed: AtomicUsize::new(0),
            stripes: [const { Stripe([AtomicUsize::new(0), AtomicUsize::new(0)]) }; STRIPES],
            advancing: Mutex::new(()),
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

    /// Move the epochs forward if the reads of the previous epoch have ended, and return
    /// `completed`.
    ///
    /// In epoch `e`, the other half holds only reads of epoch `e - 1`: the step to `e` waited
    /// for the reads of `e - 2`, which used the same half. When that half is empty, every read
    /// below `e` has ended, and epoch `e + 1` can reuse it.
    fn advance(&self) -> usize {
        let _one = self.advancing.lock().unwrap();
        let epoch = self.epoch.load(SeqCst);
        if self.drained((epoch + 1) % 2) {
            self.completed.store(epoch, SeqCst);
            self.epoch.store(epoch + 1, SeqCst);
        }
        self.completed.load(SeqCst)
    }

    /// The ticket for an SST that no read can take from now on: the reads that may still use it
    /// started in this epoch or earlier.
    fn ticket(&self) -> usize {
        self.epoch.load(SeqCst)
    }

    /// Wait for at most `wait` until every read of the epoch `ticket` or earlier has ended.
    /// Return the `completed` value at the end.
    async fn wait_for(&self, ticket: usize, wait: Duration) -> usize {
        let deadline = Instant::now() + wait;
        loop {
            let completed = self.advance();
            if completed > ticket || Instant::now() >= deadline {
                return completed;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }
}

/// The GC filter of a member-local database. It passes every object other than compacted SSTs.
#[derive(Default)]
pub(crate) struct LiveSstFilter {
    db: OnceLock<Weak<Db>>,
    /// Unlisted SSTs that wait for the reads of their ticket's epoch to end, by SST ID.
    tickets: Mutex<HashMap<String, usize>>,
}

impl LiveSstFilter {
    /// Bind the filter to its database. Until then, it keeps every compacted SST.
    pub(crate) fn bind(&self, db: &Arc<Db>) {
        let _ = self.db.set(Arc::downgrade(db));
    }

    async fn allowed(&self, candidates: HashSet<ObjectMetadata>) -> HashSet<ObjectMetadata> {
        self.allowed_with(&READERS, candidates, GRACE_WAIT).await
    }

    async fn allowed_with(
        &self,
        readers: &Readers,
        candidates: HashSet<ObjectMetadata>,
        wait: Duration,
    ) -> HashSet<ObjectMetadata> {
        let (ssts, mut allowed): (HashSet<_>, HashSet<_>) = candidates
            .into_iter()
            .partition(|object| compacted_sst(object).is_some());
        if ssts.is_empty() {
            return allowed;
        }
        let Some(db) = self.db.get().and_then(Weak::upgrade) else {
            return allowed;
        };
        // Reads that start after this point take this SST list or a later one.
        let listed: HashSet<String> = db
            .manifest()
            .all_sst_views()
            .map(|view| view.sst.id.value().to_string())
            .collect();
        drop(db);
        let unlisted: Vec<_> = ssts
            .into_iter()
            .filter(|object| compacted_sst(object).is_some_and(|id| !listed.contains(id)))
            .collect();
        if unlisted.is_empty() {
            return allowed;
        }
        // Read the epoch after the list: a read that took an older list holds a guard of this
        // epoch or an earlier one. An SST keeps the ticket of the first run that saw it unlisted.
        let ticket = readers.ticket();
        let tickets: HashMap<String, usize> = {
            let mut pending = self.tickets.lock().unwrap();
            let tickets: HashMap<_, _> = unlisted
                .iter()
                .filter_map(compacted_sst)
                .map(|id| (id.to_owned(), pending.get(id).copied().unwrap_or(ticket)))
                .collect();
            // Forget SSTs that GC no longer offers, for example because it deleted them.
            *pending = tickets.clone();
            tickets
        };
        let newest = tickets.values().copied().max().unwrap_or(ticket);
        let completed = readers.wait_for(newest, wait).await;
        let mut waiting = 0;
        for object in unlisted {
            let id = compacted_sst(&object).expect("a compacted SST");
            if tickets[id] < completed {
                allowed.insert(object);
            } else {
                waiting += 1;
            }
        }
        if waiting > 0 {
            tracing::debug!(
                count = waiting,
                "running reads delay the deletion of replaced SSTs"
            );
        }
        allowed
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

    const NO_WAIT: Duration = Duration::from_millis(1);

    /// A ticket waits for the reads of its epoch and earlier ones, not for later reads.
    #[tokio::test]
    async fn a_ticket_waits_only_for_earlier_reads() {
        let readers = readers();
        let early = readers.enter();
        let ticket = readers.ticket();
        assert!(readers.wait_for(ticket, NO_WAIT).await <= ticket);
        // This read starts after GC moved to the next epoch.
        let later = readers.enter();
        assert!(readers.wait_for(ticket, NO_WAIT).await <= ticket);
        drop(early);
        assert!(
            readers.wait_for(ticket, NO_WAIT).await > ticket,
            "`later` must not block"
        );
        drop(later);
    }

    /// A read that ends during the wait lets the wait finish.
    #[tokio::test]
    async fn a_wait_ends_when_the_read_ends() {
        let readers = readers();
        let read = readers.enter();
        let ticket = readers.ticket();
        let end = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            drop(read);
        });
        assert!(readers.wait_for(ticket, Duration::from_secs(10)).await > ticket);
        end.await.unwrap();
    }

    /// Model of GC against reads that overlap without a break, each longer than the GC wait, as
    /// with busy keys-only Range requests. The SST list has a version; SST `k` is listed only in
    /// version `k`. GC unlists SST `k` and takes a ticket. Each run frees the SSTs whose ticket
    /// has completed. A read takes the version under its guard and checks, while it holds the
    /// guard, that its SST is never freed. GC must still free SSTs.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn overlapping_reads_never_lose_an_sst_and_never_stop_gc() {
        use std::sync::atomic::{AtomicBool, AtomicI64};
        let readers = readers();
        let listed = Arc::new(AtomicI64::new(0));
        let freed = Arc::new(AtomicI64::new(-1));
        let stop = Arc::new(AtomicBool::new(false));
        let reads = Arc::new(AtomicI64::new(0));
        let mut tasks = vec![];
        for task in 0..4u64 {
            let (listed, freed, stop, reads) =
                (listed.clone(), freed.clone(), stop.clone(), reads.clone());
            tasks.push(tokio::spawn(async move {
                // Start the readers at different times, so one is always running.
                tokio::time::sleep(Duration::from_millis(5 * task)).await;
                while !stop.load(SeqCst) {
                    let _read = readers.enter();
                    let sst = listed.load(SeqCst);
                    for _ in 0..4 {
                        assert!(freed.load(SeqCst) < sst, "SST {sst} freed during a read");
                        tokio::time::sleep(Duration::from_millis(5)).await;
                    }
                    reads.fetch_add(1, SeqCst);
                }
            }));
        }
        let mut pending = std::collections::VecDeque::new();
        let mut freed_count = 0;
        let deadline = Instant::now() + Duration::from_secs(30);
        while freed_count < 100 || reads.load(SeqCst) < 400 {
            assert!(
                Instant::now() < deadline,
                "freed {freed_count}, reads {reads:?}"
            );
            // Unlist an SST, then take its ticket.
            pending.push_back((listed.fetch_add(1, SeqCst), readers.ticket()));
            let completed = readers.wait_for(pending[0].1, NO_WAIT).await;
            while pending
                .front()
                .is_some_and(|(_, ticket)| *ticket < completed)
            {
                let (sst, _) = pending.pop_front().unwrap();
                freed.fetch_max(sst, SeqCst);
                freed_count += 1;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
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

    /// The filter keeps an SST that the database's in-memory manifest still lists, even with no
    /// running read: a read that starts later could take it. It passes an unlisted SST once
    /// its ticket completes, and every object that is not a compacted SST.
    #[tokio::test]
    async fn the_filter_keeps_listed_ssts() {
        let readers = readers();
        let store = Arc::new(slatedb::object_store::memory::InMemory::new());
        let db = Arc::new(Db::builder("db", store).build().await.unwrap());
        db.put(b"k", b"v").await.unwrap();
        db.flush_with_options(slatedb::config::FlushOptions {
            flush_type: slatedb::config::FlushType::MemTable,
        })
        .await
        .unwrap();
        let listed: Vec<String> = db
            .manifest()
            .all_sst_views()
            .map(|view| view.sst.id.value().to_string())
            .collect();
        assert!(!listed.is_empty());
        let listed = object(&format!("db/compacted/{}.sst", listed[0]));
        let unlisted = object("db/compacted/01KB0000000000000000000000.sst");
        let wal = object("db/wal/00000000000000000001.sst");
        let candidates = HashSet::from([listed.clone(), unlisted.clone(), wal.clone()]);

        let filter = LiveSstFilter::default();
        // An unbound filter keeps every compacted SST.
        let allowed = filter
            .allowed_with(readers, candidates.clone(), NO_WAIT)
            .await;
        assert_eq!(allowed, HashSet::from([wal.clone()]));

        filter.bind(&db);
        let allowed = filter
            .allowed_with(readers, candidates, Duration::from_secs(10))
            .await;
        assert_eq!(allowed, HashSet::from([unlisted, wal]));
        db.close().await.unwrap();
    }
}
