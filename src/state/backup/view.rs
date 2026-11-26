//! Shared-mode backups: stream the image of a pinned view.
//!
//! Apply pins the view at the covered index: the base and the overlay segments of the applied
//! state. The stream task then reads the view in the two passes of `Rows::copy`. The base pins
//! its checkpoint, and the member reports it to the leader, so retention keeps it. A backup
//! creates no checkpoint.
//!
//! The pinned segments stay in memory after a base switch releases them. `Backups` bounds that
//! memory on each member. See `docs/shared-state.md#backups`.

use std::collections::{HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use anyhow::Result;
use tokio::sync::{mpsc, oneshot};

use super::{omitted, Chunk, Rows, Stream, TooLarge};
use crate::state::overlay::Segment;
use crate::state::shared::{Shared, Source};
use crate::state::view::{Base, SharedView, StateView};
use crate::state::{quota, StateMachine};

/// The most backups that stream at one time on a member.
pub(crate) const MAX_STREAMS: usize = 4;
/// A backup ends when the client takes no chunk for this time. Its view is exempt from
/// `--view-pin-max`, so this bounds the pin of a client that stops reading.
pub(crate) const IDLE_MAX: Duration = Duration::from_secs(60);
/// The interval of the budget check while backups stream.
const CHECK_EVERY: Duration = Duration::from_millis(100);

/// The backups that stream on one member, oldest first, and their memory budget.
///
/// A backup holds the overlay segments of its view. The budget charges each segment once, also
/// when several backups hold it. A new backup that would exceed the budget fails. When pinned
/// segments grow past the budget, the oldest backup ends.
pub(crate) struct Backups {
    active: Mutex<Active>,
    /// The budget in key and value bytes. The node sets it to `--overlay-max-bytes`.
    limit: AtomicU64,
    gauges: OnceLock<Gauges>,
    /// Tests hold the first pass here, as for a long count of a large state.
    #[cfg(test)]
    pub(crate) hold: Mutex<Option<Arc<tokio::sync::Semaphore>>>,
}

impl Default for Backups {
    fn default() -> Self {
        Self {
            active: Mutex::default(),
            limit: AtomicU64::new(u64::MAX),
            gauges: OnceLock::new(),
            #[cfg(test)]
            hold: Mutex::default(),
        }
    }
}

#[derive(Default)]
struct Active {
    next: u64,
    streams: VecDeque<Entry>,
}

struct Entry {
    id: u64,
    segments: Arc<[Arc<Segment>]>,
    cancel: oneshot::Sender<()>,
}

struct Gauges {
    streams: prometheus::IntGauge,
    retained: prometheus::IntGauge,
}

/// The bytes of the distinct segments in `views`.
fn retained<'a>(views: impl Iterator<Item = &'a Arc<[Arc<Segment>]>>) -> u64 {
    let mut seen = HashSet::new();
    views
        .flat_map(|segments| segments.iter())
        .filter(|segment| seen.insert(Arc::as_ptr(segment)))
        .map(|segment| segment.bytes())
        .sum()
}

impl Backups {
    /// Set the budget and register the gauges. The node calls this once at the start.
    pub(crate) fn configure(&self, limit: u64, registry: &prometheus::Registry) -> Result<()> {
        self.limit.store(limit, Ordering::Relaxed);
        let gauge = |name: &str, help: &str| -> Result<prometheus::IntGauge> {
            let gauge = prometheus::IntGauge::new(name, help)?;
            registry.register(Box::new(gauge.clone()))?;
            Ok(gauge)
        };
        let gauges = Gauges {
            streams: gauge(
                "slatedb_etcd_shared_backup_streams",
                "Native backups that stream a pinned view of this member.",
            )?,
            retained: gauge(
                "slatedb_etcd_shared_backup_retained_bytes",
                "Bytes of the overlay segments that the streaming backups pin.",
            )?,
        };
        let _ = self.gauges.set(gauges);
        Ok(())
    }

    /// The streaming backups and the bytes of the segments that they pin.
    #[cfg(test)]
    pub(crate) fn status(&self) -> (usize, u64) {
        let active = self.active.lock().unwrap();
        let bytes = retained(active.streams.iter().map(|entry| &entry.segments));
        (active.streams.len(), bytes)
    }

    /// Reserve a stream for a view with `segments`. Fail with ResourceExhausted, which clients
    /// retry, at the stream limit or above the budget.
    fn admit(self: &Arc<Self>, segments: &Arc<[Arc<Segment>]>) -> Result<Slot> {
        let mut active = self.active.lock().unwrap();
        if active.streams.len() >= MAX_STREAMS {
            return Err(TooLarge(format!(
                "{MAX_STREAMS} native snapshots stream on this member; retry later"
            ))
            .into());
        }
        let limit = self.limit.load(Ordering::Relaxed);
        let views = active.streams.iter().map(|entry| &entry.segments);
        let bytes = retained(views.chain([segments]));
        if bytes > limit {
            return Err(TooLarge(format!(
                "native snapshots would pin {bytes} overlay bytes, above the limit of {limit}; \
                 retry later"
            ))
            .into());
        }
        let id = active.next;
        active.next += 1;
        let (cancel, cancelled) = oneshot::channel();
        active.streams.push_back(Entry {
            id,
            segments: segments.clone(),
            cancel,
        });
        self.publish(&active);
        Ok(Slot {
            backups: self.clone(),
            id,
            cancelled,
        })
    }

    /// End the oldest backups while their segments exceed the budget. An ended backup frees its
    /// share of the budget at once, before its task drops the view.
    fn enforce(&self) {
        let mut active = self.active.lock().unwrap();
        let limit = self.limit.load(Ordering::Relaxed);
        while retained(active.streams.iter().map(|entry| &entry.segments)) > limit {
            let Some(oldest) = active.streams.pop_front() else {
                break;
            };
            tracing::warn!(
                limit,
                "end the oldest native snapshot above the backup budget"
            );
            let _ = oldest.cancel.send(());
        }
        self.publish(&active);
    }

    /// Check the budget every `CHECK_EVERY`. Never return. Each stream runs this, because the
    /// open segment of a view grows with later writes until the next announcement.
    async fn watch(&self) -> std::convert::Infallible {
        let mut ticks = tokio::time::interval(CHECK_EVERY);
        ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticks.tick().await;
            self.enforce();
        }
    }

    fn publish(&self, active: &Active) {
        if let Some(gauges) = self.gauges.get() {
            let bytes = retained(active.streams.iter().map(|entry| &entry.segments));
            gauges.streams.set(active.streams.len() as i64);
            gauges.retained.set(bytes as i64);
        }
    }

    #[cfg(test)]
    async fn held(&self) {
        let hold = self.hold.lock().unwrap().clone();
        if let Some(hold) = hold {
            let _ = hold.acquire().await;
        }
    }
}

/// One admitted backup. Dropping it frees its stream and its share of the budget.
struct Slot {
    backups: Arc<Backups>,
    id: u64,
    /// Resolves when the budget ends the backup.
    cancelled: oneshot::Receiver<()>,
}

impl Drop for Slot {
    fn drop(&mut self) {
        let mut active = self.backups.active.lock().unwrap();
        active.streams.retain(|entry| entry.id != self.id);
        self.backups.publish(&active);
    }
}

impl StateMachine {
    /// Pin the applied view and stream its image from a task. Return right after the pin.
    ///
    /// The image holds the same bytes as `image_job(true)`, followed by their digest. The count
    /// pass runs in the task, so a large state does not delay apply. It checks the row sizes
    /// before the first chunk.
    pub(in crate::state) fn shared_backup(&self, shared: &Shared) -> Result<Stream> {
        let StateView::Shared(view) = shared.pinned() else {
            anyhow::bail!("a shared state pins a shared view");
        };
        let mut slot = self.backups.admit(&view.segments)?;
        let image = Rows {
            applied: self.applied,
            members: serde_json::to_vec(self.members.as_ref())?,
            // Shared mode ignores a cluster quota row from a converted state. A replicated
            // restore of this image must not use that old value.
            omit: |key| omitted(key) || key == quota::KEY,
        };
        let source = shared.source.clone();
        let backups = self.backups.clone();
        let timer = self.metrics.snapshot.start_timer();
        let (stream, (tx, end)) = Stream::new(crate::mvcc::header(self.revision));
        tokio::spawn(async move {
            let result = tokio::select! {
                result = async {
                    #[cfg(test)]
                    backups.held().await;
                    copy_view(view, &source, &image, &tx).await
                } => result,
                _ = &mut slot.cancelled => Err(anyhow::anyhow!(
                    "backup too slow: the native snapshots of this member pin more overlay \
                     bytes than --overlay-max-bytes"
                )),
                () = tx.closed() => Err(anyhow::anyhow!("the snapshot client went away")),
                never = backups.watch() => match never {},
            };
            // The select dropped the view. Free the stream before the client reads the end.
            drop(slot);
            if result.is_ok() {
                timer.observe_duration();
            } else {
                timer.stop_and_discard();
            }
            let _ = end.send(result);
        });
        Ok(stream)
    }

    /// The backups of a shared-mode member.
    pub(crate) fn backups(&self) -> Option<Arc<Backups>> {
        self.is_shared().then(|| self.backups.clone())
    }
}

/// Send the image of `view`. The base rows come from a reader of their own, with bounded
/// retries and without the member cache. The view keeps the base, and so the checkpoint, pinned
/// until this ends.
async fn copy_view(
    view: SharedView,
    source: &Source,
    image: &Rows,
    tx: &mpsc::Sender<Chunk>,
) -> Result<()> {
    let base = match view.base.checkpoint {
        Some(id) => Arc::new(Base::open_for_scan(source.store.clone(), &source.state(), id).await?),
        None => view.base.clone(),
    };
    let state = SharedView::new(base, view.segments.clone(), view.seq);
    image.copy(&state, tx, Some(IDLE_MAX)).await
}
