//! The clock checks of the shared writer. See "Clocks" in `docs/shared-state.md`.

use super::*;

/// The clock check of a new writer. SlateDB starts a writer's clock at the manifest's
/// `last_l0_clock_tick`, and a lagging clock makes each write wait and then fail.
#[derive(Clone)]
pub(in crate::node) struct Clock {
    /// Milliseconds since the Unix epoch. Tests skew it.
    pub now_ms: Arc<dyn Fn() -> i64 + Send + Sync>,
    pub max_skew: Duration,
    /// The last measured skew: `last_l0_clock_tick` minus the local time, at least zero.
    pub skew: Arc<std::sync::atomic::AtomicI64>,
    /// Set while the skew is over the limit and the writer does not start.
    pub alarm: Arc<std::sync::atomic::AtomicBool>,
    pub gauge: Option<prometheus::IntGauge>,
}

impl Clock {
    pub fn system(max_skew: Duration, gauge: Option<prometheus::IntGauge>) -> Self {
        Self {
            now_ms: Arc::new(|| {
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_or(0, |since| since.as_millis() as i64)
            }),
            max_skew,
            skew: Arc::default(),
            alarm: Arc::default(),
            gauge,
        }
    }

    /// Wait until the local clock passes `tick`. Return false, and raise the alarm, when the
    /// local clock is behind by more than the limit.
    pub async fn pass(&self, tick: i64) -> bool {
        use std::sync::atomic::Ordering;
        // A new database has the minimum tick, so the subtraction saturates.
        let skew = tick.saturating_sub((self.now_ms)()).max(0);
        self.skew.store(skew, Ordering::Relaxed);
        if let Some(gauge) = &self.gauge {
            gauge.set(skew);
        }
        if skew > self.max_skew.as_millis() as i64 {
            if !self.alarm.swap(true, Ordering::Relaxed) {
                tracing::error!(
                    skew_ms = skew,
                    "the local clock is behind the shared writer"
                );
            }
            return false;
        }
        self.alarm.store(false, Ordering::Relaxed);
        loop {
            let behind = tick.saturating_sub((self.now_ms)());
            if behind < 0 {
                return true;
            }
            tokio::time::sleep(Duration::from_millis((behind as u64 + 1).min(100))).await;
        }
    }
}
