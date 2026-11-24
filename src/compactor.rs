//! Choose automatic Compact revisions with the rules of etcd's `v3compactor` package.
//!
//! The leader proposes each Compact through Raft, like a client Compact. Every member samples
//! revisions, so a new leader can compact at once with its own samples. A restart starts with
//! no samples and waits one retention period again, as etcd does.

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use anyhow::{bail, ensure, Context, Result};

/// The `--auto-compaction-mode` values of etcd.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, clap::ValueEnum)]
pub enum Mode {
    /// Keep the revisions of the retention duration.
    #[default]
    Periodic,
    /// Keep the given number of recent revisions.
    Revision,
}

/// What automatic compaction keeps.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Retention {
    /// Compact to the revision that was current this long ago.
    Periodic(Duration),
    /// Compact to the current revision minus this count.
    Revision(i64),
}

/// The pause between revision-mode attempts. etcd uses the same fixed interval.
const REVISION_INTERVAL: Duration = Duration::from_secs(5 * 60);
/// Periodic mode samples the revision this many times per compaction interval.
const RETRY_DIVISOR: u32 = 10;
const HOUR: Duration = Duration::from_secs(3600);

impl Retention {
    /// Parse the etcd flag pair. Zero retention disables automatic compaction.
    ///
    /// An integer means hours in periodic mode and a revision count in revision mode. Another
    /// value is a Go duration, such as `30m` or `1.5h`. Revision mode then uses its nanosecond
    /// count, as etcd does.
    pub fn parse(mode: Mode, retention: &str) -> Result<Option<Self>> {
        let retention = retention.trim();
        let parsed = match retention.parse::<u64>() {
            Ok(count) => match mode {
                Mode::Periodic => Self::Periodic(
                    HOUR.checked_mul(u32::try_from(count).context("retention is too long")?)
                        .context("retention is too long")?,
                ),
                Mode::Revision => {
                    Self::Revision(i64::try_from(count).context("retention is too large")?)
                }
            },
            Err(_) => {
                let duration = parse_go_duration(retention)
                    .with_context(|| format!("invalid auto compaction retention {retention:?}"))?;
                match mode {
                    Mode::Periodic => Self::Periodic(duration),
                    Mode::Revision => Self::Revision(
                        i64::try_from(duration.as_nanos()).context("retention is too large")?,
                    ),
                }
            }
        };
        Ok(match parsed {
            Self::Periodic(d) if d.is_zero() => None,
            Self::Revision(0) => None,
            retention => Some(retention),
        })
    }

    /// The wait between two attempts.
    fn retry_interval(self) -> Duration {
        match self {
            Self::Periodic(period) => period.min(HOUR) / RETRY_DIVISOR,
            Self::Revision(_) => REVISION_INTERVAL,
        }
    }
}

/// Parse a non-negative Go `time.ParseDuration` value.
fn parse_go_duration(text: &str) -> Result<Duration> {
    ensure!(!text.is_empty(), "empty duration");
    if text == "0" {
        return Ok(Duration::ZERO);
    }
    let mut rest = text.strip_prefix('+').unwrap_or(text);
    ensure!(!rest.starts_with('-'), "negative duration");
    let mut total = 0_f64;
    while !rest.is_empty() {
        let number = rest
            .find(|c: char| !c.is_ascii_digit() && c != '.')
            .unwrap_or(rest.len());
        ensure!(number > 0, "missing number");
        let value: f64 = rest[..number].parse().context("invalid number")?;
        rest = &rest[number..];
        let unit = rest
            .find(|c: char| c.is_ascii_digit() || c == '.')
            .unwrap_or(rest.len());
        let scale = match &rest[..unit] {
            "ns" => 1e-9,
            "us" | "µs" | "μs" => 1e-6,
            "ms" => 1e-3,
            "s" => 1.0,
            "m" => 60.0,
            "h" => 3600.0,
            "" => bail!("missing unit"),
            other => bail!("unknown unit {other:?}"),
        };
        rest = &rest[unit..];
        total += value * scale;
    }
    Duration::try_from_secs_f64(total).context("duration is out of range")
}

/// One member's automatic compaction schedule. It follows etcd's `Periodic` and `Revision`
/// loops, with a caller-supplied clock so simulation can drive it.
pub(crate) struct Compactor {
    retention: Retention,
    /// Periodic samples, oldest first. The first is the revision one period ago.
    samples: VecDeque<i64>,
    /// The end of the current wait.
    wait_until: Instant,
    last_revision: i64,
    last_success: Instant,
    /// Periodic mode waits one full period first, then at most one hour between compactions.
    base_interval: Duration,
    /// The proposed revision while its Compact runs.
    running: Option<i64>,
}

impl Compactor {
    pub fn new(retention: Retention, now: Instant, revision: i64) -> Self {
        let mut compactor = Self {
            retention,
            samples: VecDeque::new(),
            wait_until: now,
            last_revision: 0,
            last_success: now,
            base_interval: match retention {
                Retention::Periodic(period) => period,
                Retention::Revision(_) => Duration::ZERO,
            },
            running: None,
        };
        compactor.restart(now, revision);
        compactor
    }

    /// Start the next wait. Periodic mode first samples the revision, as etcd does.
    fn restart(&mut self, now: Instant, revision: i64) {
        if let Retention::Periodic(period) = self.retention {
            self.samples.push_back(revision);
            let interval = self.retention.retry_interval();
            // etcd keeps period / interval + 1 samples, so the first is one period old.
            let keep = if interval.is_zero() {
                1
            } else {
                (period.as_nanos() / interval.as_nanos()) as usize + 1
            };
            while self.samples.len() > keep {
                self.samples.pop_front();
            }
        }
        self.wait_until = now + self.retention.retry_interval();
    }

    /// Return the revision to compact now, if any. Only the leader compacts. A follower keeps
    /// its samples, so it can compact as soon as it becomes the leader.
    pub fn poll(&mut self, now: Instant, revision: i64, leader: bool) -> Option<i64> {
        if self.running.is_some() || now < self.wait_until {
            return None;
        }
        let target = match self.retention {
            _ if !leader => None,
            Retention::Periodic(period) => {
                let target = self.samples.front().copied().unwrap_or(0);
                if now.duration_since(self.last_success) < self.base_interval
                    || target == self.last_revision
                {
                    None
                } else {
                    if self.base_interval == period {
                        self.base_interval = period.min(HOUR);
                    }
                    Some(target)
                }
            }
            Retention::Revision(count) => {
                Some(revision.saturating_sub(count)).filter(|&t| t != self.last_revision)
            }
        };
        // Revision zero has no history. etcd skips it in revision mode.
        match target.filter(|&target| target > 0) {
            Some(target) => {
                self.running = Some(target);
                Some(target)
            }
            None => {
                self.restart(now, revision);
                None
            }
        }
    }

    /// Record the result of the running Compact. An already compacted revision counts as done.
    pub fn finish(&mut self, now: Instant, revision: i64, succeeded: bool) -> Option<i64> {
        let target = self.running.take();
        if succeeded {
            if let Some(target) = target {
                self.last_revision = target;
                self.last_success = now;
            }
        }
        self.restart(now, revision);
        target
    }

    pub fn retention(&self) -> Retention {
        self.retention
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flags_parse_like_etcd() {
        let p = |mode, text| Retention::parse(mode, text);
        assert_eq!(p(Mode::Periodic, "0").unwrap(), None);
        assert_eq!(p(Mode::Revision, "0").unwrap(), None);
        assert_eq!(p(Mode::Periodic, "0s").unwrap(), None);
        assert_eq!(
            p(Mode::Periodic, "1").unwrap(),
            Some(Retention::Periodic(HOUR))
        );
        assert_eq!(
            p(Mode::Periodic, "72").unwrap(),
            Some(Retention::Periodic(HOUR * 72))
        );
        assert_eq!(
            p(Mode::Periodic, "30m").unwrap(),
            Some(Retention::Periodic(Duration::from_secs(1800)))
        );
        assert_eq!(
            p(Mode::Periodic, "1h30m").unwrap(),
            Some(Retention::Periodic(Duration::from_secs(5400)))
        );
        assert_eq!(
            p(Mode::Periodic, "1.5h").unwrap(),
            Some(Retention::Periodic(Duration::from_secs(5400)))
        );
        assert_eq!(
            p(Mode::Periodic, "500ms").unwrap(),
            Some(Retention::Periodic(Duration::from_millis(500)))
        );
        assert_eq!(
            p(Mode::Revision, "1000").unwrap(),
            Some(Retention::Revision(1000))
        );
        // etcd reads a duration in revision mode as its nanosecond count.
        assert_eq!(
            p(Mode::Revision, "1us").unwrap(),
            Some(Retention::Revision(1000))
        );
        for bad in ["", "-1", "-1h", "1x", "h", "1h-1m", "abc"] {
            assert!(p(Mode::Periodic, bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn periodic_compacts_the_revision_from_one_period_ago() {
        let start = Instant::now();
        let period = Duration::from_secs(100);
        let step = period / RETRY_DIVISOR;
        let mut compactor = Compactor::new(Retention::Periodic(period), start, 1);
        let mut proposed = vec![];
        // The revision grows by ten per sampling interval.
        for tick in 1..=40_u32 {
            let now = start + step * tick;
            let revision = 1 + 10 * i64::from(tick);
            if let Some(target) = compactor.poll(now, revision, true) {
                proposed.push((tick, target));
                compactor.finish(now, revision, true);
            }
        }
        // The first compaction waits one period and uses the sample from the start.
        // Later ones follow once per period.
        assert_eq!(proposed, [(10, 1), (20, 91), (30, 191), (40, 291)]);
    }

    #[test]
    fn periodic_retries_failures_and_waits_for_leadership() {
        let start = Instant::now();
        let period = Duration::from_secs(10);
        let step = period / RETRY_DIVISOR;
        let mut compactor = Compactor::new(Retention::Periodic(period), start, 5);
        for tick in 1..=10_u32 {
            assert_eq!(compactor.poll(start + step * tick, 5, false), None);
        }
        // Followers kept sampling, so a new leader compacts at once.
        let now = start + step * 11;
        assert_eq!(compactor.poll(now, 9, true), Some(5));
        // Another poll waits for the running Compact.
        assert_eq!(compactor.poll(now + period, 9, true), None);
        assert_eq!(compactor.finish(now, 9, false), Some(5));
        // A failed Compact retries after one sampling interval.
        assert_eq!(compactor.poll(now + step, 9, true), Some(5));
        compactor.finish(now + step, 9, true);
        // The next compaction waits one period. An unchanged revision does not compact again.
        let mut later = vec![];
        for tick in 2..=40_u32 {
            let time = now + step * tick;
            if let Some(target) = compactor.poll(time, 9, true) {
                later.push((tick, target));
                compactor.finish(time, 9, true);
            }
        }
        assert_eq!(later, [(11, 9)]);
    }

    #[test]
    fn long_periods_compact_hourly_after_the_first_period() {
        let start = Instant::now();
        let period = HOUR * 3;
        let step = HOUR / RETRY_DIVISOR;
        let mut compactor = Compactor::new(Retention::Periodic(period), start, 1);
        let mut proposed = vec![];
        for tick in 1..=50_u32 {
            let now = start + step * tick;
            if let Some(target) = compactor.poll(now, i64::from(tick) + 1, true) {
                proposed.push((tick, target));
                compactor.finish(now, i64::from(tick) + 1, true);
            }
        }
        // etcd keeps 31 samples, so the oldest is about three hours old. After the first period,
        // it compacts every hour.
        assert_eq!(proposed, [(30, 1), (40, 10), (50, 20)]);
    }

    #[test]
    fn revision_mode_keeps_the_retention_count() {
        let start = Instant::now();
        let mut compactor = Compactor::new(Retention::Revision(100), start, 0);
        assert_eq!(compactor.poll(start, 500, true), None);
        let first = start + REVISION_INTERVAL;
        assert_eq!(compactor.poll(first, 50, true), None);
        let second = first + REVISION_INTERVAL;
        assert_eq!(compactor.poll(second, 500, true), Some(400));
        compactor.finish(second, 500, true);
        let third = second + REVISION_INTERVAL;
        assert_eq!(compactor.poll(third, 500, true), None);
        let fourth = third + REVISION_INTERVAL;
        assert_eq!(compactor.poll(fourth, 650, false), None);
        let fifth = fourth + REVISION_INTERVAL;
        assert_eq!(compactor.poll(fifth, 650, true), Some(550));
    }
}
