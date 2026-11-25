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
    /// Parse the etcd flag pair. Zero or empty retention disables automatic compaction.
    ///
    /// An integer means hours in periodic mode and a revision count in revision mode. Another
    /// value is a Go duration, such as `30m` or `1.5h`. Revision mode then uses its nanosecond
    /// count, as etcd does. Like etcd, this does not trim spaces.
    pub fn parse(mode: Mode, retention: &str) -> Result<Option<Self>> {
        // etcd reads an empty retention as "0".
        if retention.is_empty() {
            return Ok(None);
        }
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
                let nanos = parse_go_duration(retention)
                    .with_context(|| format!("invalid auto compaction retention {retention:?}"))?;
                match mode {
                    Mode::Periodic => Self::Periodic(Duration::from_nanos(nanos)),
                    Mode::Revision => {
                        Self::Revision(i64::try_from(nanos).context("retention is too large")?)
                    }
                }
            }
        };
        parsed.validate()
    }

    /// Check a retention that a caller built directly. Zero disables automatic compaction, as
    /// it does for the flags. A negative revision count is an error.
    pub fn validate(self) -> Result<Option<Self>> {
        Ok(match self {
            Self::Periodic(d) if d.is_zero() => None,
            Self::Revision(0) => None,
            Self::Revision(count) if count < 0 => {
                bail!("the auto compaction revision retention must not be negative")
            }
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

/// Parse a non-negative Go `time.ParseDuration` value into nanoseconds.
///
/// This follows Go's integer algorithm: each component is truncated to whole nanoseconds, and
/// the total must fit in an `int64`. A negative nonzero duration is an error.
fn parse_go_duration(text: &str) -> Result<u64> {
    const MAX: u64 = i64::MAX as u64;
    let mut s = text.as_bytes();
    let mut negative = false;
    if let Some((&sign @ (b'-' | b'+'), rest)) = s.split_first() {
        negative = sign == b'-';
        s = rest;
    }
    // Go reads a bare "0" after the sign as zero.
    if s == b"0" {
        return Ok(0);
    }
    ensure!(!s.is_empty(), "empty duration");
    let mut total: u64 = 0;
    while !s.is_empty() {
        ensure!(s[0] == b'.' || s[0].is_ascii_digit(), "missing number");
        // The integer part. Go rejects a value above 1 << 63.
        let digits = s.iter().take_while(|c| c.is_ascii_digit()).count();
        let mut whole: u64 = 0;
        for &c in &s[..digits] {
            whole = whole
                .checked_mul(10)
                .and_then(|v| v.checked_add(u64::from(c - b'0')))
                .filter(|&v| v <= 1 << 63)
                .context("duration is out of range")?;
        }
        s = &s[digits..];
        // The fraction. Go keeps digits while they fit and ignores the rest.
        let mut fraction: u64 = 0;
        let mut scale = 1_f64;
        let mut fraction_digits = 0;
        if let Some((b'.', rest)) = s.split_first() {
            s = rest;
            fraction_digits = s.iter().take_while(|c| c.is_ascii_digit()).count();
            let mut overflow = false;
            for &c in &s[..fraction_digits] {
                if overflow {
                    continue;
                }
                match fraction
                    .checked_mul(10)
                    .and_then(|v| v.checked_add(u64::from(c - b'0')))
                    .filter(|&v| fraction <= MAX / 10 && v <= 1 << 63)
                {
                    Some(v) => {
                        fraction = v;
                        scale *= 10.0;
                    }
                    None => overflow = true,
                }
            }
            s = &s[fraction_digits..];
        }
        ensure!(digits > 0 || fraction_digits > 0, "missing number");
        let unit_len = s
            .iter()
            .take_while(|&&c| c != b'.' && !c.is_ascii_digit())
            .count();
        let unit: u64 = match &s[..unit_len] {
            b"ns" => 1,
            b"us" => 1_000,
            u if u == "µs".as_bytes() || u == "μs".as_bytes() => 1_000,
            b"ms" => 1_000_000,
            b"s" => 1_000_000_000,
            b"m" => 60_000_000_000,
            b"h" => 3_600_000_000_000,
            b"" => bail!("missing unit"),
            other => bail!("unknown unit {:?}", String::from_utf8_lossy(other)),
        };
        s = &s[unit_len..];
        ensure!(whole <= (1 << 63) / unit, "duration is out of range");
        let mut value = whole * unit;
        if fraction > 0 {
            // Go also uses a float here and truncates the product.
            value += (fraction as f64 * (unit as f64 / scale)) as u64;
            ensure!(value <= 1 << 63, "duration is out of range");
        }
        total = total
            .checked_add(value)
            .filter(|&v| v <= 1 << 63)
            .context("duration is out of range")?;
    }
    if negative {
        ensure!(total == 0, "negative duration");
        return Ok(0);
    }
    ensure!(total <= MAX, "duration is out of range");
    Ok(total)
}

/// One member's automatic compaction schedule. It follows etcd's `Periodic` and `Revision`
/// loops, with a caller-supplied clock so simulation can drive it.
pub(crate) struct Compactor {
    retention: Retention,
    /// Periodic samples with their times, oldest first. The first is the newest sample that is
    /// at least one period old, once one exists.
    samples: VecDeque<(Instant, i64)>,
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
        if let Retention::Periodic(_) = self.retention {
            self.samples.push_back((now, revision));
            self.trim(now);
        }
        self.wait_until = now + self.retention.retry_interval();
    }

    /// Drop the samples before the newest one that is at least one period old.
    ///
    /// etcd keeps `period / interval + 1` samples, which assumes one sample per interval. The
    /// Raft owner polls on its tick, so a sample can come later than the interval. The sample
    /// times keep the chosen revision one period old, plus at most the delay of one poll.
    fn trim(&mut self, now: Instant) {
        let Retention::Periodic(period) = self.retention else {
            return;
        };
        while self
            .samples
            .get(1)
            .is_some_and(|&(time, _)| now.duration_since(time) >= period)
        {
            self.samples.pop_front();
        }
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
                self.trim(now);
                // Never compact a revision that was current less than one period ago.
                let target = self
                    .samples
                    .front()
                    .filter(|&&(time, _)| now.duration_since(time) >= period)
                    .map_or(0, |&(_, revision)| revision);
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
        for bad in ["-1", "-1h", "1x", "h", "1h-1m", "abc", " 1s ", " 1"] {
            assert!(p(Mode::Periodic, bad).is_err(), "{bad:?}");
        }
        // etcd reads an empty retention as zero in both modes.
        assert_eq!(p(Mode::Periodic, "").unwrap(), None);
        assert_eq!(p(Mode::Revision, "").unwrap(), None);
    }

    /// Go's `time.ParseDuration` truncates each component to whole nanoseconds and accepts any
    /// total up to `i64::MAX`. The expected values come from Go.
    #[test]
    fn durations_parse_like_go() {
        let cases: &[(&str, Option<u64>)] = &[
            ("0", Some(0)),
            ("+0", Some(0)),
            ("-0", Some(0)),
            ("0s", Some(0)),
            ("0.9ns", Some(0)),
            ("1.9ns", Some(1)),
            ("1.5h", Some(5_400_000_000_000)),
            (".5s", Some(500_000_000)),
            ("5.s", Some(5_000_000_000)),
            ("1h30m", Some(5_400_000_000_000)),
            ("1.0000000001s", Some(1_000_000_000)),
            ("0.3333333333333333333h", Some(1_200_000_000_000)),
            ("1µs", Some(1_000)),
            ("1μs", Some(1_000)),
            ("9223372036854775807ns", Some(i64::MAX as u64)),
            ("2562047h47m16.854775807s", Some(i64::MAX as u64)),
            ("9223372036854775808ns", None),
            ("2562048h", None),
            ("+", None),
            ("-", None),
            ("", None),
            (".s", None),
            ("1", None),
            ("1s-1ns", None),
            // Go accepts negative durations. Retention rejects them.
            ("-1ns", None),
            ("1d", None),
        ];
        for &(text, nanos) in cases {
            assert_eq!(parse_go_duration(text).ok(), nanos, "{text:?}");
        }
        let p = |mode, text| Retention::parse(mode, text);
        // A sub-nanosecond retention is zero, so it disables compaction as in etcd.
        assert_eq!(p(Mode::Revision, "0.9ns").unwrap(), None);
        assert_eq!(p(Mode::Periodic, "0.9ns").unwrap(), None);
        assert!(p(Mode::Periodic, "+").is_err());
        assert_eq!(
            p(Mode::Revision, "9223372036854775807ns").unwrap(),
            Some(Retention::Revision(i64::MAX))
        );
    }

    #[test]
    fn direct_retention_is_validated_like_the_flags() {
        assert_eq!(Retention::Revision(0).validate().unwrap(), None);
        assert_eq!(
            Retention::Periodic(Duration::ZERO).validate().unwrap(),
            None
        );
        assert!(Retention::Revision(-2).validate().is_err());
        assert_eq!(
            Retention::Revision(5).validate().unwrap(),
            Some(Retention::Revision(5))
        );
    }

    /// The Raft owner polls once per tick, which is longer than the sampling interval of a
    /// short period. The chosen revision must still be about one period old.
    #[test]
    fn periodic_targets_stay_one_period_old_with_the_raft_tick() {
        let tick = crate::node::TICK;
        let period = Duration::from_millis(100);
        assert!(period / RETRY_DIVISOR < tick);
        let start = Instant::now();
        // The revision is the elapsed time in milliseconds.
        let revision = |now: Instant| now.duration_since(start).as_millis() as i64 + 1;
        let mut compactor = Compactor::new(Retention::Periodic(period), start, 1);
        let mut ages = vec![];
        for n in 1..=50_u32 {
            let now = start + tick * n;
            if let Some(target) = compactor.poll(now, revision(now), true) {
                ages.push(revision(now) - target);
                compactor.finish(now, revision(now), true);
            }
        }
        assert!(ages.len() >= 20, "{ages:?}");
        let (low, high) = (
            period.as_millis() as i64,
            (period + tick).as_millis() as i64,
        );
        for age in ages {
            assert!((low..=high).contains(&age), "age {age} ms");
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
        // Later ones follow once per period, each with the revision from one period before.
        assert_eq!(proposed, [(10, 1), (20, 101), (30, 201), (40, 301)]);
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
        // The target is the revision from three hours before. After the first period, it
        // compacts every hour.
        assert_eq!(proposed, [(30, 1), (40, 11), (50, 21)]);
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
