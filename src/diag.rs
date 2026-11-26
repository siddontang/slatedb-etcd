//! TEMPORARY steady-tail diagnostics (not for merge).
//!
//! Each second, one `DIAG` line on stderr with the count, mean, max, and counts over 20, 50,
//! and 100 ms of each stage, plus the SST lane state. All counters reset each second.

use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering::Relaxed};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

pub struct Stat {
    name: &'static str,
    n: AtomicU64,
    sum_us: AtomicU64,
    max_us: AtomicU64,
    ge20: AtomicU64,
    ge50: AtomicU64,
    ge100: AtomicU64,
}

impl Stat {
    const fn new(name: &'static str) -> Self {
        Self {
            name,
            n: AtomicU64::new(0),
            sum_us: AtomicU64::new(0),
            max_us: AtomicU64::new(0),
            ge20: AtomicU64::new(0),
            ge50: AtomicU64::new(0),
            ge100: AtomicU64::new(0),
        }
    }

    pub fn since(&self, start: Instant) {
        self.observe(start.elapsed());
    }

    pub fn observe(&self, d: Duration) {
        let us = d.as_micros() as u64;
        self.n.fetch_add(1, Relaxed);
        self.sum_us.fetch_add(us, Relaxed);
        self.max_us.fetch_max(us, Relaxed);
        if us >= 20_000 {
            self.ge20.fetch_add(1, Relaxed);
        }
        if us >= 50_000 {
            self.ge50.fetch_add(1, Relaxed);
        }
        if us >= 100_000 {
            self.ge100.fetch_add(1, Relaxed);
        }
    }

    fn take(&self) -> String {
        let n = self.n.swap(0, Relaxed);
        let sum = self.sum_us.swap(0, Relaxed);
        let max = self.max_us.swap(0, Relaxed);
        let (a, b, c) = (
            self.ge20.swap(0, Relaxed),
            self.ge50.swap(0, Relaxed),
            self.ge100.swap(0, Relaxed),
        );
        let avg = if n > 0 { sum as f64 / n as f64 / 1000.0 } else { 0.0 };
        format!(
            "{}={n}/{avg:.1}/{:.1}/{a}/{b}/{c}",
            self.name,
            max as f64 / 1000.0
        )
    }
}

/// Raft log `fdatasync`.
pub static WAL_FSYNC: Stat = Stat::new("wal");
/// One Raft log job (append + sync) on the log thread.
pub static LOG_JOB: Stat = Stat::new("logjob");
/// One apply worker job.
pub static APPLY_JOB: Stat = Stat::new("apply");
/// SlateDB `write_with_options` in apply.
pub static DB_WRITE: Stat = Stat::new("dbw");
/// Proposal to client reply on the node.
pub static REPLY: Stat = Stat::new("reply");
/// gRPC Put handler, end to end.
pub static GRPC_PUT: Stat = Stat::new("gput");
/// One node event loop turn (handle + drive_ready).
pub static LOOP: Stat = Stat::new("loop");
/// Wait for a lane window.
pub static LANE_WAIT: Stat = Stat::new("lwait");
/// One window: start of writeback to its wait end.
pub static WINDOW: Stat = Stat::new("win");
/// Final SST sync, rename, and directory sync.
pub static PUBLISH: Stat = Stat::new("pub");
/// Put group proposed to the log job that persisted its last entry being handled.
pub static PERSIST: Stat = Stat::new("persist");
/// Log job finished on the log thread to the node handling it.
pub static LOGWAKE: Stat = Stat::new("logwake");
/// Deferred write: deferral to proposal.
pub static DEFER: Stat = Stat::new("defer");
/// Tokio timer overshoot of a 5 ms sleep (runtime scheduling delay).
pub static SCHED: Stat = Stat::new("sched");
/// Node event queue length at each receive (max per second).
pub static EVQ_MAX: AtomicU64 = AtomicU64::new(0);
/// Proposals deferred because of a backlog.
pub static DEFERRED: AtomicU64 = AtomicU64::new(0);

static PROPOSED: std::sync::Mutex<std::collections::VecDeque<(u64, Instant)>> =
    std::sync::Mutex::new(std::collections::VecDeque::new());

pub fn proposed(last: u64) {
    PROPOSED.lock().unwrap().push_back((last, Instant::now()));
}

pub fn persisted(last: u64) {
    let mut q = PROPOSED.lock().unwrap();
    while q.front().is_some_and(|(i, _)| *i <= last) {
        PERSIST.since(q.pop_front().unwrap().1);
    }
    if q.len() > 100_000 {
        q.clear();
    }
}

/// Start the runtime scheduling probe once, from a task on the server runtime.
pub fn start_sched() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        tokio::spawn(async {
            loop {
                let t = Instant::now();
                tokio::time::sleep(Duration::from_millis(5)).await;
                SCHED.observe(t.elapsed().saturating_sub(Duration::from_millis(5)));
            }
        });
    });
}

pub static FLUSH_BYTES: AtomicU64 = AtomicU64::new(0);
pub static COMPACT_BYTES: AtomicU64 = AtomicU64::new(0);
pub static INFLIGHT_BYTES: AtomicI64 = AtomicI64::new(0);
pub static INFLIGHT_MAX: AtomicI64 = AtomicI64::new(0);
pub static INFLIGHT_WIN: AtomicI64 = AtomicI64::new(0);
pub static INFLIGHT_WIN_MAX: AtomicI64 = AtomicI64::new(0);
pub static BACKLOG: AtomicBool = AtomicBool::new(false);
pub static UNFLUSHED: AtomicI64 = AtomicI64::new(0);
pub static UNFLUSHED_MAX: AtomicI64 = AtomicI64::new(0);

pub fn window_start(bytes: u64) {
    let b = INFLIGHT_BYTES.fetch_add(bytes as i64, Relaxed) + bytes as i64;
    INFLIGHT_MAX.fetch_max(b, Relaxed);
    let w = INFLIGHT_WIN.fetch_add(1, Relaxed) + 1;
    INFLIGHT_WIN_MAX.fetch_max(w, Relaxed);
}

pub fn window_end(bytes: u64) {
    INFLIGHT_BYTES.fetch_sub(bytes as i64, Relaxed);
    INFLIGHT_WIN.fetch_sub(1, Relaxed);
}

pub fn unflushed(v: i64) {
    UNFLUSHED.store(v, Relaxed);
    UNFLUSHED_MAX.fetch_max(v, Relaxed);
}

/// Start the reporter once.
pub fn start() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        std::thread::Builder::new()
            .name("diag".into())
            .spawn(report)
            .expect("diag thread");
    });
}

fn report() {
    let stats = [
        &GRPC_PUT, &REPLY, &LOOP, &LOG_JOB, &WAL_FSYNC, &APPLY_JOB, &DB_WRITE, &LANE_WAIT,
        &WINDOW, &PUBLISH, &PERSIST, &LOGWAKE, &DEFER, &SCHED,
    ];
    let mut backlog_samples = 0u32;
    let mut last = Instant::now();
    loop {
        std::thread::sleep(Duration::from_millis(50));
        if BACKLOG.load(Relaxed) {
            backlog_samples += 1;
        }
        if last.elapsed() < Duration::from_secs(1) {
            continue;
        }
        last = Instant::now();
        let t = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs_f64();
        let mut line = format!("DIAG t={t:.1}");
        for s in stats {
            line.push(' ');
            line.push_str(&s.take());
        }
        let cur = INFLIGHT_BYTES.load(Relaxed).max(0);
        let curw = INFLIGHT_WIN.load(Relaxed).max(0);
        line.push_str(&format!(
            " evq_max={} deferred={} fl_mb={:.1} co_mb={:.1} infl_kb_max={} infl_win_max={} backlog20={} unfl_mb={} unfl_mb_max={}",
            EVQ_MAX.swap(0, Relaxed),
            DEFERRED.swap(0, Relaxed),
            FLUSH_BYTES.swap(0, Relaxed) as f64 / 1e6,
            COMPACT_BYTES.swap(0, Relaxed) as f64 / 1e6,
            INFLIGHT_MAX.swap(cur, Relaxed) / 1024,
            INFLIGHT_WIN_MAX.swap(curw, Relaxed),
            backlog_samples,
            UNFLUSHED.load(Relaxed) >> 20,
            UNFLUSHED_MAX.swap(UNFLUSHED.load(Relaxed), Relaxed) >> 20,
        ));
        backlog_samples = 0;
        eprintln!("{line}");
    }
}
