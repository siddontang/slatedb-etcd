//! The progress of the shared writer: the written sequence, the publication requests, and the
//! flush age. The materializer, the publisher, and flush waiters share it.

use std::sync::Mutex;
use std::time::{Duration, Instant};

#[cfg(test)]
use super::{clock, Hold};
#[cfg(test)]
use anyhow::Result;

/// Progress that the materializer, the publisher, and flush waiters share.
pub(in crate::node) struct Progress {
    pub(super) inner: Mutex<Inner>,
    /// The sequence of the newest batch in the shared `Db`.
    written: tokio::sync::watch::Sender<u64>,
    pub(super) wake: tokio::sync::Notify,
    /// Tests: the next `wrote` takes the pair. It sends on the first and waits on the second
    /// before it changes the progress.
    #[cfg(test)]
    pub(super) hold: Mutex<Option<clock::ReadHold>>,
    /// Tests: the next writer task takes the pair before its reset. It notifies the first and
    /// waits for the second.
    #[cfg(test)]
    pub(super) reset_hold: Hold,
}

#[derive(Default)]
pub(super) struct Inner {
    /// The time of the oldest write that no flush covers.
    dirty_since: Option<Instant>,
    /// Publish a checkpoint after the materializer writes this sequence.
    pub(super) publish_through: Option<u64>,
    /// Publish a new checkpoint even at an unchanged sequence. Defragment sets this.
    pub(super) republish: bool,
    /// The time of the oldest request that no publication covered yet.
    pub(super) requested: Option<Instant>,
    /// The time of the oldest republish that no publication served yet.
    pub(super) republished: Option<Instant>,
    /// The writer opens so far. Only the materializer of the newest open moves `written`. A
    /// stopped materializer of an older open can end after the newer open.
    owner: u64,
}

impl Progress {
    pub(in crate::node) fn new(written: u64) -> Self {
        Self {
            inner: Mutex::default(),
            written: tokio::sync::watch::channel(written).0,
            wake: tokio::sync::Notify::new(),
            #[cfg(test)]
            hold: Mutex::default(),
            #[cfg(test)]
            reset_hold: Hold::default(),
        }
    }

    pub fn written(&self) -> u64 {
        *self.written.borrow()
    }

    /// The materializer of open `owner` wrote `seq`.
    pub(super) fn wrote(&self, owner: u64, seq: u64) {
        #[cfg(test)]
        {
            let hold = self.hold.lock().unwrap().take();
            if let Some((reached, resume)) = hold {
                let _ = reached.send(());
                let _ = resume.recv();
            }
        }
        let mut inner = self.inner.lock().unwrap();
        if inner.owner != owner {
            return;
        }
        self.written.send_replace(seq);
        let first = inner.dirty_since.is_none();
        inner.dirty_since.get_or_insert_with(Instant::now);
        let requested = inner.publish_through.is_some_and(|through| seq >= through);
        drop(inner);
        // The publisher sleeps until the oldest write reaches its age.
        if first || requested {
            self.wake.notify_one();
        }
    }

    /// Start again after a writer open at `written`. The writes that the age limit tracked
    /// were in the old writer. A publication request and a republish stay: a manual checkpoint
    /// or a Defragment can ask before the new writer opens, or a failed pass gives them back.
    /// Return the owner of the new open. The writer task calls this only while it still leads
    /// its term. See `lead`.
    pub(super) fn reset(&self, written: u64) -> u64 {
        let mut inner = self.inner.lock().unwrap();
        inner.owner += 1;
        inner.dirty_since = None;
        self.written.send_replace(written);
        inner.owner
    }

    /// Publish a new checkpoint at the current sequence, for a changed layout.
    pub(super) fn republish(&self) {
        self.give_back(None, true);
        self.wake.notify_one();
    }

    pub(super) fn take_republish(&self) -> bool {
        std::mem::take(&mut self.inner.lock().unwrap().republish)
    }

    pub(super) fn request(&self, through: u64) {
        self.give_back(Some(through), false);
        self.wake.notify_one();
    }

    /// Add a request and a republish without a wake. Each keeps the wait of an earlier one.
    pub(super) fn give_back(&self, through: Option<u64>, republish: bool) {
        let mut inner = self.inner.lock().unwrap();
        if let Some(through) = through {
            let through = inner
                .publish_through
                .map_or(through, |old| old.max(through));
            inner.publish_through = Some(through);
            inner.requested.get_or_insert_with(Instant::now);
        }
        if republish {
            inner.republish = true;
            inner.republished.get_or_insert_with(Instant::now);
        }
    }

    /// The time of the oldest request or republish that no publication served yet.
    pub(super) fn requested(&self) -> Option<Instant> {
        let inner = self.inner.lock().unwrap();
        inner.requested.into_iter().chain(inner.republished).min()
    }

    /// A pass ended. A request and a republish keep separate waits. A wait starts again only
    /// when the pass covered that kind of demand and a newer one waits. A pass that covered no
    /// request, for example one above the written batch, keeps the wait of the request.
    pub(super) fn served(&self, covered: bool, republished: bool) {
        let now = Instant::now();
        let mut inner = self.inner.lock().unwrap();
        if inner.publish_through.is_none() {
            inner.requested = None;
        } else if covered {
            inner.requested = Some(now);
        }
        if !inner.republish {
            inner.republished = None;
        } else if republished {
            inner.republished = Some(now);
        }
    }

    /// A new writer starts the wait of the pending requests, so it has a whole interval to
    /// reach them.
    pub(super) fn rearm(&self) {
        let now = Instant::now();
        let mut inner = self.inner.lock().unwrap();
        inner.requested = inner.publish_through.map(|_| now);
        inner.republished = inner.republish.then_some(now);
    }

    /// Wait until the shared `Db` holds every batch through `seq`.
    #[cfg(test)]
    pub async fn wait_written(&self, seq: u64) -> Result<()> {
        let mut written = self.written.subscribe();
        written.wait_for(|written| *written >= seq).await?;
        Ok(())
    }

    /// The time when the oldest unflushed write reaches `max_age`.
    pub(super) fn deadline(&self, max_age: Duration) -> Option<Instant> {
        Some(self.inner.lock().unwrap().dirty_since? + max_age)
    }

    /// Return whether a flush is due, and the request that is one of its causes. Clear the
    /// causes that it covers.
    pub(super) fn take_flush(&self, max_age: Duration) -> (bool, Option<u64>) {
        let written = self.written();
        let mut inner = self.inner.lock().unwrap();
        let requested = inner.publish_through.filter(|through| written >= *through);
        let aged = inner
            .dirty_since
            .is_some_and(|since| since.elapsed() >= max_age);
        if requested.is_some() {
            inner.publish_through = None;
        }
        if requested.is_some() || aged {
            inner.dirty_since = None;
        }
        (requested.is_some() || aged, requested)
    }
}
