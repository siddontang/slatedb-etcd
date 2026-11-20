//! Publish applied views and Watch event batches to local watch streams.

use std::sync::Arc;

use tokio::sync::{broadcast, watch};

use crate::watch::{Batch, FeedState, Subscription};

pub(super) struct Publisher {
    events: broadcast::Sender<Arc<Batch>>,
    state: watch::Sender<FeedState>,
}

impl Publisher {
    pub(super) fn new(state: FeedState) -> Self {
        Self {
            events: broadcast::channel(crate::watch::FEED_BATCHES).0,
            state: watch::channel(state).0,
        }
    }

    /// Both receivers start at the same applied view. Later batches follow that view.
    pub(super) fn subscribe(&self) -> Subscription {
        Subscription {
            events: self.events.subscribe(),
            state: self.state.subscribe(),
        }
    }

    /// Publish the view first. A stream that sees a batch can then read its storage rows.
    pub(super) fn publish(&self, state: FeedState, batch: Option<Arc<Batch>>) {
        self.state.send_if_modified(|current| {
            let changed = !Arc::ptr_eq(&current.view, &state.view)
                || current.header != state.header
                || current.leader != state.leader;
            if changed {
                *current = state;
            }
            changed
        });
        if let Some(batch) = batch {
            // An error only means that no stream is subscribed.
            let _ = self.events.send(batch);
        }
    }
}
