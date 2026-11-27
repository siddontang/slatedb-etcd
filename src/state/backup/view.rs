//! Shared-mode backups: stream the image of a pinned view.
//!
//! Apply pins the view at the covered index: the base and the overlay segments of the applied
//! state. The stream task then reads the view in the two passes of `Rows::copy`. The base pins
//! its checkpoint, and the member reports it to the leader, so retention keeps it. A backup
//! creates no checkpoint.
//!
//! The pinned segments stay in memory after a base switch releases them. `overlay::Holders`
//! bounds that memory on each member. See `docs/shared-state.md#backups`.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use tokio::sync::mpsc;

use super::{omitted, Chunk, Rows, Stream, TooLarge};
use crate::state::shared::{Shared, Source};
use crate::state::view::{Base, SharedView};
use crate::state::StateMachine;

/// A backup ends when the client takes no chunk for this time. Its view is exempt from
/// `--view-pin-max`, so this bounds the pin of a client that stops reading.
pub(crate) const IDLE_MAX: Duration = Duration::from_secs(60);

impl StateMachine {
    /// Pin the applied view and stream its image from a task. Return right after the pin.
    ///
    /// The image holds the same bytes as `image_job(true)`, followed by their digest. The count
    /// pass runs in the task, so a large state does not delay apply. It checks the row sizes
    /// before the first chunk.
    pub(in crate::state) fn shared_backup(&self, shared: &Shared) -> Result<Stream> {
        let view = shared.pinned_view();
        let mut hold = shared
            .holders
            .admit(&view.segments)
            .map_err(|refused| TooLarge(refused.0))?;
        let image = Rows {
            applied: self.applied,
            members: serde_json::to_vec(self.members.as_ref())?,
            omit: omitted,
        };
        let source = shared.source.clone();
        let holders = shared.holders.clone();
        let timer = self.metrics.snapshot.start_timer();
        let (stream, (tx, end)) = Stream::new(crate::mvcc::header(self.revision));
        tokio::spawn(async move {
            let result = tokio::select! {
                result = async {
                    #[cfg(test)]
                    holders.held().await;
                    copy_view(view, &source, &image, &tx).await
                } => result,
                () = hold.ended() => Err(anyhow::anyhow!(
                    "backup too slow: the native snapshots of this member pin more overlay \
                     bytes than --overlay-max-bytes"
                )),
                () = tx.closed() => Err(anyhow::anyhow!("the snapshot client went away")),
                never = holders.watch() => match never {},
            };
            // The select dropped the view. Free its place before the client reads the end.
            drop(hold);
            if result.is_ok() {
                timer.observe_duration();
            } else {
                timer.stop_and_discard();
            }
            let _ = end.send(result);
        });
        Ok(stream)
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
    let result = image.copy(&state, tx, Some(IDLE_MAX)).await;
    // The scan reader is not a registered base. The pinned view keeps the checkpoint reported
    // until the copy ends.
    drop(view);
    result
}
