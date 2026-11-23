//! Make an applied prefix durable for local log truncation.

use super::*;

type CheckpointJob =
    std::pin::Pin<Box<dyn std::future::Future<Output = Result<Option<Vec<u8>>>> + Send>>;

impl StateMachine {
    /// Pin the applied prefix now, then flush it outside the Raft loop.
    ///
    /// The flush makes the state and its apply marker durable through the pinned index. That
    /// state covers the local log without a copy. Only a peer that needs a snapshot requests an
    /// image. The image is a descriptor of a SlateDB checkpoint at the pinned index.
    pub(crate) async fn checkpoint_job(&self, image: bool) -> Result<CheckpointJob> {
        if image {
            // A peer copies the SlateDB checkpoint objects. See `transfer`.
            ensure!(
                self.db.snapshot()?.seq() == self.applied,
                "checkpoint does not match the apply marker"
            );
            let members = serde_json::to_vec(self.members.as_ref())?;
            let job = Box::pin(self.location.begin(&self.db, self.applied, members)).await?;
            return Ok(Box::pin(async move { job.await.map(Some) }));
        }
        ensure!(
            self.db.snapshot()?.seq() == self.applied,
            "checkpoint does not match the apply marker"
        );
        let metrics = self.metrics.clone();
        let db = self.db.clone();
        Ok(Box::pin(async move {
            metrics.flush_db(&db).await?;
            Ok(None)
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_flushed_checkpoint_survives_reopen_without_an_image() {
        let dir = tempfile::tempdir().unwrap();
        let mut state = StateMachine::open(dir.path()).await.unwrap();
        for i in 1_u64..=3 {
            let operations = vec![Operation::Put {
                key: format!("key-{i}"),
                value: format!("value-{i}"),
            }];
            state
                .apply(
                    i,
                    Some(&Command {
                        id: i.to_string(),
                        request: crate::Request { operations },
                    }),
                )
                .await
                .unwrap();
        }
        let job = state.checkpoint_job(false).await.unwrap();
        // Later entries can apply while the flush runs.
        state.apply(4, None).await.unwrap();
        assert!(job.await.unwrap().is_none());
        let image = state
            .checkpoint_job(true)
            .await
            .unwrap()
            .await
            .unwrap()
            .unwrap();
        let descriptor = super::transfer::Descriptor::decode(&image)
            .unwrap()
            .unwrap();
        assert_eq!(descriptor.applied, 4);
        assert!(!descriptor.ssts.is_empty());
        // Model a crash: close without the shutdown flush.
        state.close().await.unwrap();
        drop(state);
        let state = StateMachine::open(dir.path()).await.unwrap();
        assert!(state.applied >= 3);
        assert_eq!(
            state.local_value("key-3").await.unwrap().as_deref(),
            Some("value-3")
        );
        state.close().await.unwrap();
    }
}
