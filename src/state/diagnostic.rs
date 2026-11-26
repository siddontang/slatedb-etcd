use super::*;

impl StateMachine {
    pub async fn apply(&mut self, index: u64, command: Option<&Command>) -> Result<Reply> {
        ensure!(
            index == self.applied + 1,
            "non-contiguous state machine apply"
        );
        let mut batch = WriteBatch::new();
        let mut values = vec![];
        // A small overlay preserves request order within an atomic multi-operation batch.
        let mut overlay = std::collections::BTreeMap::<String, Option<String>>::new();
        if let Some(command) = command {
            for operation in &command.request.operations {
                match operation {
                    Operation::Put { key, value } => {
                        batch.put(data_key(key), value.as_bytes());
                        overlay.insert(key.clone(), Some(value.clone()));
                        values.push(None);
                    }
                    Operation::Delete { key } => {
                        batch.delete(data_key(key));
                        overlay.insert(key.clone(), None);
                        values.push(None);
                    }
                    Operation::Get { key } => {
                        // The reads come before any state change, so a storage error of a
                        // read leaves the entry unapplied.
                        let value = match overlay.get(key) {
                            Some(value) => value.clone(),
                            None => self.read_value(key).await?,
                        };
                        values.push(value);
                    }
                }
            }
        }
        self.mark_applied(&mut batch, index);
        // Raft already made this command durable. Waiting for SlateDB here would wait for L0.
        self.write_batch(batch, index).await?;
        self.applied = index;
        Ok(Reply { index, values })
    }

    pub async fn local_value(&self, key: &str) -> Result<Option<String>> {
        self.read_value(key).await
    }

    /// Read the diagnostic value of `key`. A storage error keeps its class, and a value that
    /// is not UTF-8 is corrupt stored data.
    async fn read_value(&self, key: &str) -> Result<Option<String>> {
        let value = self
            .applied_state()
            .get(&data_key(key))
            .await
            .map_err(crate::mvcc::storage_fault::read)?;
        value
            .map(|value| {
                String::from_utf8(value.to_vec()).map_err(|error| {
                    crate::mvcc::storage_fault::stored(format!(
                        "the diagnostic value of {key:?} is not UTF-8: {error}"
                    ))
                })
            })
            .transpose()
    }
}
