//! Reuse unchanged rows from the last full Raft snapshot.

use super::*;
use bytes::Bytes;
use std::collections::BTreeMap;

const DELTA_BYTES: usize = 8 * 1024 * 1024;
const ROW_OVERHEAD: usize = 128;

pub(super) struct Delta {
    base: u64,
    rows: BTreeMap<Bytes, Option<Bytes>>,
    // Charge all changes since the base. Repeated writes can trigger an earlier full scan.
    charged: usize,
}

impl Delta {
    fn new(base: u64) -> Self {
        Self {
            base,
            rows: BTreeMap::new(),
            charged: 0,
        }
    }

    pub(super) fn record(&mut self, key: Bytes, value: Option<Bytes>) -> bool {
        if key == crate::cluster::KEY {
            return true;
        }
        self.charged = self
            .charged
            .saturating_add(key.len() + value.as_ref().map_or(0, Bytes::len) + ROW_OVERHEAD);
        if self.charged > DELTA_BYTES {
            return false;
        }
        self.rows.insert(key, value);
        true
    }

    fn merge(self, base: &[u8], applied: u64, members: &[u8]) -> Result<Vec<u8>> {
        let mut input = base;
        let (format, index, rows): (u32, u64, u64) = bincode::deserialize_from(&mut input)?;
        ensure!(
            format == STORAGE_FORMAT && index == self.base,
            "wrong checkpoint base"
        );
        let mut out = Vec::with_capacity(base.len().saturating_add(self.charged));
        bincode::serialize_into(&mut out, &(format, applied, 0_u64))?;
        let count_offset = out.len() - 8;
        let mut count = 0_u64;
        let mut changes = self.rows.into_iter().peekable();
        for _ in 0..rows {
            let before = input;
            let key = field(&mut input)?;
            field(&mut input)?;
            if key == crate::cluster::KEY {
                continue;
            }
            while changes.peek().is_some_and(|(k, _)| k.as_ref() < key) {
                let (k, v) = changes.next().unwrap();
                count += write_row(&mut out, &k, v.as_deref())?;
            }
            if changes.peek().is_some_and(|(k, _)| k.as_ref() == key) {
                let (k, v) = changes.next().unwrap();
                count += write_row(&mut out, &k, v.as_deref())?;
            } else {
                out.extend_from_slice(&before[..before.len() - input.len()]);
                count += 1;
            }
        }
        ensure!(input.is_empty(), "trailing checkpoint data");
        for (k, v) in changes {
            count += write_row(&mut out, &k, v.as_deref())?;
        }
        count += write_row(&mut out, crate::cluster::KEY, Some(members))?;
        out[count_offset..count_offset + 8].copy_from_slice(&count.to_le_bytes());
        out.shrink_to_fit();
        Ok(out)
    }
}

fn field<'a>(input: &mut &'a [u8]) -> Result<&'a [u8]> {
    let len = usize::try_from(bincode::deserialize_from::<_, u64>(&mut *input)?)?;
    let (value, rest) = input
        .split_at_checked(len)
        .context("short checkpoint field")?;
    *input = rest;
    Ok(value)
}

fn write_row(out: &mut Vec<u8>, key: &[u8], value: Option<&[u8]>) -> Result<u64> {
    if let Some(value) = value {
        // Bincode prefixes each byte sequence with its length.
        for field in [key, value] {
            bincode::serialize_into(&mut *out, &(field.len() as u64))?;
            out.extend_from_slice(field);
        }
        return Ok(1);
    }
    Ok(0)
}

type CheckpointJob = std::pin::Pin<Box<dyn std::future::Future<Output = Result<Vec<u8>>> + Send>>;

impl StateMachine {
    pub(crate) fn checkpoint_job(&mut self, base: Snapshot) -> Result<CheckpointJob> {
        let applied = self.applied;
        let previous = self.checkpoint_delta.replace(Delta::new(applied));
        let Some(delta) = previous.filter(|d| d.base == base.get_metadata().index) else {
            return Ok(Box::pin(self.snapshot_job()?));
        };
        let metrics = self.metrics.clone();
        let db = self.db.clone();
        let members = self.members.clone();
        Ok(Box::pin(async move {
            metrics.flush_db(&db).await?;
            tokio::task::spawn_blocking(move || {
                delta.merge(&base.data, applied, &serde_json::to_vec(members.as_ref())?)
            })
            .await?
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn checkpoint_updates_match_pinned_scans_across_deletes_and_concurrent_apply() {
        let dir = tempfile::tempdir().unwrap();
        let mut state = StateMachine::open(dir.path()).await.unwrap();
        let mut base = Snapshot::default();
        for i in 1_u64..=5 {
            let operations = vec![
                Operation::Put {
                    key: format!("key-{i}"),
                    value: format!("value-{i}"),
                },
                Operation::Delete {
                    key: format!("key-{}", i.saturating_sub(2)),
                },
                Operation::Put {
                    key: "repeat".into(),
                    value: format!("new-{i}"),
                },
            ];
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
            let full = state.snapshot_job().unwrap();
            let job = state.checkpoint_job(base).unwrap();
            let (full, saved) = tokio::join!(full, job);
            let saved = saved.unwrap();
            assert_eq!(full.unwrap(), saved);
            base = Snapshot::default();
            base.mut_metadata().index = i;
            base.data = saved.into();
        }
        state.apply(6, None).await.unwrap();
        let full = state.snapshot_job().unwrap();
        let job = state.checkpoint_job(base).unwrap();
        state.apply(7, None).await.unwrap();
        assert_eq!(job.await.unwrap(), full.await.unwrap());
        state.close().await.unwrap();
    }

    #[test]
    fn dropping_a_delta_at_its_limit_bounds_repeated_writes() {
        let mut delta = Delta::new(7);
        for i in 0..9 {
            if !delta.record(
                Bytes::from_static(b"key"),
                Some(vec![1; 1024 * 1024].into()),
            ) {
                assert_eq!(i, 7);
                return;
            }
        }
        panic!("the delta exceeded its byte limit");
    }
}
