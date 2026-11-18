//! Cache small current rows for ordinary Put evaluation on the apply worker.

use bytes::Bytes;
use std::collections::hash_map::RandomState;
use std::hash::BuildHasher;

const SLOTS: usize = 2048;
const MAX_ROW: usize = 2048;

pub(crate) struct CurrentRows {
    hash: RandomState,
    slots: Vec<Option<(usize, Bytes)>>,
}

impl Default for CurrentRows {
    fn default() -> Self {
        Self {
            hash: RandomState::new(),
            slots: vec![None; SLOTS],
        }
    }
}

impl CurrentRows {
    fn slot(&self, key: &[u8]) -> usize {
        self.hash.hash_one(key) as usize % self.slots.len()
    }

    pub(crate) fn get(&self, key: &[u8]) -> Option<Bytes> {
        let (len, row) = self.slots[self.slot(key)].as_ref()?;
        (row[..*len] == *key).then(|| row.slice(*len..))
    }

    pub(super) fn publish(&mut self, key: &[u8], value: Option<&Bytes>) {
        if !key.starts_with(crate::mvcc::CURRENT) {
            return;
        }
        let slot = self.slot(key);
        self.slots[slot] = value
            .filter(|v| key.len() + v.len() <= MAX_ROW)
            .map(|value| {
                // Own only this row. A slice can otherwise retain a larger storage buffer.
                let mut row = Vec::with_capacity(key.len() + value.len());
                row.extend_from_slice(key);
                row.extend_from_slice(value);
                (key.len(), row.into())
            });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn collisions_deletes_and_large_replacements_fall_back_to_storage() {
        let mut cache = CurrentRows {
            slots: vec![None],
            ..CurrentRows::default()
        };
        let a = b"etcd/current/a";
        let b = b"etcd/current/b";
        let value = Bytes::from_static(b"small");
        cache.publish(a, Some(&value));
        assert_eq!(cache.get(a).unwrap(), value);
        cache.publish(b, Some(&value));
        assert!(cache.get(a).is_none());
        cache.publish(b, Some(&Bytes::from(vec![0; MAX_ROW])));
        assert!(cache.get(b).is_none());
        cache.publish(b, Some(&value));
        cache.publish(b, None);
        assert!(cache.get(b).is_none());
    }
}
