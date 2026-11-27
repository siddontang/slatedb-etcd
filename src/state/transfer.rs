//! The Raft snapshot format of shared state mode, and the object namespace of the state.

use std::sync::Arc;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

/// The Raft snapshot format of shared state mode. It names a `view-` checkpoint of the shared
/// database. Every member reads the same objects, so any member can send it.
pub(crate) const VIEW_SNAPSHOT_FORMAT: u32 = 6;

/// The Raft snapshot data of format 6. The snapshot metadata holds the index, term, and
/// ConfState from the checkpoint.
///
/// A record above the checkpoint row also holds that row `F` after the encoded fields. Earlier
/// binaries ignore the trailing bytes, and they never get such a record. See `node::shared`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ViewDescriptor {
    pub version: u32,
    pub checkpoint: String,
    /// The checkpoint row, when the record index is above it. `None` means the record index.
    #[serde(skip)]
    pub row: Option<u64>,
}

impl ViewDescriptor {
    pub fn new(checkpoint: uuid::Uuid) -> Self {
        Self {
            version: VIEW_SNAPSHOT_FORMAT,
            checkpoint: checkpoint.to_string(),
            row: None,
        }
    }

    /// A record of `checkpoint` at an index above its row `row`.
    pub fn above_row(checkpoint: uuid::Uuid, row: u64) -> Self {
        Self {
            row: Some(row),
            ..Self::new(checkpoint)
        }
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut data = crate::codec::encode(self)?;
        if let Some(row) = self.row {
            data.extend(crate::codec::encode(&row)?);
        }
        Ok(data)
    }

    /// Decode format 6 data. Other formats return `None`.
    pub fn decode(data: &[u8]) -> Result<Option<Self>> {
        if format(data) != Some(VIEW_SNAPSHOT_FORMAT) {
            return Ok(None);
        }
        let (mut descriptor, used): (Self, _) =
            crate::codec::decode_prefix(data).context("invalid view descriptor")?;
        if used < data.len() {
            descriptor.row =
                Some(crate::codec::decode(&data[used..]).context("invalid view descriptor row")?);
        }
        descriptor.checkpoint()?;
        Ok(Some(descriptor))
    }

    pub fn checkpoint(&self) -> Result<uuid::Uuid> {
        uuid::Uuid::parse_str(&self.checkpoint).context("invalid view checkpoint")
    }
}

/// Return true for a view snapshot above its checkpoint row. It travels on its own route. See
/// `peer::COVERED_ROUTE`.
pub(crate) fn above_row(snapshot: &raft::eraftpb::Snapshot) -> bool {
    matches!(
        ViewDescriptor::decode(snapshot.get_data()),
        Ok(Some(view)) if view.row.is_some_and(|row| row < snapshot.get_metadata().index)
    )
}

/// The snapshot format of `data`, if it has one.
pub(crate) fn format(data: &[u8]) -> Option<u32> {
    crate::codec::decode::<u32>(data).ok()
}

/// Return true when this snapshot data can go to a peer: a format 6 descriptor.
pub(crate) fn sendable(data: &[u8]) -> bool {
    matches!(ViewDescriptor::decode(data), Ok(Some(_)))
}

/// The object path of the state database and its cache.
pub struct Location {
    root: String,
    pub(crate) cache: super::StateCache,
}

impl Location {
    pub(crate) fn new(root: &str, cache: super::StateCache) -> Arc<Self> {
        Arc::new(Self {
            root: root.to_owned(),
            cache,
        })
    }

    /// The object path of the database.
    pub(crate) fn root(&self) -> String {
        self.root.clone()
    }
}

#[cfg(test)]
#[path = "transfer_tests.rs"]
mod tests;
