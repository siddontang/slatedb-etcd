//! Read the Raft snapshot record of a stopped member.

use anyhow::{ensure, Context, Result};

use crate::state::transfer::ViewDescriptor;

/// The Raft snapshot record of a stopped member: its index, term, and the view checkpoint that it
/// names. Retention and manual cleanup must keep that checkpoint.
pub fn member_record(dir: &std::path::Path) -> Result<serde_json::Value> {
    let raft = dir.join("raft");
    ensure!(raft.exists(), "the data directory has no Raft log");
    let storage = crate::storage::RaftStorage::open_existing(&raft)
        .context("cannot open the Raft log; stop the member first")?;
    let record = storage.recovery_snapshot();
    let checkpoint = ViewDescriptor::decode(record.get_data())?
        .map(|view| view.checkpoint())
        .transpose()?;
    Ok(serde_json::json!({
        "index": record.get_metadata().index,
        "term": record.get_metadata().term,
        "checkpoint": checkpoint,
    }))
}
