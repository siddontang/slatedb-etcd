# Raft source and wire format

This directory uses the TiKV raft-rs revision in the package metadata in [Cargo.toml](Cargo.toml).
The service and its embedders select these sources through a Cargo path dependency.
Both use one Prost version. Neither needs an override in the host application's Cargo manifest.

The Raft core, quorum tests, and configuration change tests come from the pinned revision.
The local change in `src/raw_node.rs` selects shared byte buffers for proposal data.
The `proto` crate uses Prost for encoding, decoding, and field sizes.
Its field accessors connect the generated messages to the upstream Raft interface.
The build uses the system Protobuf compiler.

The schema keeps the upstream field numbers. It omits a rust-protobuf compiler option.
It selects the previous unpacked output for repeated member IDs in configuration records.
Prost also accepts packed input for these fields.
Entries, message contexts, and snapshots use shared byte buffers.

When updating Raft, compare the core and wire schema with the pinned revision.
Run the upstream tests, wire fixtures, fault tests, and mixed-version recovery checks.

[TiKV source](https://github.com/tikv/raft-rs/tree/ad13f3d90780f53aea2488c6a4b76c0d334bf136)
