# Raft dependencies

The service takes Raft from a Git branch in a fork of the TiKV repository, and Raft Engine
from an upstream Git commit. Cargo.lock pins the exact commits.

| Crate | Source | Upstream work |
|---|---|---|
| `raft`, `raft-proto` | Fork branch [`codex/prost-0-14`][raft] | [tikv/raft-rs#603] (open) |
| `raft-engine` | Upstream commit [`32b68d36ca`][engine] | [#415][re415], [#416][re416] (merged) |
| `protobuf-build` | Pinned by `raft-proto` | [tikv/protobuf-build#78] (open) |

Replace the Raft branch with a crates.io release after its pull request merges and ships.
Replace the Raft Engine commit with a crates.io release when one includes both pull requests.

## Why the Prost 0.14 changes exist

Prost 0.14 types must come from the same Prost version as the generated code.
The service uses Prost 0.14 through Tonic. The released Raft crates use Prost 0.11 or
rust-protobuf 2, so their messages do not implement the service's `prost::Message` trait.

- `protobuf-build` generates the Raft messages. The open pull request moves its generator to
  Prost 0.14.
- `raft-proto` builds with that generator and the Prost 0.14 runtime.
- `raft-engine` has `ProstCodec`, which stores Prost 0.14 messages in the existing format.

## Raft Engine fixes

The pinned commit includes a cache fix on top of the Prost codec.
Each open log gets a unique identity, and the thread block cache keys on it.
Separate engines in one process can have equal file positions and different bytes.
DST seed 70 reproduced this collision. Upstream report: [tikv/raft-engine#386].
Upstream fix: [tikv/raft-engine#416], merged.

Upstream `master` already sends explicit sync and empty synchronous batches to the file system.
See [tikv/raft-engine#395].

## Wire format

Upstream Prost output writes packed member IDs in `ConfState`. Raft 0.7.0 wrote unpacked IDs.
Both decoders accept both forms, so old logs and mixed-version peers still work.
Raft messages hold `Vec<u8>` fields, so decoding a peer frame copies entry data.

## Regression checks

```sh
scripts/cargo.sh test --locked --test recovery
scripts/cargo.sh test --locked --test engine_sync
python3 scripts/check-dst.py --seeds 58,70
```

[raft]: https://github.com/siddontang/raft-rs/tree/codex/prost-0-14
[engine]: https://github.com/tikv/raft-engine/commit/32b68d36ca
[tikv/raft-rs#603]: https://github.com/tikv/raft-rs/pull/603
[tikv/raft-engine#415]: https://github.com/tikv/raft-engine/pull/415
[tikv/protobuf-build#78]: https://github.com/tikv/protobuf-build/pull/78
[tikv/raft-engine#416]: https://github.com/tikv/raft-engine/pull/416
[re415]: https://github.com/tikv/raft-engine/pull/415
[re416]: https://github.com/tikv/raft-engine/pull/416
[tikv/raft-engine#386]: https://github.com/tikv/raft-engine/issues/386
[tikv/raft-engine#395]: https://github.com/tikv/raft-engine/issues/395
