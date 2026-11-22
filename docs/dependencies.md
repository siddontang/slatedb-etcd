# Raft dependencies

The service takes Raft and Raft Engine from Git branches in forks of the TiKV repositories.
Each branch carries an open upstream pull request. Cargo.lock pins the exact commits.

| Crate | Fork branch | Upstream work |
|---|---|---|
| `raft`, `raft-proto` | [`codex/prost-0-14`][raft] | [tikv/raft-rs#603] |
| `raft-engine` | [`codex/cache-identity`][engine] | [tikv/raft-engine#415] and a cache fix |
| `protobuf-build` | Pinned by `raft-proto` | [tikv/protobuf-build#78] |

Replace each branch with a crates.io release after its pull request merges and ships.

## Why the forks exist

Prost 0.14 types must come from the same Prost version as the generated code.
The service uses Prost 0.14 through Tonic. The released Raft crates use Prost 0.11 or
rust-protobuf 2, so their messages do not implement the service's `prost::Message` trait.

- `protobuf-build` generates the Raft messages. The fork moves its generator to Prost 0.14.
- `raft-proto` builds with that generator and the Prost 0.14 runtime.
- `raft-engine` adds `ProstCodec`, which stores Prost 0.14 messages in the existing format.

## Raft Engine fixes

The `codex/cache-identity` branch adds a cache fix on top of the Prost codec.
Each open log gets a unique identity, and the thread block cache keys on it.
Separate engines in one process can have equal file positions and different bytes.
DST seed 70 reproduced this collision. Upstream report: [tikv/raft-engine#386].

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
[engine]: https://github.com/siddontang/raft-engine/tree/codex/cache-identity
[tikv/raft-rs#603]: https://github.com/tikv/raft-rs/pull/603
[tikv/raft-engine#415]: https://github.com/tikv/raft-engine/pull/415
[tikv/protobuf-build#78]: https://github.com/tikv/protobuf-build/pull/78
[tikv/raft-engine#386]: https://github.com/tikv/raft-engine/issues/386
[tikv/raft-engine#395]: https://github.com/tikv/raft-engine/issues/395
