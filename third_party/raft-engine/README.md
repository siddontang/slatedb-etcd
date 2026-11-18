# Local Raft Engine fixes

This directory contains the runtime, examples, and tests from the published `raft-engine` 0.4.2
crate.
Cargo selects this copy through `[patch.crates-io]`. It keeps the 0.4.2 storage format and API.
The original [README](README.upstream) and [Apache-2.0 license](LICENSE) remain here.

The crate archive has SHA-256:

```text
1213c3a24e3fee8afcc74b2be08c4081adde96f092e0fc1c607abb3e16ae722e
```

## Changed runtime files

- `src/pipe_log.rs` adds an optional cache identity. Unknown implementations bypass the block cache.
- `src/file_pipe_log/pipe.rs` assigns a unique identity to each open log, including reopened logs.
- `src/engine.rs` includes that identity in both decoded-entry and raw-entry cache keys.
- `src/engine.rs` sends explicit sync and empty synchronous batches to the underlying log file.

The cache remains one block per thread. Its key previously contained only file position.
Separate engines can have equal file positions and different bytes.
The unique identity prevents cache reuse across these engines without adding a new read queue.
An exhausted identity counter stops construction instead of reusing an identity.

The original `sync()` called `write()` with an empty batch, which returned without syncing.
The local fix follows the direct sync behavior now present in upstream code.
Nonempty synchronous writes already reached the file system before this fix.

Related upstream reports: [multiple engines][engines] and [missing explicit sync][sync].
Remove these local patches when an upstream release includes both fixes.
Do not edit unrelated upstream files. Test dependency changes against the regressions below.

## Regression checks

```sh
scripts/cargo.sh test --locked --test recovery separate_logs_do_not_share_cached_entries
scripts/cargo.sh test --locked --test engine_sync
python3 scripts/check-dst.py --seeds 58,70
```

The first two tests fail with the unmodified crate and pass with these patches.
The first test checks values and terms across two engines and reopened instances on one thread.
The second counts actual file sync calls for explicit sync and empty synchronous batches.
DST seed 70 reproduced a cross-engine cache collision before the patch.

[engines]: https://github.com/tikv/raft-engine/issues/386
[sync]: https://github.com/tikv/raft-engine/issues/395
