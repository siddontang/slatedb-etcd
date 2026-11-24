# Retained results

These files contain selected evidence, not a test suite that runs by reading JSON.
Run the commands in the [test guide](../tests/README.md) to produce new reports.
Use `tmp/test-results/` for local output unless you intend to update a retained record.

| Files | Scope |
| --- | --- |
| `etcd-all-capabilities.json` | Administration milestone; 387 leaf cases, no failures or skips |
| `etcd-admin-recovery.json` | Learner lifecycle, strict promotion, and minority queries |
| `etcd-compatibility.json` | Local client profile recorded during the administration work |
| `etcd-core-fault-tests.json` | Eight process fault checks |
| `dst.json` | 100 deterministic seeds, each replayed twice |
| `robustness.json` | Four process campaigns checked by the etcd model, Knossos, and Elle |
| `etcd-upstream.json` | Earlier imported-suite record; predates administration support |
| `fault-tests.json` | Original three-replica experiment |
| `latency.json`, `latency-with-snapshots.json` | Original local JSON benchmarks |
| `before-background-snapshots.json` | An earlier failed benchmark kept for comparison |
| `performance/shared-state.md`, `.jsonl` | Shared state mode compared with replicated mode |

Read `passed`, `complete`, and `leaf_counts` where present.
A parent Go test can pass while a child skips. Do not add overlapping profile counts together.
Older failures and skips do not describe the current capability profile.
Each report identifies its tested binary or trace hashes where applicable.

Publication cleanup replaced paths with `<repo>`, `<home>`, and `<tmp>` labels.
It did not change measurements, outcomes, test counts, seeds, or hashes.
The referenced logs and traces are local artifacts and are not included in this repository.
CI prints fresh reports in its logs and attempts an optional artifact upload.

The [remote storage evidence](remote-storage/README.md) adds S3 faults, native crash boundaries,
downstream projects, deterministic object faults, three-guest recovery, and gRPC comparisons.
