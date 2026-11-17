# State performance

This report measures two changes against fixed binaries and etcd v3.6.5.
Stage 1 replaces a full logical-state scan with incremental quota accounting.
Stage 2 adds a 64 MiB weighted cache for immutable SST blocks and metadata.

## Method

- Host: Apple M4, 10 logical CPUs, 16 GiB RAM, macOS, shared local disk.
- Topology: three server processes, one Go client, separate client and peer loopback listeners.
- Data: 1,000 keys, 256-byte values, uniform seeded access, 200 warmup operations.
- Load: 2,000 operations for write and mixed cases; 30,000 operations for read-only cases.
- Stage 1: concurrency 1 and 8; read ratios 0%, 50%, and 100%; three repeats; 54 samples.
- Stage 2: concurrency 8; the same read ratios and repeats; 27 samples.
- Storage: local object store, 2 GiB quota setting, 1,000-entry snapshot setting, normal Raft sync.
- Builds: Rust 1.91.1 release without crash hooks; Go 1.25.6; pinned dependencies.
- Baseline source: `0d41aaa8288253cfc9bd61af4999f1e0d6de0d91`.
- Quota-only runtime source: `6edef42`.
- Final runtime source: `03eba4f`, with the state cache and current-format snapshot checks.
- Each report retains the three binary SHA-256 values for its stage.

Each sample starts a fresh cluster. Each repeat shuffles workload and backend order.
All backends share the same client and operation seed for that repeat.
The client checks read values and treats each failed request as a failed sample.
Resource barriers exclude startup, preload, and warmup from measured CPU deltas.
RSS is the maximum sampled sum for the three server processes. It includes retained preload memory.
CPU per operation is the sum of server CPU time divided by successful requests.
Tables show medians of three repeats. They do not represent pooled latency percentiles.
The summary JSON retains minimum and maximum values for each metric.

The OS page cache remains warm. The host also runs normal desktop services.
Builds and profiling ran outside the measured matrices.
Short source, unit, and report checks ran on the host and can add scheduling noise.
Repeated measurements show host variation and downward drift; use the reported ranges.
CPU frequency was not fixed, and this run did not record temperature.
This is a closed-loop local comparison. It does not establish capacity, S3 cost, or cross-host
latency.

On macOS, etcd calls `F_FULLFSYNC`; the pinned Raft engine calls `fsync`.
The calls differ in device cache guarantees, so write results do not compare equal power-loss
durability.
The [benchmark guide](../../docs/benchmarks.md) links both implementations.
The PR checks also run the service and etcd matrix on Linux.
Both Linux log engines use `fdatasync`.
Use that check's `benchmark-linux.json` artifact for a separate Linux comparison.
Do not combine results from the two hosts into one ranking.

## Measurements

All 81 measured samples passed without request or resource errors.
The final Rust suite passed 46 tests, plus five DST seeds with two replays each.

Stage 1 increased pure-write throughput by 33.6x at concurrency 1 and 22.6x at concurrency 8.
At concurrency 8, median CPU work fell 94.2%, and p99 latency fell 95.2%.
Pure reads did not show a stable gain from quota accounting alone.

Stage 2 increased write throughput another 2.63x and read throughput 2.07x in its own matched runs.
Write CPU work fell 57.0%, and read CPU work fell 49.4%.
The cache increased combined RSS by 20.3 MiB for writes and 7.3 MiB for reads in this dataset.
This trades bounded cache memory for less storage work. It does not reduce resident memory.
Do not multiply ratios from the two stages into one capacity claim.

### Final build and cache comparison

Here, `baseline` means quota accounting without a state cache. `service` means the final cached
build.
`C` means client concurrency. CPU and RSS cover all three server processes.

| Backend | C | Read % | ops/s | p50 ms | p99 ms | CPU us/op | RSS MiB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| baseline | 8 | 0 | 726 | 11.05 | 32.25 | 6505 | 177.3 |
| baseline | 8 | 50 | 1383 | 5.35 | 16.79 | 3755 | 139.2 |
| baseline | 8 | 100 | 3446 | 2.07 | 4.86 | 1701 | 105.9 |
| etcd | 8 | 0 | 178 | 46.05 | 79.96 | 1825 | 159.2 |
| etcd | 8 | 50 | 233 | 31.27 | 94.63 | 1635 | 155.1 |
| etcd | 8 | 100 | 24099 | 0.33 | 0.64 | 229 | 171.6 |
| service | 8 | 0 | 1912 | 1.61 | 18.48 | 2800 | 197.6 |
| service | 8 | 50 | 2922 | 2.53 | 11.91 | 1910 | 151.1 |
| service | 8 | 100 | 7124 | 0.93 | 2.36 | 861 | 113.2 |

The final service still trails etcd for pure reads: 7,124 versus 24,099 operations per second.
Its read CPU cost is 861 versus 229 microseconds per operation.
The write comparison has different macOS disk flush guarantees, as described above.
The current implementation also spends more write CPU per operation than etcd.

### Quota accounting comparison

Here, `baseline` means the original full-scan implementation. `service` means quota accounting only.
This earlier matrix ran separately from the cache matrix; compare backends within each stage.

| Backend | C | Read % | ops/s | p50 ms | p99 ms | CPU us/op | RSS MiB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| baseline | 1 | 0 | 18 | 57.87 | 114.25 | 175155 | 167.2 |
| baseline | 1 | 50 | 51 | 6.43 | 72.34 | 61965 | 132.4 |
| baseline | 1 | 100 | 1299 | 0.78 | 0.91 | 1050 | 104.5 |
| baseline | 8 | 0 | 20 | 301.68 | 1004.12 | 164855 | 169.5 |
| baseline | 8 | 50 | 58 | 122.84 | 491.14 | 58675 | 134.8 |
| baseline | 8 | 100 | 2586 | 2.39 | 6.69 | 2136 | 104.5 |
| etcd | 1 | 0 | 107 | 8.15 | 25.04 | 2575 | 164.4 |
| etcd | 1 | 50 | 208 | 3.99 | 18.00 | 1375 | 157.5 |
| etcd | 1 | 100 | 4806 | 0.19 | 0.39 | 436 | 171.1 |
| etcd | 8 | 0 | 188 | 42.93 | 79.85 | 1415 | 159.2 |
| etcd | 8 | 50 | 252 | 29.20 | 81.83 | 1335 | 156.8 |
| etcd | 8 | 100 | 22814 | 0.33 | 0.81 | 217 | 172.8 |
| service | 1 | 0 | 619 | 1.58 | 3.83 | 4790 | 177.2 |
| service | 1 | 50 | 759 | 0.92 | 4.54 | 3060 | 139.0 |
| service | 1 | 100 | 1299 | 0.78 | 0.92 | 1050 | 105.5 |
| service | 8 | 0 | 458 | 17.00 | 48.06 | 9595 | 177.6 |
| service | 8 | 50 | 895 | 7.84 | 30.30 | 4865 | 138.2 |
| service | 8 | 100 | 2855 | 2.25 | 6.79 | 1892 | 105.5 |

See the [stage 1 samples](quota-only-macos-m4.json) and
[stage 1 ranges](quota-only-macos-m4-summary.json) for all repeats.

## Bottleneck and change

A separate native stack sample repeatedly entered `StateMachine::logical_size` and SlateDB
iterators.
This path scanned current values, history, Watch records, and other logical state before each write.
The scan cost grew with retained history, even when each request changed one small key.

The apply worker now keeps a derived byte count. A tracked batch keeps the final mutation per key.
For each changed logical key, it reads the old value and calculates the byte difference.
It publishes the new count only after SlateDB accepts the atomic batch.
Startup rebuilds the counter from stored state. Snapshot replacement uses the same batch path.
The counter adds no stored fields and keeps the same quota definition.

Before this change, each quota check scanned N logical records: O(N) work per checked write.
Now the quota check costs O(1). Accounting uses K point reads for K changed logical keys.
Batch assembly uses O(K log K) map work and O(K) retained mutations.
Startup still performs an O(N) scan. Large compactions and snapshots still process many keys.

The tests compare the counter with an independent full scan after:

- Repeated mutations, absent deletes, value shrinkage, and storage errors.
- KV changes, nested transactions, rejected writes, and lease grant, renewal, revoke, and expiry.
- Auth, membership, alarms, logical compaction, and physical compaction.
- Snapshot replacement, restart, and five DST schedules with two identical replays each.

The PR checks also run upstream compatibility, downstream clients, native crash hooks, S3 recovery,
process faults, Jepsen models, and bounded API fuzzing.

## State cache

Quota-only stack samples still showed frequent SST file opens during repeated reads.
The dependency disabled default features without enabling either supported cache backend.
Both default cache factories therefore returned no cache.

The final build enables Moka and supplies one 64 MiB weighted cache per member.
Blocks, indexes, filters, and SST statistics share this budget. Raft log budgets remain separate.
The cache stores immutable SST entries. New SST identifiers prevent stale values after writes.
It has no TTL or idle expiry. The budget measures entry weights, not total process RSS.
Cache and allocator overhead can add memory beyond the weight limit.

A targeted fault test blocks SST reads after warmup and confirms cached reads still return data.
It then writes and flushes a new version and checks that reads return the new value.
The full Rust suite and five replayed DST seeds also pass with the cache enabled.

## Remaining work

Pure reads still require per-request ReadIndex work and peer HTTP exchanges.
Internal read replies also pass through JSON values before the gRPC response.
The write path retains multiple logical records per revision and performs point reads during apply.
These are follow-up profiling targets; this change does not claim to remove their cost.

The matrix does not cover large values, large datasets, Watch fan-out, lease churn, or cold S3
reads.
Long runs must measure memory stability, compaction interference, and snapshot catch-up separately.
Object-reference snapshots and lease indexes still require separate correctness work.

## Reproduce

Build each listed source commit in a separate checkout with the same toolchain.
Save `0d41aaa` as `tmp/performance/slatedb-etcd-baseline`.
Save `6edef42` as `tmp/performance/slatedb-etcd-optimized`.
Save `03eba4f` as `tmp/performance/slatedb-etcd-cached`, then run:

```sh
python3 scripts/benchmark-etcd.py \
  --binary tmp/performance/slatedb-etcd-optimized \
  --baseline-binary tmp/performance/slatedb-etcd-baseline \
  --requests 2000 --read-requests 30000 --keys 1000 --value-size 256 \
  --concurrency 1 8 --read-percent 0 50 100 --repeat 3 --warmup 200 \
  --output results/performance/quota-only-macos-m4.json
python3 scripts/benchmark-etcd.py \
  --binary tmp/performance/slatedb-etcd-cached \
  --baseline-binary tmp/performance/slatedb-etcd-optimized \
  --requests 2000 --read-requests 30000 --keys 1000 --value-size 256 \
  --concurrency 8 --read-percent 0 50 100 --repeat 3 --warmup 200 \
  --output results/performance/macos-m4.json
python3 scripts/summarize-benchmark.py results/performance/macos-m4.json \
  --output results/performance/macos-m4-summary.json
```

See the [raw samples](macos-m4.json), [repeat summary](macos-m4-summary.json), and
[profile evidence](profile-summary.json).
Profile runs are separate from the measured matrix. Stack counts are not CPU percentages.
