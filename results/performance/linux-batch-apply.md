# Independent reads and grouped state writes

This report measures the runtime refactor against the previous main branch and etcd v3.6.5.
The baseline source is `fd16ca476dc0c4a163850205cdd66211ca6768a0`.
The final runtime source is `c7d9661ca2716fa0faced433fe20ced3d7071359`.
Later test and report commits do not change the measured runtime.
Raw reports record each binary hash.

## Changes

KV reads use bounded tasks and pinned data with matching revision, Auth, and Lease metadata.
Safe ReadIndex still requires a quorum proof and local apply progress.
Ordinary unleased puts share a state write while retaining separate revisions and Watch events.
Other operations form batch boundaries. The final apply marker belongs to the same storage write.

KV Raft commands, MVCC history, and Watch journals use protobuf encoding.
Snapshot construction streams rows into one output and releases spare buffer capacity.
Unchanged Auth and Lease maps share immutable allocations.
The peer receiver admits bounded messages while older processing receipts wait.
It returns receipts in order. Durable Raft acknowledgements keep their existing barriers.

The [module guide](../../docs/modules.md) records ownership and request paths.
The [upstream review](../../docs/raft-performance-notes.md) maps the etcd and TiKV references.
Storage format 3 requires fresh directories and object prefixes. No old-format adapter remains.
The etcd client protocol remains unchanged.

## Method

Ubuntu 24.04 runs in an ARM64 VM with four virtual CPUs and 6 GiB RAM on an Apple M4 host.
Rust 1.91.1 builds use release optimization and line symbols. Go 1.25.6 builds the client and etcd.
All members and the official Go client share the VM's CPU pool, local disk, and loopback network.
Both services retain normal Linux durable log writes. The service uses local object storage.

Each sample starts a fresh cluster with 1,000 keys and 256-byte values.
Both backends use a 2 GiB quota setting and a 1,000-entry snapshot setting.
Equal settings do not mean equal accounting, snapshot work, or retention.
Each sample runs 200 warmup operations, then waits one second before measurement.
Write and mixed samples measure 10,000 requests. Read samples measure 60,000 linearizable reads.

The three-member matrix uses concurrency 1, 8, and 32. It includes the baseline at concurrency 8.
The separate one-member matrix compares all backends at concurrency 8.
Both matrices use 0%, 50%, and 100% reads, with three repeats in shuffled order.
Tables show medians. Summary files retain each metric's minimum and maximum across repeats.
Percentiles are not pooled across samples.

CPU means summed server CPU time per successful request. RSS sums sampled server resident memory.
Resource barriers exclude startup, preload, warmup, and diagnostic counter requests.
Counter samples can include follower progress between the resource and diagnostic boundaries.
Client resources appear separately in the raw reports.
Compilation, native tests, and CPU profiling run outside these measurement phases.

The VM shares a physical host with desktop services. CPU frequency and temperature are not fixed.
The page cache is not cleared. RSS is not unique physical memory.
These short closed-loop runs do not establish capacity, long-term memory use, or S3 cost.
They do not measure independent physical hosts, encrypted transport, or cross-zone latency.

## Results

### Three members on ARM64

All 63 samples passed without request or resource errors.

| Backend | C | Read % | ops/s | p50 ms | p99 ms | CPU us/op | RSS MiB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| baseline | 8 | 0 | 4067 | 1.67 | 9.45 | 585 | 311.6 |
| baseline | 8 | 50 | 6047 | 1.17 | 4.80 | 357 | 264.9 |
| baseline | 8 | 100 | 26641 | 0.28 | 0.82 | 91 | 90.3 |
| etcd | 1 | 0 | 3111 | 0.30 | 0.65 | 433 | 160.9 |
| etcd | 1 | 50 | 4188 | 0.23 | 0.58 | 334 | 159.5 |
| etcd | 1 | 100 | 5900 | 0.17 | 0.35 | 256 | 148.4 |
| etcd | 8 | 0 | 8611 | 0.91 | 2.27 | 185 | 166.0 |
| etcd | 8 | 50 | 10363 | 0.71 | 2.13 | 176 | 168.4 |
| etcd | 8 | 100 | 27015 | 0.27 | 0.93 | 89 | 153.2 |
| etcd | 32 | 0 | 18305 | 1.69 | 4.64 | 92 | 166.9 |
| etcd | 32 | 50 | 19830 | 1.46 | 4.44 | 95 | 170.2 |
| etcd | 32 | 100 | 57919 | 0.47 | 1.81 | 41 | 153.9 |
| service | 1 | 0 | 2069 | 0.43 | 1.45 | 898 | 348.5 |
| service | 1 | 50 | 3079 | 0.30 | 0.93 | 530 | 267.3 |
| service | 1 | 100 | 5176 | 0.20 | 0.25 | 283 | 85.4 |
| service | 8 | 0 | 5411 | 1.21 | 7.59 | 459 | 293.2 |
| service | 8 | 50 | 8088 | 0.86 | 4.24 | 290 | 231.2 |
| service | 8 | 100 | 27920 | 0.27 | 0.78 | 86 | 85.1 |
| service | 32 | 0 | 9874 | 2.58 | 16.38 | 266 | 309.2 |
| service | 32 | 50 | 13621 | 1.90 | 10.58 | 178 | 225.2 |
| service | 32 | 100 | 59783 | 0.47 | 1.61 | 38 | 85.2 |

Changes from the matched baseline at concurrency 8:

| Read % | Throughput change | CPU/request change | p99 change | RSS change |
| ---: | ---: | ---: | ---: | ---: |
| 0 | +33.1% | -21.5% | -19.7% | -5.9% |
| 50 | +33.8% | -18.8% | -11.6% | -12.7% |
| 100 | +4.8% | -6.6% | -5.1% | -5.7% |

The service reaches 62.8% of etcd write throughput at concurrency 8 and 53.9% at concurrency 32.
At concurrency 8, each write uses about 2.5 times the CPU and 1.8 times the RSS of etcd.
Write p99 is 3.3 times etcd p99. Read throughput is slightly higher at concurrency 8 and 32.
Read throughput is lower at concurrency 1. Read RSS stays near 85 MiB, below etcd's 148 to 154 MiB.

Raw data: [samples](linux-batch-three.json) and [repeat ranges](linux-batch-three-summary.json).

The service shares about 1.00, 2.57, and 8.43 applied entries per state write at concurrency
1, 8, and 32. These are medians of per-run counter ratios across all three replicas.
Counters confirm shared state writes under concurrency. They do not prove each component gain.

### One member on ARM64

All 27 samples passed without request or resource errors.

| Backend | C | Read % | ops/s | p50 ms | p99 ms | CPU us/op | RSS MiB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| baseline | 8 | 0 | 9462 | 0.71 | 3.66 | 127 | 102.1 |
| baseline | 8 | 50 | 13806 | 0.53 | 2.40 | 71 | 81.8 |
| baseline | 8 | 100 | 62220 | 0.12 | 0.35 | 22 | 30.6 |
| etcd | 8 | 0 | 21108 | 0.37 | 1.18 | 47 | 42.5 |
| etcd | 8 | 50 | 26831 | 0.27 | 0.98 | 47 | 43.7 |
| etcd | 8 | 100 | 54331 | 0.13 | 0.47 | 30 | 38.1 |
| service | 8 | 0 | 13058 | 0.51 | 3.01 | 92 | 102.8 |
| service | 8 | 50 | 19231 | 0.36 | 2.07 | 60 | 77.2 |
| service | 8 | 100 | 60545 | 0.12 | 0.34 | 23 | 28.4 |

Changes from the matched baseline at concurrency 8:

| Read % | Throughput change | CPU/request change | p99 change | RSS change |
| ---: | ---: | ---: | ---: | ---: |
| 0 | +38.0% | -27.6% | -17.6% | +0.7% |
| 50 | +39.3% | -15.5% | -13.9% | -5.6% |
| 100 | -2.7% | +4.5% | -3.0% | -7.1% |

Single-member write throughput rises 38.0%, and CPU per write falls 27.6%.
Write RSS changes by less than 1%; this run does not establish a write-memory improvement.
Read throughput falls 2.7%, and read CPU per request rises 4.5%.

The service reaches 61.9% of etcd write throughput and 71.7% of its mixed throughput.
Read throughput is 11.4% higher than etcd in this matrix.
Write CPU is about twice etcd CPU. Write RSS is 2.4 times etcd RSS.
These measurements do not establish general performance superiority.

Raw data: [samples](linux-batch-one.json) and [repeat ranges](linux-batch-one-summary.json).

### Three members on Linux x86

A separate GitHub runner uses four virtual CPUs and about 15.3 GiB RAM.
It uses the same Rust toolchain, release settings, keys, values, quota, and snapshot settings.
Write and mixed samples use 5,000 requests. Read samples use 60,000 requests.
Do not combine its absolute results with the ARM64 measurements.
All 63 samples pass. The table shows concurrency 8; the raw report retains concurrency 1 and 32.

| Backend | C | Read % | ops/s | p50 ms | p99 ms | CPU us/op | RSS MiB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| baseline | 8 | 0 | 2661 | 2.60 | 12.52 | 1204 | 276.4 |
| baseline | 8 | 50 | 4112 | 1.79 | 6.65 | 732 | 168.5 |
| baseline | 8 | 100 | 10312 | 0.76 | 1.60 | 254 | 104.8 |
| etcd | 8 | 0 | 7110 | 1.04 | 2.73 | 384 | 168.3 |
| etcd | 8 | 50 | 7398 | 1.00 | 2.70 | 376 | 159.2 |
| etcd | 8 | 100 | 11739 | 0.65 | 1.70 | 228 | 161.4 |
| service | 8 | 0 | 3588 | 1.87 | 9.18 | 880 | 245.0 |
| service | 8 | 50 | 5109 | 1.42 | 5.64 | 580 | 153.4 |
| service | 8 | 100 | 10697 | 0.73 | 1.54 | 243 | 98.2 |

Changes from the matched baseline at concurrency 8:

| Read % | Throughput change | CPU/request change | p99 change | RSS change |
| ---: | ---: | ---: | ---: | ---: |
| 0 | +34.8% | -26.9% | -26.6% | -11.4% |
| 50 | +24.3% | -20.8% | -15.2% | -8.9% |
| 100 | +3.7% | -4.3% | -4.1% | -6.3% |

Write throughput rises 34.8%, CPU per write falls 26.9%, and write RSS falls 11.4%.
The service still reaches only 50.5% of etcd write throughput and 91.1% of its read throughput.
The read advantage from the ARM64 matrix does not carry over to this x86 run.

Raw data: [samples](linux-batch-ci.json), [repeat ranges](linux-batch-ci-summary.json), and
[CPU profile reports](linux-batch-ci-profiles.json).
The [performance job][ci-run] passed all measurements and four symbolized CPU profiles.

[ci-run]: https://github.com/siddontang/slatedb-etcd/actions/runs/44270177226

### Isolated receive pipeline experiment

A separate 27-sample experiment compares pipeline commit `ff11652` with `f7c317f`.
That baseline already contains independent reads, grouped writes, and compact encodings.
Do not combine its absolute values with the main matrix.
All samples use three members and 10,000 writes, with three repeats per concurrency.

| C | Throughput change | CPU/request change | p99 change | RSS change |
| ---: | ---: | ---: | ---: | ---: |
| 1 | -2.0% | +0.0% | +7.9% | +0.0% |
| 8 | +7.7% | -9.4% | -10.5% | -9.6% |
| 32 | +12.3% | -15.9% | +9.8% | -0.6% |

The receive pipeline improves concurrent throughput and CPU cost. It does not help one request.
Concurrency 32 has higher p99 in this isolated experiment. Retain this tradeoff when tuning limits.
The main matrix measures all changes together and cannot isolate the other individual gains.

Raw data: [samples](linux-batch-pipeline.json) and
[repeat ranges](linux-batch-pipeline-summary.json).

## CPU findings and remaining work

Four ARM64 profiles sample three server processes at concurrency 8 after warmup.
They use `cpu-clock` at 199 Hz with 16 KiB DWARF stacks and no inline expansion.
Write profiles use 20,000 requests; read profiles use 60,000 requests.
All profiles contain resolved user symbols and report zero lost samples.
The [profile report](linux-batch-profiles.json) records exact sample counts and symbol shares.
Profiled throughput and latency do not enter the performance tables.

The separate x86 profiles use 99 Hz. Their write profiles contain 586 and 401 samples.
Use those small profiles for coarse checks, not precise symbol comparisons.

| Inclusive stack | Baseline write | Final write |
| --- | ---: | ---: |
| Snapshot construction | 14.34% | 16.59% |
| Apply entries | 11.41% | 7.41% |
| Log executor | 8.25% | 7.94% |
| JSON decoding | 3.78% | Not reported |
| HTTP/2 client processing | 5.63% | 6.04% |

Inclusive percentages overlap. Do not add them into one CPU budget.
“Not reported” means absent from the 0.5% symbol report, not zero work.
Inlining can hide individual function symbols.
A larger share does not establish an absolute cost increase when total CPU work falls.
Read profiles still spend about 15% in HTTP/2 client processing.
The independent read task appears outside the apply executor in the final profile.
The flat profile does not isolate every ReadIndex wait or async stage.

Full snapshots remain the largest reported application stack in the write profile.
The current builder still scans retained history and writes a full buffer into raft-engine.
It reduces temporary allocations but does not remove size-dependent checkpoint work.
Snapshot writes also share the log worker with ordinary durable entries.
This design is a likely contributor to write tails and memory use; these profiles do not prove
its exact share of end-to-end latency.

Next, measure per-stage queue time and bytes before separating local checkpoints from transfer
snapshots. That change needs durable object ownership, checksums, recovery fences, and deletion
rules. Do not replace a synced recovery snapshot with an unproven flush shortcut.
Then measure transaction overlays, Watch fan-out, lease churn, larger state, and slow storage.
Keep low-concurrency latency and independent memory limits as acceptance criteria.

## Correctness evidence

The [client report](batch-apply-compatibility.json) records 185 leaf cases with zero skips.
Watch compaction checks now wait for local apply on each endpoint.
A partition case verifies local history replay and compacted-history rejection after recovery.
Both Watch cases passed ten consecutive runs. The service schemas also compile with protoc 3.9.

The final runtime passed 81 native Rust tests, excluding the separate DST campaign.
Formatting and all-target, all-feature Clippy checks passed.
The [DST report](batch-apply-dst.json) records 100 seeds and two identical replays per seed.
The campaign contains 429,051 recorded events across the 100 distinct traces.
It controls Raft delivery, log work, apply work, and read completion.
It does not simulate HTTP/2 scheduling, wall-clock lease expiry, or partial sector writes.

The [crash report](batch-apply-failpoints.json) records 21 process crash boundaries with no skips.
The two grouped-write hooks require an actual group with more than one entry.
Recovery checks retain every acknowledged value and its exact modification revision.
Real socket tests also check receive limits, ordered receipts, timeout, reconnect, and snapshots.

PR checks run differential, upstream, downstream, S3, process fault, fuzz, and Jepsen tests.
Use the final [PR check results](https://github.com/siddontang/slatedb-etcd/pull/5/checks).
The [compatibility contract](../../COMPATIBILITY.md) lists unsupported formats and deployment
limits.
