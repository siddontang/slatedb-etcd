# Independent reads and grouped state writes

This report measures the runtime refactor against the previous main branch and etcd v3.6.5.
The baseline source is `fd16ca476dc0c4a163850205cdd66211ca6768a0`.
The final runtime source is `ff116521c4398a746e1ab2fb4403b03a47086df5`.
Report-only commits do not change the measured runtime. Raw reports record each binary hash.

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

### Three members

All 63 samples passed without request or resource errors.

| Backend | C | Read % | ops/s | p50 ms | p99 ms | CPU us/op | RSS MiB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| baseline | 8 | 0 | 3984 | 1.69 | 8.99 | 588 | 325.5 |
| baseline | 8 | 50 | 6080 | 1.17 | 5.09 | 359 | 250.3 |
| baseline | 8 | 100 | 27411 | 0.28 | 0.81 | 89 | 90.5 |
| etcd | 1 | 0 | 3134 | 0.30 | 0.67 | 432 | 161.4 |
| etcd | 1 | 50 | 4174 | 0.23 | 0.57 | 335 | 163.2 |
| etcd | 1 | 100 | 5979 | 0.17 | 0.23 | 254 | 147.3 |
| etcd | 8 | 0 | 8622 | 0.90 | 2.22 | 186 | 167.4 |
| etcd | 8 | 50 | 10582 | 0.69 | 1.99 | 175 | 169.8 |
| etcd | 8 | 100 | 27024 | 0.27 | 0.93 | 90 | 152.3 |
| etcd | 32 | 0 | 18492 | 1.67 | 4.85 | 93 | 168.2 |
| etcd | 32 | 50 | 19743 | 1.47 | 4.62 | 94 | 171.5 |
| etcd | 32 | 100 | 57959 | 0.47 | 1.81 | 41 | 153.9 |
| service | 1 | 0 | 2067 | 0.43 | 1.36 | 899 | 341.5 |
| service | 1 | 50 | 3133 | 0.30 | 0.81 | 527 | 263.9 |
| service | 1 | 100 | 5232 | 0.20 | 0.24 | 279 | 85.6 |
| service | 8 | 0 | 5350 | 1.23 | 8.42 | 466 | 293.5 |
| service | 8 | 50 | 7916 | 0.88 | 4.43 | 293 | 235.4 |
| service | 8 | 100 | 28003 | 0.27 | 0.79 | 85 | 85.7 |
| service | 32 | 0 | 9750 | 2.64 | 14.86 | 262 | 306.9 |
| service | 32 | 50 | 14054 | 1.87 | 10.35 | 174 | 232.5 |
| service | 32 | 100 | 60437 | 0.47 | 1.67 | 38 | 86.1 |

At concurrency 8, write throughput rises 34.3% and mixed throughput rises 30.2% from the baseline.
Write CPU per request falls 20.7%, write RSS falls 9.8%, and write p99 falls 6.3%.
Read throughput rises 2.2%; this small change needs longer tests before a broad read-speed claim.

The service reaches 62.1% of etcd write throughput at concurrency 8.
It uses 2.5 times the CPU per write and 1.8 times the RSS. Write p99 is 3.8 times etcd p99.
At concurrency 32, write throughput reaches 52.7% of etcd throughput.
Read throughput is slightly higher at concurrency 8 and 32, but lower at concurrency 1.
Read RSS stays near 86 MiB, compared with 147 to 154 MiB for etcd.

Raw data: [samples](linux-batch-three.json) and [repeat ranges](linux-batch-three-summary.json).

The service shares about 1.00, 2.53, and 8.63 applied entries per state write at concurrency
1, 8, and 32. These are medians of per-run counter ratios across all three replicas.
Counters confirm shared state writes under concurrency. They do not prove each component gain.

### One member

All 27 samples passed without request or resource errors.

| Backend | C | Read % | ops/s | p50 ms | p99 ms | CPU us/op | RSS MiB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| baseline | 8 | 0 | 9584 | 0.70 | 3.84 | 126 | 111.3 |
| baseline | 8 | 50 | 14017 | 0.52 | 2.37 | 72 | 82.9 |
| baseline | 8 | 100 | 62183 | 0.12 | 0.35 | 22 | 30.7 |
| etcd | 8 | 0 | 21392 | 0.36 | 1.16 | 47 | 42.1 |
| etcd | 8 | 50 | 26665 | 0.27 | 1.06 | 47 | 43.5 |
| etcd | 8 | 100 | 54396 | 0.13 | 0.46 | 30 | 38.3 |
| service | 8 | 0 | 13195 | 0.51 | 3.08 | 93 | 96.7 |
| service | 8 | 50 | 19114 | 0.36 | 2.48 | 60 | 72.1 |
| service | 8 | 100 | 61089 | 0.12 | 0.34 | 23 | 28.4 |

Single-member write throughput rises 37.7% from the baseline. CPU per write falls 26.2%.
Write RSS falls 13.1%, and write p99 falls 19.8%.
Mixed throughput rises 36.4%, but mixed p99 rises 4.3%.
Read throughput falls 1.8%, and read CPU per request rises 3.8%.
The repeat ranges overlap for these small read changes.

The service reaches 61.7% of etcd write throughput and 71.7% of its mixed throughput.
Its read throughput is 12.3% higher than etcd in this matrix.
Write CPU is about twice etcd CPU. Write RSS is 2.3 times etcd RSS.
This is not general performance superiority.

Raw data: [samples](linux-batch-one.json) and [repeat ranges](linux-batch-one-summary.json).

### Isolated receive pipeline experiment

A separate 27-sample experiment compares the final runtime with the preceding commit.
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

Four profiles sample three server processes at concurrency 8 after warmup.
They use `cpu-clock` at 199 Hz with 16 KiB DWARF stacks and no inline expansion.
Write profiles use 20,000 requests; read profiles use 60,000 requests.
All profiles contain resolved user symbols and report zero lost samples.
The [profile report](linux-batch-profiles.json) records exact sample counts and symbol shares.
Profiled throughput and latency do not enter the performance tables.

| Inclusive stack | Baseline write | Final write |
| --- | ---: | ---: |
| Snapshot construction | 14.52% | 16.20% |
| Apply entries | 10.57% | 6.80% |
| Log executor | 7.51% | 7.37% |
| JSON decoding | 3.71% | Not reported |
| HTTP/2 client processing | 6.69% | 7.20% |

Inclusive percentages overlap. Do not add them into one CPU budget.
“Not reported” means absent from the 0.5% symbol report, not zero work.
Inlining can hide individual function symbols.
A larger share does not establish an absolute cost increase when total CPU work falls.
Read profiles still spend about 18% in HTTP/2 client processing.
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
