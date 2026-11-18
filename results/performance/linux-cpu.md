# Linux CPU and gRPC performance

This report compares the ReadIndex and apply changes with etcd v3.6.5.
The baseline is `c84849b4d1aa2294a45594f539fd2586474eb71a`.
The final runtime source is `3d0d5c0dfd91ce417d2802f84433e61e2217940e`.
Each measurement report records its binary hashes.

## Method

The local Linux environment uses four ARM64 virtual CPUs and 6 GiB RAM on one Apple M4 host.
Ubuntu 24.04 runs Linux 6.8.0. Rust 1.91.1 builds use release optimization and line symbols.
Go 1.25.6 builds the official client and etcd server from pinned modules.
Data lives on the VM filesystem. Three members and one client share the same disk and CPU pool.

The workload uses 1,000 keys, 256-byte values, and 200 unmeasured warmup operations.
Each sample measures 5,000 write or mixed operations, or 60,000 linearizable reads.
Both backends use a 2 GiB quota setting and a 1,000-entry snapshot setting.
The service uses local object storage. Both keep their normal Linux Raft sync behavior.

Each sample starts a fresh cluster. The driver shuffles workloads and backends for each repeat.
The final comparison runs three repeats at concurrency 1, 8, and 32.
A separate matched comparison measures the baseline, final service, and etcd at concurrency 8.
Tables show medians across repeats. Summary files retain the full minimum and maximum ranges.
CPU means summed server CPU time per successful operation. RSS sums the three server processes.
Client resources are separate. Resource barriers exclude startup, preload, and warmup.

Builds, Rust tests, and CPU sampling run outside these final measurement phases.
The VM shares a physical host with desktop services. CPU frequency and temperature are not fixed.
The host page cache is not cleared. RSS does not measure unique physical memory.
These closed-loop measurements do not establish capacity, S3 cost, or cross-host latency.

## Measured results

All 54 final samples passed without request or resource errors.
The table compares the final service and etcd on the same Linux ARM64 VM.

| Backend | C | Read % | ops/s | p50 ms | p99 ms | CPU us/op | RSS MiB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| etcd | 1 | 0 | 3292 | 0.29 | 0.58 | 422 | 157.3 |
| etcd | 1 | 50 | 4390 | 0.22 | 0.55 | 330 | 139.8 |
| etcd | 1 | 100 | 5999 | 0.17 | 0.22 | 254 | 147.1 |
| etcd | 8 | 0 | 8609 | 0.91 | 1.96 | 186 | 159.0 |
| etcd | 8 | 50 | 10840 | 0.70 | 1.81 | 168 | 151.8 |
| etcd | 8 | 100 | 27343 | 0.27 | 0.88 | 88 | 152.7 |
| etcd | 32 | 0 | 20125 | 1.60 | 3.21 | 86 | 161.3 |
| etcd | 32 | 50 | 21325 | 1.40 | 3.34 | 86 | 149.2 |
| etcd | 32 | 100 | 59941 | 0.46 | 1.67 | 40 | 153.6 |
| service | 1 | 0 | 1536 | 0.59 | 1.52 | 1126 | 286.5 |
| service | 1 | 50 | 2396 | 0.40 | 0.89 | 682 | 162.9 |
| service | 1 | 100 | 4365 | 0.25 | 0.30 | 399 | 93.5 |
| service | 8 | 0 | 3353 | 2.10 | 8.37 | 762 | 275.7 |
| service | 8 | 50 | 4463 | 1.58 | 4.78 | 566 | 159.9 |
| service | 8 | 100 | 12407 | 0.65 | 1.35 | 201 | 93.4 |
| service | 32 | 0 | 3626 | 8.01 | 21.70 | 734 | 283.5 |
| service | 32 | 50 | 6070 | 4.19 | 15.60 | 424 | 159.5 |
| service | 32 | 100 | 31502 | 0.98 | 2.32 | 80 | 93.4 |

At concurrency 8, the service reaches 39% of etcd write throughput and 45% of its read throughput.
Write CPU is 4.1 times etcd CPU per operation. Read CPU is 2.3 times etcd CPU per operation.
Write RSS is higher. Read RSS is lower in this dataset.
These results show improvement work remains; they do not establish parity with etcd.

Raw data: [samples](linux-arm64.json) and [repeat ranges](linux-arm64-summary.json).

## Matched baseline comparison

All 27 matched samples passed without request or resource errors.
This separate run compares the baseline, final service, and etcd at concurrency 8.
Do not combine absolute values from the two measurement runs.

| Backend | C | Read % | ops/s | p50 ms | p99 ms | CPU us/op | RSS MiB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| baseline | 8 | 0 | 1732 | 3.21 | 15.14 | 1428 | 301.0 |
| baseline | 8 | 50 | 2981 | 2.29 | 6.87 | 882 | 174.0 |
| baseline | 8 | 100 | 8886 | 0.88 | 1.85 | 311 | 93.6 |
| etcd | 8 | 0 | 8884 | 0.89 | 1.92 | 180 | 160.1 |
| etcd | 8 | 50 | 11017 | 0.68 | 1.71 | 170 | 152.1 |
| etcd | 8 | 100 | 27375 | 0.27 | 0.87 | 88 | 152.4 |
| service | 8 | 0 | 3362 | 2.10 | 8.57 | 764 | 277.7 |
| service | 8 | 50 | 4431 | 1.58 | 5.00 | 568 | 161.5 |
| service | 8 | 100 | 12863 | 0.63 | 1.29 | 195 | 93.4 |

Write throughput increased 94.1%, and read throughput increased 44.8%.
Write CPU per operation fell 46.5%, and read CPU per operation fell 37.4%.
Write p99 latency fell 43.4%, and read p99 latency fell 29.9%.
Write RSS fell about 8%. Read RSS changed by less than 1%, within normal measurement variation.

Raw data: [matched samples](linux-arm64-before-after.json) and
[repeat ranges](linux-arm64-before-after-summary.json).

## Longer write samples

Six additional samples measure 20,000 writes each, with three repeats at concurrency 8.
All requests pass. The longer samples retain more history between checkpoints.

| Backend | C | Read % | ops/s | p50 ms | p99 ms | CPU us/op | RSS MiB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| etcd | 8 | 0 | 9103 | 0.87 | 1.89 | 177 | 171.8 |
| service | 8 | 0 | 2542 | 2.64 | 15.43 | 1048 | 644.4 |

The service's throughput falls and RSS rises as retained history grows.
This confirms that full snapshot work remains a material limit after the CPU changes.
This short test is not a memory-stability or soak-test claim.

Raw data: [write samples](linux-arm64-long-write.json) and
[repeat ranges](linux-arm64-long-write-summary.json).

## CPU findings

CPU profiles use `cpu-clock` at 99 Hz and 16 KiB DWARF stacks.
Sampling starts after warmup and stops when measured client operations finish.
Profiles cover the three server processes. They exclude client and controller stacks.
Inclusive symbol percentages overlap. They cannot be added into one CPU budget.
Profiled throughput and latency are excluded from the performance tables.

The [symbol summary](linux-cpu-profiles.json) records sample counts and report hashes.
The table shows inclusive percentages from matched service profiles at concurrency 8.
Read profiles use 100,000 requests. Write profiles use 10,000 requests.
A larger CPU share need not mean more CPU per operation after total work falls.

| Stack | Baseline read | Final read | Baseline write | Final write |
| --- | ---: | ---: | ---: | ---: |
| Peer transport | 17.03% | 18.71% | 11.61% | 11.00% |
| Log executor | 8.84% | Not reported | 14.67% | 6.57% |
| Disk statistics | 7.90% | Not reported | 7.76% | Not reported |
| Apply entries | Not reported | Not reported | 9.26% | 11.00% |
| State batch write | Not reported | Not reported | 6.77% | 4.30% |
| Snapshot build | Not reported | Not reported | 7.69% | 7.96% |

“Not reported” means absent from the 0.5% symbol report.
The etcd write profile has only 129 samples. Do not use it for precise symbol comparisons.
All six profiles contain user symbols and report zero lost samples.

The initial profile found repeated log directory scans in both read and write paths.
Every Ready used a log worker completion, even without entries or HardState changes.
Every completion scanned log filenames and metadata to refresh diagnostic statistics.

The final runtime batches reads before a quorum round. Later reads require a new round.
Empty Ready barriers finish on the owner after earlier writes complete.
Disk statistics require an explicit diagnostic request.
Forwarded KV results wait for local revision progress without another quorum request.

Write apply skips quota lookups for new history, Watch, and hash records.
It still reads prior values for records that can already exist.
The snapshot builder shares immutable row buffers and sends its pinned membership with the result.
The Raft owner no longer decodes the whole snapshot to obtain membership.
Queued Ready jobs share a durable write. The batch keeps the final HardState and log suffix.
Other log job types form batch boundaries. Replies remain behind durable completion.

## Failures found during development

A longer write profile exposed apply advancing beyond the committed entries delivered by Raft.
Later cache invalidation could remove entries that Raft still needed for Ready delivery.
Apply now waits for both Ready delivery and local durable commit.
A regression test limits Ready delivery while the log queue stays full.

A checkpoint larger than the 16 MiB entry target caused proposal rejections.
Snapshot bytes now use a separate 64 MiB admission target. Entry and job limits still apply.
One Ready can exceed an admission target. These targets do not bound total snapshot memory.

An exploratory baseline sample at concurrency 32 rejected 87 mixed requests with a backlog error.
A candidate sample also failed at startup with an address-in-use error.
The port allocator now holds all six reservations while it selects distinct addresses.
The failed exploratory matrix does not supply performance claims.

## Validation

The local Rust suite passed 54 tests outside the DST runner.
Clippy passed for all targets and features.
The [DST report](cpu-work-dst.json) contains 17 seeds, each replayed twice with identical traces.
Schedules include concurrent read batches, canceled clients, delayed apply, delayed log completion,
partitions, crashes, snapshots, and revision waiters.
The [Linux process report](linux-cpu-faults.json) passed six targeted checks.
They cover read quorum loss, slow and failed log writes, snapshot publication, and forwarded apply.

PR checks also run upstream compatibility, S3 recovery, downstream projects, native failpoints,
process histories with etcd and Jepsen models, and API fuzzing.
The Linux x86 performance job compares head and etcd, then profiles the PR base and head.
Its `linux-performance` artifact retains raw samples, summaries, and CPU stack reports.
Do not combine Linux ARM64 and x86 measurements into one ranking.

## Remaining work

The service still trails etcd. Peer HTTP exchanges, thread wakeups, and durable log work remain.
Read replies still convert through JSON values before gRPC encoding.
Full snapshots scan retained history and rewrite it into the local recovery log.
Longer write runs therefore have different costs from these bounded samples.

The next storage change should separate cheap local checkpoints from full snapshot transfer.
It must preserve recovery, membership, and object-store failure guarantees.
Peer transport batching and typed internal responses need separate profiles and fault tests.
Large values, large state, Watch fan-out, lease churn, cold S3 reads, and independent hosts remain
outside this matrix.

See the [benchmark guide](../../docs/benchmarks.md) for measurement and sampling commands.
