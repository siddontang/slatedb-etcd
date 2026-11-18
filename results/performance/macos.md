# Native macOS Raft and KV performance

This report compares one member and three local processes with etcd v3.6.5.
The full matrix uses optimization runtime `0410b9a7c559bddb319106b9eb2e640493b07dcb`.
Later dependency fixes use `70e3e1555e166cc957dcfc4cb4ee14b8493112dc`.
The follow-up section compares that runtime with the matrix runtime and etcd.
The baseline is `64a17a3`, which adds matching macOS durability to the previous runtime.
Each raw report records the server binary hashes.

The formal Mac runs total 171 samples and 21,210,000 successful requests without request errors.
They comprise 126 matrix samples, 36 dependency checks, and nine worker-count checks.
Exploratory probes and profiled runs remain separate.

## Method

The host has an Apple M4, 10 logical CPUs, and 16 GiB RAM.
The native ARM64 runs use Darwin 27.2.0, Rust 1.91.1, and Go 1.25.6.
Rust builds use release optimization and line symbols. No VM or container runs these samples.
The service uses its default two async workers. Etcd retains its default Go runtime settings.
CPU differences therefore include runtime scheduling choices, not only Raft and storage changes.

Each sample starts a fresh cluster and uses the official etcd Go gRPC client.
The workload uses 1,000 keys, 256-byte values, and 200 unmeasured warmup operations.
Write and mixed samples measure 5,000 requests. Read samples measure 300,000 requests.
Both servers receive one idle second after warmup and before the measured phase.
All reads are linearizable. Both use a 2 GiB quota and a 1,000-entry snapshot setting.
The service uses local object storage. Both retain durable Raft log writes.

Both servers use `F_FULLFSYNC` on macOS for required log synchronization.
The earlier service used plain `fsync`, which did not match etcd's drive-cache flush behavior.
This baseline includes the durability fix. Earlier write rates do not form a fair comparison.
The new wrapper fails if the full flush fails; it does not fall back to weaker synchronization.

Each cluster size has three repeats at concurrency 1, 8, and 32.
Each repeat shuffles the backends and workloads. Baseline cases run at concurrency 8.
The controller measures startup, preload, and warmup separately from client operations.
CPU means summed server CPU time per successful request. RSS sums server resident memory.
Client resources remain separate. Summary files retain minimum and maximum values across repeats.
Tables show medians; latency percentiles are not pooled across repeats.

Builds, tests, and profilers run outside the final measurement phases.
All processes share one disk and physical host. Desktop services and thermal changes remain.
The page cache is not cleared. RSS is not unique physical memory.
Equal quota and snapshot settings do not imply equal retention or storage work.
These closed-loop tests do not establish sustained capacity or independent-host behavior.

## Reproduce the workload

Build `64a17a3` and `0410b9a` with the same release settings in separate checkouts.
Set `CARGO_PROFILE_RELEASE_DEBUG=1` for both release builds.
Keep their binaries as `tmp/baseline` and `tmp/service`. Run the current controller separately.

```sh
for members in 1 3; do
  python3 scripts/benchmark-etcd.py --members "$members" \
    --binary tmp/service --baseline-binary tmp/baseline --baseline-concurrency 8 \
    --requests 5000 --read-requests 300000 --keys 1000 --value-size 256 \
    --concurrency 1 8 32 --read-percent 0 50 100 --warmup 200 --repeat 3 \
    --output "results/macos-$members.json"
done
```

## Full matrix before dependency fixes

### One member

All 63 samples passed without request or resource errors.

| Backend | Members | C | Read % | ops/s | p50 ms | p99 ms | CPU us/op | RSS MiB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| baseline | 1 | 8 | 0 | 521 | 15.09 | 26.43 | 556 | 130.8 |
| baseline | 1 | 8 | 50 | 643 | 13.67 | 19.65 | 432 | 57.7 |
| baseline | 1 | 8 | 100 | 53994 | 0.14 | 0.29 | 43 | 35.0 |
| etcd | 1 | 1 | 0 | 224 | 4.01 | 14.27 | 738 | 45.1 |
| etcd | 1 | 1 | 50 | 421 | 2.83 | 13.02 | 552 | 42.4 |
| etcd | 1 | 1 | 100 | 18989 | 0.05 | 0.12 | 70 | 43.5 |
| etcd | 1 | 8 | 0 | 900 | 7.99 | 22.41 | 308 | 46.9 |
| etcd | 1 | 8 | 50 | 1020 | 7.95 | 21.99 | 286 | 42.5 |
| etcd | 1 | 8 | 100 | 61821 | 0.12 | 0.33 | 51 | 44.2 |
| etcd | 1 | 32 | 0 | 3424 | 8.04 | 25.19 | 156 | 46.8 |
| etcd | 1 | 32 | 50 | 3623 | 8.03 | 25.95 | 162 | 43.5 |
| etcd | 1 | 32 | 100 | 108167 | 0.27 | 0.72 | 45 | 45.6 |
| service | 1 | 1 | 0 | 236 | 4.07 | 5.95 | 856 | 130.2 |
| service | 1 | 1 | 50 | 447 | 1.46 | 5.46 | 504 | 57.4 |
| service | 1 | 1 | 100 | 20771 | 0.05 | 0.07 | 34 | 35.0 |
| service | 1 | 8 | 0 | 965 | 8.02 | 18.89 | 394 | 130.0 |
| service | 1 | 8 | 50 | 1009 | 8.09 | 16.48 | 322 | 57.5 |
| service | 1 | 8 | 100 | 68050 | 0.11 | 0.25 | 23 | 35.0 |
| service | 1 | 32 | 0 | 3573 | 7.87 | 28.10 | 146 | 108.4 |
| service | 1 | 32 | 50 | 3108 | 10.30 | 22.17 | 146 | 58.9 |
| service | 1 | 32 | 100 | 90636 | 0.35 | 0.65 | 18 | 35.6 |

At concurrency 8, write throughput rose 85.1% against the matched baseline.
Read throughput rose 26.0%. Write and read CPU per request fell 29.1% and 46.6%.

Against etcd, write throughput is 4.3% to 7.2% higher across the three concurrency levels.
Read throughput is 9.4% and 10.1% higher at concurrency 1 and 8.
At concurrency 32, read throughput is 16.2% lower and mixed throughput is 14.2% lower.
Read CPU per request is 51.5% to 59.3% lower. This does not imply higher throughput in every case.
At concurrency 8, write CPU is 27.9% higher and write RSS is about 2.8 times etcd RSS.

Raw data: [samples](macos-single.json) and [repeat ranges](macos-single-summary.json).

### Three local processes

All 63 samples passed without request or resource errors.

| Backend | Members | C | Read % | ops/s | p50 ms | p99 ms | CPU us/op | RSS MiB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| baseline | 3 | 8 | 0 | 201 | 39.37 | 66.00 | 3820 | 394.4 |
| baseline | 3 | 8 | 50 | 207 | 37.83 | 81.13 | 3164 | 174.5 |
| baseline | 3 | 8 | 100 | 14780 | 0.57 | 1.00 | 274 | 106.6 |
| etcd | 3 | 1 | 0 | 187 | 4.98 | 15.77 | 1684 | 184.1 |
| etcd | 3 | 1 | 50 | 218 | 3.40 | 20.25 | 1432 | 169.7 |
| etcd | 3 | 1 | 100 | 8025 | 0.13 | 0.19 | 276 | 176.2 |
| etcd | 3 | 8 | 0 | 180 | 46.02 | 76.41 | 1574 | 184.1 |
| etcd | 3 | 8 | 50 | 236 | 30.98 | 95.54 | 1382 | 171.6 |
| etcd | 3 | 8 | 100 | 24572 | 0.32 | 0.60 | 235 | 175.8 |
| etcd | 3 | 32 | 0 | 692 | 47.72 | 89.36 | 626 | 187.1 |
| etcd | 3 | 32 | 50 | 604 | 48.77 | 119.33 | 550 | 168.1 |
| etcd | 3 | 32 | 100 | 69510 | 0.44 | 1.01 | 88 | 178.4 |
| service | 3 | 1 | 0 | 111 | 8.79 | 15.60 | 4150 | 379.0 |
| service | 3 | 1 | 50 | 224 | 3.83 | 13.11 | 2336 | 172.8 |
| service | 3 | 1 | 100 | 8985 | 0.11 | 0.16 | 187 | 106.1 |
| service | 3 | 8 | 0 | 219 | 36.97 | 62.04 | 2742 | 392.9 |
| service | 3 | 8 | 50 | 302 | 26.91 | 62.45 | 1954 | 174.2 |
| service | 3 | 8 | 100 | 27790 | 0.29 | 0.52 | 141 | 106.2 |
| service | 3 | 32 | 0 | 780 | 39.57 | 77.88 | 1246 | 322.3 |
| service | 3 | 32 | 50 | 698 | 41.45 | 93.80 | 1118 | 175.9 |
| service | 3 | 32 | 100 | 63050 | 0.50 | 0.95 | 68 | 107.0 |

At concurrency 8, write throughput rose 8.9% against the matched baseline.
Read throughput rose 88.0%. Write CPU per request fell 28.2%.
Read CPU per request fell 48.6%.

Against etcd at concurrency 8, write throughput is 22.0% higher.
Read throughput is 13.1% higher.
At concurrency 1, write throughput remains 40.6% lower.
At concurrency 32, read throughput remains 9.3% lower.
At concurrency 8, write CPU per request is 74.2% higher.
At concurrency 8, write RSS is 2.1 times etcd RSS.

Disk synchronization varies across repeats, especially with three replicas on one disk.
Use the retained ranges when judging small gains. These medians do not establish universal parity.

Raw data: [samples](macos-three.json) and [repeat ranges](macos-three-summary.json).

## Follow-up after dependency fixes

Runtime `70e3e1555e166cc957dcfc4cb4ee14b8493112dc` adds the local Raft Engine fixes.
This follow-up checks both cluster sizes at concurrency 8, with pure writes and pure reads.
It uses the same workload sizes, three shuffled repeats, and release settings as the full matrix.
All 36 samples passed. Builds, correctness tests, and profiles ran outside these measurements.
These are regression checks. Small rate changes do not prove a benefit from either dependency fix.
Use the repeat ranges to judge variation.

Here, `service` means the corrected runtime. `baseline` means matrix runtime `0410b9a`.
This baseline differs from the older `64a17a3` baseline in the full matrix above.
Use these rows for the corrected runtime at concurrency 8.
Other full-matrix concurrency levels still describe the earlier optimization runtime.

### One member

| Backend | Members | C | Read % | ops/s | p50 ms | p99 ms | CPU us/op | RSS MiB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| baseline | 1 | 8 | 0 | 891 | 8.77 | 21.06 | 412 | 130.5 |
| baseline | 1 | 8 | 100 | 68505 | 0.11 | 0.25 | 23 | 35.2 |
| etcd | 1 | 8 | 0 | 873 | 8.04 | 25.03 | 312 | 46.1 |
| etcd | 1 | 8 | 100 | 62262 | 0.12 | 0.33 | 51 | 44.3 |
| service | 1 | 8 | 0 | 976 | 8.32 | 20.31 | 388 | 129.3 |
| service | 1 | 8 | 100 | 68299 | 0.11 | 0.25 | 23 | 34.9 |

Write throughput changed +9.6% against the matrix runtime.
Write throughput is 1.12 times etcd throughput.
Write CPU per request is 1.24 times etcd CPU per request.
Read throughput changed -0.3% against the matrix runtime.
Read throughput is 1.10 times etcd throughput.
Read CPU per request is 0.45 times etcd CPU per request.

Raw data: [samples](macos-single-engine-fix.json) and
[repeat ranges](macos-single-engine-fix-summary.json).

### Three local processes

| Backend | Members | C | Read % | ops/s | p50 ms | p99 ms | CPU us/op | RSS MiB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| baseline | 3 | 8 | 0 | 222 | 36.61 | 60.17 | 2736 | 391.8 |
| baseline | 3 | 8 | 100 | 27723 | 0.29 | 0.53 | 141 | 106.4 |
| etcd | 3 | 8 | 0 | 194 | 42.30 | 81.58 | 1556 | 186.5 |
| etcd | 3 | 8 | 100 | 24406 | 0.33 | 0.61 | 234 | 176.6 |
| service | 3 | 8 | 0 | 219 | 37.05 | 64.17 | 2560 | 394.5 |
| service | 3 | 8 | 100 | 27724 | 0.29 | 0.52 | 141 | 105.7 |

Write throughput changed -1.5% against the matrix runtime.
Write throughput is 1.13 times etcd throughput.
Write CPU per request is 1.65 times etcd CPU per request.
Read throughput changed +0.0% against the matrix runtime.
Read throughput is 1.14 times etcd throughput.
Read CPU per request is 0.60 times etcd CPU per request.

Raw data: [samples](macos-three-engine-fix.json) and
[repeat ranges](macos-three-engine-fix-summary.json).

Reproduce these checks with the corrected binary as `--binary` and `0410b9a` as
`--baseline-binary`. Select `--concurrency 8 --read-percent 0 100 --repeat 3` for each cluster size.
Keep the other full-matrix workload settings unchanged.

### Worker count follow-up

This separate three-process check uses the corrected binary for both service labels.
`service` selects one async worker per member. `baseline` keeps the default two workers.
Etcd retains its default Go runtime settings. All nine samples passed.
The workload has concurrency 32, 100% reads, 300,000 requests, and three shuffled repeats.

| Backend | Members | C | Read % | ops/s | p50 ms | p99 ms | CPU us/op | RSS MiB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| baseline | 3 | 32 | 100 | 63487 | 0.50 | 0.97 | 67 | 106.7 |
| etcd | 3 | 32 | 100 | 69360 | 0.44 | 1.00 | 89 | 177.6 |
| service | 3 | 32 | 100 | 65222 | 0.49 | 0.93 | 36 | 106.2 |

One worker changed throughput +2.7% against two workers.
It changed CPU per request -46.1%.
Its throughput is 0.94 times etcd throughput.

This is a workload-specific tradeoff. The default remains two workers.
The [exploratory probes](macos-worker-probes.json) cover one to four workers with fewer repeats.
They are excluded from the formal matrix and this repeated follow-up.

To repeat this worker check, supply the corrected binary to both binary options.
Use `--service-runtime-workers 1 --concurrency 32 --read-percent 100 --repeat 3`.
The binary's default supplies two workers for the `baseline` label.

Raw data: [samples](macos-three-worker-tuning.json) and
[repeat ranges](macos-three-worker-tuning-summary.json).

## Changes and etcd references

The [etcd WAL][wal] calls Raft's [MustSync][mustsync] rule.
Entries and term or vote changes require a sync; commit-only HardState changes do not.
The service now follows that rule and also synchronizes snapshots.
Follower acknowledgements remain behind durable completion.
Recovery can repair a lost commit marker from flushed state only when durable entries cover it.
Checkpoints persist the commit marker with the snapshot and log truncation.

The [etcd Raft loop][raftloop] overlaps leader replication with local log persistence.
Its [read loop][readloop] batches quorum confirmation, then waits for local apply.
The service retains both barriers. Later reads cannot reuse an earlier frozen quorum proof.
Local read tracking now uses integer identifiers and builds one wire context per proof batch.
The context retains a process nonce and rejects malformed or stale proofs.

The [etcd peer transport][streams] uses long-lived streams.
The service now sends ordinary Raft traffic on bounded bidirectional gRPC streams.
Each peer admits 64 unacknowledged messages and targets 16 MiB of encoded payload.
One frame can exceed the byte target. A frame must fit 8 MiB plus 1 KiB.
Snapshot transfer retains a separate durable acknowledgement and an ordering barrier.
Stalled or failed streams close, discard old queued traffic, and let Raft retry.

KV replies now keep their typed representation through the owner and apply worker.
Point reads avoid mutation planning. Read completions omit unchanged member, Auth, and Lease data.
The apply worker groups up to 16 adjacent point reads without a batching timer.
The CLI defaults to two async workers. Embedded programs retain their caller's runtime.
Worker-count probes found that more workers increased CPU cost on this host.
Queued writes permit a cooperative yield before draining events, which improves group commit.
Read-only events do not take that extra yield.

## Stack evidence

macOS `sample` records wall-clock thread stacks, including blocked threads.
Its counts are not CPU percentages. Profiled throughput is excluded from the result tables.
Four [sanitized profiles](macos-stack-samples.json) cover single-member reads and writes.
They use `64a17a3` and `0410b9a`, before the dependency fixes.
Each profile samples five seconds at a one-millisecond interval.

Baseline stacks exposed JSON conversion, allocation, and full-sync waits.
The optimized read stacks still contain network I/O, allocation, and thread coordination.
The unprofiled process CPU deltas quantify total work per request.
Stack observations identify candidate work; they do not prove a single bottleneck.

Linux CI also compares three-process workloads and captures `cpu-clock` profiles.
The profiles use 99 Hz sampling and 16 KiB DWARF stacks after warmup.
The extra inline-symbol view includes compiler-inlined functions when symbols are available.
Linux results remain separate because the CPU, scheduler, and storage differ.

### Linux cross-check

[CI run 44238866415][linux-run] measured runtime `49e0fa8` on a Linux x86 host.
It passed 54 unprofiled samples. The later dependency selection change keeps the same runtime.
These results do not replace the Mac samples or the earlier Linux ARM64 measurements.

| Backend | C | Read % | ops/s | p50 ms | p99 ms | CPU us/op | RSS MiB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| service | 8 | 0 | 1690 | 4.24 | 16.11 | 1930 | 281.7 |
| etcd | 8 | 0 | 3828 | 2.02 | 4.18 | 734 | 170.3 |
| service | 8 | 100 | 5450 | 1.46 | 2.82 | 492 | 103.9 |
| etcd | 8 | 100 | 6010 | 1.30 | 2.83 | 451 | 162.1 |

At concurrency 8, service write and read throughput remain about 56% and 9% lower than etcd.
Write and read CPU per request remain about 2.63 and 1.09 times etcd CPU per request.
Raw data: [samples](linux-raft.json) and [repeat ranges](linux-raft-summary.json).

The [CPU samples](linux-raft-cpu.json) retain the top self and inclusive costs without local paths.
The head read profile places 30.5% of inclusive samples under `__x64_sys_writev`.
The head write profile places 12.5% there. These costs overlap their parent stacks.
The profiles include both client-facing and peer traffic; they do not separate those traffic types.
Many application costs still lack narrow function attribution.
Measure stage timing and client/peer call counts before assigning the gap to ReadIndex or apply.

## Validation

The native Rust suite passed 69 tests outside the separate DST runner.
All-target, all-feature Clippy passed. The benchmark controller passed 11 tests.
The [native compatibility report](macos-compatibility.json) passed 201 test records and 184 leaves.
It has no skipped tests.
The [DST report](macos-dst.json) passed 100 seeds, each replayed twice with identical traces.
The [native crash report](macos-failpoints.json) passed all 19 injected crash boundaries.

The extended run found a fault-injection setup error in seed 58.
Reordered appends left one follower without the latest commit index.
Its clean flush did not issue a PUT, so the injected PUT error did not run.
The setup now waits for the new write to apply and verifies each fault was reached.
Seed 58 failed before this test change and passed twice after it.
The default CI seed set now includes 58 and 70.

Seed 70 exposed Raft Engine's cross-instance block-cache collision.
A focused regression also proved that the published crate can return another log's values.
The local dependency patch isolates cache identities and restores explicit sync.
A second regression counts actual file sync calls; the published crate issued none.
Both regressions failed before their fixes and passed after them.
See the [dependency notes](../../third_party/raft-engine/README.md) for provenance and patch scope.
The follow-up measurements use this corrected dependency.
An external Cargo consumer also resolves the corrected copy through the direct path dependency.

Peer socket tests cover message and byte bounds, malformed frames, timeouts, cancellation,
reconnection, and snapshot barriers.
Process tests cover TLS and failover through the real transport.
DST schedules Raft messages directly; it does not simulate HTTP/2 frames or kernel disk behavior.

PR checks also run the imported upstream suite, S3 recovery, downstream projects, process faults,
etcd and Jepsen model checks, and bounded API fuzzing.
The PR's Linux performance artifact retains separate measurements and CPU profiles.


## Limits and next work

Read execution still passes through one apply worker.
The next read experiment should use bounded concurrent reads with a pinned state view.
It must retain revision, Auth, compaction, snapshot-install, and ReadIndex barriers.

Follower writes still use a gRPC forward and a local revision wait.
Etcd proposes through Raft and waits for local apply.
Native follower proposals need a separate test.
They must preserve request identity, permissions, reply ordering, and uncertain-write handling.

Full snapshots still scan retained history and write a complete local recovery image.
The next write experiment should separate local checkpoints from full snapshot transfer.
Longer runs must test retained-history growth and snapshot catch-up before capacity claims.

Large values, large state, Watch fan-out, Lease churn, cold S3 reads, and independent hosts remain
outside this matrix. Process crashes do not simulate power loss or partial sector writes.
Mac write gains do not establish Linux parity. A universal claim of beating etcd is unsupported.

See the [etcd and TiKV review](../../docs/raft-performance-notes.md) for upstream code and
articles.
It separates implemented changes from experiments that need new correctness proofs.

See the [benchmark guide](../../docs/benchmarks.md) for commands and resource definitions.
See the [architecture](../../docs/architecture.md) for ownership and recovery barriers.

[wal]: https://github.com/etcd-io/etcd/blob/v3.6.5/server/storage/wal/wal.go
[mustsync]: https://github.com/etcd-io/raft/blob/v3.6.0/rawnode.go
[raftloop]: https://github.com/etcd-io/etcd/blob/v3.6.5/server/etcdserver/raft.go
[readloop]: https://github.com/etcd-io/etcd/blob/v3.6.5/server/etcdserver/v3_server.go
[streams]: https://github.com/etcd-io/etcd/blob/v3.6.5/server/etcdserver/api/rafthttp/stream.go
[linux-run]: https://github.com/siddontang/slatedb-etcd/actions/runs/44238866415
