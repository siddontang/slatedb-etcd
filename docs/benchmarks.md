# Local benchmark guide

Run these commands from the repository root.

See the [quorum Put report](../results/performance/quorum-put/README.md) for slow-leader latency,
Linux CPU cost, and native macOS comparisons.
See the [read and apply report](../results/performance/linux-batch-apply.md) for the earlier Linux
one-member and three-member comparison, grouped writes, and module changes.
See the [native macOS report](../results/performance/macos.md) for one-member and three-member
results.
The [Linux CPU report](../results/performance/linux-cpu.md) covers earlier ReadIndex and apply work.
The [earlier report](../results/performance/README.md) covers quota and state-cache changes.
The [etcd and TiKV review](raft-performance-notes.md) maps upstream techniques to this service.

## Measure write latency

These commands benchmark the diagnostic JSON API. They do not measure gRPC or MVCC overhead.

```sh
python3 scripts/harness.py bench \
  --requests 20000 --concurrency 1 16 64 --value-size 256 \
  --output results/latency.json

python3 scripts/harness.py bench \
  --requests 20000 --concurrency 1 16 --value-size 256 --snapshot-every 1000 \
  --output results/latency-with-snapshots.json
```

Each concurrency level sends the requested total number of single-key writes after a shared warmup.
Clients reuse HTTP connections. Latency includes client serialization, HTTP, quorum persistence,
leader apply, and the response. The report includes failures, throughput, and latency percentiles.
This is a closed-loop Python client. Client scheduling and the GIL can affect concurrent results.

The default harness disables automatic snapshots to measure the hot write path. The second command
includes periodic snapshots. Compare both results before drawing conclusions about tail latency.

All replicas and the client share one host and disk. SlateDB uses `LocalFileSystem` as its object
store. These measurements are not S3, cross-host, or cross-AZ results. SIGKILL tests cover process
crashes. They do not verify power-loss behavior, failed disks, or independent failure domains.

## Compare the real gRPC path with etcd

```sh
scripts/cargo.sh build --locked --release
python3 scripts/benchmark-etcd.py --requests 10000 --keys 1000 \
  --concurrency 1 8 32 --read-percent 0 50 100 --value-size 256 --repeat 3 \
  --output results/ci/benchmark-etcd.json
```

The driver builds etcd v3.6.5 from the pinned Go module and calls its original command entry point.
Each backend uses three separate server processes by default. Use `--members 1` for one member.
Run the one-member and three-member comparisons separately.
Each workload starts with a fresh cluster.
Both use the same official gRPC client, key count, value size, request count, and concurrency.
All reads are linearizable. Both retain normal Raft sync behavior.
The default value repeats one byte. Use `--value-pattern binary` for generated binary values.
Use `--value-pattern metadata` for generated JSON records of at least 256 bytes.
The client builds its value pool before measurement. Pure writes change values on each operation.
Mixed workloads keep values stable per key so the client can check each read.
Both use a 2 GiB quota setting and a 1,000-entry snapshot setting. Their accounting and retention
implementations differ. Equal settings do not mean equal storage overhead.

The report records successful throughput, error counts, p50/p95/p99/p999 latency, CPU seconds,
peak combined server RSS, allocated local data bytes, and server binary hashes.
Schema version 2 separates startup, preload, warmup, and measurement with client barriers.
Both backends receive one idle second after warmup, before the measured phase.
This reduces overlap with background work from preload. It does not stop background work.
Service state counters are captured before and after measurement, outside resource sampling.
`state_write_batches` resets at startup and counts all state writes, including non-Put operations.
Use counter deltas to check whether a workload actually shared storage writes.
CPU deltas and peak RSS cover the measured phase. RSS still includes retained preload memory.
The report separates client resources from the sum of all server processes.
CPU per successful operation measures work. Average CPU cores measure utilization.
The controller samples RSS every 100 ms and includes both measurement boundaries.
RSS is not unique physical memory. Short samples can amplify CPU rounding and barrier overhead.
Latency percentiles include successful operations only. Any failed request makes the report fail.
CI compares the PR head and etcd on one Linux x86 runner.
It includes matched PR base cases at concurrency 8 and 32 in the same shuffled matrix.
It also samples CPU stacks for the PR base and head.
It uses identical release settings with line symbols for both service builds.
It runs three repeats at concurrency 1, 8, and 32, with 0%, 50%, and 100% reads.
The `linux-performance` artifact includes samples, summaries, and CPU stack reports.
CI checks request success and report completeness. It does not enforce a performance threshold.
Shared CI hosts can vary in CPU scheduling, storage latency, and available memory.
Linux resource sampling reads process CPU ticks and resident pages from `/proc`.
Each report includes CPU time and peak RSS for each member.
Linux also records the change in each server's storage read and write bytes from `/proc/PID/io`.
Page cache hits do not add storage read bytes. Write bytes include dirty pages charged to a process.
These counters do not measure physical device traffic or remote object bytes.

CI also compares all three builds at 1,000 scheduled puts per second with 32 client workers.
Use `--rate` to set a target for the measured phase. Zero selects the unpaced workload.
Paced latency starts at the scheduled arrival and includes time waiting for a client worker.
Compare achieved throughput before comparing resource use at a fixed rate.
Use `--compact-every 5000 --retain-revisions 1000` to test physical compaction during writes.
Every 5,000th successful measured Put asks the cluster to compact. The last 1,000 revisions stay.
The caller waits for Compact. Other workers continue to issue requests.
The report includes completed compactions and their durations. Put latency includes these waits.
macOS sampling reads `ps` counters. Both exclude child processes and the controller.

Use `--rustfs` for a temporary local RustFS server. The report records its version and binary hash.
The client uses HTTP over loopback. CPU and RSS cover the etcd-compatible servers.
They exclude the RustFS process. The fixture removes its data after the run.
Linux CI compares the base and head at 300 puts/s. It also checks the head at 1,000 puts/s.

```sh
python3 scripts/benchmark-etcd.py --rustfs --requests 6000 --keys 1000 --repeat 3 \
  --value-pattern metadata --concurrency 32 --read-percent 0 --warmup 200 --rate 300 \
  --compact-every 1500 --retain-revisions 500 --output results/ci/benchmark-rustfs.json
```

Use `--s3-bucket`, `--s3-endpoint`, and optional `--s3-allow-http` to measure remote state storage.
Create the bucket first and supply credentials through the supported AWS environment settings.
The etcd reference still uses local disks. The service keeps its local Raft logs in this profile.
Object request counts, remote bytes, and provider charges require separate S3 telemetry.

The initial 1,000-operation baseline uses 100 keys and 256-byte values.
It is a smoke-scale measurement on one Apple Silicon host, not a capacity or cross-AZ result.
The service uses a separate 64 MiB weighted SST cache per member.
Current quota checks use a derived byte counter. Each atomic batch updates only its changed keys.
Startup rebuilds this counter from stored state. No storage format change is required.
The previous implementation scanned all logical etcd state before each quota-limited write.

Use `--baseline-binary` to compare a saved release binary with the current service and etcd.
Use `--baseline-concurrency 8` to limit baseline cost while retaining all service and etcd cases.
The selected baseline cases still share the shuffled measurement order.
Use `--read-requests` for longer read-only samples, and `--warmup` to set unmeasured operations.
The driver shuffles backend and workload order for each repeat.
Use `--service-runtime-workers` for explicit service worker comparisons.
An omitted value uses the binary's default. The override does not change the baseline or etcd.
Each repeat shares one operation seed.
Keys and read choices use a seeded uniform distribution. Reads verify the expected value.
Summarize complete runs with medians and ranges across repeats:

```sh
python3 scripts/summarize-benchmark.py results/ci/benchmark-etcd.json \
  --output results/ci/benchmark-summary.json
```

The summary keeps each repeat's percentiles separate. It does not pool or average latency samples.
An incomplete run or failed resource sample cannot produce a passing report.
Keep build, test, and profiler workloads separate from final measurements.

On macOS, both services now use `F_FULLFSYNC` for required durable log writes.
The service wraps the pinned Raft engine's filesystem to flush the drive cache.
Earlier macOS reports used the engine's plain `fsync`. Their write results had weaker guarantees.
Do not compare those earlier write rates with the current durability profile.
See the [etcd sync implementation][etcd-sync] and [Raft engine implementation][raft-sync].

[etcd-sync]: https://github.com/etcd-io/etcd/blob/v3.6.5/client/pkg/fileutil/sync_darwin.go
[raft-sync]: https://github.com/tikv/raft-engine/blob/084c5ca/src/env/log_fd/unix.rs

Further benchmarks must add larger state, warm and cold reads, Watch fan-out, lease churn,
compaction, snapshot catch-up, S3 request costs, and independent-host network latency.
Record recovery latency and correctness alongside throughput during each fault campaign.

## Sample Linux CPU stacks

Install a Linux `perf` binary and allow CPU sampling in an isolated test environment.
Build with line symbols. The code still uses release optimization.

```sh
CARGO_PROFILE_RELEASE_DEBUG=1 cargo build --locked --release
python3 scripts/profile-linux.py --binary target/release/slatedb-etcd \
  --read-percent 100 --requests 100000 --output results/ci/profile-read
python3 scripts/profile-linux.py --binary target/release/slatedb-etcd \
  --read-percent 0 --requests 10000 --output results/ci/profile-write
```

The controller attaches to all three server processes after preload and warmup.
A perf control acknowledgment starts sampling before the client enters its measured phase.
The controller stops sampling after the client finishes. It excludes client CPU stacks.
The default uses `cpu-clock` at 99 Hz with 16 KiB DWARF call stacks.
Reports retain self and inclusive symbol costs. Inclusive percentages overlap; do not add them.
The extra `inline.txt` view includes compiler-inlined functions when symbols are available.
Empty profiles, unresolved user symbols, lost samples, and failed requests fail validation.

Use `--backend etcd --binary tmp/benchmark-tools/etcd` to profile the reference process.
Keep raw `perf.data` local. Share sanitized symbol summaries and measurement reports.
Use `--skip-inline` if inline symbol expansion stalls.
Flat symbols and call stacks remain available.
Profiled latency and throughput include sampling overhead. Use separate runs for comparisons.
Full snapshots and retained MVCC history increase cost during longer write runs.
Check failures before comparing successful throughput or latency.

## Measure write costs per client Put

The Linux performance job saves worker counters and native CPU samples in `linux-write-cost`.
It uses three members, 32 clients, 1,000 keys, and 12,000 writes of 256-byte metadata values.

```sh
python3 scripts/summarize-write-profile.py --per-put results/ci/write-cost.json
```

The report subtracts warmup counters and sums all three members for each completed client Put.
It shows the median of three runs. Requested bytes include reallocations.
The diagnostic build reads the worker thread CPU clock around each active poll or WAL job.
Waiting for a device or a task does not consume thread CPU.
The three stages exclude RPC handling, checkpoint tasks, and background storage work.
Use the separate native runs for throughput, p99 latency, process CPU, and peak RSS.
Use `--client-target leader` or `--client-target follower` to measure one endpoint.
The client checks the role and Raft term again after the run. All three servers still replicate.
The default client uses all endpoints.

In the first instrumented run, apply used 50 µs and 189 allocations per completed Put.
Raft used 30 µs and 16 allocations. The WAL used 23 µs and 2.7 allocations.
The three server processes used 395 µs per Put. Most CPU time fell outside the three workers.
Native stacks included memory allocation, socket writes, and thread scheduling.
The [write cost comparison][write-cost-pr] records the paired capacity measurements.

[write-cost-pr]: https://github.com/siddontang/slatedb-etcd/pull/18
