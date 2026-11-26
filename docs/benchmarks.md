# Local benchmark guide

[Run all official etcd benchmarks](../tests/upstream/run-suites.md#official-benchmark-cli).

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
The `linux-performance-<group>` artifacts include samples, summaries, and CPU stack reports.
CI runs the `writes` and `throughput` groups on separate runners in parallel.
Each group compares head and base on one runner, so a comparison never spans two hosts.
A pull request reuses the base binaries that the main run of its base commit saved.
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
The service uses separate 64 MiB weighted caches for SST blocks and SST metadata per member.
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

## Measure scale against etcd

These runs compare the service with etcd v3.6.5 at about 1 GiB of state. They measure load rate,
restart time, memory, LIST cost, Watch delivery, and data directory size.
The scale driver and its case files are not in this repository yet.

### Method

The driver is a Go program on the official clientv3 client. It sends the same phases to both
backends. Each backend and repeat starts a fresh cluster with a 16 GiB quota. `scale-1g` and
`scale-k8s` ran twice per backend, and `scale-1g-settle` ran once.

- `scale-1g`: load 1,000,000 keys of 1 KiB in 1,000 prefixes with 256 writers. Then 60 s of
  overwrites at 2,000/s, and the same with two paginated LIST loops (limit 500).
  Then one paginated LIST at a revision 1,000 behind the head, capped at 300 s.
- `scale-k8s`: load 150,000 pod objects, 90% of 4-8 KiB and 10% of 8-100 KiB (about 1.2 GiB).
  Then 60 s of 500 Events/s (1 KiB, 1-hour leases) and 50 pod updates/s, with 100 watchers on
  the pod prefix over 10 streams. Then the same with four paginated LIST loops. The first page
  reads the head revision and later pages read that revision, as the apiserver does.
  Then one unpaginated LIST of all pods.
- Both cases then restart the member with SIGTERM and time the first accepted write.
  Then they add a learner, wait until it reaches the leader revision, and promote it.
- `scale-1g-settle`: the `scale-1g` load and write burst, then 420 s of 20 writes/s.
  It measures the data directory after a longer period of light writes.

The main phases use one member. The learner phase adds a second member on the same host.
The driver measures Watch delivery from a timestamp in each value.
A 0.2 s sampler records peak RSS and CPU of the server processes for each phase.
Restart time is the time until the first write succeeds, with 0.5 s resolution.

The host has an Intel Xeon E3-1241 v3 (4 cores, 8 threads), 32 GiB of memory, one SATA SSD
(Samsung 860), and Ubuntu 24.04 with Linux 6.8. The server uses 4 logical CPUs (2 cores). The
driver uses the other 4. No other benchmark runs on the host. An idle test service also runs.
Both backends keep their default sync settings. No unsafe or no-sync option is set.
The service runs commit 7f0e74b as a release build with SlateDB on the local disk.

### Results

Each cell shows the lowest and highest value of the repeats. Times are p99 unless named.
RSS is the peak of the phase. A "page" is one LIST request of 500 keys.

`scale-1g` (1,000,000 keys of 1 KiB):

| Metric | etcd | This service |
|---|---:|---:|
| Load rate, keys/s | 9,690-10,420 | 13,601-13,824 |
| Load peak RSS, MiB | 542-555 | 2,161-2,192 |
| Data directory after load, MiB | 1,804-1,865 | 3,791-3,876 |
| Writes at 2,000/s, write p99, ms | 45-118 | 110 |
| Writes with two LIST loops, achieved writes/s | 261-265 | 1,980-1,995 |
| Writes with two LIST loops, write p99, ms | 502-518 | 94-99 |
| Writes with two LIST loops, LIST page p99, ms | 513-521 | 783-842 |
| Writes with two LIST loops, RSS, MiB | 913-925 | 1,976-2,043 |
| LIST of all keys at an older revision, s | 243-244 | 75-81 |
| LIST at an older revision, page p99 ms / RSS MiB | 239-240 / 1,950-1,965 | 138-141 / 1,731-1,825 |
| Restart until the first write, s (±0.5) | 5.0 | 3.5 |
| RSS 5 s after restart, MiB | 1,955-1,973 | 278-318 |
| Learner catch-up, s / learner RSS, MiB | 20 / 2,019-2,029 | 30-33 / 291-343 |
| Peak RSS of the case, MiB | 1,950-1,965 | 2,161-2,192 |
| Data directory at the end, MiB | 1,860 | 9,317-9,412 |

`scale-k8s` (150,000 pod objects, about 1.2 GiB):

| Metric | etcd | This service |
|---|---:|---:|
| Load rate, objects/s | 2,948-3,046 | 2,551-2,655 |
| Load peak RSS, MiB | 249-267 | 1,614-1,680 |
| Data directory after load, MiB | 2,375-2,439 | 4,264-4,267 |
| Steady, write p99, ms | 94-98 | 139-210 |
| Steady, Watch delivery p99, ms | 94-99 | 139-242 |
| Steady, RSS, MiB | 245-251 | 1,485-1,495 |
| Steady with four LIST loops, write p99, ms | 170-195 | 119-147 |
| Steady with four LIST loops, Watch delivery p99, ms | 168-329 | 174-214 |
| Steady with four LIST loops, one full paginated LIST, s | 23 | 21-23 |
| Steady with four LIST loops, LIST page p99, ms | 197-198 | 565-626 |
| Steady with four LIST loops, RSS, MiB | 1,822-1,852 | 1,504-1,509 |
| One unpaginated LIST of 1.3 GB, s | 4.2-4.3 | 9.8-45.6 |
| One unpaginated LIST, RSS, MiB | 4,436-4,444 | 3,764-4,756 |
| Restart until the first write, s (±0.5) | 1.7-2.5 | 1.1-3.0 |
| RSS 5 s after restart, MiB | 1,787-1,802 | 95-376 |
| Learner catch-up, s / learner RSS, MiB | 15-16 / 1,854-1,882 | 30-69 / 334-562 |
| Peak RSS of the case, MiB | 4,436-4,444 | 3,764-4,756 |
| Data directory at the end, MiB | 1,967-1,968 | 9,042-9,664 |

`scale-1g-settle` (one repeat; 60 s of writes at 2,000/s, then 420 s at 20/s):

| Metric | etcd | This service |
|---|---:|---:|
| Load rate, keys/s | 9,669 | 11,721 |
| Data directory after load, MiB | 1,865 | 3,690 |
| Light writes, write p99, ms | 15 | 43 |
| Light writes, server CPU, s | 11.5 | 91.5 |
| Data directory at the end, MiB | 1,847 | 8,636 |

The main points:

- Writes under LIST load: in `scale-1g`, etcd writes fall to about 260/s with two LIST loops.
  The service keeps 2,000 writes/s. Its LIST pages are slower.
- LIST at an older revision is 3 times faster on the service. An unpaginated LIST of all pods
  is 2-11 times slower.
- Steady Watch delivery p99 is higher on the service: 139-242 ms against 94-99 ms.
- etcd maps its whole bbolt file, so its RSS after a restart is near the data size. The service
  keeps 1.5-2.2 GiB during heavy writes, and about 0.1-0.4 GiB after a restart.
- A learner catches up 1.5-4.5 times more slowly on the service. It uses less memory.
- The service data directory at the end of a case is 4.6-5.1 times that of etcd. It is still
  4.7 times after 420 s of light writes. These runs do not show the size after replaced SSTs
  can leave the disk. See [disk use](operations.md#disk-use-and-quota).

### Limits

- One host, one disk, and one member for the main phases. These are not three-member, cross-host,
  or S3 results. The learner shares the disk with the leader.
- Two repeats per case, or one. Some p99 values change by 2 times between repeats. Use the
  ranges, not single values, and repeat a run before you rely on a small difference.
- RSS is resident memory of all server processes, not unique physical memory.
- Each case ends less than 15 minutes after its load starts. SlateDB keeps replaced SSTs for
  15-16 minutes, so the data directory sizes include them. This is also true for
  `scale-1g-settle`, which ends about 9.5 minutes after its load starts.
- etcd's data directory includes free bbolt pages. Neither size is the logical data size.
- A real apiserver keeps a watch cache and sends fewer LISTs to etcd than this driver does.

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

[write-cost-pr]: https://github.com/slatedb/slatedb-etcd/pull/18
