# Local benchmark guide

Run these commands from the repository root.

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
Each backend uses three separate server processes. Each workload starts with a fresh cluster.
Both use the same official gRPC client, key count, value size, request count, and concurrency.
All reads are linearizable. Both retain normal Raft sync behavior.
Both use a 2 GiB quota setting and a 1,000-entry snapshot setting. Their accounting and retention
implementations differ. Equal settings do not mean equal storage overhead.

The report records successful throughput, error counts, p50/p95/p99/p999 latency, CPU seconds,
peak combined server RSS, allocated local data bytes, and server binary hashes.
Schema version 2 separates startup, preload, warmup, and measurement with client barriers.
CPU deltas and peak RSS cover the measured phase. RSS still includes retained preload memory.
The report separates client resources from the sum of the three server processes.
CPU per successful operation measures work. Average CPU cores measure utilization.
The controller samples RSS every 100 ms and includes both measurement boundaries.
RSS is not unique physical memory. Short samples can amplify CPU rounding and barrier overhead.
Latency percentiles include successful operations only. Any failed request makes the report fail.
CI runs three repeats of each workload on Linux with the normal release binary.
The `validation-reports` artifact includes `benchmark-linux.json`.
CI checks request success and report completeness. It does not enforce a performance threshold.
Shared CI hosts can vary in CPU scheduling, storage latency, and available memory.
Linux resource sampling reads process CPU ticks and resident pages from `/proc`.
macOS sampling reads `ps` counters. Both exclude child processes and the controller.

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
Use `--read-requests` for longer read-only samples, and `--warmup` to set unmeasured operations.
The driver shuffles backend and workload order for each repeat.
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

On macOS, etcd uses `F_FULLFSYNC`; the pinned Raft engine uses `fsync`.
These calls differ in device cache guarantees.
Keep both defaults and state this limit in comparisons.
Do not treat macOS write throughput as a comparison with equal power-loss durability.
See the [etcd sync implementation][etcd-sync] and [Raft engine implementation][raft-sync].

[etcd-sync]: https://github.com/etcd-io/etcd/blob/v3.6.5/client/pkg/fileutil/sync_darwin.go
[raft-sync]: https://github.com/tikv/raft-engine/blob/084c5ca/src/env/log_fd/unix.rs

Further benchmarks must add larger state, warm and cold reads, Watch fan-out, lease churn,
compaction, snapshot catch-up, S3 request costs, and independent-host network latency.
Record recovery latency and correctness alongside throughput during each fault campaign.
