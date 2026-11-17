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
  --concurrency 1 8 32 --read-percent 0 50 100 --value-size 256 \
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
Latency and throughput exclude preload. Resource samples include preload and measured operations.
Latency percentiles include successful operations only. Any failed request makes the report fail.
A short CI run tests the driver; it does not enforce a performance threshold.

Use `--s3-bucket`, `--s3-endpoint`, and optional `--s3-allow-http` to measure remote state storage.
Create the bucket first and supply credentials through the supported AWS environment settings.
The etcd reference still uses local disks. The service keeps its local Raft logs in this profile.
Object request counts, remote bytes, and provider charges require separate S3 telemetry.

The initial 1,000-operation baseline uses 100 keys and 256-byte values.
It is a smoke-scale measurement on one Apple Silicon host, not a capacity or cross-AZ result.
The service currently scans its logical etcd state during quota checks.
This can make write CPU and tail latency grow with retained history.
An incremental quota counter needs atomic apply, compaction, and snapshot recovery tests before
it can replace that scan.

Further benchmarks must add larger state, warm and cold reads, Watch fan-out, lease churn,
compaction, snapshot catch-up, S3 request costs, and independent-host network latency.
Record recovery latency and correctness alongside throughput during each fault campaign.
