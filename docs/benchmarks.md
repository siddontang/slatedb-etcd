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
