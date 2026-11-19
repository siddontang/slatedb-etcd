# Remote storage milestone evidence

These reports record successful local tests of remote storage and recovery.
Each report records binary hashes, trace hashes, or a precise source pin where applicable.
The S3 and three-guest reports record RustFS 1.0.6 runs from November 19, 2026.
The current CI workflow reruns portable checks on every pull request.

| Report | Scope |
| --- | --- |
| [S3](s3.json) | Five RustFS fault and recovery cases |
| [Crash boundaries](crash-boundaries.json) | 19 native crash hooks |
| [Upstream](upstream.json) | 395 leaf cases, including two TLS failover cases |
| [Downstream](downstream.json) | 200 leaf cases across Kubernetes, CoreDNS, and reference etcd |
| [DST](dst.json) | Five seeds, with two identical traces required per seed |
| [Three guests](multihost.json) | TLS, VM crash, partition, and S3 recovery |
| [Benchmark](benchmark.json) | Twelve gRPC samples against two backends |

The guest report confirms removal of all three temporary VMs. Each guest had its own kernel,
network stack, and data disk. All guests and RustFS shared one physical Apple Silicon host.
The harness owns each SSH tunnel and checks both forwarded ports before use.
Recovery checks every acknowledged value at its recorded revision on all three members.
No independent power-domain, disk-controller, availability-zone, or managed AWS S3 test is claimed.

The benchmark used 100 keys, 256-byte values, and 1,000 measured operations per sample.
Concurrency was one or eight. Read percentages were zero, 50, or 100.
The report includes preload in CPU, RSS, and disk samples, but excludes it from request latency.
Other validation work ran on the same host. Treat these numbers as a preliminary smoke baseline.
Use the [benchmark guide](../../docs/benchmarks.md) for repeatable and larger measurements.

The [fault map](../../tests/upstream/fault-mapping.md) lists equivalent boundaries and remaining
gaps.
Passing these cases does not establish complete etcd compatibility or a supported Kubernetes
backend.
