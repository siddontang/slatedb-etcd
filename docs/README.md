# Documentation

Start with the quick start. Read compatibility and security limits before running a real workload.
Commands use the repository root unless a guide states otherwise.

| Task | Guide |
| --- | --- |
| Build and run a local service | [Quick start](getting-started.md) |
| Embed a member in a Rust program | [Embedding](embedding.md) |
| Select flags and memory limits | [Configuration](configuration.md) |
| Enable Auth, TLS, and member changes | [Administration](administration.md) |
| Check health, disk use, and recovery | [Operations](operations.md) |
| Understand storage and the roadmap | [Architecture](architecture.md) |
| Inspect current implementation details | [Implementation notes](internals.md) |
| Find module owners and change rules | [Module boundaries](modules.md) |
| Use local diagnostics and injected faults | [Diagnostic APIs](diagnostics.md) |
| Compare gRPC and local write latency | [Benchmarks](benchmarks.md) |
| Compare Raft optimization techniques | [etcd and TiKV notes](raft-performance-notes.md) |
| Configure S3 and remote hosts | [Remote storage](remote-storage.md) |
| Understand Raft snapshot transfer | [Object storage snapshots](object-snapshots.md) |
| Review the shared-state design proposal | [Shared state](shared-state.md) |
| Assess etcd compatibility | [Compatibility report](../COMPATIBILITY.md) |
| Monitor a member with Prometheus | [Metrics and compatibility](metrics.md) |
| Run tests, Jepsen, fuzzing, and DST | [Test guide](../tests/README.md) |
| Check the upstream test coverage map | [Upstream audit](../tests/upstream/README.md) |
| Contribute a change | [Contributing](../CONTRIBUTING.md) |
| Report a security issue | [Security policy](../SECURITY.md) |
| Review the public release audit | [Publication review](publication.md) |

The architecture separates current behavior from target behavior.
Historical benchmark results describe older binaries. Each retained report records its scope.
