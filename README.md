# SlateDB etcd service

> [!WARNING]
> **Early development.** This project changes fast. It gives no compatibility guarantee: APIs,
> flags, configuration, on-disk and S3 formats, and backup formats can change in any release
> without a migration path. Do not use it for data that you cannot rebuild.

An experimental etcd v3 compatible service built with Raft and SlateDB.
Each replica uses `raft-rs` for consensus, `raft-engine` for durable logs, and SlateDB for state.
SlateDB's native WAL is disabled. Raft protects acknowledged writes.

The service has one Raft group, a configurable voter set, and one optional learner.
It exposes etcd v3 gRPC APIs to the official etcd client.
Each replica owns an independent local or S3 object namespace and manifest.

**Status:** experimental research software. It is not a supported Kubernetes backend or a complete
etcd replacement. S3 storage and remote mutual TLS are available for controlled validation.
See [remote storage](docs/remote-storage.md) for ownership, recovery, and deployment limits.

## Why evaluate SlateDB etcd?

Operators can move key-value data to S3 and recover write capacity without defragmenting a member.

| Operator task | etcd | SlateDB etcd |
| --- | --- | --- |
| Size storage | Keep a bbolt database on each member's disk | Keep state on local disk or in S3 |
| Free quota after NOSPACE | Compact history and defragment the database | Delete and compact |
| Recover a follower | Stream a snapshot from the leader | Copy bulk data within S3 off the leader |
| Embed a member | Use the Go embed package | Use the Rust API on your Tokio runtime |

The [Rust embedding API](docs/embedding.md) lets an application manage its own member while it
serves existing etcd v3 clients.

With S3, operators can size the data store separately from each member's local Raft log.
A normal write commits to a quorum of durable logs without waiting for an S3 upload.
During follower recovery, a server-side copy keeps bulk data off the leader's network.
It requires a common bucket, endpoint, region, and read access across member prefixes.
The HTTP fallback transfers the files through the leader.
See [remote storage](docs/remote-storage.md) and
[snapshot transfer](docs/object-snapshots.md#transfer).

After a NOSPACE alarm, etcd counts unused pages in the database file toward its quota.
Defragmentation shrinks that file and blocks reads and writes on the member while it runs.
SlateDB etcd counts logical bytes. Delete unneeded keys and compact old revisions until usage falls
below quota, then disarm the alarm to resume writes. Physical space reclamation can happen later.
See [etcd maintenance][etcd-maintenance] and [SlateDB etcd administration](docs/administration.md).

Both services use one Raft write leader and default to a 2 GiB backend quota.
Use `--quota-backend-bytes` to change the quota. When sizing SlateDB etcd, allow memory for an index
that grows with the live key count. Native backup and restore stream the image with bounded memory.
The image has no size limit, but each row has a 64 MiB limit. In shared state mode, a backup image
is limited to 64 MiB.
See [snapshot limits](docs/object-snapshots.md#limits).

[etcd-maintenance]: https://etcd.io/docs/v3.6/op-guide/maintenance/

## Start here

See the [quick start](docs/getting-started.md) for prerequisites, a single node, and a three-node
cluster. The [documentation index](docs/README.md) links all guides.

With Rust 1.91.1, a C/C++ toolchain, and a compatible protobuf compiler installed:

```sh
scripts/cargo.sh build --locked --release
mkdir -p tmp/demo
target/release/slatedb-etcd \
  --id 1 --listen 127.0.0.1:7001 --peer 1=127.0.0.1:7001 \
  --dir tmp/demo/node-1 --cluster-id demo
```

Keep that terminal open. In another terminal, use etcdctl v3.6.5:

```sh
etcdctl --endpoints=http://127.0.0.1:7001 put hello world
etcdctl --endpoints=http://127.0.0.1:7001 get hello
```

The quick start includes a reproducible etcdctl build command.
This plaintext example has no authentication. Use the [administration guide](docs/administration.md)
for Auth and separate TLS client and peer listeners.

## Current capabilities

| Area | Implemented behavior |
| --- | --- |
| Embedding | Rust configuration, startup, quorum readiness, shutdown, and generated gRPC clients |
| KV and Txn | Binary data, MVCC, historical reads, nested transactions, and Safe ReadIndex |
| Watch and Lease | Durable event replay, progress, fragments, keepalive, expiry, and recovery |
| Auth | Users, roles, range grants, bcrypt passwords, simple tokens, and RS256 JWT |
| Cluster | Learner join, catch-up, promotion, member changes, and leader transfer |
| Maintenance | Status, health, HashKV, alarms, native backups, restore, and defragmentation |
| Transport | Client TLS, peer TLS, mutual authentication, and certificate identity |
| Storage | Physical Compact, background snapshots, separate apply, and bounded log pools |
| Shared state | Prototype: one SlateDB in S3 for the cluster, written by the leader |

Native backups and the deprecated backend Hash differ from etcd bbolt.
Use the native restore flag, not `etcdutl`. Binary Downgrade returns Unimplemented.
Lease remaining TTL is not checkpointed.
Large scans and native backups still need memory controls.
Raft snapshots copy SlateDB checkpoint objects.
See [object storage snapshots](docs/object-snapshots.md).
See [compatibility results and limits](COMPATIBILITY.md) before choosing a workload.

## Verification

The project compares the real service with etcd v3.6.5 and the unchanged official Go client.
The imported common suite covers single-voter, three-voter, client TLS, and peer TLS configurations.
The administration milestone passed 387 leaf cases with no failures or skips.

Process fault histories run through the original etcd model, Knossos, and Elle.
DST controls message delivery, logical Raft time, log execution, and apply completion.
These checks do not reproduce every etcd or Jepsen test environment.
See [test commands](tests/README.md) and the [upstream audit](tests/upstream/README.md).

```sh
scripts/cargo.sh test --locked
python3 scripts/check-compat.py --output tmp/test-results/compatibility.json
python3 scripts/check-dst.py --seeds 1,2,42,58,70 --output tmp/test-results/dst.json
```

CI runs formatting, Clippy, Rust tests, compatibility, faults, Jepsen controls, and bounded fuzzing.
It also checks documentation links and scans current files and fetched Git history for secrets.
Raw logs and generated data stay outside tracked reports.

## Project guides

- [Architecture and roadmap](docs/architecture.md): storage rules, execution, and future phases.
- [Rust embedding](docs/embedding.md): run a member inside an existing application.
- [Configuration](docs/configuration.md): all server flags and their defaults.
- [Administration](docs/administration.md): authentication, TLS, membership, and backup restore.
- [Operations](docs/operations.md): health, shutdown, disk use, and recovery diagnosis.
- [Implementation notes](docs/internals.md): current MVCC, Watch, Lease, and log behavior.
- [Diagnostic APIs](docs/diagnostics.md): trusted local controls and fault injection.
- [Benchmarks](docs/benchmarks.md): gRPC comparisons, resource measurements, and limits.
- [Native macOS results](results/performance/macos.md): one member and three local processes.
- [Linux read and apply results](results/performance/linux-batch-apply.md): refactor and CPU costs.
- [Contributing](CONTRIBUTING.md): repository layout, checks, and upstream test imports.
- [Security](SECURITY.md): trust boundaries and private issue reporting.

The next work is independent-host fault validation, bounded state scans and snapshots,
and lease TTL checkpoints. The architecture document defines the phase exit criteria.

## License and provenance

The repository uses [Apache License 2.0](LICENSE). See [NOTICE](NOTICE) for upstream attribution.
Protocol definitions and imported tests come from etcd v3.6.5.
[Historical results](RESULTS.md) record the original experiment; they are not current performance
claims.
