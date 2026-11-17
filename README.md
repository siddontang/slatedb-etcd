# SlateDB etcd service

An experimental etcd v3 compatible service built with Raft and SlateDB.
Each replica uses `raft-rs` for consensus, `raft-engine` for durable logs, and SlateDB for state.
SlateDB's native WAL is disabled. Raft protects acknowledged writes.

The service has one Raft group, a configurable voter set, and one optional learner.
It exposes etcd v3 gRPC APIs to the official etcd client.
Each replica owns an independent local object directory and manifest.

**Status:** local research software. It is not a supported Kubernetes backend or a complete etcd
replacement. All listeners use loopback. Remote object storage and cross-host operation are future
work.

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
| KV and Txn | Binary data, MVCC, historical reads, nested transactions, and Safe ReadIndex |
| Watch and Lease | Durable event replay, progress, fragments, keepalive, expiry, and recovery |
| Auth | Users, roles, range grants, bcrypt passwords, simple tokens, and RS256 JWT |
| Cluster | Learner join, catch-up, promotion, member changes, and leader transfer |
| Maintenance | Status, health, HashKV, alarms, native backups, restore, and defragmentation |
| Transport | Client TLS, peer TLS, mutual authentication, and certificate identity |
| Storage | Physical Compact, background snapshots, separate apply, and bounded log pools |

Native backups and the deprecated backend Hash differ from etcd bbolt.
Use the native restore flag, not `etcdutl`. Binary Downgrade returns Unimplemented.
Lease remaining TTL is not checkpointed. Large scans and full snapshots still need memory controls.
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
python3 scripts/check-dst.py --seeds 1,2,42 --output tmp/test-results/dst.json
```

CI runs formatting, Clippy, Rust tests, compatibility, faults, Jepsen controls, and bounded fuzzing.
It also checks documentation links and scans current files and fetched Git history for secrets.
Raw logs and generated data stay outside tracked reports.

## Project guides

- [Architecture and roadmap](docs/architecture.md): storage rules, execution, and future phases.
- [Configuration](docs/configuration.md): all server flags and their defaults.
- [Administration](docs/administration.md): authentication, TLS, membership, and backup restore.
- [Operations](docs/operations.md): health, shutdown, disk use, and recovery diagnosis.
- [Implementation notes](docs/internals.md): current MVCC, Watch, Lease, and log behavior.
- [Diagnostic APIs](docs/diagnostics.md): trusted local controls and fault injection.
- [Benchmarks](docs/benchmarks.md): local JSON measurements and their limits.
- [Contributing](CONTRIBUTING.md): repository layout, checks, and upstream test imports.
- [Security](SECURITY.md): trust boundaries and private issue reporting.

The next work is remote object storage, cross-host faults, bounded state scans and snapshots,
and lease TTL checkpoints. The architecture document defines the phase exit criteria.

## License and provenance

The repository uses [Apache License 2.0](LICENSE). See [NOTICE](NOTICE) for upstream attribution.
Protocol definitions and imported tests come from etcd v3.6.5.
[Historical results](RESULTS.md) record the original experiment; they are not current performance
claims.
