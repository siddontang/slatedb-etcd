# etcd v3 KV compatibility results

Run date: 2026-11-15, America/Los_Angeles.
The reference server and official Go client both use etcd v3.6.5.
The test host uses ARM64 macOS. All three service replicas share one host and disk.

Tested release binary SHA-256:

```text
2bbaae2f834a122f8ea396fa33509b95a847c62f84c7abd4bf8adb067ffeec4e
```

## Results

| Check | Result |
| --- | --- |
| Differential KV scenarios against real etcd | 55 passed |
| Official client integration suites | 4 passed |
| Rust storage and snapshot recovery tests | 5 passed |
| Original process fault scenarios | 8 passed |
| Rust formatting and Clippy with warnings denied | Passed |

The differential suite compares response values, revisions, key metadata, counts, and errors.
It ignores cluster IDs, member IDs, and Raft terms when comparing two different clusters.
It separately checks nonzero response identity and the member ID on a forwarded request.

Coverage includes:

- Binary keys and values, empty values, previous values, and ignore flags.
- Historical reads, deletion history, recreation, and version counters.
- Prefix and interval reads, limits, sorting, count-only reads, and revision filters.
- Transaction comparisons, both branches, nested predicates, and one revision per mutation.
- Invalid requests, duplicate writes, overlapping ranges, and rollback without partial changes.
- Mutually exclusive nested writes and the reference behavior for negative revision requests.

The integration suites also test:

- Writes through a follower and reads after the leader dies.
- Full process restart before explicit SlateDB flush.
- An offline follower recovering current and historical state from a snapshot after log GC.
- Full restart after snapshot installation and continued revision allocation.
- Twelve concurrent clients completing 120 successful compare-and-swap increments.
- An isolated replica serving serializable Range and Txn calls while rejecting linearizable reads.

The final core fault run recovered write availability 1.75 seconds after leader SIGKILL.
It removed eight old log files. Log bytes fell from 2,199,792 to 139,670.
These are individual observations, not latency bounds.

Raw reports:

- [Official client and reference tests](results/etcd-compatibility.json)
- [Process fault and log GC tests](results/etcd-core-fault-tests.json)

## Reproduce

```sh
scripts/cargo.sh build --locked --release
scripts/cargo.sh test --locked
scripts/cargo.sh clippy --locked --all-targets -- -D warnings
python3 scripts/check-compat.py
python3 scripts/harness.py test --output results/etcd-core-fault-tests.json
```

The compatibility runner starts real service processes and a real embedded reference etcd.
It uses the unmodified official Go client over gRPC. It does not use a mock client or server.
The GitHub workflow runs the same checks on Linux.

## Scope

This is partial etcd v3 compatibility. It does not implement Watch, Lease, Auth, Cluster,
Maintenance, or MVCC Compact. It does not claim Kubernetes compatibility.
Range, Put, DeleteRange, and Txn are the supported RPCs.

The service retains all MVCC history. Raft log GC does not compact MVCC history.
Historical range reads scan history and can use substantial memory. Large snapshots remain limited
by the 64 MiB peer message limit. See [storage limits](README.md#mvcc-storage-and-recovery).

These tests do not cover cross-host failures, object-store outages, power loss, or rolling upgrades.
The original JSON PoC latency figures are not measurements of the new gRPC and MVCC path.
