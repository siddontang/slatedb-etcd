# etcd v3 test results

Run date: 2026-11-16, Asia/Singapore.
The reference server, common suite, model, and official Go client use etcd v3.6.5.
The test host uses ARM64 macOS. All three service replicas share one host and disk.

Tested release binary SHA-256:

```text
23e223e90be4074912ff0b1e01b08e5473857a4e0eaec2274dccf84a60a1f92e
```

## Results

| Check | Result |
| --- | --- |
| Differential KV cases against real etcd | 55 passed |
| Differential Compact cases | 39 passed |
| Official client recovery and concurrency suites | 4 passed |
| Fixed API fuzz corpus | 3 seeds passed |
| Bounded API fuzz run | 10 completed inputs passed |
| Supported profile with upstream common tests | 110 leaf tests passed; 79 skipped |
| Rust state and storage recovery tests | 7 passed |
| Original process fault scenarios | 8 passed |
| Native etcd model with process faults | 4 campaigns passed |
| Jepsen register and Elle append checks | Both checkers passed all 4 campaigns |
| Jepsen checker controls | 10 assertions passed |
| Native checker controls and saved history replay | Passed |
| Deterministic event schedules | 100 seeds passed; two identical runs per seed |
| Rust formatting and Clippy with warnings denied | Passed |

The upstream profile includes the local differential and integration suites.
Its counts are not additional to the other rows. Parent test results do not count as leaf tests.
All skips have explicit reasons in the report. They do not establish compatibility.
The full capability probe rejects the missing Watch API instead of skipping it.

The differential suites compare values, revisions, key metadata, counts, and errors.
They ignore cluster IDs, member IDs, and Raft terms across independent clusters.
They separately check response identity and the receiving member ID on forwarded requests.

Coverage includes binary keys, historical ranges, sorting, filters, tombstones,
and version counters.
Transactions cover both branches, nested predicates, rollback, duplicate writes,
and revision guards.
Compaction tests check boundary reads, errors, deletion history, restart, and snapshot recovery.
Compaction stores the boundary, history deletion, and applied index in one state-machine batch.

The process campaigns inject a leader partition, leader kill, leader pause, or full cluster restart.
Traffic continues during each fault. Final reads check state after recovery.
The original etcd model checks the complete KV history, including uncertain writes.
Jepsen checks register operations. Elle checks multi-key append transactions.
The adapters correct two recorder issues: long Txn value encoding and nil read ambiguity.
Negative controls verify that the checkers reject known invalid histories.

Raw reports:

- [Official client and reference tests](results/etcd-compatibility.json)
- [Upstream common profile](results/etcd-upstream.json)
- [Process recovery and log GC](results/etcd-core-fault-tests.json)
- [Model and Jepsen campaigns](results/robustness.json)
- [DST schedules and trace hashes](results/dst.json)

See [test commands](tests/README.md) for reproduction, fuzzing, and saved history checks.
The [source audit](tests/upstream/README.md) maps all upstream families and coverage gaps.
The CI workflow runs the same test layers on Linux.

## Scope

This is partial etcd v3 compatibility. Supported RPCs are Range, Put, DeleteRange, Txn, and Compact.
Compact supports logical compaction. The physical option returns Unimplemented.
Watch, Lease, Auth, Cluster, and Maintenance remain unsupported.
The service does not claim Kubernetes compatibility.

The repository indexes 1,771 upstream functions and imports the complete common suite.
The index includes tests of etcd internals that cannot run against this Rust implementation.
It is not a claim that every upstream test passed or every fault was reproduced.

DST uses real node storage with a simulated transport and logical Raft ticks.
It requires repeatable event traces and checks acknowledged values after recovery.
It does not control operating system threads, storage completion, or SlateDB background scheduling.

The Jepsen integration uses real checkers with a local process driver.
The upstream SSH deployment, clock faults, membership faults, bit flips, and lazyfs are not ported.
Tests also exclude cross-host faults, object-store outages, power loss, and rolling upgrades.

Historical reads scan stored history. Logical compaction also scans history on the node loop.
Large scans can delay heartbeats. Physical SST reclamation remains asynchronous.
Snapshot transfers retain the 64 MiB peer limit.
See [storage limits](README.md#mvcc-storage-and-recovery).
