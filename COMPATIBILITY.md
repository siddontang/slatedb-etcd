# etcd v3 test results

Run date: 2026-11-16, America/Los_Angeles.
The reference server, common suite, model, and official Go client use etcd v3.6.5.
The test host uses ARM64 macOS. All three service replicas share one host and disk.

Tested release binary SHA-256:

```text
a6647ba35ba4d1c1060a046beffc483b0372478bfed1175c012c52549ed2fad6
```

## Results

| Check | Result |
| --- | --- |
| Differential KV cases against real etcd | 55 passed |
| Differential Compact cases | 39 passed |
| Differential Lease cases | 25 passed |
| Differential Watch history cases | 5 passed |
| Watch stream control checks against real etcd | 2 passed |
| Watch and Lease process and stream suites | 4 passed |
| Apply isolation and backlog recovery suites | 3 passed |
| ReadIndex and log I/O fault suites | 5 passed |
| Official client recovery and concurrency suites | 4 passed |
| Fixed API fuzz corpus | 3 seeds passed |
| Bounded API fuzz run | 10 completed inputs passed |
| Supported profile with upstream common tests | 162 leaf tests passed; 73 skipped |
| Rust node, state, and storage recovery tests | 23 passed |
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
The full capability probe still fails on missing APIs, including Auth and Maintenance.

The differential suites compare values, revisions, key metadata, counts, and errors.
They ignore cluster IDs, member IDs, and Raft terms across independent clusters.
They separately check response identity and the receiving member ID on forwarded requests.

Coverage includes binary keys, historical ranges, sorting, filters, tombstones,
and version counters.
Transactions cover both branches, nested predicates, rollback, duplicate writes,
and revision guards.
Compaction tests check boundary reads, errors, deletion history, restart, and snapshot recovery.
Compaction stores the boundary, history deletion, and applied index in one state-machine batch.
Watch checks cover filters, event order, previous values, compaction, progress, and fragments.
Lease checks cover attachment, reassignment, renewal, revocation, expiry, and recovery.
A minority cannot acknowledge a renewal. A stale expiry command cannot delete a renewed lease.
Apply isolation tests delay state work beyond an election timeout without losing the leader.
They check reply ordering, committed backlog recovery, and peer progress during snapshot
installation.
ReadIndex tests check quorum loss, delayed apply, term changes, and bounded pending requests.
Range requests and nested read-only transactions leave log indexes and sync counts unchanged.
Log tests delay writes and checkpoint publication without losing the leader.
They check failed writes, restart before completion delivery, and worker shutdown.

The process campaigns inject a leader partition, leader kill, leader pause, or full cluster restart.
Traffic continues during each fault. Final reads check state after recovery.
The original etcd model checks KV and Lease history, including uncertain writes.
Lease campaigns use long TTLs; separate process tests check expiry.
A local Watch checker compares all three replicas, progress, previous values, and final state.
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

This is partial etcd v3 compatibility. It supports KV, Watch, and Lease RPCs.
Compact supports logical compaction. The physical option returns Unimplemented.
Auth, Cluster, and Maintenance remain unsupported. Their Watch and Lease cross-tests still skip.
The service does not claim Kubernetes compatibility.

The repository indexes 1,771 upstream functions and imports the complete common suite.
The index includes tests of etcd internals that cannot run against this Rust implementation.
It is not a claim that every upstream test passed or every fault was reproduced.

DST uses real node storage with a simulated transport and logical Raft ticks.
It requires repeatable event traces and checks acknowledged values after recovery.
It also checks retained lease generations and complete Watch journals on each replica.
The scheduler controls apply jobs, log execution, and log completion delivery.
It injects a failed log write, restarts the node, and checks dependent vote and append messages.
Each successful read must include writes acknowledged before that read began.
It does not control lease wall-clock expiry, operating-system threads, or partial sector writes.
SlateDB background scheduling remains outside its control.

The Jepsen integration uses real checkers with a local process driver.
The upstream SSH deployment, clock faults, membership faults, bit flips, and lazyfs are not ported.
Tests also exclude cross-host faults, object-store outages, power loss, and rolling upgrades.

Historical reads and logical compaction scan history on the apply worker.
Large scans can delay state reads and write completion. Physical SST reclamation stays asynchronous.
A separate log thread handles fsync, snapshot publication, and disk statistics.
The Raft owner keeps the full retained log in memory; the cache has no independent memory quota.
Snapshot transfers retain the 64 MiB peer limit.
See [storage limits](README.md#mvcc-storage-and-recovery).

Lease remaining TTL is not checkpointed. Leader promotion resets surviving leases to their full TTL.
Older databases cannot replay events before the new Watch journal begins.
See [Watch and Lease recovery limits](README.md#watch-and-lease-recovery).
