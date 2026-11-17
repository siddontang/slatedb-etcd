# etcd v3 test results

Run date: 2026-11-17, Asia/Singapore.
The reference server, common suite, model, and official Go client use etcd v3.6.5.
The test host uses ARM64 macOS. All three service replicas share one host and disk.

Tested release binary SHA-256:

```text
68c3a60a79ebdf59ebefc8cd893792d69e77a93ba030f7114f0b1a4133ff0372
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
| Mutex, election, and STM workflows against both servers | 10 passed |
| Bounded cache and asynchronous read process cases | 2 passed |
| Follower read and forwarded write barriers | 2 passed |
| Known capability gap controls | 4 differences confirmed |
| Fixed API fuzz corpus | 3 seeds passed |
| Bounded API fuzz run | 10 completed inputs passed |
| Supported profile with upstream common tests | 180 leaf tests passed; 73 skipped |
| Full capability probe | 180 passed; 75 failed; 12 skipped |
| Rust node, state, and storage recovery tests | 32 passed |
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
The full capability probe fails on missing APIs and unsupported cluster configurations.
Four direct controls compare Auth status, member listing, Maintenance status, and physical Compact.
Real etcd supports all four operations. This service returns Unimplemented.
These passing controls verify gaps; they do not establish compatibility.

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
A 1 KiB cache forces disk reads for a lagging follower without snapshots.
Read delay preserves heartbeats. Read failure stops the node, which recovers after restart.
Unit cases check memory admission and stale reads after overwrite, compaction, and snapshots.
A full pending pool still permits Raft prefix rejections and duplicate append acknowledgements.

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
- [Full capability probe with expected failures](results/etcd-all-capabilities.json)
- [Process recovery and log GC](results/etcd-core-fault-tests.json)
- [Model and Jepsen campaigns](results/robustness.json)
- [DST schedules and trace hashes](results/dst.json)

See [test commands](tests/README.md) for reproduction, fuzzing, and saved history checks.
The [source audit](tests/upstream/README.md) maps all upstream families and coverage gaps.
The CI workflow runs the same test layers on Linux.

## Client integration findings

The unchanged etcd client runs each workflow against the service and embedded etcd 3.6.5.

| Workflow | Assertions |
| --- | --- |
| Mutex | One owner, contention, unlock, and handoff after session revocation |
| Election | Campaign, observe, proclaim, resign, and lease cleanup |
| STM | Balance transfers, forced conflicts, retries, and three isolation modes |

STM stays on one member so historical serializable reads use a locally available revision.
Mutex and election cases use all three service endpoints.
These cases do not establish STM behavior across endpoint changes.
All ten workflow cases pass. The [test guide](tests/README.md#official-client-integration-workflows)
links the official APIs and gives reproduction commands.

The STM case found a follower consistency defect during development.
A forwarded result could expose a revision before the receiving follower applied it.
The next local historical read could then report a future revision.
Followers now use a local ReadIndex apply barrier for linearizable reads and forwarded results.
Separate delayed-apply tests guard both paths.
A new-term regression also checks retries when Raft drops an early ReadIndex request.

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
It injects failed log reads and writes, restarts nodes, and checks dependent vote and append
messages.
Each seed uses a 1 KiB cache and checks all four pool limits.
A delayed replication fetch completes after a higher term removes the old leader.
Each successful read must include writes acknowledged before that read began.
It does not control lease wall-clock expiry, operating-system threads, or partial sector writes.
SlateDB background scheduling remains outside its control.

The Jepsen integration uses real checkers with a local process driver.
The upstream SSH deployment, clock faults, membership faults, bit flips, and lazyfs are not ported.
Tests also exclude cross-host faults, object-store outages, power loss, and rolling upgrades.

Historical reads and logical compaction scan history on the apply worker.
Large scans can delay state reads and write completion. Physical SST reclamation stays asynchronous.
A separate log thread handles fsync, snapshot publication, and disk statistics.
The Raft owner uses separate limits for cached history, pending entries, read results, and terms.
Those accounting limits do not bound process RSS, state scans, or snapshot buffers.
Term exhaustion stops the node. Large uncommitted recovery tails require a larger pending limit.
Snapshot transfers retain the 64 MiB peer limit.
See [storage limits](README.md#mvcc-storage-and-recovery).

Lease remaining TTL is not checkpointed. Leader promotion resets surviving leases to their full TTL.
Older databases cannot replay events before the new Watch journal begins.
See [Watch and Lease recovery limits](README.md#watch-and-lease-recovery).
