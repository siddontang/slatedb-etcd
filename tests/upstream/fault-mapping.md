# Fault coverage map

The baseline is etcd v3.6.5, commit `a0614505aff9b8ff469e9c59c2d979a5936d13f4`.
The server source contains 41 named gofail hooks. This map accounts for each hook.
Equivalent boundaries are not identical implementations or copies of the etcd test harness.

The `fault-injection` Cargo feature enables named process exits. Normal builds omit the hooks.
Each process test arms one hook through a local file, requires a synced reachability witness, and
restarts the member. It checks previously acknowledged data and subsequent writes.
The native names below identify cases under `TestFailpointRecovery`.

| etcd hook or hooks | Native boundary or coverage limit |
| --- | --- |
| `walBeforeSync`, `raftBeforeSave` | `wal-before-sync` |
| `walAfterSync`, `raftAfterSave` | `wal-after-sync` |
| `raftBeforeAdvance` | `raft-before-advance`, after log completion |
| `raftBeforeLeaderSend` | `raft-before-leader-send` |
| `raftBeforeFollowerSend` | `raft-before-follower-send` |
| `raftBeforeSaveSnap`, `raftBeforeWALSaveSnaphot` | `snapshot-before-save` |
| `raftAfterSaveSnap` | `snapshot-after-save` |
| `raftBeforeApplySnap`, `applyBeforeOpenSnapshot` | `snapshot-before-install` |
| `raftAfterApplySnap`, `applyAfterOpenSnapshot` | Snapshot write and flush crash cases |
| `raftAfterWALRelease` | `checkpoint-after-gc`; publication and GC use one synced batch |
| `beforeApplyOneEntryNormal` | `apply-before-entry`, `apply-after-entry` |
| `beforeApplyOneConfChange` | Membership tests; no exact hook |
| `beforePublishing` | Member publication and restart tests; no exact hook |
| `afterIsMemberExist` | Member validation tests; no exact concurrent hook schedule |
| `beforeSendWatchResponse` | `watch-before-send`; replay must return the retained event |
| `beforeLookupWhenLeaseTimeToLive` | Lease quorum and TTL tests; exact lookup hook remains open |
| `beforeLookupWhenForwardLeaseTimeToLive` | Follower TTL tests; hook remains open |
| `beforeCheckpointInLeaseRenew` | Renewal DST; no etcd lease checkpoint |
| `beforeStartDBTxn`, `afterStartDBTxn` | Atomic apply tests; no bbolt txn |
| `beforeWritebackBuf`, `afterWritebackBuf` | Apply tests; no bbolt buffer |
| `beforeCommit`, `commitBeforePreCommitHook` | Apply and checkpoint crash boundaries |
| `commitAfterPreCommitHook` | Apply completion and checkpoint publication boundaries |
| `afterCommit` | Apply completion and checkpoint publication boundaries |
| `compactBeforeCommitScheduledCompact` | `compact-before-apply` |
| `compactAfterCommitScheduledCompact` | `compact-after-apply` |
| `compactBeforeCommitBatch`, `compactAfterCommitBatch` | Atomic apply; no batch loop |
| `compactBeforeSetFinishedCompact`, `compactAfterSetFinishedCompact` | Atomic marker update |
| `defragBeforeCopy`, `defragBeforeRename` | SST rewrite tests; no file rename |
| `defragOpenFileError`, `defragdbFail` | Object error tests; bbolt is absent |

WAL sync hooks run for entries, term changes, vote changes, and snapshots.
Commit-only progress does not require a device sync. Rust tests model loss of that marker.
Recovery requires a covering local log and rejects a state position beyond its entries.

The state batch adds `apply-batch-before-write` and `apply-batch-after-write` crash boundaries.
Repeated Put tests preserve each request's revision and Watch event across group recovery.
The separate checkpoint cases cover before-save, after-save, and after-GC crashes.
The snapshot cases also distinguish buffered state replacement from completed object flush.
The S3 suite checks delayed PUT, ambiguous PUT success, HTTP failures, restart GET failure, and
snapshot transfer. DST checks the same object API boundaries with explicit scheduling.

`TestClientFailoverWithTLS` adapts both client creation timings from etcd's integration failover
suite. It removes the leader, requires a successful write through surviving endpoints, and rejoins
that member. Guest tests add abrupt VM stop and kernel peer partitions.

The etcd defrag failover test depends on `StopGRPCServiceOnDefrag` and bbolt hooks.
This service does not implement that flag or the same defrag scheduling contract.
Clock jumps, arbitrary sector corruption, lazyfs, and every upstream Watch issue schedule remain
open. The project does not claim complete gofail, e2e, Jepsen nemesis, or etcd compatibility.

## Downstream projects

`tests/downstream` imports Kubernetes apiserver v0.34.1 and CoreDNS v1.12.3.
It runs 20 original Kubernetes storage test functions through the real etcd3 storage implementation.
The cases cover TTL, revision reads, deletion, pagination, conflict retries, and Watch.
CoreDNS resolves leased records through its actual etcd plugin, then checks removal after revoke.
Both projects run against this service and an embedded etcd v3.6.5 reference.

Kubernetes' delayed-consumer test exposed polling latency in this service's Watch delivery.
Apply completion now wakes Watch streams. Historical replay also starts immediately after creation.
These selected tests do not certify a complete Kubernetes control plane or a CoreDNS deployment.

```sh
scripts/cargo.sh build --locked --release --features fault-injection
python3 scripts/check-compat.py --tags failpoints --run '^TestFailpointRecovery$' \
  --output results/ci/failpoints.json
python3 scripts/check-compat.py --suite downstream --output results/ci/downstream.json
```

Sources:

- [etcd fault driver][etcd-faults]
- [etcd failover cases][etcd-failover]
- [Kubernetes storage tests][k8s-tests]
- [CoreDNS etcd plugin][coredns-etcd]

[etcd-faults]: https://github.com/etcd-io/etcd/tree/v3.6.5/tests/robustness/failpoint
[etcd-failover]: https://github.com/etcd-io/etcd/blob/v3.6.5/tests/integration/v3_failover_test.go
[k8s-tests]: https://github.com/kubernetes/apiserver/tree/v0.34.1/pkg/storage/testing
[coredns-etcd]: https://github.com/coredns/coredns/tree/v1.12.3/plugin/etcd
