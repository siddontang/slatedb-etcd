# Raft performance notes from etcd and TiKV

This review maps upstream techniques to the current single-group service.
It does not claim that TiKV's benchmark gains transfer to this project.
The TiKV source is pinned at `68d123e1756e5c774c27ab923040261f81589051`.
The etcd source uses v3.6.5. The reviewed paths are TiKV's classic raftstore implementation.
TiKV's tablet-based raftstore-v2 is a separate design and is outside this review.
Historical articles explain the original designs. The pinned source defines the reviewed behavior.

## Current position

| Technique | Current service | Next step |
| --- | --- | --- |
| Separate Raft, log I/O, and apply | Implemented | Measure each queue and execution stage |
| Share log writes | Implemented for adjacent Ready jobs | Measure batch size before adding waits |
| Avoid commit-only sync | Implemented with recovery checks | Preserve entry and vote barriers |
| Reuse peer streams | Implemented with count and byte limits | Test bounded message batches |
| Batch quorum reads | Implemented with frozen batches | Later requests need a new proof |
| Serve reads outside apply | Implemented with pinned views | Measure queue time and scan cost |
| Batch state writes across entries | Ordinary unleased puts | Extend overlays to transactions |
| Lease-based local reads | Not enabled | Require a clock and lease safety design |

## Separate and measure the stages

PingCAP's 2017 [Raft optimization article] describes bounded batching, replication pipelines,
parallel leader appends, asynchronous apply, and separate read execution.
These techniques overlap work while retaining completion barriers.
A follower must still persist entries before it acknowledges them to the leader.

TiKV's [StoreWriter] combines write tasks, writes the log, sends dependent messages, and reports
persistence completion. It also tracks queue, database, message, and callback time.
The [latency guide] explains why commit delay can come from replication or local persistence.
Its [thread guide] warns that more threads can add switching and disk pressure.

The service already separates its Raft owner, log thread, and apply worker.
The next useful measurement is per-stage queue time, execution time, batch size, and bytes.
Report read-proof wait, apply wait, log sync, and forwarded-result wait separately.
A high end-to-end latency or blocked stack alone does not identify the limiting stage.

TiKV's [entry storage] can return `LogTemporarilyUnavailable` and schedule a background fetch.
It also keeps synchronous fallbacks for contexts that cannot fetch asynchronously.
This project uses bounded entry, pending, result, and term pools and validates stale fetch results.
Its reads and writes still share one log worker. Separate readers may help slow-follower catch-up,
but the current hot-cache matrix does not measure that benefit.

The extended DST run also exposed a dependency cache collision across log instances.
Cache keys must identify the open log as well as the file position.
The [local dependency notes](../third_party/raft-engine/README.md) describe this fix and an explicit
sync fix. Both have regressions that fail with the published Raft Engine 0.4.2 crate.

## Batch at distinct boundaries

TiKV's [proposal builder] combines compatible Put and Delete requests.
It checks request type, headers, request size, and total batch size.
Its [apply worker] shares a database write batch and reuses its allocation after writing.
Large batches cause the worker to replace that allocation with a smaller one.

The service groups Ready log writes and applies up to 32 entries per worker job.
Consecutive ordinary unleased puts now share one SlateDB write within that job.
They retain separate revisions, results, Watch events, and sequential quota checks.
Other entry types form group boundaries and retain individual writes.

A combined state batch needs an overlay for reads of earlier writes in that batch.
Each etcd request must retain its revision, Watch events, quota result, and response.
Auth, Lease, membership, compaction, and snapshot transitions need explicit batch boundaries.
The final SlateDB sequence and apply marker must match the last covered Raft index.
Do not collapse distinct etcd requests into one transaction revision.

[StoreWriter] also has adaptive waiting based on recent batch sizes and configured limits.
The service currently drains available work and uses a cooperative write yield without a timer.
Add an adaptive wait only after measuring underfilled batches and testing low-load p99 latency.
TiKV's wait settings are not defaults for this different storage engine and one Raft group.

## Batch network traffic within bounds

TiKV's [Raft client] collects messages into `BatchRaftMessage`.
It limits batch count and estimated bytes and carries an overflow message into the next batch.
The service now reuses a stream, but each stream frame still contains one Raft message.
A bounded frame batch could reduce serialization, wakeups, and network calls.

This is a candidate experiment, not a measured gain.
Keep snapshot ordering, durable Raft acknowledgements, cancellation, and reconnection behavior.
Test tiny heartbeat batches and large entry messages separately.

## Separate read proof from read execution

TiKV's [local reader] uses read delegates and cached snapshots outside the Raft state machine.
It checks the applied term and lease state and redirects requests when local conditions fail.
Its [read queue] tracks proof contexts, ready requests, role changes, and retries.

The service keeps Safe ReadIndex and now runs bounded reads outside the apply queue.
Each read uses a pinned view that covers its confirmed index.
The view must include matching revision, compaction, Auth, Lease, and snapshot-generation metadata.
A snapshot cache needs invalidation and memory limits; a cached value alone is insufficient.
This split removes the shared apply queue without assuming a leader lease is valid.

TiKV's [read amendment] can attach later requests to an earlier pending read under lease rules.
It checks proposal time, lease state, and special request constraints.
The service has no equivalent lease proof, so its current batches remain frozen before confirmation.
Copying only the request-coalescing code would remove a required correctness condition.

## Distinguish two early-apply techniques

TiKV's [early-apply change] moves apply ahead of commit-marker persistence.
A committed entry and a durable commit marker are different requirements.
The service now avoids syncing commit-only changes and repairs lost markers from covered state.
It still requires quorum commit, Ready delivery, and durable local entries before apply.

Modern TiKV also supports applying committed entries before local entry persistence.
The [configuration reference] documents the separate `max-apply-unpersisted-log-limit` control.
The [peer implementation] disables and later restores this mode around unsafe transitions.
This is a broader recovery change. The current service does not enable it.
Its restart proof, checkpoint truncation, snapshots, and state flush rules must change together.

## Cross-check with etcd

The [etcd WAL] uses Raft's `MustSync` rule for entries, terms, and votes.
The [etcd Raft loop] overlaps leader replication with persistence and fences follower responses.
These rules support the service's existing log batching and commit-marker changes.

The [etcd read path] batches read notifications, confirms ReadIndex, and waits for apply progress.
Its Range execution then runs in the request path.
The service now sends confirmed reads to bounded tasks with a matching pinned state view.
Both upstream designs informed this split. Matched measurements must establish its actual benefit.

## Lease reads and multi-group techniques

The [lease-read article] explains the extra clock and election assumptions for local lease reads.
A lease design must address clock-rate bounds, process suspension, partitions, leader transfer,
and membership changes. DST must model those conditions before this service can use the shortcut.
The etcd Lease API is a separate feature; implementing it does not establish a Raft leader lease.

TiKV also uses multi-group scheduling and inactive-region hibernation.
The [region tuning guide] describes their purpose and tradeoffs.
This project currently has one Raft group. Extra region scheduling would not address that group's
serialized reads or full-state checkpoint cost.

## Encoding and allocation work

The ordinary KV log path now uses protobuf requests, as etcd does for its Raft requests.
The service also uses protobuf for MVCC history and Watch journals.
A point Put reads its prior value directly. History key encoding uses one allocation.
Snapshot construction writes rows directly into the output buffer and releases spare capacity.
These changes target allocation and parsing costs. Matched measurements determine their benefit.

## Experiment order and acceptance

1. Add sampled stage timing and batch-size counters with a measured overhead budget.
2. Measure the bounded read executor while retaining Safe ReadIndex.
3. Measure ordinary Put groups, then extend the overlay to transaction scans.
4. Test bounded peer message batches and compare low-load and saturated latency.
5. Separate cheap local checkpoints from full snapshot transfer.
6. Consider lease reads or early apply only with their full recovery and timing proofs.

Each runtime experiment needs matched one-member and three-member repeats.
Retain throughput, p50/p99, CPU per request, RSS, and every request error.
Reject gains that depend on weaker durability or stale reads.
Run partitions, delayed apply and log writes, crashes, snapshot replacement, and deterministic
replay.
Use process histories with the etcd model and Jepsen checks before merging.

[StoreWriter]:
  https://github.com/tikv/tikv/blob/68d123e/components/raftstore/src/store/async_io/write.rs
[proposal builder]:
  https://github.com/tikv/tikv/blob/68d123e/components/raftstore/src/store/fsm/peer.rs
[apply worker]:
  https://github.com/tikv/tikv/blob/68d123e/components/raftstore/src/store/fsm/apply.rs
[Raft client]: https://github.com/tikv/tikv/blob/68d123e/src/server/raft_client.rs
[local reader]:
  https://github.com/tikv/tikv/blob/68d123e/components/raftstore/src/store/worker/read.rs
[read queue]: https://github.com/tikv/tikv/blob/68d123e/components/raftstore/src/store/read_queue.rs
[entry storage]:
  https://github.com/tikv/tikv/blob/68d123e/components/raftstore/src/store/entry_storage.rs
[read amendment]:
  https://github.com/tikv/tikv/blob/68d123e/components/raftstore/src/store/peer.rs#L659
[peer implementation]:
  https://github.com/tikv/tikv/blob/68d123e/components/raftstore/src/store/peer.rs
[early-apply change]: https://github.com/tikv/tikv/pull/6154
[latency guide]: https://docs.pingcap.com/tidb/v6.5/latency-breakdown/
[thread guide]: https://docs.pingcap.com/tidb/stable/tune-tikv-thread-performance/
[configuration reference]: https://docs.pingcap.com/tidb/stable/tikv-configuration-file/
[lease-read article]: https://tikv.org/blog/lease-read/
[region tuning guide]: https://tikv.org/blog/tune-with-massive-regions-in-tikv/
[Raft optimization article]: https://segmentfault.com/a/1190000008674222
[etcd WAL]: https://github.com/etcd-io/etcd/blob/v3.6.5/server/storage/wal/wal.go
[etcd Raft loop]: https://github.com/etcd-io/etcd/blob/v3.6.5/server/etcdserver/raft.go
[etcd read path]: https://github.com/etcd-io/etcd/blob/v3.6.5/server/etcdserver/v3_server.go
