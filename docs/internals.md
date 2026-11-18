# Implementation notes

This document describes the current local implementation.
See [architecture](architecture.md) for the target design and roadmap.

## Commit and recovery rules

Each node persists Raft entries, terms, and votes with a non-empty synchronous raft-engine batch.
A dedicated log thread persists these batches. Leader replication can overlap local disk I/O.
Vote and append acknowledgements wait for their dependent writes.
The node also persists the commit index before applying entries.

The leader replies only after quorum commit and local apply. Followers apply the same committed
commands in order. Each command updates its business keys and `meta/applied` in one SlateDB batch.
The SlateDB sequence number is the Raft index. Even an empty Raft entry advances the apply marker.
This index is not an etcd revision.

Reads in `/kv` enter the Raft log as barriers. This provides a simple linearizable read path, but
adds log writes. The etcd gRPC read path uses ReadIndex and an apply barrier.

On restart, the node reads the apply marker from SlateDB. It installs a newer durable Raft snapshot
if necessary, then replays committed log entries after that marker. It never treats an in-memory
apply position as a durable recovery boundary.

For a checkpoint, the node:

1. Pins a SlateDB snapshot at the current applied index.
2. Flushes SlateDB and serializes that pinned view in a background task.
3. Saves the snapshot and log truncation command in one synchronous raft-engine batch.
4. Calls raft-engine's purge routine to reclaim obsolete files.

The saved snapshot retains the boundary term and membership at the same applied index. On snapshot
receipt,
the node atomically replaces its persistent Raft state before changing SlateDB. A crash in between
is safe: startup can repeat the state replacement from the durable snapshot payload.

Full snapshots deliberately duplicate state in raft-engine. This makes recovery independent of
uncopied local object files, but limits the PoC to small data sets. Snapshots use the peer HTTP body
limit of 64 MiB. Snapshot creation runs in the background.
A dedicated log I/O thread persists snapshots.
Slow persistence can still raise client tail latency.
A separate apply task owns SlateDB, installs snapshots, and executes committed state changes.
Production use still needs bounded snapshot transfer and bounded state scans.

The local PoC polls compaction and manifest updates every 100 ms. The default slower polling caused
L0 backpressure during repeated checkpoints in an earlier run. That run is retained in
[the earlier failure](../results/before-background-snapshots.json).
Cloud deployments need their own polling and cost policy.

The `durable_applied_index` status field is the last observed explicit flush boundary. Automatic
SlateDB flushes can advance storage beyond it. `snapshot_index` is the durable log GC boundary.

Each replica owns an independent object-store directory and manifest. The PoC does not let three
writers share a manifest. A shared SST design needs a separate ownership and reference-GC protocol.

## Raft and apply execution

The Raft owner stages and replicates entries. A dedicated thread persists them in raft-engine.
One apply task owns SlateDB.
Only worker completion advances `applied_index` or permits a successful write response.
Committed work stays in the durable log until the worker can accept it.
A batch has at most 32 entries and a 512 KiB target. One larger entry can exceed the byte target.
The worker also handles serializable reads, Watch scans, snapshot installation, and checkpoint pins.
Local reads receive service between entry batches. They can lag durable commit.

The owner queues at most 64 local reads and 64 checkpoint requests.
It rejects new proposals at 1,024 committed but unapplied entries.
Worker failure or panic stops the node. Shutdown cancels the worker and its snapshot builder.
The log thread performs steady-state disk I/O, file reclamation, and disk statistics.
Apply waits for Raft to deliver committed entries through Ready, then waits for local durability.
The owner pins unapplied entries and keeps a bounded cache of applied log payloads.
Cache misses fetch entries asynchronously through the disk worker.
Term runs support synchronous Raft term lookup without retaining every payload.
The apply task shares the process runtime; this change does not isolate CPU or disk resources.

Status reports `apply_lag`, `apply_busy`, `apply_target`, and `queued_state_reads`.
`POST /debug/apply-delay` accepts `{"delay_ms":2500}` to delay entry apply and snapshot
installation.
This loopback test hook has a five-second limit. Set the delay to zero to restore normal operation.

Linearizable Range requests and read-only transactions use Safe ReadIndex.
The leader confirms a quorum. The receiving member waits for local apply through the read index.
Followers use Raft to request that index, then evaluate the read locally.
Forwarded KV results wait for local apply through the leader response revision.
Lease and Compact results still require a local ReadIndex barrier.
Later STM reads on that member can then use their revisions.
Both branches of nested transactions must contain only reads.
Leadership loss, timeout, or cancellation removes the pending read. At most 64 reads can wait.
A frozen batch shares one quorum context. Later requests use a new context.
These reads do not append log entries. Unconfirmed batches retry every two Raft ticks.
Serializable reads remain local and can return older state.

Log work uses a FIFO queue with 64 jobs and a 16 MiB entry admission target.
Snapshots use a separate 64 MiB target. One Ready can exceed a target.
New proposals stop at the entry or job limit.
Empty Ready barriers finish on the owner after earlier writes finish.
Adjacent queued Ready jobs share a sync. Other log job types form batch boundaries.
Disk statistics run only for `/debug/log-stats` requests.
Raft advances durable progress and releases dependent messages only after log completion.
Leader replication can run before local persistence, as permitted by raft-rs.
Ordinary HTTP receipt confirms delivery; snapshot receipt waits for durable snapshot storage.

Status separates `raft_commit_index`, durable `commit_index`, and `applied_index`.
It also reports `cached_last_log_index`, `log_busy`, `queued_log_jobs`, `queued_log_bytes`,
`queued_snapshot_bytes`, `pending_read_indexes`, and `pending_revision_waiters`.
`log_memory` reports bytes and limits for the cache, pending entries, read results,
and term metadata.
It also reports hits, misses, evictions, disk reads, and discarded stale results.
`POST /debug/log-fault` accepts `{"delay_ms":2500,"fail":false}` for local fault tests.
The delay has a five-second limit. `fail=true` stops the node on its next log write.
`POST /debug/log-read-fault` accepts the same fields for asynchronous entry reads.
Set both fields to zero or false to clear future injected faults.
Shutdown cancels queued work and injected delays. An active kernel I/O call cannot be cancelled.

## MVCC storage and recovery

SlateDB stores the current value for each key and one history record for each changed revision.
History records include deletions. The revision, key changes, and Raft apply marker use one batch.
Raft log replay and full snapshots restore both current state and historical versions.
The revision is independent of the SlateDB sequence number, which tracks the Raft apply index.

Point reads use the current-key index. Current range reads scan the requested key interval.
Historical point reads scan one key's versions. Historical range reads currently scan all history.
Range responses collect matching keys in memory before sorting or applying a limit.
These choices favor compatibility testing. Large data sets need bounded scans and a history index.

Compact uses one Raft command and one SlateDB batch for history deletion, the boundary,
and the apply marker. It does not advance the KV revision. Reads below the boundary return the
standard compacted-revision error. Reads at the boundary remain valid.

Compaction keeps the latest version at or before the boundary for each key, including tombstones.
It keeps all newer versions. Current keys and their revision metadata remain unchanged.
Restart and snapshot installation restore the boundary with the retained history.
Raft log GC remains separate from MVCC compaction.

Compaction scans all history and builds one deletion batch on the apply worker.
Large histories can delay state reads and write completion. Raft keeps processing ticks and peers.
Physical Compact waits for the deletion batch to flush before it returns.
Defragment rewrites the current SST set and waits for manifest publication.
SlateDB garbage collection reclaims obsolete objects later. Pinned checkpoints can retain them.
Do not downgrade to an older binary after compaction; older binaries do not enforce its boundary.

The diagnostic JSON keyspace is separate from the etcd keyspace and does not affect etcd revisions.

## Watch and Lease recovery

Each MVCC write stores ordered Watch events with the key changes and revision in one batch.
A transaction keeps request order within one revision. Lease revocation deletes all attached keys
at one revision. Watch replay uses this journal after restart and snapshot installation.
Compaction removes events below the boundary. Events at the boundary remain available.
Previous values are omitted when the revision before the event is below the boundary.

Watch runs on the receiving replica and can lag the leader. It does not provide linearizable reads.
Streams poll the node every 50 ms. Each page contains at most 32 revisions.
The output channel buffers 32 responses.
Each stream supports at most 1,024 watches. Automatic progress uses a 10-second interval.
Progress replies follow all events through their reported revision. Future watches delay progress.
The fragment option splits large responses near 1 MiB. Each encoded response must fit within 64 MiB.

Databases from binaries without Watch have no complete event journal.
Historical watches before the first recorded revision return a compacted-revision error.
Existing KV history remains readable. Upgrade all nodes together; mixed-version operation is
untested.
Do not downgrade after lease commands enter the Raft log.

Lease grants, renewals, revocations, TTL queries, and listing enter Raft.
A minority cannot acknowledge a renewal. The leader measures expiry with a monotonic clock.
Expiry includes the lease generation, so an old expiry command cannot remove a renewed lease.
Lease metadata, attached key deletion, Watch events, and the apply marker commit together.
Followers do not expire leases from their local clocks. SlateDB TTL does not control leases.

The minimum TTL is two seconds. A new leader gives each surviving lease its full granted TTL.
The service does not checkpoint remaining TTL. Repeated leader changes can extend a lease's
lifetime.
Loss of quorum delays deletion until a majority can commit it. Clients must not treat TTL as a
maximum lifetime or use lease expiry alone to fence external writes.
Lease key lookup and revocation scan current keys. Large leases can delay the apply worker.

## Log memory limits

The following flags set independent log pool limits:

| Flag | Default |
| --- | --- |
| `--log-cache-bytes` | 8388608 |
| `--log-pending-bytes` | 33554432 |
| `--log-read-bytes` | 16777216 |
| `--log-term-bytes` | 1048576 |

The history cache evicts entries even when automatic checkpoints are disabled.
Unapplied entries remain pinned. Admission reserves 64 KiB for control entries.
Each entry charge includes its encoded size and 256 bytes of structure allowance.
Each compressed term run costs 64 bytes. An encoded entry cannot exceed 8 MiB.
Read batches target 512 KiB and contain at most 128 entries. One large entry can exceed the target.
The read quota must hold one maximum entry. A cache smaller than an entry uses the read result pool.

These limits do not bound process RSS, snapshots, transport buffers, or SlateDB allocations.
Term exhaustion or a control entry beyond the hard pending limit stops the node.
Startup replays a large committed backlog in bounded batches before activation.
An uncommitted tail beyond its quota requires a larger pending limit before restart.
See the [log pool protocol](architecture.md#bounded-log-pools)
for accounting and recovery rules.
