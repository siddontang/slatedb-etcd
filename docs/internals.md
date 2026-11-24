# Implementation notes

This document describes the current local implementation.
See [architecture](architecture.md) for the target design and roadmap.
See [module boundaries](modules.md) for ownership and extension rules.

## Command and state encoding

The log stores a protobuf envelope. KV commands contain typed requests and optional credentials.
Administrative commands use JSON inside the same envelope. The diagnostic HTTP API also uses JSON.
This avoids JSON number arrays and untagged enum trials on the common replicated KV path.
MVCC versions and Watch journals use protobuf messages. Native snapshots retain a binary envelope.
Format 3 accepts no older data directory or snapshot. There is no migration or dual decoder.

## Commit and recovery rules

Each node persists Raft entries, terms, and votes with a non-empty synchronous raft-engine batch.
A dedicated log thread persists these batches. Leader replication can overlap local disk I/O.
Vote and append acknowledgements wait for their dependent writes.
The node writes commit progress before applying entries. Commit-only updates need no device sync.
This follows Raft's persistence rule, which requires stable entries, terms, and votes.

The leader returns a computed result after quorum commit. Followers apply the same prefix.
An ordinary Put can use the bounded preparation path in the [Raft design](architecture.md).
Consecutive ordinary puts can share one SlateDB write.
Each request retains its own etcd revision, Watch events, quota result, and response.
The batch stores its final `meta/applied` marker and `meta/applied-term` with all mutations.
The term is the Raft term of the last entry in the batch.
The SlateDB sequence is that final Raft index. Even an empty Raft entry advances the apply marker.
This index is not an etcd revision.

Reads in `/kv` enter the Raft log as barriers. This provides a simple linearizable read path, but
adds log writes. The etcd gRPC read path uses ReadIndex and an apply barrier.

On restart, the node reads the apply marker from SlateDB. It installs a newer durable Raft snapshot
if necessary, then replays committed log entries after that marker. It never treats an in-memory
apply position as a durable recovery boundary.
Flushed state can exceed the recovered commit marker because commit-only updates can be lost.
Startup restores commit progress from that state only when the durable log covers its applied index.
Missing log entries still prevent startup. Checkpoints save their commit marker in the synced batch.

The [Raft Engine fork](dependencies.md#raft-engine-fixes) separates the thread cache by open log
instance, including reopened instances. Explicit sync reaches the file system even when no new write
batch exists.
Regressions check cross-engine values and count the underlying sync calls.

An automatic or membership checkpoint does not copy state. The node:

1. Records the applied index and membership after the apply worker finishes earlier entries.
2. Flushes SlateDB. The flushed state holds the apply marker at the same sequence number.
3. Syncs snapshot metadata without an image, the commit marker, and truncation in one WAL batch.
4. Removes any prior image file and asks raft-engine to reclaim obsolete WAL segments.

Local objects use synced writes, so a flush survives power loss like an S3 write.
Restart reads an apply marker at or above the truncation index and replays the later entries.
If state is behind a record without an image, startup stops with an explicit error.

A manual checkpoint, or a leader whose peer needs a snapshot, builds a complete image:

1. Pins a full SlateDB scan at the applied index, then flushes SlateDB and encodes the scan.
2. Writes and syncs a private temporary file on a blocking worker. The worker computes its CRC.
3. Renames the complete image and syncs its directory on the WAL thread.
4. Syncs its reference, checksum, metadata, commit marker, and truncation in one WAL batch.
5. Removes the prior file and asks raft-engine to reclaim obsolete WAL segments.

Raft asks for a snapshot through synchronous storage. Without an image, storage reports a
temporary failure and records the request. The owner then builds an image at the applied index.
That image can replace a record without an image at the same index. Raft retries on a later
heartbeat and sends the image. Automatic truncation waits while a peer receives a snapshot.
The image stays in memory until the next truncation replaces it.

The saved snapshot retains the boundary term and membership at the same applied index. On snapshot
receipt,
the node atomically replaces its persistent Raft state before changing SlateDB. A crash in between
is safe: startup can repeat the state replacement from the durable snapshot payload.

An image file contains a full copy of the state. This makes recovery independent of
uncopied local object files, but limits the PoC to small data sets. Snapshots use the peer HTTP body
limit of 64 MiB. Snapshot creation runs in the background.
The file writer samples four 4 KiB regions in images of at least 64 KiB.
It selects LZ4 when the sample shrinks by more than half. Other images use the original bytes.
The encoder uses 64 KiB blocks. The WAL records the stored length for compressed files.
Recovery checks both lengths and the CRC of the decoded image. Peer transfers use the full image.
Members use different positions in each automatic checkpoint interval.
Their rank in the configured peer list sets the position. Manual checkpoints bypass this schedule.
Incoming peer snapshots use the ordered WAL thread for both file preparation and publication.
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
The standalone server defaults to one async worker per available CPU. `--runtime-workers` can
change this count.
Embedded servers use their caller's runtime.
Only a completed state write advances `applied_index`. Quorum Put preparation is memory-only.
Committed work stays in the durable log until the worker can accept it.
A batch has at most 32 entries and a 512 KiB target. One larger entry can exceed the byte target.
The worker also handles Watch scans, snapshot installation, and checkpoint pins.
KV reads use separate bounded tasks. Diagnostic queries still receive service between entry batches.

The owner queues at most 64 state queries, 64 KV reads, and 64 checkpoint requests.
At most four KV read jobs run. Each point-read job contains at most 16 requests.
Up to three streaming scans run together. A streaming scan is a current Range in key order
without revision filters. It decodes at most `limit` rows and stops the scan at a 4 MiB row budget.
A scan over budget runs again alone on the same pinned view and with the same deadline.
Historical reads, sorted or filtered Ranges, and read-only transactions also run alone.
While one of these reads runs or waits, no new scan starts. Wide reads start in queue order.
Current point reads can pass queued scans and keep at least one task.
Each job has a five-second deadline.
Shared scans hold at most 3 x (4 MiB + one row) of decoded rows. The exclusive read keeps the old
single-read bound: its result size, up to the logical data size. The two cases never overlap.
These bounds limit tasks, retained views, and scan rows, not encoded response bytes.
At 1,024 committed but unapplied entries, the owner stops admitting new Raft proposals.
A leader can hold up to 256 client writes before proposal when apply or log I/O falls behind.
Puts, transactions, deletes, lease and auth writes, and compactions share one FIFO queue.
Membership changes still fail at once. The queue holds up to 4 MiB of encoded data.
A write that waits five seconds fails with `too many requests`, like etcd's apply-lag error.
A leadership change fails queued writes with `not leader` before Raft can propose them.
A cancelled client drops its queued write. Every queued-write error means it was not proposed.
Worker failure or panic stops the node. Shutdown joins reads before closing state storage.
The log thread performs steady-state disk I/O, file reclamation, and disk statistics.
Apply waits for committed Ready delivery and local entry durability.
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
Leadership loss, timeout, or cancellation removes the pending read. At most 256 reads can wait.
The gRPC service admits 192 linearizable KV reads at a time. Up to 8,192 more wait for admission.
A waiter that gets no slot in ten seconds fails with `Unavailable: request timed out`.
A cancelled client releases its waiting slot.
Forwarded writes use a separate admission queue while they wait for local apply.
Matching wide reads can share a scan after each has a quorum proof. They use one pinned view,
the same request, and the same credentials. Each RPC owns its response before encoding it.
A frozen batch shares one quorum context. Later requests use a new context.
These reads do not append log entries. Unconfirmed batches retry every two Raft ticks.
Serializable reads remain local and can return older state.
The gRPC KV path passes typed responses internally. Diagnostic callers retain JSON responses.
The read executor groups up to 16 adjacent point reads. Other read types end a group.
Each job shares a pinned SlateDB view with matching revision, compaction, Auth, and Lease metadata.
The apply owner publishes this view after a complete state write or snapshot installation.
A read can finish from an older view while later writes apply. Its proof must cover that view.
Read completion does not move the apply index or copy state metadata.
The owner still checks the read proof and apply position before it sends a response.
Read evaluation does not construct a discarded write batch.

The apply owner keeps an in-memory key index with revision metadata for each current key.
Put, transaction, and quota paths read old metadata from it instead of SlateDB.
See the [key index](architecture.md#key-index) for its update rules and memory cost.
Consecutive etcd KV entries, including transactions, share writes within an apply job.
A bounded overlay lets later point reads and compares see earlier writes in that group.
A request with a range scan, a historical read, or an authorized leased put starts a new group.
Lease, Auth, administration, compaction, membership, and empty entries end a group.
Quota checks use each request's updated logical size. Failed requests do not advance the revision.
No response leaves the worker before the combined storage write completes.
`state_write_batches` counts storage writes since startup. It includes non-Put state operations.

Log work uses a FIFO queue with 64 jobs and a 16 MiB entry admission target.
Snapshots use a separate 64 MiB target. One Ready can exceed a target.
New proposals stop at the entry or job limit.
Empty Ready barriers finish on the owner after earlier writes finish.
Adjacent queued Ready jobs share a sync. Other log job types form batch boundaries.
Disk statistics run only for `/debug/log-stats` requests.
Raft advances durable progress and releases dependent messages only after log completion.
Leader replication can run before local persistence, as permitted by raft-rs.
Each peer transport uses a persistent gRPC stream for ordinary Raft messages.
A sequence check and a 64-message receipt window bound outstanding work.
The receiver admits messages within the same count and byte limits while earlier receipts wait.
This permits shared log writes across frames. It returns receipts in arrival order.
Encoded payloads also have a separate 16 MiB admission target per peer.
One frame can exceed that target. Each frame has an 8 MiB plus 1 KiB limit.
The stream requires progress within two seconds when messages are pending.
Failure reports the peer as unreachable. Raft retries lost messages on a later connection.
A snapshot waits for older receipts and uses the durable HTTP endpoint.
Later messages wait for its result. Failed streams also reject any waiting snapshot.
Ordinary stream receipts confirm delivery; snapshot receipts confirm durable snapshot storage.
Neither receipt replaces Raft's durable vote or append acknowledgement.

Status separates `raft_commit_index`, log-worker-confirmed `commit_index`, and `applied_index`.
The worker confirms entry durability before advancing `commit_index`.
Its marker can remain unsynced.
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
History rows sort by user key and then by revision, because the hex key encoding keeps key order.
A historical point read uses one descending scan that ends at the requested revision.
A key-ordered historical Range without filters first tries the Watch journal after its revision.
A key that no later journal event names has the same current row now as at the revision.
The Range merges those current rows with the versions at the revision of the named keys.
It stops after `limit + 1` rows. Its count starts from the current-key count of the pinned view.
Each named key then adds or removes one.
The journal row of a revision shares its batch, and compaction keeps rows after the boundary.
A page then costs about `limit` current rows plus the writes after its revision.
It does not depend on the history size.
Debug builds also read the history rows and check the result against them.

The Range reads the history rows of its key interval instead in two cases.
The view has no key order, or the later journal has more events than 1024 or the current-key
count of the interval, whichever is larger.
This scan reads the versions of a key up to the revision by row key and decodes only one.
It reads up to eight later versions and then seeks past the rest.
A key with more than 16 versions up to the revision reads its current row instead.
If that row is newer than the revision, one descending scan reads the version.
The cost is about one row and one seek per key in the interval.
Other historical Ranges collect the interval in memory before they sort or filter it.
Range scans set `cache_blocks`, so repeated scans read blocks from the state block cache.
SlateDB scans otherwise bypass that cache and fetch every block from the object store.

Compact stores the boundary and a progress record with the apply marker in one SlateDB batch.
It does not advance the KV revision. Reads below the boundary return the standard
compacted-revision error at once. Reads at the boundary remain valid.

Compaction keeps the latest version at or before the boundary for each key, including tombstones.
It keeps all newer versions. Current keys and their revision metadata remain unchanged.
Restart and snapshot installation restore the boundary, the progress record, and retained history.
Raft log GC remains separate from MVCC compaction.

Physical removal runs in later `CompactStep` Raft entries. The leader proposes one step at a time
and pauses 2 ms between steps. Each step scans about 2,048 rows and writes one SlateDB batch.
A step first hashes the rows for the retained compaction hash. It then removes old history
versions and their hash records in key order, removes old Watch events, and finishes the hash.
All versions of one key change in the same step. Every replica applies the same steps at the
same indexes, so quota accounting and snapshots stay deterministic.
The progress record shares each step batch with the apply marker. A restart replays lost steps
from the Raft log. A later Compact waits in the record until earlier boundaries finish.
HashKV skips rows that the pending pass will remove, so its result does not depend on progress.
Quota accounting uses the stored row lengths. Each deleted row contributes its original size once.
Each member flushes SlateDB outside ordered apply when a step finishes a physical boundary.
Writes can apply during this flush. One flush can run at a time.
The highest queued index covers intervening requests.
A physical reply requires the finished pass and a flush through its own Compact index.
These replies do not use the 5 s proposal timeout. They wait until the client disconnects.
A forwarding member also waits for its local durable physical boundary.
A repeated physical request marks the pending boundary as physical. It waits for that boundary
before it returns the compacted error.
A storage error stops the node. On restart, Raft replays the missing steps.
Defragment rewrites the current SST set and waits for manifest publication.
SlateDB garbage collection reclaims obsolete objects later. Pinned checkpoints can retain them.
Do not downgrade to an older binary after compaction; older binaries do not enforce its boundary.

The diagnostic JSON keyspace is separate from the etcd keyspace and does not affect etcd revisions.

## Watch and Lease recovery

Each MVCC write stores ordered Watch events with the key changes and revision in one batch.
A Put event stores the history revision of its previous value. Watch reads that version only
when the request asks for `prev_kv`. Delete events keep the previous value inline.
A transaction keeps request order within one revision. Lease revocation deletes all attached keys
at one revision. Watch replay uses this journal after restart and snapshot installation.
Compaction removes events below the boundary. Events at the boundary remain available.
Previous values are omitted when the revision before the event is below the boundary.

Watch runs on the receiving replica and can lag the leader. It does not provide linearizable reads.
After each apply job, the node publishes the new read view, then that job's journal rows.
The feed is an in-memory broadcast channel with 1,024 batches.
A batch with more than 128 KiB of journal rows, or a snapshot installation, is incomplete.
Each stream filters feed events in memory for its synced watchers.
A watcher that starts in history, follows a feed gap, or follows an incomplete batch reads the
journal from the newest pinned view. That scan runs in the stream task, not on the apply worker.
One scan page serves all such watchers of the stream and contains at most 32 revisions.
Each delivered revision advances the watcher start revision, so the feed handoff has no duplicates.
The output channel buffers 32 responses.
Each stream supports at most 1,024 watches.
Automatic progress uses the configured progress interval, 10 seconds by default.
A stream check runs at that interval, or every second if shorter. It sends automatic progress and
rechecks token expiry.
Progress replies follow all events through their reported revision. Future watches delay progress.
The fragment option splits large responses near 1 MiB. Each encoded response must fit within 64 MiB.

Databases from binaries without Watch have no complete event journal.
Historical watches before the first recorded revision return a compacted-revision error.
Existing KV history remains readable. Upgrade all nodes together; mixed-version operation is
untested.
Do not downgrade after lease commands enter the Raft log.

Lease grants, revocations, expiry, TTL queries, and listing enter Raft.
The leader serves KeepAlive without a Raft entry or a disk write, as etcd does.
A follower returns `not leader`, and its gRPC layer forwards the request to the leader.
Each renewal waits for a ReadIndex proof from a quorum in the current term.
Renewals that arrive during one proof round share the next proof.
The leader then waits until it applies the proof index and extends its in-memory deadline.
A minority leader cannot get a proof, so it cannot acknowledge a renewal.
The leader measures expiry with a monotonic clock.

The leader proposes expiry only after the deadline passes. A renewal after the deadline fails.
It returns TTL 0, and the deadline does not move again in that term.
So an expiry proposal in one term cannot race a renewal in the same term.
An expiry proposal from an older term commits before the first entry of the current term.
A renewal waits for apply through its proof index, so it sees the result of that expiry.
Expiry also includes the lease grant generation. An old expiry cannot remove a new grant.
With Auth enabled, KeepAlive keeps its Raft path, because apply checks attached key permissions.
That entry sets a new generation, so an older expiry has no effect on the renewed lease.
Lease metadata, attached key deletion, Watch events, and the apply marker commit together.
Followers do not expire leases from their local clocks. SlateDB TTL does not control leases.

The minimum TTL is two seconds. A new leader gives each surviving lease its full granted TTL
plus the longest election timeout, two seconds by default. etcd's Promote adds the same extension.
Local renewals are lost on leader change. The service does not checkpoint remaining TTL.
A renewal is cancelled if apply reaches its proof index more than that extension after the proof.
The old leader therefore cannot promise a deadline later than the one the next leader sets.
Repeated leader changes can extend a lease's lifetime.
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
