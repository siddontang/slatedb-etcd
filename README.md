# SlateDB etcd service

This service runs one Raft group with three fixed voters. Each process uses `raft-rs` for consensus,
`raft-engine` for durable logs, and SlateDB for its state machine. SlateDB's native WAL is disabled.

The service exposes the etcd v3 gRPC KV, Watch, and Lease APIs. It supports binary keys, values, and
MVCC history.
The original JSON API remains available in a separate diagnostic keyspace.
All endpoints bind to loopback. There is no authentication or dynamic membership.

See the [architecture and roadmap](docs/architecture.md) for design rules and implementation phases.
See [compatibility results](COMPATIBILITY.md) for the etcd checks.
See [test commands](tests/README.md) for upstream tests, fault checks, Jepsen, fuzzing, and DST.
The [upstream audit](tests/upstream/README.md) maps the source suites and lists coverage gaps.
The [original PoC results](RESULTS.md) describe measurements before this repository was extracted.

## etcd v3 APIs

The same port serves HTTP diagnostics and plaintext gRPC. No separate client port is required.
Any replica accepts client requests. Followers forward writes to the leader.
Serializable Range requests read local applied state. Linearizable reads use Safe ReadIndex.
The receiving member waits for local apply and executes the read.
The same rules apply to read-only transactions, including both nested branches.

Supported behavior:

- Range: exact keys, byte ranges, prefixes, historical revisions, sort, limit, count, and filters.
- Put: create and modification revisions, key version, previous value, and ignore flags.
- DeleteRange: exact keys, ranges, previous values, tombstones, and no-op deletion.
- Txn: comparisons, success and failure branches, nested transactions, and atomic multi-key writes.
- Compact: a replicated history boundary, removal of older versions, and recovery of that boundary.
- Watch: live events, history replay, filters, previous values, cancellation, progress, and
  fragments.
- Lease: grant, revoke, streamed keepalive, TTL, attached keys, listing, and replicated expiry.

A mutating transaction advances the global revision once. Reads and empty deletes do not advance it.
The initial revision is 1. Invalid transactions leave the keyspace and revision unchanged.
Nested predicates use the state before the transaction. Operations then execute in request order.
The implementation rejects duplicate writes and overlapping Put/DeleteRange operations.

Use a standard etcd client against any node:

```sh
etcdctl --endpoints=http://127.0.0.1:7001 put hello world
etcdctl --endpoints=http://127.0.0.1:7002 get hello --write-out=json
etcdctl --endpoints=http://127.0.0.1:7003 del hello --prev-kv
```

The official Go client integration tests also exercise Txn and historical reads.
Auth, Cluster, and Maintenance are not implemented.
Compact supports the default `physical=false` option. `physical=true` returns Unimplemented.
A Put with an unknown lease returns the standard lease-not-found error.
This service is not yet a complete etcd replacement or a supported Kubernetes backend.

## Build and verify

Run these commands from the repository root:

```sh
scripts/cargo.sh build --release -p slatedb-etcd
scripts/cargo.sh test -p slatedb-etcd
scripts/cargo.sh clippy -p slatedb-etcd --all-targets -- -D warnings
python3 scripts/harness.py test \
  --output results/etcd-core-fault-tests.json
python3 scripts/check-compat.py
```

Rust uses the repository's pinned toolchain. Python 3.9 or later needs no extra packages.
The compatibility suite requires Go 1.24 or later. It pins the official client and etcd to v3.6.5.
It starts its own reference etcd and three service processes, then stops them after the tests.
The build wrapper handles an old `protobuf-build` version check on Apple Silicon. It uses that
crate's bundled protoc 3.9 through Rosetta when the system protoc is incompatible. Alternatively,
set `PROTOC` to a working protoc 3.x compiler. The wrapper does not change the system installation.

The harness starts three separate processes with separate data directories and TCP ports. It sends
SIGKILL for crash tests. It keeps run directories and logs for inspection, and stops its processes
when the run ends. Its JSON report records the binary hash and the run directory.

The fault suite checks:

- One leader and atomic batch ordering.
- Acknowledged writes after the leader dies, followed by follower recovery.
- All three processes dying before any explicit SlateDB flush.
- Logical log truncation and physical log file deletion.
- An offline follower installing a snapshot after its missing logs have been deleted.
- Full restart after log GC.
- Minority write rejection and replacement of an uncommitted suffix after a partition.
- Replicated deletion and linearizable reads.
- Writes and heartbeats during a background snapshot delayed beyond the election timeout.

Rust tests also cover persistent terms, votes, conflicting log suffixes, node identity checks, and
recovery when a snapshot was saved before the state machine could install it.
They also check that a background snapshot excludes writes after its pinned apply position.

The imported etcd common suite runs through a real cluster adapter. Unsupported capabilities produce
explicit skips. Process fault histories use the original etcd model, plus Jepsen and Elle checks.
The DST harness drives real node storage through seeded message delivery and logical Raft ticks.
It repeats each schedule and compares traces. It does not simulate the operating system or disk.

## Measure write latency

These commands benchmark the diagnostic JSON API. They do not measure gRPC or MVCC overhead.

```sh
python3 scripts/harness.py bench \
  --requests 20000 --concurrency 1 16 64 --value-size 256 \
  --output results/latency.json

python3 scripts/harness.py bench \
  --requests 20000 --concurrency 1 16 --value-size 256 --snapshot-every 1000 \
  --output results/latency-with-snapshots.json
```

Each concurrency level sends the requested total number of single-key writes after a shared warmup.
Clients reuse HTTP connections. Latency includes client serialization, HTTP, quorum persistence,
leader apply, and the response. The report includes failures, throughput, and latency percentiles.
This is a closed-loop Python client. Client scheduling and the GIL can affect concurrent results.

The default harness disables automatic snapshots to measure the hot write path. The second command
includes periodic snapshots. Compare both results before drawing conclusions about tail latency.

All replicas and the client share one host and disk. SlateDB uses `LocalFileSystem` as its object
store. These measurements are not S3, cross-host, or cross-AZ results. SIGKILL tests cover process
crashes. They do not verify power-loss behavior, failed disks, or independent failure domains.

## Run a node manually

Run this command in three terminals. Set `NODE_ID` to 1, 2, and 3, respectively.

```sh
NODE_ID=1
target/release/slatedb-etcd \
  --id "$NODE_ID" --listen "127.0.0.1:700$NODE_ID" \
  --peer 1=127.0.0.1:7001 --peer 2=127.0.0.1:7002 --peer 3=127.0.0.1:7003 \
  --dir "tmp/raft-poc/node-$NODE_ID" --cluster-id demo --snapshot-every 1000
```

Check each `/status` endpoint to find the leader. Send requests to that node:

```sh
curl http://127.0.0.1:7001/status
curl -H 'Content-Type: application/json' http://127.0.0.1:7001/kv \
  -d '{"operations":[{"op":"put","key":"hello","value":"world"}]}'
curl -H 'Content-Type: application/json' http://127.0.0.1:7001/kv \
  -d '{"operations":[{"op":"get","key":"hello"}]}'
curl -H 'Content-Type: application/json' http://127.0.0.1:7001/checkpoint -d '{}'
```

A follower returns HTTP 409 with `leader_id`. A timeout or leadership change can leave the outcome
unknown. The client must not interpret this response as proof that a request was never committed.
Requests have no retry deduplication guarantee.

`GET /debug/value?key=hello` reads a local replica without a consensus barrier. It is only a test
endpoint and can return stale data. `POST /debug/partition` accepts `{"blocked":[2,3]}` to drop peer
traffic in both the receive and send paths. Send `{"blocked":[]}` to restore traffic.
`POST /debug/snapshot-delay` accepts `{"delay_ms":2500}` for the background snapshot test.

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

The saved snapshot retains the boundary term and fixed voter configuration. On snapshot receipt,
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
`results/before-background-snapshots.json`.
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
Forwarded KV, Lease, and Compact results also wait for a local barrier.
Later STM reads on that member can then use their revisions.
Both branches of nested transactions must contain only reads.
Leadership loss, timeout, or cancellation removes the pending read. At most 64 reads can wait.
These reads do not append log entries. Unconfirmed requests retry every two Raft ticks.
Serializable reads remain local and can return older state.

Log work uses a FIFO queue with 64 jobs and a 16 MiB admission target.
One Ready or full snapshot can exceed the target. New proposals stop at the queue limit.
Raft advances durable progress and releases dependent messages only after log completion.
Leader replication can run before local persistence, as permitted by raft-rs.
Ordinary HTTP receipt confirms delivery; snapshot receipt waits for durable snapshot storage.

Status separates `raft_commit_index`, durable `commit_index`, and `applied_index`.
It also reports `cached_last_log_index`, `log_busy`, `queued_log_jobs`, `queued_log_bytes`,
and `pending_read_indexes`.
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
SlateDB reclaims old SST data in the background; pinned snapshots can
retain older data. The service rejects `physical=true` because it cannot yet wait for that
reclamation.
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
See the [log pool protocol](docs/architecture.md#bounded-log-pools)
for accounting and recovery rules.

## Next steps

1. Bound compaction work and range scans, and use checkpoint references for large snapshots.
2. Add lease TTL checkpoints and a key index for leases. Bound Watch catch-up work.
3. Add membership changes, TLS, authentication, backend quotas, and maintenance APIs.
4. Add remote object storage, cross-host tests, and operational metrics.

Test object-store outages, slow disks, interrupted snapshot installation, and cross-host failover
before using the service for coordination.
