# SlateDB etcd service

This service runs one Raft group with three fixed voters. Each process uses `raft-rs` for consensus,
`raft-engine` for durable logs, and SlateDB for its state machine. SlateDB's native WAL is disabled.

The service exposes the etcd v3 gRPC KV API. It supports binary keys, values, and MVCC history.
The original JSON API remains available in a separate diagnostic keyspace.
All endpoints bind to loopback. There is no authentication or dynamic membership.

See [compatibility results](COMPATIBILITY.md) for the etcd checks.
See [test commands](tests/README.md) for upstream tests, fault checks, Jepsen, fuzzing, and DST.
The [upstream audit](tests/upstream/README.md) maps the source suites and lists coverage gaps.
The [original PoC results](RESULTS.md) describe measurements before this repository was extracted.

## etcd v3 KV API

The same port serves HTTP diagnostics and plaintext gRPC. No separate client port is required.
Any replica accepts client requests. Followers forward writes and linearizable reads to the leader.
Serializable Range requests read the local replica. Linearizable reads currently enter the Raft log.
A read-only Txn also stays local when both branches contain only serializable Range operations.

Supported behavior:

- Range: exact keys, byte ranges, prefixes, historical revisions, sort, limit, count, and filters.
- Put: create and modification revisions, key version, previous value, and ignore flags.
- DeleteRange: exact keys, ranges, previous values, tombstones, and no-op deletion.
- Txn: comparisons, success and failure branches, nested transactions, and atomic multi-key writes.
- Compact: a replicated history boundary, removal of older versions, and recovery of that boundary.

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
Watch, Lease, Auth, Cluster, and Maintenance are not implemented.
Compact supports the default `physical=false` option. `physical=true` returns Unimplemented.
A Put with a nonzero lease returns the standard lease-not-found error.
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
The baseline persists before sending all Ready messages, including those Raft permits it to send
early. It also persists the commit index before applying entries. This is conservative and leaves
room to overlap leader disk I/O with replication later.

The leader replies only after quorum commit and local apply. Followers apply the same committed
commands in order. Each command updates its business keys and `meta/applied` in one SlateDB batch.
The SlateDB sequence number is the Raft index. Even an empty Raft entry advances the apply marker.
This index is not an etcd revision.

Reads in `/kv` enter the Raft log as barriers. This provides a simple linearizable read path, but
adds log writes. A later implementation can use ReadIndex and an apply barrier instead.

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
limit of 64 MiB. Snapshot creation runs in the background. Snapshot persistence, installation, and
state apply still run on the node loop and can raise tail latency. Production use needs separate
apply workers and bounded snapshot transfer.

The local PoC polls compaction and manifest updates every 100 ms. The default slower polling caused
L0 backpressure during repeated checkpoints in an earlier run. That run is retained in
`results/before-background-snapshots.json`.
Cloud deployments need their own polling and cost policy.

The `durable_applied_index` status field is the last observed explicit flush boundary. Automatic
SlateDB flushes can advance storage beyond it. `snapshot_index` is the durable log GC boundary.

Each replica owns an independent object-store directory and manifest. The PoC does not let three
writers share a manifest. A shared SST design needs a separate ownership and reference-GC protocol.

## MVCC storage and recovery

SlateDB stores the current value for each key and one history record for each changed revision.
History records include deletions. The revision, key changes, and Raft apply marker use one batch.
Raft log replay and full snapshots restore both current state and historical versions.
The revision is independent of the SlateDB sequence number, which tracks the Raft apply index.

Point reads use the current-key index. Current range reads scan the requested key interval.
Historical point reads scan one key's versions. Historical range reads currently scan all history.
Range responses collect matching keys in memory before sorting or applying a limit.
These choices favor compatibility testing. Large data sets need bounded scans and a history index.

Compact runs through Raft and writes history deletions, the compaction boundary, and the apply marker
in one SlateDB batch. It does not advance the KV revision. Reads below the boundary return the
standard compacted-revision error. Reads at the boundary remain valid.

Compaction keeps the latest version at or before the boundary for each key, including tombstones.
It keeps all newer versions. Current keys and their revision metadata remain unchanged.
Restart and snapshot installation restore the boundary with the retained history.
Raft log GC remains separate from MVCC compaction.

Compaction scans all history and builds one deletion batch on the node loop. Large histories can
delay heartbeats and requests. SlateDB reclaims old SST data in the background; pinned snapshots can
retain older data. The service rejects `physical=true` because it cannot yet wait for that reclamation.
Do not downgrade to an older binary after compaction; older binaries do not enforce its boundary.

The diagnostic JSON keyspace is separate from the etcd keyspace and does not affect etcd revisions.

## Next steps

1. Add ReadIndex-based linearizable reads and separate state-apply workers.
2. Bound compaction work and range scans, and use checkpoint references for large snapshots.
3. Add Watch history replay, live delivery, compaction errors, and progress notifications.
4. Add leases with replicated revocation and deletion. Do not use local SlateDB TTL for leases.
5. Add membership changes, TLS, authentication, quotas, maintenance APIs, and operational metrics.

Test object-store outages, slow disks, interrupted snapshot installation, and cross-host failover
before using the service for coordination.
