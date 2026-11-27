# Module boundaries

The service has one Raft group. Each replica owns its Raft log and its in-memory overlay.
All replicas share one SlateDB in S3. Only the leader writes it.
The CLI and embedded API share the same lifecycle and runtime paths.

## Ownership

The table gives the responsibility of each module. A module is not always the owner of its data:

- `Node` is one type. Its `impl` blocks are in `node.rs`, `node/auto_compaction`,
  `node/cluster`, `node/compaction`, `node/lease`, `node/maintenance`, `node/proposal`,
  `node/quorum_put`, `node/read`, `node/ready`, `node/schedule`, `node/shared`, and `node/step`.
  A method in each of these files can read and write each `Node` field.
- `StateMachine` is one type too. Its `impl` blocks are in `state.rs`, `state/auth`,
  `state/backup`, `state/checkpoint`, `state/cluster`, `state/compaction`, `state/diagnostic`,
  `state/kv`, `state/lease`, `state/maintenance`, `state/put_batch`, `state/quota`,
  `state/read`, `state/shared`, `state/shared/adopt`, and `state/snapshot`.
- Some modules own a type with private fields, and the other modules call its methods:
  `Writer` in `node/shared/writer`, `EarlyPuts` in `node/quorum_put`, `Group` in
  `node/proposal`, `Publisher` in `node/forwarding` and `node/watch`, the log `Worker` in
  `node/log`, `KeyIndex` in `state/key_index`, `Overlay` in `state/overlay`, and `RowCache` in
  `state/rows`.

| Module | Responsibility |
| --- | --- |
| `metrics/rpc` | Count gRPC messages, status, and optional call duration |
| `metrics/peer` | Measure send failures, disconnects, and peer round trips |
| `server` | Start listeners, report readiness, and join workers at shutdown |
| `grpc` | Validate public requests and encode etcd responses |
| `grpc/forwarding` | Resolve peer channels and wait for local revision progress |
| `node/forwarding` | Publish applied routes and the member revision |
| `command` | Encode and decode internal Raft commands |
| `node` | Own RawNode, admission, elections, and client completion |
| `node/ready` | Order Ready delivery, durable completions, and peer acknowledgements |
| `node/log` and `node/cache` | Isolate disk work and bound retained log memory |
| `node/schedule` | Dispatch state work and publish completed apply positions |
| `node/watch` | Publish applied views and Watch event batches to local streams |
| `node/apply` | Apply committed entries in order and group ordinary puts |
| `node/proposal` | Group consecutive leader Put proposals within one event turn |
| `node/quorum_put` | Evaluate Puts before commit. Reply after commit in the term (`EarlyPuts`) |
| `state/key_index` | Keep revision metadata for every current key in memory |
| `node/read` | Confirm Safe ReadIndex and reject obsolete proofs |
| `node/read/executor` | Run bounded reads on immutable applied views |
| `node/lease` | Schedule leader lease expiry from replicated generations |
| `state/quota` | Check the logical size against the quota of the cluster claim |
| `node/cluster` | Propose the term-start entries, publish client URLs, and serve member requests |
| `state/cluster` | Seed, change, and check the membership row |
| `state` | Open storage and own mutable applied metadata |
| `state/kv` and `state/put_batch` | Commit MVCC changes and maintain quota accounting |
| `state/read` | Pin data, revision, Auth, and Lease metadata together |
| `state/source` | Define the read interface that apply, read views, and Watch catch-up share |
| `state/view` | Read a shared base and its overlay |
| `state/overlay` | Keep the batches above the base for views and the materializer |
| `state/shared` | Claim the shared state, open, switch, adopt, and rebase checkpoint bases |
| `node/shared` | Run the materializer and publisher with fence and clock checks |
| `node/shared/writer` | Own the writer task state, the apply pause, base switches, and proofs |
| `status` | Read the Raft snapshot record of a stopped member for `shared-status` |
| `state/auth` and `state/lease` | Apply their domain changes |
| `state/backup` and `state/shared/restore` | Export and restore complete state |
| `state/maintenance` | Execute hashes, alarms, and physical storage work |
| `state/diagnostic` | Execute the separate diagnostic keyspace API |
| `mvcc` and `watch` | Define revision, transaction, history, and event semantics |
| `storage`, `peer`, `tls`, and `object` | Bind durable logs, transport, security, and objects |
| `peer/receive` | Bound incoming Raft work and return ordered processing receipts |

Domain modules extend the same StateMachine. They do not add independent mutable state owners.
The read view contains immutable metadata, a pinned base checkpoint, and overlay segments.
Only completed apply work can publish a new view.
Views share unchanged Auth and Lease maps. Apply completions also share membership metadata.
Their mutation paths copy retained maps before changing them.

## Request paths

A write enters gRPC, passes admission, and becomes a Raft command.
The leader replicates it. The apply worker waits for quorum commit and durable local entries.
Compatible consecutive puts share one storage batch. Each request keeps its own revision and result.
Transactions, Lease, Auth, membership, compaction, and snapshots form batch boundaries.
A leader can return ordinary Put results after a durable quorum commits them.
The prepared changes stay in memory until the local log covers their publication.

A linearizable read first obtains a Safe ReadIndex proof.
The owner waits for local apply to cover that index, then captures the current immutable view.
A read task evaluates the request. The owner checks the proof again before returning its result.
Serializable reads use a local view without a quorum proof.
Watch streams take events from an in-memory feed and scan history on pinned views.
Administrative scans still use the apply owner.

Apply adds each batch to the overlay, not to SlateDB.
The leader opens the shared `Db` and writes the batches into it with their own sequences.
The publisher flushes by age, creates one checkpoint per L0 advance, and proposes `ViewCheckpoint`.
That entry closes an overlay segment. The opened checkpoint becomes the base of later views.
Each member truncates its log through its base and sends that checkpoint as a format 6 snapshot.
The log worker saves the snapshot record and truncates its covered entries in one synced batch.
Restart opens a checkpoint, installs a newer durable snapshot, and replays the remaining entries.

## Change rules

Keep etcd response headers, errors, revisions, transactions, and permissions in the compatibility
contract. Internal API and storage changes need no old-version adapter.
Storage format 4 rejects older data directories. Native backups have a separate format.
Mixed-version clusters are unsupported.

Use a named domain module for new state transitions. Keep one ordered mutation owner.
Do not read live storage with revision or Auth metadata from a different applied position.
Do not merge separate requests into one etcd transaction revision.
Do not bypass local durability, quorum commit, or the ReadIndex apply barrier for a benchmark gain.

## Validation and performance

Run differential cases against pinned etcd v3.6.5, imported common tests, and downstream clients.
Run crash boundaries, deterministic schedules, process histories, and the etcd and Jepsen models.
Use the [test guide](../tests/README.md) for commands and known coverage limits.

Compare the base, candidate, and etcd on the same host with identical request distributions.
Use repeated one-member and three-member runs. Retain every sample and request error.
Report throughput, p50 and p99 latency, server CPU per operation, and summed peak server RSS.
Keep CPU profiling and compilation outside performance measurements.

A faster result in one case does not establish general superiority over etcd.
Report regressions and workload limits. Use [benchmarks](benchmarks.md) for the measured matrix.
The [upstream notes](raft-performance-notes.md) explain the etcd and TiKV design references.
