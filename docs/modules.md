# Module boundaries

The service has one Raft group. Each replica owns its log, state, and object namespace.
The CLI and embedded API share the same lifecycle and runtime paths.

## Ownership

| Module | Owner and responsibility |
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
| `node/apply` | Apply committed entries in order and group ordinary puts |
| `node/proposal` | Group consecutive leader Put proposals within one event turn |
| `node/quorum_put` | Prepare exact Put results after quorum commit |
| `state/checkpoint` | Merge bounded row changes into a complete checkpoint |
| `node/read` | Confirm Safe ReadIndex and reject obsolete proofs |
| `node/read/executor` | Run bounded reads on immutable applied views |
| `node/lease` | Schedule leader lease expiry from replicated generations |
| `state` | Open storage and own mutable applied metadata |
| `state/kv` and `state/put_batch` | Commit MVCC changes and maintain quota accounting |
| `state/read` | Pin data, revision, Auth, and Lease metadata together |
| `state/auth`, `state/cluster`, and `state/lease` | Apply their replicated domain changes |
| `state/snapshot` and `state/backup` | Build, install, export, and restore complete state |
| `state/maintenance` | Execute hashes, alarms, and physical storage work |
| `state/diagnostic` | Execute the separate diagnostic keyspace API |
| `mvcc` and `watch` | Define revision, transaction, history, and event semantics |
| `storage`, `peer`, `tls`, and `object` | Bind durable logs, transport, security, and objects |
| `peer/receive` | Bound incoming Raft work and return ordered processing receipts |

Domain modules extend the same StateMachine. They do not add independent mutable state owners.
The read view contains immutable metadata and a pinned SlateDB snapshot.
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
Watch and administrative scans still use the apply owner.

A checkpoint freezes one applied prefix. The builder flushes and encodes that prefix outside Raft.
The log worker saves the snapshot and truncates its covered entries in one synced batch.
Restart installs a newer durable snapshot, then replays the remaining committed entries.

## Change rules

Keep etcd response headers, errors, revisions, transactions, and permissions in the compatibility
contract. Internal API and storage changes need no old-version adapter.
Storage format 3 rejects older directories and snapshots. Mixed-version clusters are unsupported.

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
