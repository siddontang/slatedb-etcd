# Three-replica PoC results

Run date: 2026-11-15, Asia/Shanghai. Branch: `codex/raft-cluster-poc`.
SlateDB base: `8b68800252064164397ca6ce53e5883dcfbc37b9`.
Versions: SlateDB 0.18.1, raft-rs 0.7.0, raft-engine 0.4.2, Rust 1.91.1.

The final runs use the same release binary:

```text
933c21d5a44ccb9150c6cd4bdaac765a830af598da92123bdbb7eb9c94460456
```

## Conditions

- Three separate processes on one ARM64 macOS host with 10 logical CPUs.
- Three independent local raft-engine directories and SlateDB object-store directories.
- HTTP over loopback. A Python 3.9 client reuses connections.
- Each request writes one new key and a 256-byte value.
- Each concurrency level sends 20,000 requests after one shared 100-request warmup.
- Raft entries and metadata use synchronous writes. The leader replies after commit and apply.
- SlateDB's native WAL is disabled. Periodic snapshot creation and its flush run in the background.

These are single-run measurements. They are not cross-host, S3, or cross-AZ measurements.
The closed-loop Python client can limit throughput. These figures do not measure engine capacity.
No comparison with etcd or SlateDB's native object-store WAL was run.

## Write latency

All times are milliseconds. All 100,000 measured requests succeeded.

| Snapshot trigger | Clients | p50 | p95 | p99 | Maximum | Writes/s |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Disabled | 1 | 0.249 | 0.299 | 0.331 | 7.799 | 3,761 |
| Disabled | 16 | 3.259 | 6.192 | 9.659 | 13.480 | 4,530 |
| Disabled | 64 | 12.997 | 19.999 | 22.879 | 25.772 | 4,636 |
| Every 1,000 entries | 1 | 0.258 | 0.348 | 0.462 | 14.011 | 3,494 |
| Every 1,000 entries | 16 | 3.387 | 6.812 | 9.919 | 18.518 | 4,195 |

The periodic-snapshot run completed 16, 16, and 17 snapshots on the three replicas.
It kept one leader in term 1. A trigger waits for any active snapshot job to finish.
It does not create a snapshot for every exact multiple of 1,000 entries.

Raw data: [hot path](results/latency.json) and
[periodic snapshots](results/latency-with-snapshots.json).

## Recovery and GC

All eight process-level scenarios passed. The run tested:

1. Election, atomic batch ordering, and replication.
2. Leader SIGKILL after an acknowledged write, election, and follower rejoin.
3. SIGKILL of all replicas before explicit SlateDB flush, followed by Raft log replay.
4. Physical log GC and snapshot recovery of an offline follower.
5. Full restart after GC removed the original log prefix.
6. Minority rejection and removal of an uncommitted suffix after a partition.
7. Replicated deletion and reads through the consensus log.
8. Writes and heartbeats while snapshot creation was delayed by 2.5 seconds.

The measured leader failure took 946 ms from SIGKILL to a successful write on the new leader.
The GC check removed eight old log files. Log bytes fell from 2,199,792 to 139,634.
The slow-snapshot check completed 126 writes without a leader or term change.
These are individual observations, not latency bounds.

Five Rust recovery tests passed. They cover terms, votes, conflicting suffixes, node identity,
snapshot boundaries, interrupted snapshot installation, and a pinned snapshot during later writes.
The package build, formatting check, and Clippy with warnings denied also passed.

Raw data: [fault checks](results/fault-tests.json).

## Failure found during development

An earlier long run failed after a checkpoint blocked the node loop on SlateDB L0 backpressure.
The pause delayed heartbeats and caused a leader change. That benchmark reported 11,101 failures.
The [failure report](results/before-background-snapshots.json) retains the old binary hash.
It also records the log summary and run directory.

The fix pins the applied view, builds the snapshot in a background task, and polls local compaction
and manifest updates every 100 ms. The final periodic-snapshot run passed with zero request errors.
The slow-snapshot test and pinned-snapshot Rust test cover this change.

Snapshot persistence, snapshot installation, and state apply still use the node loop.
Slow storage can still delay that loop. Full snapshots also duplicate the live state in raft-engine.
The current peer transport accepts at most 64 MiB per message.

## Next implementation boundary

The service has a small JSON KV API. It is not yet an etcd replacement.
The next layer needs explicit MVCC revisions and key metadata, followed by etcd v3 Range and Txn.
Watch and lease behavior need separate replicated state. A Raft index is not an etcd revision.

Before deployment, add separate apply workers, bounded snapshot transfer, and cross-host tests.
Also test storage faults and power loss. SIGKILL only checks process-crash recovery.
