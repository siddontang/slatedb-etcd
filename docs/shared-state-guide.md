# Shared state operator guide

Shared state mode is a prototype. The leader writes one SlateDB for the whole cluster in S3.
Each member reads a checkpoint of it, its base, under an in-memory overlay of newer applied
batches. The design is in `docs/shared-state.md`. The flags are in
[configuration](configuration.md#shared-state-mode).

## When to use it

Use shared mode when S3 storage and S3 writes cost more than the extra risk below.

| Property | Replicated mode | Shared mode |
| --- | --- | --- |
| Copies of each SST | One for each member | One for the cluster |
| S3 writes, flushes, and compactions | Each member | The leader only |
| Raft snapshot for a new member | Copy of the SSTs | A checkpoint name, no data copy |
| Fault domain | Each member has its own copy | A lost object affects all members |
| Write access | Each member writes its own prefix | Every voter can write the shared prefix |
| Leader change | No clock dependency | The new writer waits for the manifest clock |

Keep replicated mode, the default, when members must not share a fault domain. Shared mode is for
new clusters. A stopped replicated S3 cluster can be converted offline. See
[Conversion](#convert-a-replicated-cluster).

## Object layout

```text
<prefix>/<sha256(cluster-id)>/
    mode.json                  the cluster mode, written before any state
    shared/owner.json          the cluster claim: mode, quota, and the initial voters
    shared/bootstrap.json      written once after the first checkpoint
    shared/state/              the one SlateDB: manifest/, compacted/, checkpoints
    member-<id>/binding.json   the member identity and mode. It holds no state.
```

Each member keeps its Raft log in its local data directory. S3 does not replace the log.
All members use the same bucket, endpoint, region, and prefix.

## Credentials and access

These rules follow the accepted design decision: all voters of one trusted cluster share write
access.

- Give each voter read and write access to `shared/state/` and to its own
  `member-<id>/binding.json`. Any voter can become the leader and the writer.
- Scope each credential to the cluster prefix `<prefix>/<sha256(cluster-id)>/`. Do not give a
  member access to the prefixes of other clusters.
- A learner only reads the shared state. A read-only credential is enough until its promotion.
  Give it write access before you promote it.
- A credential that can delete objects can delete the state of every member. Keep backups and
  recovery copies under a separate credential and a separate prefix.

An example S3 policy for one voter has these statements:

| Action | Resource |
| --- | --- |
| `s3:ListBucket` | the bucket, with the condition `s3:prefix` = `<cluster prefix>/*` |
| `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject` | `<cluster prefix>/shared/*` |
| `s3:GetObject`, `s3:PutObject` | `<cluster prefix>/mode.json`, `<cluster prefix>/member-<id>/*` |

## Checkpoints and retention

The leader flushes the shared database when its oldest unflushed write reaches
`--view-max-age`, or on request. It then creates one `view-` checkpoint and announces it through
Raft. Each checkpoint pins its SSTs, so storage grows with the number of kept checkpoints.

| `--view-retention` | Behavior | Storage cost |
| --- | --- | --- |
| `off`, the default | The leader deletes no checkpoint | Each checkpoint keeps its SSTs |
| `fenced` | Deletes old checkpoints that no member reported in 10 min | Two plus pinned ones |

`fenced` checks the writer epoch before each deletion. SlateDB 0.18.1 deletes a checkpoint without
its own epoch check, so a deposed leader can still delete a checkpoint that a member of the new
leader pins. That member then rebuilds its state from a newer checkpoint. Keep `off` when you
cannot accept this race.

Manual cleanup with `off`:

1. Stop all members.
2. List the `view-` checkpoints of `shared/state` with the SlateDB admin API.
3. Keep the newest checkpoint. Also keep the checkpoint that each member's Raft snapshot record
   names. `GET /status` shows it before the stop.
4. Delete the older `view-` checkpoints. Run the SlateDB garbage collector to delete the SSTs
   that no checkpoint pins.
5. Start the members.

`slatedb_etcd_shared_view_checkpoints` reports the number of checkpoints after each publication.

## Clocks

SlateDB starts a writer's clock at the `last_l0_clock_tick` of the manifest. A new leader waits
until its local clock passes that tick. If the local clock is behind by more than
`--max-clock-skew`, 5 s by default, the writer does not start. The member logs an error, sets
`clock_alarm` in `/status`, sets `slatedb_etcd_shared_clock_skew_milliseconds`, and tries again
each second. The cluster then accepts no writes until the skew drops or another member leads.
Keep host clocks synchronized with NTP.

## Memory

- The overlay holds the applied batches above the base. It holds about `--view-max-age` of
  writes, or more when S3 is slow.
- The leader rejects client proposals while the overlay is above `--overlay-max-bytes`. Apply
  waits above twice that limit. Raft still persists and acknowledges entries.
- The leader also holds the memtable of the writer. Expect about two times the unflushed data on
  the leader.
- A read view keeps its base for at most `--view-pin-max`. A longer read fails with a retryable
  error.

## Operations

| Operation | Shared mode behavior |
| --- | --- |
| Put, Txn, and reads | As in replicated mode. A follower reads its own base and overlay |
| Physical Compact | Each member applies the steps. The reply waits for a covering base |
| Defragment | Runs on the leader writer. A follower forwards it to the leader |
| Manual checkpoint, `POST /checkpoint` | Returns when the member's base covers the commit index |
| Status `db_size` | Read from the shared manifest. All members report about the same value |
| HashKV and Hash | Computed from the member's view, as in replicated mode |
| Native backup | Scans the member's view. It needs no flush |
| `--restore-snapshot` | Seeds a new shared cluster at index 1 |
| MoveLeader | Requests a checkpoint first, then transfers the lead within 3 s |

Defragment flushes the writer, compacts every SST into one sorted run, and requests a new
checkpoint. The next base reads the compacted run. Old checkpoints keep their SSTs until
retention or a manual cleanup deletes them.

On the leader, a manual checkpoint requests a publication through the commit index. A follower
waits for the next publication of the leader, at most `--view-max-age` after the last write.

To restore a native snapshot, start each member of the new cluster with
`--restore-snapshot <file>`, `--state-mode shared`, and an empty data directory. One member
writes the rows into the shared database at index 1, publishes the first checkpoint, and writes
the bootstrap marker. The other members find the marker and skip the restore. A member that does
not use the flag also works: it adopts the first checkpoint. The native backup limit is 64 MiB.

## Convert a replicated cluster

`slatedb-etcd convert-to-shared` converts a stopped replicated cluster that keeps its state in
S3. It needs the same S3 credentials as the members.

1. Run `POST /checkpoint` on each member. The conversion uses the flushed state.
2. Stop all members.
3. On each member host, run:

   ```sh
   slatedb-etcd convert-to-shared --id <id> --dir <data dir> --cluster-id <cluster>
   ```

4. Start all members with `--state-mode shared` and their earlier flags.

The first run reads the flushed index of each member from S3 and takes the highest one. Different
indexes log a warning. It copies that member's SSTs and manifest into `shared/state`, changes the
mode claim, writes the cluster claim with the voters of the copied membership, publishes the
first checkpoint, and writes the bootstrap marker. Each run then adopts the checkpoint in the
member's Raft log and changes the member's binding. A member keeps its log entries above the
checkpoint and replays them at start.

The conversion refuses a running member, a member of another cluster, and a member without S3
state. A shared member refuses to start while any member namespace has a replicated owner claim,
so the cluster starts only after every member is converted. The replicated state objects under
`member-<id>/state*` stay in place. Delete them after you check the converted cluster.

## Failure modes

| Case | Result |
| --- | --- |
| The leader stops with unflushed writes | The new leader materializes them from its overlay |
| The old leader is partitioned | Its writer is fenced. It cannot change the manifest |
| S3 is slow or down | New proposals fail at the overlay limit. Apply pauses at 2x |
| A checkpoint open fails | The member tries again with a backoff from 100 ms to 2 s |
| An announcement is lost | The writer announces the checkpoint again with the same backoff |
| A member reads a deleted SST | The read fails and can retry. Apply rebuilds from a newer base |
| The new leader's clock is behind | The writer waits, or refuses above `--max-clock-skew` |
| All members restart | Each member opens the newest checkpoint and replays its log |
| The shared database is lost | All members lose state together. Restore from a backup |

## Metrics

See [shared state gauges](metrics.md#shared-state-gauges).
