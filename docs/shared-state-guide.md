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
    shared/bootstrap-op.json   the one bootstrap operation: empty, or a snapshot restore
    shared/conversion.json     the plan of an offline conversion
    shared/state/              the one SlateDB: manifest/, compacted/, checkpoints
    member-<id>/binding.json   the member identity and mode. It holds no state.
```

Each member keeps its Raft log in its local data directory. S3 does not replace the log.
All members use the same bucket, endpoint, region, and prefix.

## Credentials and access

These rules follow the accepted design decision: all voters of one trusted cluster share write
access.

- Give each voter read and write access to `shared/`. Any voter can become the leader and the
  writer.
- At startup each member reads `mode.json`, the shared claims, and the owner claim of every
  member namespace. A replicated owner claim stops the start.
- Each member, also a learner, writes its own `member-<id>/binding.json` at its first start.
- A learner only reads `shared/`. Give it the voter statements before you promote it.
- Scope each credential to the cluster prefix `<prefix>/<sha256(cluster-id)>/`. Do not give a
  member access to the prefixes of other clusters.
- A credential that can delete objects can delete the state of every member. Keep backups and
  recovery copies under a separate credential and a separate prefix.

An S3 policy has these statements. `<cluster>` is the cluster prefix. A unit test,
`shared_members_touch_the_keys_of_the_guide_policy`, checks the keys that members use.

| Role | Action | Resource |
| --- | --- | --- |
| All | `s3:ListBucket` | the bucket, with the condition `s3:prefix` = `<cluster>/*` |
| All | `s3:GetObject` | `<cluster>/mode.json`, `<cluster>/member-*/owner.json` |
| All | `s3:GetObject`, `s3:PutObject` | `<cluster>/member-<id>/*` |
| Voter | `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject` | `<cluster>/shared/*` |
| Voter | `s3:PutObject` | `<cluster>/mode.json`, for the first start |
| Learner | `s3:GetObject` | `<cluster>/shared/*` |

The offline converter also needs `s3:GetObject`, `s3:PutObject`, and `s3:DeleteObject` on
`<cluster>/member-*/*`. It reads the replicated states, creates and deletes detached checkpoints
in their manifests, writes bindings, and retires owner claims. The converter test checks these
keys.

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
2. On each member host, read the checkpoint of the member's Raft snapshot record:

   ```sh
   slatedb-etcd shared-status --dir <data dir>
   ```

   The command prints the record index, its term, and its `checkpoint` ID. It reads the stopped
   member, so the record cannot change after the read. `GET /status` shows the same fields as
   `record_checkpoint` and `base_checkpoint` while the member runs, but they can change before
   the stop.
3. List the `view-` checkpoints of `shared/state` with the SlateDB admin API. Keep the newest
   one and each checkpoint from step 2.
4. Delete the other `view-` checkpoints. Run the SlateDB garbage collector to delete the SSTs
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

Defragment flushes the writer and compacts every SST into one sorted run. The leader then
publishes a new checkpoint, also at an unchanged index, and announces it. Each member switches
its base to the newer checkpoint at the same index, so it reads the compacted run without a
further write. Old checkpoints keep their SSTs until retention or a manual cleanup deletes them.

A manual checkpoint keeps its commit index. When apply reaches that index, the leader requests a
publication that covers it. A follower sends the request to the leader in its view report, and
repeats it while it waits. The request also survives a leader change before the new writer
opens.

To restore a native snapshot, use a new cluster prefix and empty member directories. Start each
member with `--restore-snapshot <file>` and `--state-mode shared`.

1. Before any writer opens, a member claims the one bootstrap operation,
   `shared/bootstrap-op.json`, as a restore with the snapshot checksum.
2. It writes the rows into the shared database at index 1, publishes the first checkpoint, and
   writes the bootstrap marker with the checksum.
3. Another member with the same snapshot finds the claim and completes or skips the same steps.
   This also completes a restore that failed between its first write and the marker.
4. A member without the restore flag waits while a restore is pending. Then it adopts the
   restored checkpoint. An ordinary first leader claims the operation as an empty bootstrap.
5. A restore into a cluster that an ordinary start or another snapshot created fails. Use a new
   cluster prefix.

A restore that keeps failing logs the error and names the snapshot. Only for that case: check
that `shared/bootstrap-op.json` names your snapshot, that `shared/bootstrap.json` is missing,
and that no member process runs. Then delete `shared/` under the cluster prefix and start the
restore again. Never delete `shared/` of a cluster that has a bootstrap marker: it holds the
state of all members. The native backup limit is 64 MiB.

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

The first run is the preflight. It reads the flushed index of each replicated member namespace
from a detached checkpoint and takes the highest one. Different indexes log a warning. It checks
the index, the term at the index, the membership, and the end of the copied manifest before it
changes any claim, so a failed preflight leaves the replicated cluster usable. A state without a
term takes it from the local Raft log. If that log does not hold the index, run the conversion
on the source member first.

The run then writes the plan, `shared/conversion.json`, with a create-only put. A concurrent or
later run uses that plan and deletes only its own detached checkpoints. Until the bootstrap
marker exists, each run completes the seed: it copies the planned SSTs and manifest into
`shared/state`, changes the mode claim, writes the cluster claim with the bootstrap voters of the
Raft logs, publishes the first checkpoint, retires the owner claims of members that the copied
membership does not hold, and writes the marker. No step deletes a view checkpoint.

Each run then installs the checkpoint as the member's Raft snapshot record and changes the
member's binding. A member keeps its log entries above the checkpoint and replays them at start.

The conversion refuses a running member, a member of another cluster, a member without S3
state, and a member outside the copied membership. A shared member refuses to start while an
active member namespace has a replicated owner claim, so the cluster starts only after every
active member is converted. The replicated state objects under `member-<id>/state*` stay in
place. Delete them after you check the converted cluster.

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
