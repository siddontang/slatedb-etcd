# Shared state operator guide

Shared state is the only state mode. The leader writes one SlateDB for the whole cluster in an
S3-compatible store. Each member reads a checkpoint of it, its base, under an in-memory overlay
of newer applied batches. The design is in `docs/shared-state.md`. The flags are in
[configuration](configuration.md#shared-state).

## Properties

| Property | Shared state |
| --- | --- |
| Copies of each SST | One for the cluster |
| S3 writes, flushes, and compactions | The leader only |
| Raft snapshot for a new member | A checkpoint name, no data copy |
| Fault domain | A lost object affects all members |
| Write access | Every voter can write the shared prefix |
| Leader change | The writer clock must stay near the voter clocks |

Every cluster needs an S3-compatible store, also a single member. A data directory of the removed
replicated state mode, or of a member-local store, is not supported. A member refuses to start on
it.

## Object layout

```text
<prefix>/<sha256(cluster-id)>/
    shared/owner.json          the cluster claim: quota, initial voters, and store ID
    shared/bootstrap.json      written once after the first checkpoint
    shared/bootstrap-op.json   the one bootstrap operation: empty, or a snapshot restore
    shared/restore-input.json  the snapshot digest, row count, and membership of a restore
    shared/restore-lease.json  the member that writes the rows of a restore
    shared/state/              the one SlateDB: manifest/, compacted/, checkpoints
    member-<id>/binding.json   the member identity. It holds no state.
```

Each member keeps its Raft log in its local data directory. S3 does not replace the log.
All members use the same bucket, endpoint, region, and prefix.

### Shared store ID

The first voter creates the cluster claim, `shared/owner.json`, with a random store ID. The other
members read the ID from the claim. A member refuses a claim with no store ID or with an ID that
is not a UUID. Each member sends the ID with its Raft traffic and view reports. A member refuses
peer traffic with another ID, or with no ID, before Raft sees it.

So a member with the correct cluster ID and voters, but another bucket or prefix, cannot join the
quorum. It cannot open the checkpoints of the other store. Both sides increment
`slatedb_etcd_peer_store_refusals_total` and log this error at most every 10 s:

```text
ERROR refused a peer on another shared store: all members must use the same bucket and prefix
local=Some("<this store ID>") peer=Some("<peer store ID>")
```

`/status` shows the ID as `shared_state.store_id`. Compare it on all members, and compare their
bucket and prefix flags. Start a member with the wrong flags again with the correct bucket and
prefix and an empty data directory, because its binding keeps the old location.

A mismatch shows only that two members use different stores. It does not show which store is in
use or who owns it. Do not delete objects under the other prefix before you verify its owner and
that no member uses it.

The ID is a property of the objects, not of the location:

- A copy of the whole cluster namespace to another prefix copies the claim and its ID. Members
  on the copy accept members on the original. Do not run both.
- A native snapshot restore into a new prefix creates a new claim. That store is independent and
  has a new ID.

A member records the ID in its local `object-store.json` at its first open. At each later start,
it refuses a store at the same location with another ID or with no claim, before it writes:

```text
the data directory belongs to shared store <recorded ID>, but the store at this location has
store ID <new ID>: the store was replaced. Use the original store, or a new data directory
```

## Credentials and access

These rules follow the accepted design decision: all voters of one trusted cluster share write
access.

- Give each voter read and write access to `shared/`. Any voter can become the leader and the
  writer.
- At startup each member reads the shared claims.
- Each member, also a learner, writes its own `member-<id>/binding.json` at its first start.
- A learner only reads `shared/`. Give it the voter statements before you promote it.
  Promotion fails until the learner creates and deletes a scratch object under
  `shared/state/.probe/`. Give that prefix the same write policy as the rest of `shared/state/`.
  A policy that allows writes only to `.probe/` passes the check, but the new voter cannot write
  the state.
- Scope each credential to the cluster prefix `<prefix>/<sha256(cluster-id)>/`. Do not give a
  member access to the prefixes of other clusters.
- A credential that can delete objects can delete the state of every member. Keep backups and
  recovery copies under a separate credential and a separate prefix.

An S3 policy has these statements. `<cluster>` is the cluster prefix. A unit test,
`shared_members_touch_the_keys_of_the_guide_policy`, checks the keys that members use.

| Role | Action | Resource |
| --- | --- | --- |
| All | `s3:ListBucket` | the bucket, with the condition `s3:prefix` = `<cluster>/*` |
| All | `s3:GetObject`, `s3:PutObject` | `<cluster>/member-<id>/*` |
| Voter | `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject` | `<cluster>/shared/*` |
| Learner | `s3:GetObject` | `<cluster>/shared/*` |

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
until its local clock passes that tick and the time of the newest L0 SST ID. If the local clock
is behind by more than `--max-clock-skew`, 5 s by default, the writer does not start. The member
logs an error, sets `clock_alarm` to `behind` in `/status`, sets
`slatedb_etcd_shared_clock_skew_milliseconds`, and tries again with a backoff.

The leader also probes the clock of each voter every 500 ms. It caps each tick at
`--max-clock-skew` above the voter-majority clock that it estimates at admission. A leader
clock that is further ahead writes at the cap and sets `clock_alarm` to `ahead`. While too few
voters answer the probe, the writer waits and `/status` shows `clock.waiting`.

After three clock refusals the leader hands the lead to a voter without a writer fault. Keep host
clocks synchronized with NTP, also on the host of a snapshot restore. See
[Clocks](shared-state.md#clocks).

## Memory

- The overlay holds the applied batches above the base. It holds about `--view-max-age` of
  writes, or more when S3 is slow.
- The leader rejects client proposals while the overlay is above `--overlay-max-bytes`. Apply
  waits above twice that limit. Raft still persists and acknowledges entries.
- `--view-pin-max` is a storage deadline of each read attempt. A read that runs again keeps
  its view. A native backup has no deadline. It ends when its client takes no message for 60 s.
- Reads and backups keep the overlay segments of their views after a base switch. Reads have no
  byte limit on these. Over `--overlay-max-bytes`, the oldest of at most 4 backups is stopped.
- The leader also holds the writer memtable, and SlateDB limits it. See the
  [memory bound per member](shared-state.md#overlay) for each term.

## Operations

| Operation | Behavior |
| --- | --- |
| Put, Txn, and reads | A follower reads its own base and overlay |
| Physical Compact | Each member applies the steps. The reply waits for a covering base |
| Defragment | Runs on the leader writer. A follower forwards it to the leader |
| Manual checkpoint, `POST /checkpoint` | Returns when the member's base covers the commit index |
| Status `db_size` | Read from the shared manifest. All members report about the same value |
| HashKV and Hash | Computed from the member's view |
| Native backup | Streams the member's view. See [backups](shared-state.md#backups) |
| `--restore-snapshot` | Seeds a new shared cluster at the copied row count plus one |
| MoveLeader | Requests a checkpoint first, then transfers the lead within 3 s |

Defragment flushes the writer and compacts every SST into one sorted run. The leader then
publishes a new checkpoint, also at an unchanged index, and announces it. Each member switches
its base to the newer checkpoint at the same index, so it reads the compacted run without a
further write. Old checkpoints keep their SSTs until retention or a manual cleanup deletes them.
The call returns after the leader applied the announcement of that checkpoint. If the leader
changes first, the call fails with a retryable `Unavailable` error. Run it again on the new
leader.

A manual checkpoint keeps its commit index. When apply reaches that index, the leader requests a
publication that covers it. A follower sends the request to the leader in its view report, and
repeats it while it waits. The request also survives a leader change before the new writer
opens.

To restore a native snapshot, use a new cluster prefix and empty member directories. Start each
member with `--restore-snapshot <file>`. Each member first checks its file in one pass. The image
has no size limit. Each row has the 64 MiB limit of a backup.

1. Before any writer opens, a member claims the one bootstrap operation,
   `shared/bootstrap-op.json`, as a restore with the snapshot SHA-256 digest.
2. It then creates `shared/restore-input.json`. The input records the digest, the row count, the
   membership row, the alarm owner, and the transform rules. Every later attempt uses the input,
   not its own flags.
3. One member at a time holds `shared/restore-lease.json` and writes. It writes the rows in 4 MiB
   batches and flushes every 64 MiB or 10 s. Each batch also stores its last row number and the
   SHA-256 of the file through that row.
4. A last batch stores the apply marker at N, the copied row count plus one, with the membership
   and the logical size. The member then publishes the checkpoint at N and writes the bootstrap
   marker with the digest.
5. The owner renews the lease every 10 s, but only after it read file bytes or flushed rows.
   Another member with the same file takes over a lease that stays unchanged for 60 s. It reads
   the file through the last stored row, compares the digest, and continues after that row.
6. A member without the restore flag, also a joining member, waits while the lease changes. It
   fails after 60 s without a change. If the restore already created its checkpoint, the member
   writes the marker and opens on that checkpoint. Otherwise an ordinary start claims an empty
   bootstrap. A joining member never claims it. Before any claim exists, it waits up to 60 s for
   the initial members to decide, and then fails. Start it again later.
7. Once the marker exists, a start reads only the marker. It does not read the restore objects
   or open the first checkpoint again.
8. A restore into a cluster that an ordinary start or another snapshot created fails. A file
   that differs from the stored rows also fails. Use a new cluster prefix.

The restored Raft log starts at N. Endpoint status can show a higher applied index after the
election. KV responses retain the snapshot's MVCC revision until the next modifying transaction.

A member serves no request until the restore completes. A large file takes minutes to check and
to write, so give startup probes and service start timeouts that time. A stopped member can start
the same restore again while its Raft log is empty. It waits up to 60 s for the lease of its
stopped attempt to go stale.

### Abandon a partial restore

Make these checks first:

1. Every member process of the cluster is stopped.
2. `shared/bootstrap-op.json` has `"kind":"restore"`, and its `snapshot` field names your file.
   The field is the 32-byte digest at the end of the file in hexadecimal: the SHA-256 of the
   bytes before it. `sha256sum` of the whole file gives another value. Read the digest with
   `tail -c 32 <file> | xxd -p -c 32`. An ordinary bootstrap also has no marker before its first
   checkpoint. Only this check tells the two apart.
3. `shared/bootstrap.json` is missing.

Then delete every object under `shared/` except `shared/owner.json`: the `shared/state/` database
and its `shared/state.*` objects, `shared/bootstrap-op.json`, `shared/restore-input.json`, and
`shared/restore-lease.json`. Keep `shared/owner.json`, because the member directories recorded
its store ID. Then start the restore again with the same member directories. The restore can
use another snapshot file. Never delete `shared/` objects of a cluster that has a bootstrap
marker: they hold the state of all members.

## Failure modes

| Case | Result |
| --- | --- |
| The leader stops with unflushed writes | The new leader materializes them from its overlay |
| The old leader is partitioned | Its writer is fenced. It cannot change the manifest |
| S3 is slow or down | New proposals fail at the overlay limit. Apply pauses at 2x |
| One member cannot write to S3 | Its writer retries. The lead moves once to a healthy voter |
| No member can write to S3 | Each writer retries after at most 30 s. Reads continue |
| The writer cannot use the data | The writer halts and retries after at most 5 minutes |
| A writer step hangs for 30 s | The lead moves, or the writer drops the step and opens again |
| A checkpoint open fails | The member tries again with a backoff from 100 ms to 2 s |
| An announcement is lost | The writer announces the checkpoint again with the same backoff |
| A member reads a deleted SST | The read fails and can retry. Apply rebuilds from a newer base |
| The new leader's clock is behind | The writer waits, or refuses above `--max-clock-skew` |
| The leader's clock is ahead of the voters | The writer writes at the cap |
| All members restart | Each member opens the newest checkpoint and replays its log |
| A member uses another bucket or prefix | Peers refuse its traffic. It cannot join the quorum |
| The shared database is lost | All members lose state together. Restore from a backup |

`/status` reports the writer health under `shared_state.writer.health`: the failures in a row,
the time since the first one, the class and text of the last error, the fault mark, and the
pending close of a failed writer. A marked member is not an automatic handoff target until it
leads again and its writer publishes. Use MoveLeader to move the lead to it before that.
`shared_state.writer.handoff` names the target of a handoff in flight and the voters that the
incident tried.

## Metrics

See [shared state gauges](metrics.md#shared-state-gauges) and the
[read path counters](metrics.md#read-path-counters).
