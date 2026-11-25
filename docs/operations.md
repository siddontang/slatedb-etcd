# Local operations

This service supports controlled local and remote experiments.
Three local VMs passed recovery tests.
Independent physical hosts and production recovery procedures remain unvalidated.
Read [compatibility limits](../COMPATIBILITY.md) before storing valuable data.

## Health and shutdown

Use `etcdctl endpoint health` to check quorum and local apply progress.
`etcdctl endpoint status --write-out=table` reports local identity, leader, sizes, and Raft
progress.
Status and serializable member queries can work without quorum; they do not prove write
availability.

Use Ctrl-C for a foreground process. It stops connections and waits for storage workers.
Rust hosts must await `Server::shutdown`; see [embedding](embedding.md).
Each acknowledged write has quorum commit and leader apply.
A terminated or timed-out request can have an unknown outcome. Check application state after
recovery.
Keep the member ID, cluster ID, data directory, and original bootstrap arguments on restart.
Do not start two processes against the same directory.

The diagnostic `/status` adds apply lag, durable progress, queue lengths, and log memory counters.
A growing apply lag indicates that commit is ahead of state execution.
A growing log queue indicates storage delay. Rejected requests can indicate admission limits.
These counters are diagnostic JSON, not a supported Prometheus metrics interface.

## Backups and restoration

Build the native exporter with the official Go client dependency:

```sh
(cd tests/compat && go build -mod=readonly \
  -o ../../tmp/bin/slatedb-snapshot ./cmd/slatedb-snapshot)
tmp/bin/slatedb-snapshot --endpoints=http://127.0.0.1:7001 --output /path/native.snapshot
```

Use `--cacert`, `--cert`, and `--key` for TLS.
For password authentication, set `--user` and the `SLATEDB_SNAPSHOT_PASSWORD` environment variable.
The exporter verifies SHA-256, limits size, and publishes a mode 0600 file without overwriting one.
The server returns its native state image and a trailing SHA-256 digest.
Protect backup files: they contain values, user password hashes, tokens, and lease metadata.
The native image is not a bbolt database. `etcdutl snapshot restore` cannot restore it.
`etcdctl snapshot save` can reject its file length because it expects a bbolt page boundary.
Use the native exporter above, or stream the Snapshot RPC with the official client.

Follow the [native restore steps](administration.md#maintenance-and-storage) for a new cluster.
Every restored member uses the same backup, a new empty directory, and the same new peer map.
Use a new cluster ID. Restore replaces membership and invalidates old tokens.
Remove `--restore-snapshot` from later restart commands.
Check keys, permissions, member addresses, and health before resuming the workload.

Upgrade every member before a member on a newer binary sends a checkpoint. A member on an older
binary that installs a checkpoint with `meta/applied-term` exports that row in its backups.
The older restore rejects the row. Restore such a backup with the newer binary, which drops it.

Do not copy a live `objects/` directory as a complete backup.
Unflushed state can still depend on Raft logs. Native export preserves one applied state boundary.
Full backup construction uses memory and has a 64 MiB transfer limit.

## Disk use and quota

Logical Compact removes obsolete MVCC versions below a retained boundary.
Physical Compact also waits for the deletion batch to flush.
Defragment rewrites active SSTs and waits for manifest publication.
Neither operation guarantees immediate disk shrinkage. Checkpoints can retain old objects.
SlateDB's garbage collector removes objects after their retention rules permit it.

Set `--auto-compaction-retention` when clients do not compact. Otherwise history grows until
NOSPACE. See [automatic compaction](configuration.md#automatic-compaction).
Alert on `slatedb_etcd_quota_warning`. Then check which `slatedb_etcd_logical_size_bytes`
kind grows. Compaction frees `history` and `watch_journal` bytes, not `current` bytes.

Inspect alarms before retrying failed writes. A NOSPACE alarm rejects writes and lease grants.
Deletion and compaction remain available for cleanup. Disarm the alarm after resolving the cause.
The quota counts logical stored bytes; it does not reserve disk capacity.

### Change the quota

In replicated mode the quota is cluster state, so every member applies one value at each index.
Shared mode keeps the quota of its cluster claim instead; see [shared state](shared-state.md).

- `--quota-backend-bytes` seeds the cluster quota and requests changes to it. The first leader
  of a new cluster stores its flag before the first client write.
- A member records its flag in `quota-flag.json` in its data directory. A restart with another
  flag makes a request with the wall-clock time of that start. The member proposes the request
  each time it leads, until the cluster quota equals it or holds a newer request.
- The newest request wins, on every member and in every order of apply. Equal times compare the
  member ID. A newer request with the current value still replaces the older request.
- A member with an unchanged flag does not change the cluster quota when it leads. So a rolling
  restart with a new flag takes effect when the first updated member leads. Use
  `etcdctl move-leader` to select it.
- A member whose flag differs from the cluster quota logs a warning and sets the
  `slatedb_etcd_quota_flag_mismatch` gauge. The `quota` field of `/status` shows both values.
- A native backup carries the cluster quota. After a restore, it wins over the flag until an
  operator makes a new request.
- A directory from an earlier binary has no cluster quota. Its members use their own flags until
  the first leader on this binary stores one. Then every member checkpoints through that entry.

Set one value on every member. The order of requests uses member clocks. With clock skew, the
later time can belong to the earlier operator action. The mismatch gauge shows a split. To make
a request again with the same flag, restart that member once with `--quota-reassert`. Remove the
option from later restart commands.

A member with a new data directory never applies a Put, a transaction with a Put, or a lease
grant that an earlier binary wrote before the first cluster quota. Apply stops at that entry,
logs an error, and asks the leader for a snapshot. It resumes after the install. Until then
the member does not start an election while its leader is live, ignores leadership transfers,
and hands over leadership if it wins. It still votes. After three election timeouts without its
leader it may campaign, so it does not hold up the group.

A backlog above `--log-pending-bytes` does not stop its start. That member then starts as a
learner until the snapshot, so it never campaigns. If it holds the only latest log of the two
live voters, the group waits for the third member. Bring that member back, or replace the
waiting member.

Equal flags do not repair members that already applied entries under different quotas.
Compare revisions, alarms, and HashKV. Remove a member that differs, and add it again as a new
learner with an empty data directory. It then installs a snapshot of the leader. Move the
leadership to the chosen member first.

### Membership row

The `etcd/members` row is cluster state, and the quota counts its bytes.

- The `--peer` and `--client-peer` flags build the membership of a new member in memory. The
  first leader stores its own membership before the first client write. Joining members and
  restores take the row from the cluster.
- A member whose flags differ from the stored row logs a warning at startup. The row applies.
- Each leader proposes a digest of its row when it starts its term. A member whose row differs
  logs a warning and sets the `slatedb_etcd_members_mismatch` gauge. The `members_row` field of
  `/status` shows the digest and the last index that can have changed the row.
- An earlier binary stored the flags of each member. Members with other URL text then hold other
  rows. Repair such a member as for a quota difference above.
- A new member does not apply an earlier binary's entries before the first seed. It asks the
  leader for a snapshot, as for the quota.

Keep the entire Raft directory, including its checkpoint data file, when moving a member.
Older binaries reject the Raft identity after the first checkpoint in the new layout.
Do not delete Raft log files, manifests, or individual SST objects by hand.
Use native backups and a new cluster if storage must move.

## Common failures

| Symptom | Check |
| --- | --- |
| Address already in use | Stop the process that owns the port or choose a new cluster port set. |
| No leader or request timeout | Check quorum, peer URLs, TLS trust, and logs. |
| Peer TLS handshake failure | Check SANs, validity, roots, and client authentication. |
| Permission denied | Check role grants, key ranges, and attached lease keys. |
| Auth token is stale | Authenticate again after user, role, password, or permission changes. |
| Required revision compacted | Read or restart Watch at a retained revision. |
| HashKV format error | Check the database format limits in COMPATIBILITY.md. |
| Identity mismatch on startup | Use the original member ID and bootstrap arguments. |
| Native restore rejects a directory | Use a new empty directory. |
| Small log budgets stop a node | Increase the required pool before restart. |

## Binary changes

Storage format 3 uses protobuf KV commands, MVCC history, and Watch journals.
The Raft identity and native snapshot header require this format.
Use fresh data directories and object prefixes for this version.
Old directories and old native backups are not upgrade inputs. There is no format migration.
Mixed binary versions and rolling upgrades are unsupported.
An older binary stops at the first `SetQuota` or `SeedMembers` entry, because it cannot decode
it. Upgrade that member. The new binary applies the entry on restart.
Binaries that write `meta/applied-term` keep native snapshot and backup format 4. Upgrade the
followers first, as for format 5, so that no older member installs a checkpoint with the row.
The etcd client protocol remains separate from this internal storage format.
