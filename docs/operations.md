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
The exporter verifies SHA-256 and publishes a mode 0600 file without overwriting one.
`--max-bytes` rejects a larger snapshot. The default, 0, accepts any size.
`etcdctl snapshot save` also accepts the native file. It checks the length, not the digest.
The server returns its native state image and a trailing SHA-256 digest.
Protect backup files: they contain values, user password hashes, tokens, and lease metadata.
The native image is not a bbolt database. `etcdutl snapshot restore` cannot restore it.
Use the native exporter above, `etcdctl snapshot save`, or the Snapshot RPC of the official client.

Follow the [native restore steps](administration.md#maintenance-and-storage) for a new cluster.
Every restored member uses the same backup, a new empty directory, and the same new peer map.
Use a new cluster ID. Restore replaces membership and invalidates old tokens.
Remove `--restore-snapshot` from later restart commands.
Check keys, permissions, member addresses, and health before resuming the workload.

Do not copy the live `shared/state` objects as a complete backup.
Unflushed state can still depend on Raft logs. Native export preserves one applied state boundary.
A backup streams a pinned view with bounded memory. The image has no size limit, but each row has
a 64 MiB limit. A restore streams the image into the shared database.

## Disk use and quota

Logical Compact removes obsolete MVCC versions below a retained boundary.
Physical Compact also waits for the deletion batch to flush.
Defragment rewrites active SSTs and waits for manifest publication.
Neither operation guarantees immediate disk shrinkage. Checkpoints can retain old objects.
SlateDB's garbage collector removes objects after their retention rules permit it.
A SlateDB compaction keeps the SSTs that it replaces for 15 minutes, for reads that started
before it. Garbage collection keeps SlateDB's default of a run every 10 minutes, so these SSTs
leave the store 15-25 minutes after the compaction when nothing else references them.
A `view-` checkpoint keeps them until retention or a manual cleanup deletes it.
GC also keeps SSTs newer than the newest L0 SST. After a bulk load on an idle cluster, some
replaced SSTs can stay until a later flush. A bulk load can hold 2-3 times the live data in the
store until then.

Set `--auto-compaction-retention` when clients do not compact. Otherwise history grows until
NOSPACE. See [automatic compaction](configuration.md#automatic-compaction).
Alert on `slatedb_etcd_quota_warning`. Then check which `slatedb_etcd_logical_size_bytes`
kind grows. Compaction frees `history` and `watch_journal` bytes, not `current` bytes.

Inspect alarms before retrying failed writes. A NOSPACE alarm rejects writes and lease grants.
Deletion and compaction remain available for cleanup. Disarm the alarm after resolving the cause.
The quota counts logical stored bytes; it does not reserve disk capacity.

### Quota

The cluster claim `shared/owner.json` stores the quota. Every member applies that value.

- `--quota-backend-bytes` seeds the quota when the first voter creates the claim. Later flag
  values do not change it.
- A native backup does not carry the quota. A restore takes the flag of the member that creates
  the new claim.

### Membership row

The `etcd/members` row is cluster state, and the quota counts its bytes.

- The `--peer` and `--client-peer` flags build the membership of a new member in memory. The
  first leader stores its own membership before the first client write. Joining members and
  restores take the row from the cluster.
- A member whose flags differ from the stored row logs a warning at startup. The row applies.
- Each leader proposes a digest of its row when it starts its term, and again after the first
  entry of its term applies. A member whose row differs logs a warning and sets the
  `slatedb_etcd_members_mismatch` gauge. The `members_row` field of `/status` shows the digest,
  the last index that can have changed the row, and the term and index of the last comparison.

Keep the entire Raft directory, including its checkpoint data file, when moving a member.
Do not delete Raft log files, manifests, or individual SST objects by hand.
Use native backups and a new cluster if storage must move.

## Common failures

| Symptom | Check |
| --- | --- |
| Address already in use | Stop the process that owns the port or choose a new cluster port set. |
| No leader or request timeout | Check quorum, peer URLs, TLS trust, and logs. |
| Error `refused a peer on another shared store` | Give all members one bucket and prefix. |
| Error `the store was replaced` | Use the original shared store, or a new data directory. |
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
Data directories of replicated state mode or of a member-local store are not supported. A member
refuses to start on them.
The etcd client protocol remains separate from this internal storage format.
