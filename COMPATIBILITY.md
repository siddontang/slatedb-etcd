# etcd v3 compatibility milestone

See the [metrics inventory](docs/metrics.md) for Prometheus compatibility with etcd v3.6.5.

Run date: 2026-11-17, Asia/Singapore.
The reference server, imported common suite, model, and Go client use etcd v3.6.5.
Local tests use ARM64 macOS. The PR workflow repeats validation on Linux.
Each JSON report records its command, date, and tested binary hash.

See the [read and apply report](results/performance/linux-batch-apply.md) for later runtime checks
and Linux measurements.

## Results

| Check | Result |
| --- | --- |
| Local differential and integration suite | 175 leaf cases passed; zero skips |
| Complete imported capability profile | 387 leaf cases passed; zero skips |
| Member join, strict promotion, recovery, and minority queries | 3 targeted cases passed |
| Rust unit and recovery tests | 33 passed |
| Deterministic schedules | 100 seeds; two identical traces per seed |
| Original process fault scenarios | 8 passed |
| Native etcd model under process faults | 4 campaigns passed |
| Jepsen register and Elle append checks | Both checkers passed all 4 campaigns |
| Jepsen checker controls | 10 assertions passed |
| Bounded API fuzzing | 10 inputs passed |
| Formatting and Clippy with warnings denied | Passed |

The imported profile includes local integration cases. These counts must not be added together.
It runs single-voter, three-voter, client TLS, and peer TLS configurations.
The targeted member report also checks strict promotion from one initial voter.
The common adapter no longer skips Auth, Cluster, Maintenance, TLS, or physical Compact.
Three upstream Maintenance TODO helpers now exercise the native APIs.
The Downgrade helper checks authorization and the explicit unsupported binary-format result.
It does not assert that an etcd binary downgrade works.

Reports:

- [Local client and differential cases](results/etcd-compatibility.json)
- [Complete imported profile](results/etcd-all-capabilities.json)
- [Member recovery regressions](results/etcd-admin-recovery.json)
- [Deterministic schedules](results/dst.json)
- [Original process faults](results/etcd-core-fault-tests.json)
- [Native model and Jepsen campaigns](results/robustness.json)

The PR workflow writes its JSON reports to the job logs and lists results in the summary.
It also attempts to upload `validation-reports` when GitHub artifact storage is available.
Older reports in `results/` describe earlier implementation stages.

## Verified behavior

KV and Txn tests compare values, revisions, metadata, ranges, errors, and atomicity with real etcd.
Compact tests check boundary reads, physical completion, restart, and snapshot recovery.
HashKV tests compare exact CRC32C values before and after writes, deletes, transactions, and
compaction.

Auth covers users, roles, permission ranges, bcrypt passwords, simple tokens, RS256 JWT, and expiry.
Lease attachment checks deny access to a lease that contains keys outside the caller's grants.
DST pauses apply after a revocation enters the log. A later queued write must fail authorization.
Restart and snapshot checks retain permission changes and reject stale tokens.

Member tests add a learner, install a snapshot, catch up, promote it, and transfer leadership.
They remove a voter and restart using the old bootstrap addresses.
The durable member configuration remains authoritative. Local status and member queries work
without quorum; linearizable member queries still require it.

TLS checks cover client and peer encryption, mutual authentication, certificate CN identity,
unknown CAs, missing client certificates, hostname errors, and forged forwarding headers.
Peer and client tests use different certificate authorities.

Maintenance tests cover status, health, HashKV, native Hash, alarms, defrag, and leader transfer.
Native snapshot tests check response chunks, SHA-256, new-cluster restore, retained revisions,
leases, auth state, changed membership, and stale-token rejection.
Restore rejects damaged checksums and existing data directories.

Watch and Lease checks cover replay, cancellation, progress, fragments, renewal, expiry, and
recovery.
The progress ordering fixture first waits for the Watch member to apply acknowledged cluster writes.
A progress notification reports local applied state and is not a cluster-wide read barrier.
Mutex, election, and STM workflows use the unchanged official client against both implementations.
STM keeps historical serializable reads on one endpoint.

Fault campaigns use leader partition, SIGKILL, SIGSTOP, and full restart with concurrent traffic.
The native etcd model, Knossos register checker, and Elle append checker evaluate the histories.
DST separately controls Raft message delivery, logical ticks, log execution, and apply completion.
It checks memory limits, durable acknowledgements, revocation ordering, and recovery.

## Compatibility boundaries

This milestone provides partial etcd v3 compatibility. It is not a supported Kubernetes backend.
S3 storage and remote mutual TLS now have controlled integration coverage.
Three-guest tests cover VM crashes, peer partitions, object outages, and snapshot recovery.
Independent physical host failures, physical power loss, and rolling binary upgrades remain
untested.

Native backup and backend Hash formats differ from etcd bbolt.
Storage format 3 requires fresh directories and object prefixes.
Old backups and logs are unsupported.
Native backups and peer snapshots use format 4. This binary installs formats 3 and 4.
Format 4 rows keep a Put value only in the current and history rows.
Hash records keep a CRC of the value, and Watch journals read the value from history.
A hash digest starts with a zero byte and a layout version. No etcd `KeyValue` encoding starts
with a zero byte, so the marker does not depend on which `KeyValue` fields exist.
Older binaries reject format 4 snapshots and backups. A data directory gets a format 4 identity
marker in one synced write just before its first hash or Watch journal row, or with its first
snapshot image. Older binaries then refuse the directory. A directory that this binary only
opened, without writes, stays readable by older binaries.
They fail explicitly and do not misread the rows. A downgrade is not supported.
To upgrade a cluster, upgrade the followers first. Then transfer leadership and upgrade the old
leader. A follower on an older binary cannot install a snapshot from an upgraded leader.
An older member that installed a checkpoint with `meta/applied-term` exports that row in its
backups. Restore such a backup with the newer binary. Older restores reject the row.
Use `--restore-snapshot` for native backups; `etcdutl` cannot restore them.
Use the [native export tool](docs/operations.md#backups-and-restoration) to save a backup.
`etcdctl snapshot save` can reject the native image because it requires bbolt page alignment.
Downgrade returns Unimplemented after administrator authorization.
Automatic certificate generation and certificate reload remain unsupported.
HashKV requires revision records from database creation; older databases return a format error.

Physical Compact waits for flushed MVCC deletions. Defragment rewrites active SSTs.
Neither operation promises immediate filesystem shrinkage. SlateDB checkpoints can retain objects.
Quota uses logical bytes and can be exceeded by one request. It is not a free-space reservation.

State scans and full snapshot buffers do not have independent memory quotas.
KV responses use etcd's send limit of `math.MaxInt32` bytes.
An unpaginated Range holds its whole response in memory, as on etcd.
Native backups and peer snapshots have a 64 MiB limit.
Lease remaining TTL is not checkpointed. A new leader resets surviving leases to their full TTL.
DST does not control wall-clock lease expiry, operating system threads, or partial sector writes.
The local Jepsen driver does not reproduce the upstream SSH, clock, bit-flip, or lazyfs environment.

The [architecture](docs/architecture.md) separates the current implementation from the target
design.
The [administration guide](docs/administration.md) gives secure listener, membership, and restore
steps.
The [upstream audit](tests/upstream/README.md) indexes 1,771 upstream functions without claiming
that
all of them run against the Rust implementation.

## Downstream and fault additions

The [fault map](tests/upstream/fault-mapping.md) accounts for all 41 named gofail hooks at etcd
v3.6.5.
Twenty-five native crash boundaries and two TLS client failover cases have real process coverage.
Selected original Kubernetes storage tests and the CoreDNS etcd plugin run against both backends.
The [retained remote results](results/remote-storage/README.md) record the new evidence and limits.
