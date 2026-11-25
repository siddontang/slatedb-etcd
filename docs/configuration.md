# Server configuration

Run `target/release/slatedb-etcd --help` for the parser's current options.
Arguments use bytes unless a table states otherwise. Listeners and initial peers use loopback IPs
by default. `--allow-remote` permits remote IPs and DNS names with separate mutual TLS listeners.

## Identity and listeners

| Flag | Meaning and default |
| --- | --- |
| `--id` | Required nonzero member ID. Keep it with its existing data directory. |
| `--listen` | Required client IP and port. |
| `--allow-remote` | Permit remote addresses with mutual client and peer TLS. |
| `--peer` | Required, repeatable `ID=IP:PORT` or `ID=URL`. Include the local member. |
| `--dir` | Required directory for Raft, object binding, and optional local objects. |
| `--cluster-id` | Shared cluster identity; default `slatedb-raft-poc`. |
| `--peer-listen` | Separate peer bind address. TLS requires this flag. |
| `--client-peer` | Repeatable `ID=URL` for advertised client addresses. |
| `--join` | Join an existing cluster after MemberAdd. Start with a new directory. |
| `--restore-snapshot` | Restore a native backup into a new directory; conflicts with `--join`. |
| `--strict-reconfig-check` | `true` by default. Requires an explicit `true` or `false` value. |

Without a separate peer listener, the plaintext port also serves trusted diagnostic controls.
With separate listeners, the client port exposes gRPC and the peer port carries internal traffic.
Keep the original bootstrap arguments when restarting an existing directory.
The recovered member store controls the current voters and transport addresses.

## TLS and Auth

| Flag | Meaning |
| --- | --- |
| `--cert-file` | Client listener's PEM certificate chain. Requires `--key-file`. |
| `--key-file` | Client listener's PEM private key. |
| `--trusted-ca-file` | Client CA roots. Used with client certificate authentication. |
| `--client-cert-auth` | Require verified client certificates; disabled by default. |
| `--peer-cert-file` | Peer certificate chain, also used for outgoing peer identity. |
| `--peer-key-file` | Peer private key. |
| `--peer-trusted-ca-file` | Peer CA roots for mutual TLS with explicit certificates. |
| `--auto-tls` | Generate a self-signed certificate for the client listener. |
| `--peer-auto-tls` | Generate a self-signed certificate for the peer listener. |
| `--self-signed-cert-validity` | Set certificate validity in years. The default is one year. |
| `--auth-token` | `simple` by default; also accepts the RS256 configuration below. |

AutoTLS stores each identity in `fixtures/client` or `fixtures/peer` under the data directory.
Restarts reuse the certificate and key. Explicit certificate and key files take precedence.
A client can trust its listener's `cert.pem`. Each automatic peer checks the certificate name,
validity, and signature. Each peer supplies its own trust anchor, so this mode provides encryption
without trusted cluster membership. Remote mode continues to require mutual TLS on both listeners.

```text
jwt,pub-key=/path/public.pem,priv-key=/path/private.pem,sign-method=RS256,ttl=300s
```

Auth starts disabled. Configure users and roles through the official Auth API before enabling it.
All members must use the same JWT key pair and token settings.
The default simple-token lifetime is 300 seconds. Auth changes invalidate existing tokens.
Certificates are read at startup. Automatic generation and certificate reload are unsupported.
Keep client and peer trust roots separate. See [security](../SECURITY.md).

## Monitoring

Expose additional scrape listeners with `--listen-metrics-urls`.
The default `--metrics basic` counts gRPC calls and messages.
Select `--metrics extensive` to time calls by method.
See the [metrics guide](metrics.md) for listener security and per-member metric coverage.

## Runtime

`--runtime-workers` sets the async worker count. Zero is invalid. The default is the number of CPUs
that the process may use: the affinity mask and the cgroup CPU quota limit it, and two is the
fallback when that count is unavailable. Pin the process with `taskset` or set a cgroup quota to
reserve CPUs for other processes. Log I/O uses a separate thread. Embedded servers use the caller's
runtime. More workers can increase task handoffs. Measure the target workload before changing this
setting.

## Watch progress

Use `--watch-progress-notify-interval` to set the interval for idle watches with progress enabled.
The default is 10 seconds. Zero selects the default. Positive values below 100 ms select 100 ms.
For example, use `--watch-progress-notify-interval=200ms` for the upstream client examples.

## Storage and log memory

| Flag | Default | Meaning |
| --- | ---: | --- |
| `--snapshot-every` | 10000 | Applied entries between checkpoint opportunities; zero disables it. |
| `--snapshot-min-bytes` | 67108864 | Log bytes needed before auto truncation; zero disables it. |
| `--log-file-bytes` | 67108864 | Log file size, at least 65536. Truncation frees whole files. |
| `--quota-backend-bytes` | 0 | Seeds or changes the cluster quota. Zero selects 2 GiB. |
| `--quota-reassert` | off | Requests the quota flag again as the newest change. Use it once. |
| `--quota-warning-fraction` | 0.8 | Quota share that raises the early warning; zero disables it. |
| `--auto-compaction-mode` | periodic | `periodic` keeps a duration; `revision` keeps a count. |
| `--auto-compaction-retention` | 0 | History to keep. Zero disables automatic compaction. |
| `--log-cache-bytes` | 8388608 | Cache budget for log entries. |
| `--log-pending-bytes` | 33554432 | Pending entry budget, including unapplied entries. |
| `--log-read-bytes` | 16777216 | Budget for asynchronous log read results. |
| `--log-term-bytes` | 1048576 | Budget for compressed term metadata. |
| `--state-block-cache-bytes` | 67108864 | Weight budget for cached SST data blocks. |
| `--state-meta-cache-bytes` | 67108864 | Budget for cached SST indexes, filters, and stats. |

The read budget must hold one maximum entry. An encoded entry cannot exceed 8 MiB.
The two state cache budgets add up to 128 MiB by default. Each must be positive.
Keep the metadata budget above total SST metadata. With 32-byte keys and 256-byte values,
metadata is about 2% of live SST data.

An automatic checkpoint flushes state to synced local storage and then truncates the Raft log.
It needs both `--snapshot-every` applied entries and `--snapshot-min-bytes` appended log bytes.
Small writes therefore keep more log between truncations, and each synced flush covers about one
L0 table. Delaying truncation keeps more log; it never removes entries early.
State recovers to its last synced flush. A restart can replay up to the larger of the two
thresholds: by default 10,000 entries or 64 MiB of log, plus the entries applied after them.
Membership changes and peer snapshot requests do not wait for the byte threshold.
These flags do not bound process RSS, SlateDB, state scans, or snapshot buffers.
A memtable flush caches the SST index, filters, and current-row blocks; reads cache the rest.
Compaction reads ahead 2 x 512 KiB per input SST. Each compaction job runs at most two
subcompactions, because each one buffers SST data until the disk takes it.
On Linux, jemalloc background threads return freed pages to the OS after 200 ms.
`_RJEM_MALLOC_CONF` overrides that default.
A negative quota value disables the quota. The quota is cluster state, so a flag change takes
effect only when that member leads; see [quota changes](operations.md#change-the-quota).
The quota can be exceeded by one operation and does not reserve free disk space.
Native backups and `Maintenance.Snapshot` have a 64 MiB limit. Peer snapshots copy SlateDB
checkpoint objects and have no size limit; see [object storage Raft snapshots](object-snapshots.md).
See [log pool accounting](architecture.md#bounded-log-pools) for admission and recovery rules.

## Automatic compaction

The two `--auto-compaction-*` flags use the names, values, and schedule of etcd v3.6.
Periodic mode reads an integer retention as hours, or a Go duration such as `30m` or `1.5h`.
It samples the revision ten times per retention period, capped at one sample per six minutes.
The first Compact waits one period. Later ones run once per period, or hourly for longer periods.
Each uses the newest sample that is at least one period old. The member checks once per 100 ms
Raft tick, so a short period can keep up to one extra tick of history.
Revision mode reads a revision count. Every five minutes it compacts to the current revision
minus that count. Like etcd, it reads a duration as its nanosecond count.
A duration is parsed like Go's `time.ParseDuration`, so each part truncates to whole nanoseconds.
A negative retention is an error. etcd accepts a negative duration but cannot use it.
An embedding host that sets `ServerConfig.auto_compaction` gets the same rules: zero disables it.

Only the leader proposes. Each Compact goes through Raft with a fixed revision, like a client
Compact without Physical. Followers keep sampling, so a new leader compacts on its own schedule.
A Compact that loses leadership fails and retries later. A revision that is already compacted
counts as done. A restart drops the samples, so the member waits one full period again.
While compacted rows are still being removed, the leader keeps sampling but proposes no new
automatic Compact. A new boundary would restart the removal step that is being read.
Compaction runs as an internal request, so Auth does not apply to it.

## S3 objects

| Flag | Default | Meaning |
| --- | --- | --- |
| `--s3-bucket` | None | Select an existing S3 bucket instead of local objects. |
| `--s3-region` | `us-east-1` | Bucket region. |
| `--s3-endpoint` | AWS endpoint | Compatible S3 origin URL. |
| `--s3-prefix` | `slatedb-etcd` | Root prefix for isolated cluster and member namespaces. |
| `--s3-allow-http` | Disabled | Permit plaintext to an isolated test object service. |

See [remote storage](remote-storage.md) for credentials, ownership, retry bounds, and recovery.
The local Raft log remains necessary. S3 does not replace quorum log durability.

## Shared state mode

`--state-mode shared` selects the prototype of the shared state design, `docs/shared-state.md`,
for a new cluster. The leader writes one SlateDB for the whole cluster. Each member reads a
checkpoint of it, its base, under an in-memory overlay of the newer applied batches.
`replicated` is the default and keeps the per-member layout. The
[shared state guide](shared-state-guide.md) covers operation, access, cost, and conversion.

| Flag | Default | Meaning |
| --- | --- | --- |
| `--state-mode` | `replicated` | State layout of a new cluster: `replicated` or `shared`. |
| `--view-max-age` | `10s` | Age of the oldest unflushed write that starts a flush and checkpoint. |
| `--view-pin-max` | `5m` | Age after which a read view fails with a retryable error. |
| `--overlay-max-bytes` | 1073741824 | Leader admission limit of the overlay. Apply waits at 2x. |
| `--max-clock-skew` | `5s` | A new writer does not start with its clock this far behind. |
| `--view-retention` | `off` | `off` keeps all view checkpoints. `fenced` deletes unpinned ones. |

Rules and limits:

- All members use one S3 bucket and prefix. Learners and `--join` members are supported.
- The cluster claim `shared/owner.json` stores the mode, the quota, and the initial voters.
  Later starts use that quota. A member with other initial voters refuses to open the writer.
- On S3, `mode.json` under the cluster prefix fixes the mode before any state write. A shared start
  also refuses a prefix that holds a replicated member namespace.
- A directory keeps its mode. A member refuses to start in a different mode.
- The leader opens the writer when it takes the lead. The open fences the old writer. The leader
  then checks that it still leads the same term, and waits until its clock passes the manifest
  clock. A fence while it still leads opens the writer again.
- Over `--max-clock-skew`, the writer does not start. The member logs an error, sets
  `clock_alarm` in `/status`, exports `slatedb_etcd_shared_clock_skew_milliseconds`, and tries
  again each second.
- MoveLeader first requests a checkpoint of the applied state. It transfers the lead when the
  applied base covers the applied index of the request, or 3 s after the newest pending
  MoveLeader request even if the base still trails that index. The new leader then materializes
  the overlay above the base. The request fails after 10 s if the target does not lead.
- A new writer, and each explicit publication request, announces the newest checkpoint again
  when no applied announcement or base covers it. A writer that stopped after a checkpoint and
  before its announcement committed leaves such a checkpoint.
- Raft can drop an announcement proposal, for example during a leader transfer. The writer then
  announces the checkpoint again after a backoff from 100 ms to 2 s, also on an idle cluster. A
  failed check of the checkpoint list is also tried again. After Raft takes a proposal, the
  writer checks again after 2 s. A writer does not announce a checkpoint again when the leader
  applied its announcement, or when its index is at or below the leader's base.
- A member opens announced checkpoints one at a time. A started open runs to its end, so the
  base advances under a steady stream of announcements. Only the newest announced checkpoint
  waits for the next open. A failed open tries again after a backoff from 100 ms to 2 s, also
  when the member is idle, unless a newer checkpoint waits.
- Two writer opens can race for the same manifest version. A leader that loses the race opens
  the writer again, as after a fence.
- Each member truncates its log through its base index. Its Raft snapshot names the base
  checkpoint at that index. A member that receives the snapshot opens the checkpoint and adopts
  it. The base index can be above the checkpoint row when an announcement certified that no
  entry in between changed state.
- A follower can ask for a snapshot at its last index, for example after the guard below. A
  follower's manual checkpoint can also need a base above the leader's base index. The leader
  then announces its base checkpoint again with its covered index, if it stored no batch above
  the checkpoint row. Otherwise it publishes a new checkpoint first. One announcement serves
  each demand, and an idle cluster then appends nothing more.
- A request of an earlier term can name a suffix that the leader replaced. The leader serves
  each peer at most its last log index, pinned at the first request, so the follower takes a
  snapshot and then appends. Both state modes do this.
- A member reports whether it installs a base above its checkpoint row, with the ID of its
  process. The leader extends coverage only while every member reported it. An earlier binary
  gets only a snapshot at its checkpoint row: the leader publishes a new checkpoint for it when
  its record is above the row. While some member lacks the capability, any request above the
  record gets such a checkpoint. A snapshot above its row uses its own route, which an earlier
  binary refuses before Raft.
- A restart opens the newest checkpoint. It adopts the checkpoint when the log ends below it. A
  Raft snapshot record above the newest checkpoint row names its checkpoint, and the restart
  opens that one at the record index. If retention deleted it, the restart takes the newest
  checkpoint whose row is between the record row and the record index, and the leader then
  records and sends that checkpoint.
- A missing checkpoint object fails a read with a retryable error. In apply it rebuilds the
  state from the newest checkpoint and replays the local log.
- A member that waits on its overlay limit adopts a newer checkpoint in process when its log
  commits the checkpoint index.
- Defragment runs on the leader. A manual checkpoint returns when the member's base covers the
  commit index. `--restore-snapshot` seeds a new cluster. See the
  [shared state guide](shared-state-guide.md#operations).

### View checkpoint retention

Automatic deletion of `view-` checkpoints is off by default. SlateDB 0.18.1 deletes a
checkpoint without a writer epoch check. A deposed leader can pass its own epoch check, and a new
leader can take the writer before the deletion. The deletion can then remove a checkpoint that a
member of the new leader pins.

- Each kept checkpoint pins its SSTs, so object storage grows with the number of checkpoints.
  `slatedb_etcd_shared_view_checkpoints` reports their number after each publication.
- `--view-retention fenced` deletes old checkpoints that no member reported in the last 10
  minutes. It checks the writer epoch before each deletion, and a new leader waits 10 seconds
  for member reports first. The race above remains.
- Manual cleanup is safe with the whole cluster stopped. Keep the newest checkpoint and the
  checkpoint of each member's Raft snapshot record, then delete older `view-` checkpoints with
  the SlateDB admin API. Start the members again after the deletion.
- Checkpoint expiry is future work. It needs recovery after an offline period longer than the
  lifetime, and follower reads across missed renewals.
- `object::Config::SharedDirectory` lets members of one host share a local directory in tests.
  A member-local store serves one voter.

## Environment

`RUST_LOG` selects log filters. The default is `info`.
For example, `RUST_LOG=slatedb_etcd=info,slatedb=warn` reduces storage log volume.
`PROTOC` selects the build-time protobuf compiler; it is not a server option.
Test-only environment variables appear in the [test guide](../tests/README.md).
