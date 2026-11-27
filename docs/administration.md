# Administration and compatibility limits

The service implements the etcd v3 administration wire API over one Raft group.
Use the official etcd v3.6.5 client or `etcdctl` for the supported operations.
It is not a supported Kubernetes backend or a binary replacement for etcd.

## Authentication

Create a root user and grant the root role before enabling authentication:

```sh
etcdctl --endpoints=http://127.0.0.1:7001 user add root
etcdctl --endpoints=http://127.0.0.1:7001 user grant-role root root
etcdctl --endpoints=http://127.0.0.1:7001 auth enable
etcdctl --endpoints=http://127.0.0.1:7001 --user=root auth status
```

Users, roles, range grants, password hashes, tokens, and auth revisions replicate through Raft.
Authentication uses random tokens by default. `--auth-token` also accepts an RS256 key pair:

```text
jwt,pub-key=/path/public.pem,priv-key=/path/private.pem,sign-method=RS256,ttl=300s
```

All nodes must use the same token configuration. The token store has a 10,000-token limit.
Auth changes invalidate existing tokens. The official client obtains a new token when required.
TLS client authentication can use a certificate CN as the user name, including passwordless users.
Use TLS for password authentication on an untrusted transport.

## TLS listeners

Set separate client and peer ports. Repeat the peer and client addresses on each initial member.
For node 1, the relevant options are:

```sh
--listen 127.0.0.1:7001 --peer-listen 127.0.0.1:7101 \
--peer 1=https://127.0.0.1:7101 --peer 2=https://127.0.0.1:7102 \
--peer 3=https://127.0.0.1:7103 \
--client-peer 1=https://127.0.0.1:7001 --client-peer 2=https://127.0.0.1:7002 \
--client-peer 3=https://127.0.0.1:7003 \
--cert-file client-server.pem --key-file client-server-key.pem \
--trusted-ca-file client-ca.pem --client-cert-auth \
--peer-cert-file peer.pem --peer-key-file peer-key.pem --peer-trusted-ca-file peer-ca.pem
```

Certificates must contain the advertised IP address or DNS name in their SAN extension.
Peer TLS requires client certificates. Keep peer and client trust roots separate.
The peer listener carries Raft, forwarded requests, and trusted diagnostic controls.
The client listener carries only gRPC services and cannot supply a forwarded peer identity.
Remote listeners require `--allow-remote` and mutual TLS on separate ports.
See [remote storage](remote-storage.md). Certificate reload remains future work.
The server does not generate automatic TLS certificates.

## Member changes

MemberAdd, MemberAddAsLearner, MemberUpdate, MemberRemove, and MemberPromote use the official API.
Use a learner when replacing or adding a server. The service allows one learner at a time.

1. Add its peer URL with `etcdctl member add NAME --learner --peer-urls=URL`.
2. Start the new process with the returned member ID, `--join`, and the same cluster ID.
3. Supply the full current peer map, including the new member. Configure its client address as
   above.
4. Wait for snapshot installation and log catch-up. Then call `member promote ID`. In shared
   state mode, the learner must also prove write access to the shared state. The first call
   then fails with a retryable error and starts the check. Call it again after some seconds.
5. Use `move-leader ID` when the new voter should become leader.

MemberUpdate changes where members dial that peer. As in etcd, a working connection stays open.
Raft and forwarded requests use the new URL after that connection fails.

Keep the same bootstrap arguments when restarting an existing data directory.
The persisted membership controls voting and transport after recovery.
A removed member ID cannot rejoin. Do not reuse its old data directory for a new member.
Strict reconfiguration checks protect quorum by default. Disabling them can make a cluster
unavailable.

## Maintenance and storage

Status returns local identity, Raft progress, size estimates, quota, and alarms.
The gRPC health service checks quorum and local apply progress.
`--quota-backend-bytes` limits logical data bytes. Zero selects the default 2 GiB limit.
A negative value disables the limit. The members share the quota of the cluster claim; see
[quota](operations.md#quota). Status returns the flag, as etcd does, unless the cluster quota
differs from it.
The limit can be exceeded by one request. It does not reserve free disk space.
NOSPACE alarms reject writes; deletion and compaction can free logical space before alarm disarm.

Physical Compact waits for the MVCC deletion batch to flush to SlateDB.
Defragment rewrites the current SST set and waits for the replacement manifest.
Neither operation promises immediate filesystem shrinkage. SlateDB retains objects needed by
checkpoints and reclaims obsolete objects through its own garbage collector.

HashKV uses etcd's CRC32C revision encoding and compaction semantics.
Databases created before the hash journal cannot return a complete HashKV result.
The deprecated Hash RPC hashes native records. Its result cannot be compared with a bbolt backend
hash.

Snapshot streams the native state image with a trailing SHA-256 digest.
Save it with `etcdctl snapshot save` or the
[native export tool](operations.md#backups-and-restoration).
The server pads the membership row with JSON spaces, so the file length passes the etcdctl check.
It includes auth, membership, alarms, leases, and MVCC metadata.
It is not an etcd bbolt backup and cannot be restored with `etcdutl`.

Restore it with `--restore-snapshot /path/native.snapshot` when starting each new member.
Use new, empty data directories, a new cluster ID, and the same new peer map on all restored
members.
The restore path verifies the checksum, replaces membership, and invalidates old auth tokens.
It retains KV revisions, compaction, leases, users, roles, and alarm types.
Each lease gets generation 1. The source generation is a Raft index of the old log.
Remove `--restore-snapshot` when restarting the restored cluster. Existing directories are never
replaced. A damaged checksum stops restoration before any data directory is created.
The member pins its applied view and streams the image from it. It creates no checkpoint.
Writes continue while the stream runs and do not reach the image.
The server holds a few 64 KiB messages, not the image, so the image has no size limit.
Restore decodes one row at a time, so one encoded row can have at most 64 MiB.
The auth row holds all users and roles and is the row that can grow this large.
A backup checks every row before it sends the first message. It returns ResourceExhausted for a
larger row, so it never publishes an image that restore rejects.
The server reads the view twice: first to count the rows, then to send them.
The backup view is exempt from `--view-pin-max`. The stream ends with Unavailable when the client
takes no message for 60 s. The first pass does not count.
A member streams at most 4 backups. The overlay segments that they keep after a base switch
count against `--overlay-max-bytes`. So a backup of a full overlay can start.
A backup above a limit returns ResourceExhausted; retry it later. When base switches leave the
kept segments above the limit, the oldest backup ends with Unavailable.
See [backups](shared-state.md#backups).
A restore checks the file in one pass and then writes 4 MiB batches. It writes the rows once into
the shared database. The second pass must read a file with the checked digest. Another member
with the same file continues a stopped restore. The restored Raft log starts at N, the copied row
count plus one. Endpoint status can show a higher applied index after the election. KV responses
retain the snapshot's MVCC revision until the next modifying transaction. A member serves no
request until its restore completes, so give startup probes and service start timeouts that time.
See [the shared restore](shared-state-guide.md#operations).

Downgrade authenticates the administrator and returns Unimplemented.
The SlateDB format cannot run an older etcd binary. No rolling binary downgrade is claimed.
Status advertises the compatible etcd API version, not a shared on-disk format.
