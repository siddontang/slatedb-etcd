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
4. Wait for snapshot installation and log catch-up. Then call `member promote ID`.
5. Use `move-leader ID` when the new voter should become leader.

MemberUpdate changes where members dial that peer. As in etcd, a working connection stays open.
Raft and forwarded requests use the new URL after that connection fails.

Keep the same bootstrap arguments when restarting an existing data directory.
The persisted membership controls voting and transport after recovery.
A removed member ID cannot rejoin. Do not reuse its old data directory for a new member.
Strict reconfiguration checks protect quorum by default. Disabling them can make a cluster
unavailable.

## Write capacity and sharding

All writes go through one Raft leader and one log. More members add read capacity and fault
tolerance, not write capacity. The leader's CPU and its log sync rate set the write ceiling.
See [scale measurements](benchmarks.md#measure-scale-against-etcd) for one host.

The service cannot split one keyspace over several Raft groups. To spread writes now, run more
than one cluster and give each cluster a part of the keyspace. For kube-apiserver, use
`--etcd-servers-overrides`. It sends the listed resources to their own cluster:

```sh
kube-apiserver \
  --etcd-servers=https://main-1:2379,https://main-2:2379,https://main-3:2379 \
  --etcd-servers-overrides='/events#https://ev-1:2379;https://ev-2:2379;https://ev-3:2379'
```

Each override has the form `group/resource#server;server`. Separate overrides with commas.
The core group is empty, so `/events` selects core Events. Events are a common first choice.
They have a high write rate and a short TTL (1 hour by default). No controller needs them for
correctness.
Other resources with many writes, such as `coordination.k8s.io/leases`, can move the same way.

Each cluster is independent. Make sure that your design accepts these limits:

- Each cluster has its own revision. Resource versions from two clusters cannot be compared.
  The apiserver compares them only inside one resource, so it works with this split.
- No transaction spans two clusters. The apiserver does not need one between resources.
- Each cluster needs its own quota, compaction, monitoring, and backups. The apiserver compacts
  each cluster on `--etcd-compaction-interval`.
- A restore of two clusters does not give one consistent point in time across them.
- One resource cannot span two clusters. A single resource with too many writes still meets the
  ceiling of one cluster.

The split does not change the support status above. The service is still not a supported
Kubernetes backend. The clusters can be etcd, this service, or a mix.

## Maintenance and storage

Status returns local identity, Raft progress, size estimates, quota, and alarms.
The gRPC health service checks quorum and local apply progress.
`--quota-backend-bytes` limits logical data bytes. Zero selects the default 2 GiB limit.
A negative value disables the limit. The members share one quota; see
[quota changes](operations.md#change-the-quota). Status returns the flag, as etcd does, unless
the cluster quota differs from it.
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
Earlier Raft recovery snapshots used this format. Current ones use checkpoint descriptors.
It is not an etcd bbolt backup and cannot be restored with `etcdutl`.

Restore it with `--restore-snapshot /path/native.snapshot` when starting each new member.
Use new, empty data directories, a new cluster ID, and the same new peer map on all restored
members.
The restore path verifies the checksum, replaces membership, and invalidates old auth tokens.
It retains KV revisions, compaction, leases, users, roles, and alarm types.
Each lease gets generation 1. The source generation is a Raft index of the old log.
Remove `--restore-snapshot` when restarting the restored cluster. Existing directories are never
replaced. A damaged checksum stops restoration before any data directory is created.
The member pins its applied state in a SlateDB checkpoint and streams the image from it.
Writes continue while the stream runs and do not reach the image.
The server holds a few 64 KiB messages, not the image, so the image has no size limit.
Restore decodes one row at a time, so one encoded row can have at most 64 MiB.
The auth row holds all users and roles and is the row that can grow this large.
A backup checks every row before it sends the first message. It returns ResourceExhausted for a
larger row, so it never publishes an image that restore rejects.
The server reads the checkpoint twice: first to count the rows, then to send them.
The checkpoint lives 10 minutes. While the stream runs, even with a client that stops reading,
the member renews it every minute, and it deletes it at the end.
Restore checks the file in one pass and writes the rows in 4 MiB batches in a second pass.
The second pass must read a file with the checked digest.
It also holds only a few rows in memory. The restored Raft index is the copied row count plus one.
A restore holds the Raft directory lock while it writes, and a starting member takes the same lock.
A restore that loses the lock, or finds the directory in use, leaves the directory unchanged.
An interrupted restore leaves a `restore-incomplete` file, and startup rejects that directory.
Remove the directory and restore again.
With S3 state objects, the restore can have claimed the member namespace
`<s3-prefix>/<SHA-256 of the cluster ID>/member-<id>/` and written objects there.
The claim in its `owner.json` names the random owner in the directory's `object-store.json`,
so a retry with the same settings fails with `S3 namespace belongs to another data directory`.
Before you remove the directory, compare the two files. Delete the objects under that member
namespace only when they name the same owner. A different owner means that another data
directory, possibly a running member, uses the namespace: the restore wrote nothing there.
When they differ, or the directory is already gone, restore all members with a new cluster ID or
`--s3-prefix`.
In [shared state mode](shared-state.md) the member pins its applied view instead and creates no
checkpoint. The stream reads the view twice, as above, so the image has no size limit. Each row
has the same 64 MiB limit.
The backup view is exempt from `--view-pin-max`. The stream ends with Unavailable when the client
takes no message for 60 s. The first pass does not count.
A member streams at most 4 backups. The overlay segments that they keep after a base switch
count against `--overlay-max-bytes`. So a backup of a full overlay can start.
A backup above a limit returns ResourceExhausted; retry it later. When base switches leave the
kept segments above the limit, the oldest backup ends with Unavailable.
See [backups](shared-state.md#backups).
A restore into shared mode also checks the file in one pass and writes 4 MiB batches. It writes
the rows once into the shared database, so it has no `restore-incomplete` file. Another member
with the same file continues a stopped restore. The restored Raft log starts at N, the copied row
count plus one. Endpoint status can show a higher applied index after the election. KV responses
retain the snapshot's MVCC revision until the next modifying transaction. A member serves no
request until its restore completes, so give startup probes and service start timeouts that time.
See [the shared restore](shared-state-guide.md#operations).

Downgrade authenticates the administrator and returns Unimplemented.
The SlateDB format cannot run an older etcd binary. No rolling binary downgrade is claimed.
Status advertises the compatible etcd API version, not a shared on-disk format.
