# Remote state storage and host validation

Every cluster, also a single member, needs S3-compatible storage. Each replica keeps its Raft log
on local durable storage. A quorum commits writes through these logs. A normal write does not wait
for an S3 upload. S3 stores one shared state of the cluster, not a shared Raft log.

## Namespace ownership

`--s3-bucket BUCKET` is required. A start without it fails. `--s3-region` defaults to
`us-east-1`.
Use `--s3-endpoint https://ENDPOINT` for a compatible service. The bucket must already exist.
The endpoint cannot contain credentials, a path, a query, or a fragment.
`--s3-allow-http` permits plaintext only for an isolated test service.

`--s3-prefix` defaults to `slatedb-etcd`. The effective cluster prefix is:

```text
<prefix>/<sha256(cluster-id)>/
    shared/owner.json
    shared/state/
    member-<id>/binding.json
```

Only the leader writes the shared SlateDB. A Raft snapshot names a checkpoint of it, so an install
copies no data. See the [shared state guide](shared-state-guide.md#object-layout).
A synced `object-store.json` file binds the local directory to the backend, a random owner ID,
and the store ID of the cluster claim. An atomic conditional PUT claims the member binding. A
different local owner cannot reuse it. A lost claim response is safe to retry with the same
directory and identity.

Keep the binding and Raft directory together. Changing the bucket, endpoint, region, prefix,
cluster, or member requires a new directory and a deliberate restore or member replacement. A data
directory of replicated state mode or of a member-local store is not supported. A copied directory
must not run concurrently with its original. SlateDB writer fencing is a final defense, not an
operator lock service.

A native backup restore requires a new cluster identity or unused S3 prefix.
Losing the local Raft log can lose committed work that has not reached S3.
Replace that member from the surviving quorum. Do not treat its S3 prefix as a complete backup.

## Credentials and retries

Credentials come from AWS access-key, session-token, web-identity, container-identity, or instance
metadata providers. Shared AWS profile files are not read by this adapter.
Secret values have no command-line flags and do not appear in the binding file.
Grant access to the cluster prefix as the [shared state guide](shared-state-guide.md) states.
Conditional object creation and update must work correctly.

The HTTP client uses a three-second connection timeout and a ten-second request timeout.
It permits two retries within a fifteen-second retry window. SlateDB adds three operation retries.
These are per-operation bounds, not an end-to-end outage deadline.
SlateDB can continue to retry an SST upload. Compaction and administration can wait for storage.
Backpressure can eventually stop writes during a long outage. This is not an availability promise
for an unavailable object service.

A checkpoint cannot publish or reclaim its covered Raft log before its required object work
completes. A terminal checkpoint error stops the node. Its durable log remains the recovery source.

## Remote listeners

`--allow-remote` permits non-loopback IPs and DNS peer names. It requires:

- Separate `--listen` and `--peer-listen` addresses.
- Client certificates, trusted client roots, and `--client-cert-auth`.
- Peer certificates, private keys, and trusted peer roots.
- HTTPS peer URLs and an HTTPS `--client-peer` URL for each initial member.

Certificates must cover the advertised DNS names or IP addresses. Use different client and peer CAs.
Remote-mode membership changes cannot add plaintext peer URLs.
A peer certificate authorizes Raft transport, forwarding, and diagnostic controls.
Restrict that trust boundary to operators and member processes.

## Validation

The S3 suite starts RustFS 1.0.6 on loopback with a temporary bucket and credentials.
It verifies the release archive checksum before extracting the server.
The fixture supports Apple Silicon macOS and GNU/Linux on x86-64 and ARM64.
Run the recovery suite with these commands.

```sh
scripts/cargo.sh build --locked --release
python3 scripts/check-s3.py --output results/ci/s3.json
```

It covers delayed uploads, lost successful PUT responses, failed reads, object outages,
process crashes, and full cluster restart. It verifies acknowledged data and
checks that pending object work cannot advance the Raft reclamation boundary.

The DST suite injects delays and errors through the real ObjectStore interface.
It checks recovery reads, terminal upload errors, and ambiguous successful PUT results.
A seed selects the member and checkpoint completion schedule. Each trace must replay identically.
SlateDB background task order and the operating system remain outside the deterministic scheduler.

The three-guest test uses independent Linux kernels, data disks, and network stacks.
It uses mutual TLS, peer DNS names, and a shared temporary S3 service.
It stops a leader VM, drops peer packets with guest firewall rules, blocks S3 access, and restarts
an offline follower through a Raft snapshot. Deployment files are synced before fault injection.
The driver does not sync application data before a forced VM stop.

All guests and the S3 fixture share one physical host. These results do not establish independent
host, disk-controller, power, availability-zone, or production S3 failure tolerance.
The driver removes its temporary guests after the run. On Apple Silicon with Lima installed, run:

```sh
python3 scripts/check-multihost.py --output results/ci/multihost.json
```

The script creates three guests with 6 GiB of total assigned memory and thin 16 GiB disks.
It builds the Linux binary in the first guest.
`--linux-binary` can reuse an existing Linux ARM64 build.
`--reuse` accepts three disposable guest names. It deletes those guests after the run.

[Lima guest networking](https://lima-vm.io/docs/config/network/user-v2/) and
[RustFS release](https://github.com/rustfs/rustfs/releases/tag/1.0.6).
