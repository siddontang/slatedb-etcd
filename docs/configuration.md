# Server configuration

Run `target/release/slatedb-etcd --help` for the parser's current options.
Arguments use bytes unless a table states otherwise. All bind addresses must use loopback IPs.
Initial peer addresses also require loopback IPs; the command parser does not resolve DNS names.

## Identity and listeners

| Flag | Meaning and default |
| --- | --- |
| `--id` | Required nonzero member ID. Keep it with its existing data directory. |
| `--listen` | Required client IP and port. |
| `--peer` | Required, repeatable `ID=IP:PORT` or `ID=URL`. Include the local member. |
| `--dir` | Required data directory. Contains `raft/` and `objects/`. |
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
| `--peer-trusted-ca-file` | Peer CA roots. Peer TLS always requires mutual authentication. |
| `--auth-token` | `simple` by default; also accepts the RS256 configuration below. |

```text
jwt,pub-key=/path/public.pem,priv-key=/path/private.pem,sign-method=RS256,ttl=300s
```

Auth starts disabled. Configure users and roles through the official Auth API before enabling it.
All members must use the same JWT key pair and token settings.
The default simple-token lifetime is 300 seconds. Auth changes invalidate existing tokens.
Certificates are read at startup. Automatic generation and certificate reload are unsupported.
Keep client and peer trust roots separate. See [security](../SECURITY.md).

## Storage and log memory

| Flag | Default | Meaning |
| --- | ---: | --- |
| `--snapshot-every` | 1000 | Applied entries between checkpoint opportunities; zero disables it. |
| `--quota-backend-bytes` | 0 | Logical data quota; zero disables it. |
| `--log-cache-bytes` | 8388608 | Cache budget for log entries. |
| `--log-pending-bytes` | 33554432 | Pending entry budget, including unapplied entries. |
| `--log-read-bytes` | 16777216 | Budget for asynchronous log read results. |
| `--log-term-bytes` | 1048576 | Budget for compressed term metadata. |

The read budget must hold one maximum entry. An encoded entry cannot exceed 8 MiB.
These flags do not bound process RSS, SlateDB, state scans, or snapshot buffers.
The quota can be exceeded by one operation and does not reserve free disk space.
Native backups and peer snapshot transfers have a 64 MiB limit.
See [log pool accounting](architecture.md#bounded-log-pools) for admission and recovery rules.

## Environment

`RUST_LOG` selects log filters. The default is `info`.
For example, `RUST_LOG=slatedb_etcd=info,slatedb=warn` reduces storage log volume.
`PROTOC` selects the build-time protobuf compiler; it is not a server option.
Test-only environment variables appear in the [test guide](../tests/README.md).
