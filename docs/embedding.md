# Embed a member in Rust

Use `server::Server` to run a member inside a Rust process. The CLI uses the same API.
The host owns the Tokio runtime, logging, process signals, and shutdown policy.
The library installs the Rustls ring provider only when the process has no default provider.

This API is experimental. Pin a reviewed Git commit because the crate is not published to crates.io.
The Rust API can change before a stable release.

```toml
[dependencies]
anyhow = "1"
slatedb-etcd = { git = "https://github.com/siddontang/slatedb-etcd", rev = "<reviewed-commit>" }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

## Start and stop

```rust
use std::time::Duration;
use slatedb_etcd::server::{Server, ServerConfig};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let server = Server::start(ServerConfig::single_node("data/member-1")).await?;
    let ready = server.wait_ready(Duration::from_secs(10)).await;
    if ready.is_ok() {
        println!("Client endpoint: {}", server.client_url());
    }
    let shutdown = server.shutdown().await;
    ready.and(shutdown)?;
    Ok(())
}
```

`start` validates configuration, binds both listeners, and opens local and object storage.
It returns before leader election. This lets the host start all initial members without deadlock.
If the caller cancels startup, an owned task completes initialization.
It then shuts down any unclaimed member.
`wait_ready` confirms a ReadIndex with the quorum and waits for local apply.
Readiness describes that instant. A later failure can make the member unavailable.

`ServerConfig::single_node` creates member 1 with plaintext loopback transport and an assigned port.
Use `server.config().clone()` to obtain the effective configuration, including assigned ports.
Preserve that configuration for restart. A production deployment should use fixed advertised URLs.

`wait` reports termination and propagates member, listener, and worker failures.
The host can select between `wait` and its own shutdown signal.
`shutdown` stops network connections and waits for apply, storage, and log workers.
It releases the directory lock before returning. Do not cancel shutdown or stop the runtime early.
Dropping `Server` requests shutdown but cannot wait for completion.

Shutdown can interrupt a request with an unknown result. Acknowledged writes remain in durable Raft.
Shutdown does not require a new state checkpoint. Recovery replays any required log entries.
An object store outage can delay shutdown while SlateDB finishes existing I/O.
There is no fixed shutdown deadline.

The [complete example](../examples/embedded.rs) writes and reads through the generated gRPC client:

```sh
scripts/cargo.sh run --locked --example embedded -- tmp/embedded-demo
```

## Configure a cluster

Create one `ServerConfig` for each member. Set these fields before `Server::start`:

| Fields | Requirement |
| --- | --- |
| `id`, `cluster_id`, `dir` | Use a unique member ID and directory, with one shared cluster ID. |
| `listen`, `peer_listen` | Set the local client and peer socket addresses. |
| `peers` | Map every initial member ID to its peer URL. Include the local member. |
| `client_peers` | Map member IDs to their advertised client URLs. |
| `client_tls`, `peer_tls` | Set certificate, key, and trusted CA paths. |
| `allow_remote` | Require separate listeners and mutual client and peer TLS for remote hosts. |
| `object_store` | Select `object::Config::Local` or `object::Config::S3`. |
| `log_memory`, `quota_bytes` | Set separate log budgets and the logical data quota. |
| `snapshot_every` | Set the automatic checkpoint interval. Zero disables it. |
| `snapshot_min_bytes` | Set the log bytes an automatic checkpoint waits for. Zero disables it. |
| `auth_token` | Select the same simple or JWT token configuration as the CLI. |
| `join`, `restore_snapshot` | Select member join or native restore. They cannot both be enabled. |
| `strict_reconfiguration` | Enable quorum health checks for membership changes. |

Start all initial members before waiting for readiness.
A three-voter group needs two available voters.
See [configuration](configuration.md), [administration](administration.md), and
[remote storage](remote-storage.md) for field semantics and storage ownership rules.

Multiple members can run in one process with separate directories and ports.
They share one process failure domain. Use separate processes or machines for failure isolation.

## Integrate clients

Rust callers can use the generated Tonic clients in `proto::etcdserverpb`.
These clients expose KV, Txn, Watch, Lease, Auth, Cluster, and Maintenance methods.
Requests use the same gRPC handlers as external clients, including authentication and authorization.
The lifecycle API does not expose unrestricted access to the state machine.

Go programs use the official `go.etcd.io/etcd/client/v3` client.
Configure it with the advertised client endpoints.
This release does not provide a Go process embedding API or an `embed.Etcd` replacement.
See [compatibility](../COMPATIBILITY.md) for protocol differences and supported operations.

The Rust integration tests start real members in the test process.
They check Watch, Lease, quorum readiness, leader failure, and authentication.
They also check connection shutdown and immediate directory reuse.
The normal compatibility suite also exercises this lifecycle through the CLI.

```sh
scripts/cargo.sh test --locked --test embedded
```
