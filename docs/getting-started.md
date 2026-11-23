# Quick start

This guide starts local test nodes. It does not describe a production deployment.
Use empty data directories for a new cluster. Keep data directories when restarting existing nodes.

## Prerequisites

- Linux or macOS, a C/C++ compiler, and Git.
- Rust 1.91.1, selected by `rust-toolchain.toml`.
- A protobuf compiler compatible with the build. CI uses Ubuntu's protobuf-compiler package.
- Python 3.9 or later for the test drivers; no extra Python packages are needed.
- Go 1.24 or later for client tests. CI uses Go 1.25.
- Java 17 or later and Leiningen for Jepsen. These are not needed to run the service.

On Ubuntu, install `build-essential`, `pkg-config`, and `protobuf-compiler`.
On macOS, install the Xcode command line tools and Protobuf with `brew install protobuf`.
Set `PROTOC` to use a compiler at a different path.

## Dependency versions

Use the latest compatible published release for a service dependency.
Commit the resolved versions in `Cargo.lock`. Run the compatibility and recovery suites after edits.
SlateDB 0.18.1 comes from crates.io. Its release supports S3, Moka, and disabling the native WAL.
The server uses Tonic 0.14 with Prost. The gRPC project maintains Tonic.
The separate `grpc` 0.9 client crate is a preview. Its server examples use Tonic.

Three packages need specific version lines.

- Raft uses the same Prost version as gRPC through [Git forks](dependencies.md).
- Raft Engine needs a [cache fix](dependencies.md#raft-engine-fixes) from its fork.
- Bincode 2.0.1 uses fixed-width little-endian fields for snapshots and lease records.
  The published 3.0.0 crate contains only a compile error.

The Go test modules build the etcd 3.6.5 reference server and client.
Keep the reference server's linked dependencies when updating independent test tools.
The downstream suite tests specific Kubernetes and CoreDNS releases.
Update these test targets with their source inventories and compatibility reports.
See the [compatibility matrix](../COMPATIBILITY.md) and
[upstream cases](../tests/upstream/README.md).

## Build

```sh
git clone https://github.com/siddontang/slatedb-etcd.git
cd slatedb-etcd
scripts/cargo.sh build --locked --release
```

Cargo downloads the SlateDB release and other dependencies recorded in the lock file.
The test reference clones are optional. Imported common tests are already in the repository.

Build the matching etcdctl client without changing your global installation:

```sh
mkdir -p tmp/bin
(cd tests/compat && go build -mod=readonly -o ../../tmp/bin/etcdctl go.etcd.io/etcd/etcdctl/v3)
```

Use `tmp/bin/etcdctl` wherever another guide uses `etcdctl`.

## Start one node

```sh
mkdir -p tmp/demo
target/release/slatedb-etcd \
  --id 1 --listen 127.0.0.1:7001 --peer 1=127.0.0.1:7001 \
  --dir tmp/demo/node-1 --cluster-id demo
```

In another terminal:

```sh
tmp/bin/etcdctl --endpoints=http://127.0.0.1:7001 endpoint health
tmp/bin/etcdctl --endpoints=http://127.0.0.1:7001 put hello world
tmp/bin/etcdctl --endpoints=http://127.0.0.1:7001 get hello
tmp/bin/etcdctl --endpoints=http://127.0.0.1:7001 endpoint status --write-out=table
```

The initial revision is 1. The first Put advances it to 2.
Use Ctrl-C to stop the server. Repeat the same server command to recover its data.
A single node has no replica redundancy.

## Start three nodes

Use a different directory and cluster ID from the single-node example.
Open three terminals. Set `NODE_ID` to 1, 2, and 3 respectively, then run:

```sh
NODE_ID=1
target/release/slatedb-etcd \
  --id "$NODE_ID" --listen "127.0.0.1:710$NODE_ID" \
  --peer 1=127.0.0.1:7101 --peer 2=127.0.0.1:7102 --peer 3=127.0.0.1:7103 \
  --dir "tmp/cluster-demo/node-$NODE_ID" --cluster-id cluster-demo --snapshot-every 1000
```

Wait for election, then check every endpoint:

```sh
tmp/bin/etcdctl --endpoints=127.0.0.1:7101,127.0.0.1:7102,127.0.0.1:7103 endpoint health
tmp/bin/etcdctl --endpoints=127.0.0.1:7101 put replicated yes
tmp/bin/etcdctl --endpoints=127.0.0.1:7102 get replicated
```

A write needs a majority and leader apply. Linearizable reads also need quorum.
All processes share one host and disk, so this example does not prove independent fault tolerance.
Stop nodes with Ctrl-C. Restart each with its original arguments and directory.

## Next steps

Read [configuration](configuration.md) for memory and quota flags.
Use [administration](administration.md) to add root credentials, TLS, and a learner.
Read [operations](operations.md) before compacting, restoring, or changing binaries.
Run [tests](../tests/README.md) before using the service for an experiment.
