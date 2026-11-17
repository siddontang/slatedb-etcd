# Protocol definitions

These files contain the etcd v3 data, Auth, Cluster, and Maintenance messages and service paths.
They preserve field numbers and enum values from etcd v3.6.5.
The source project uses the Apache License 2.0. See the root LICENSE file.

Sources:

- https://github.com/etcd-io/etcd/blob/v3.6.5/api/etcdserverpb/rpc.proto
- https://github.com/etcd-io/etcd/blob/v3.6.5/api/mvccpb/kv.proto
- https://github.com/etcd-io/etcd/blob/v3.6.5/api/authpb/auth.proto

The adaptation removes Go generator options, version annotations, and unrelated services.
It does not require the Go or Google HTTP annotation imports.
The build generates Rust messages and a Tonic client and server.
Compact supports logical and physical completion. See the documented native storage format limits.
