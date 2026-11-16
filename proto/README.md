# Protocol definitions

These files contain the etcd v3 KV, Watch, and Lease wire messages and service paths used by this
project.
They preserve field numbers and enum values from etcd v3.6.5.
The source project uses the Apache License 2.0. See the root LICENSE file.

Sources:

- https://github.com/etcd-io/etcd/blob/v3.6.5/api/etcdserverpb/rpc.proto
- https://github.com/etcd-io/etcd/blob/v3.6.5/api/mvccpb/kv.proto

The adaptation removes Go generator options, version annotations, and unrelated services.
It does not require the Go or Google HTTP annotation imports.
The build generates Rust messages and a Tonic client and server.
The Compact method supports logical history compaction. Physical compaction remains unsupported.
