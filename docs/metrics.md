# Prometheus metrics

Scrape `GET /metrics` on the client listener. It returns Prometheus text format 0.0.4.
TLS and client certificate settings also apply to scrapes on this listener.
Each member has its own registry. Embedded members share process resource measurements.

```sh
curl --fail http://127.0.0.1:2379/metrics
```

Use `--listen-metrics-urls http://127.0.0.1:2381` for a separate scrape listener.
The option accepts comma-separated URLs. These listeners serve only the metrics route.
HTTPS uses the client certificate and trust settings. Remote binds require remote mode and mTLS.
The Rust configuration uses `ServerConfig::listen_metrics_urls`.

## Compatibility with etcd v3.6.5

The [inventory] records names, Prometheus types, label keys, and histogram buckets.
It contains the union of three real member scrapes after KV, Lease, and Status requests on macOS.
The workload found 122 families. Other platforms and operations can expose additional families.

| Inventory class | Families | Behavior |
| --- | ---: | --- |
| Exported | 50 | The service measures the corresponding operations or state. |
| Pending | 29 | The service omits these families. |
| Go runtime and Go version | 30 | The service runs as a native Rust process. |
| V2 store or gRPC proxy | 10 | The service implements the v3 member API. |
| bbolt internals | 3 | SlateDB uses a different storage engine. |

On Linux the registry also includes native process statistics such as CPU, RSS, and open files.
New API operations can create additional label values. Removed members leave the peer series.
Use the inventory to check dashboards before deployment. An absent series represents missing data.

| Signal | Measurement |
| --- | --- |
| Leadership | Current leader, local role, learner state, and observed leader changes |
| Raft progress | Committed index, applied index, outstanding proposals, and proposal failures |
| ReadIndex | Failed pending read proofs |
| WAL | Actual file write bytes, file write latency, and sync latency |
| Backend | Logical bytes, size, quota, explicit state flushes, backups, hashes, and defrag |
| MVCC | Current and compacted revisions, applied puts, deleted ranges, and Put bytes |
| Watch | Open streams, active watchers, and events sent |
| Lease | Grants, revocations, and granted TTL |
| gRPC | Per-method calls, messages, terminal status, client version, and payload bytes |
| Peers | Known and active peers, received bytes, and bytes with a transport receipt |

Monitoring can use the same queries as etcd for these signals.

```promql
etcd_server_has_leader == 0
etcd_server_proposals_committed_total - etcd_server_proposals_applied_total
histogram_quantile(0.99,
  sum by (instance, le) (rate(etcd_disk_wal_fsync_duration_seconds_bucket[5m]))
)
```

The WAL histogram uses the same bucket boundaries as etcd. It measures the required platform sync.
The backend commit histogram measures explicit SlateDB flush calls. Storage engines commit at
different times. The size gauge uses the same manifest estimate and logical floor as Status.
Peer byte counters exclude transport framing. A transport receipt cannot confirm a Raft commit.
The service publishes MVCC write counters when local state publication succeeds.

The gRPC collector reads the method definitions from the compiled API descriptors.
It counts complete protobuf frames and reads final status from headers or trailers.
It predefines all standard gRPC status codes. An abandoned response body counts as canceled.
Client version labels accept one or two digits after `3.`. Other values use `other`.
An absent version uses `unknown`.
The version gauges match the version reported by Maintenance Status.

The Raft owner snapshots local state for a scrape. The HTTP task reads the manifest.
A blocking worker formats the result outside the Raft owner.
Each member permits two concurrent scrapes and returns HTTP 503 on overload or timeout.
A scrape can read one remote manifest when the member uses an object store.
If this read fails or takes more than one second, the scrape omits only the physical size gauge.

The pending families include MVCC range and transaction counters, slow apply, and several
compaction and snapshot histograms. These require matching upstream operation boundaries.
The [differential tests] compare both servers with the Prometheus parser and the same etcd client.
They exercise leader changes, client TLS, Watch, Lease, physical defrag, and applied writes.

[inventory]: ../tests/compat/testdata/metrics-v3.6.5.json
[differential tests]: ../tests/compat/metrics_test.go

See the [upstream metrics guide] and [monitoring guide] for etcd's operating model.

[upstream metrics guide]: https://etcd.io/docs/v3.6/metrics/
[monitoring guide]: https://etcd.io/docs/v3.6/op-guide/monitoring/
