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
It combines real macOS member scrapes after KV, Lease, Status, and a peer failure.
Basic mode exposed 122 families in the first workload.
Extensive mode and a failed peer added four families. The combined workload found 126.
Other platforms and operations can expose additional families.

| Inventory class | Families | Behavior |
| --- | ---: | --- |
| Exported | 54 | The service measures the corresponding operations or state. |
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
| gRPC | Calls, messages, status, payload bytes, client version, and optional handling time |
| Peers | Peer state, disconnects, failed sends, payload bytes, and sampled round trips |

Monitoring can use the same queries as etcd for these signals.

One family has no etcd counterpart. `etcd_debugging_mvcc_range_too_large_total` counts reads
that failed because their rows exceed `--max-response-bytes`.

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
The quota gauge reports 2 GiB for zero, the configured capacity for a positive number,
and the configured negative number when quota enforcement is off.
Maintenance Status returns the configured value, as etcd does.

The Raft owner snapshots local state for a scrape. The HTTP task reads the manifest.
A blocking worker formats the result outside the Raft owner.
Each member permits two concurrent scrapes and returns HTTP 503 on overload or timeout.
A scrape can read one remote manifest when the member uses an object store.
If this read fails or takes more than one second, the scrape omits only the physical size gauge.

## Upstream alerts and dashboard

The v3.6.5 [alerts] contain 15 rules. The [dashboard] defines 16 query targets.
On Linux their metric inputs exist with extensive metrics and the matching scrape labels.
Prometheus supplies the `up` series. Its default job label must match the etcd selector.
Set the dashboard cluster variable to that job. The dashboard reads native RSS on Linux.

Use `--metrics extensive` to enable `grpc_server_handling_seconds`.
The embedded API uses `ServerConfig::metrics`. Basic mode omits this histogram.
Extensive mode times each RPC until its terminal status, including errors and canceled streams.
The gRPC latency alert requires extensive mode on both this service and etcd.

Each active peer stream samples the time from a Raft data frame to its receipt.
It takes an initial sample and samples subsequent traffic at most once per five seconds.
Etcd probes peer URLs separately and observes a smoothed RTT. Compare the trends across systems.
A send failure increments the destination counter. Losing an active connection counts once.
The failure series appears after a failure. Removing the member removes its peer series.

[alerts]: https://github.com/etcd-io/etcd/blob/v3.6.5/contrib/mixin/alerts/alerts.libsonnet
[dashboard]: https://github.com/etcd-io/etcd/blob/v3.6.5/contrib/mixin/dashboards/targets.libsonnet

## Collection cost

The service resolves the method and status counters when it creates a member.
Each RPC body holds one method handle. Request and response frames share its counters.
Completed messages and terminal statuses are visible to the next scrape.
Apply adds the counts for a batch after it commits.

Each scrape takes a member snapshot. It builds the state and peer samples from that snapshot.
Concurrent scrapes can report different Raft indices. They do not overwrite each other's samples.
Removing a peer also removes its series from subsequent snapshots.

TiKV uses fixed label handles and periodically flushes Raft worker counters and histograms.
See the [TiKV Raft metrics][tikv-metrics].

[tikv-metrics]:
  https://github.com/tikv/tikv/blob/master/components/raftstore/src/store/local_metrics.rs

## Remaining coverage

The pending families include MVCC range and transaction counters, slow apply, and several
compaction and snapshot histograms. These require matching upstream operation boundaries.
The [differential tests] compare both servers with the Prometheus parser and the same etcd client.
They exercise leader changes, client TLS, Watch, Lease, physical defrag, and applied writes.

[inventory]: ../tests/compat/testdata/metrics-v3.6.5.json
[differential tests]: ../tests/compat/metrics_test.go

See the [upstream metrics guide] and [monitoring guide] for etcd's operating model.

[upstream metrics guide]: https://etcd.io/docs/v3.6/metrics/
[monitoring guide]: https://etcd.io/docs/v3.6/op-guide/monitoring/
