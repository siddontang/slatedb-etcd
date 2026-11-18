# Raft replication during a slow leader log write

Build `7e1b33c` prepares ordinary Put replies after a durable quorum commits the entries.
It also reduces current-value reads and reuses unchanged checkpoint rows.
Build `06bc152` is the baseline. The reference server is etcd v3.6.5.

## Delay one leader

Five pairs of fresh three-member clusters ran on one Apple M4 host.
Each pair used the same etcdctl binary and the same Put with a previous-value response.
The test added 250 ms to each leader log job. Both followers used their normal log settings.
Elapsed time includes starting etcdctl, opening its connection, and receiving the result.

| Build | Median ms | Range ms | Raft commit at reply | Leader log | Leader applied |
| --- | ---: | ---: | ---: | ---: | ---: |
| Baseline | 535.09 | 531.43–537.83 | 3 | 3 | 3 |
| Quorum Put | 15.78 | 13.80–16.31 | 3 | 2 | 2 |

Every Put returned revision 3 and the previous value. A subsequent linear read saw revision 3.
The [individual trials](leader-delay.json) include all ten timings and index observations.
The process recovery test separately stalls the leader log for five seconds.
It receives a Put reply, kills all three members, and restarts only the two followers.
It verifies the value, revision, and previous Watch value before it restarts the old leader.

Repeat the recovery test against the release binary from the repository root.

```sh
scripts/cargo.sh build --locked --release
python3 scripts/check-compat.py --run '^TestQuorumPutSurvivesLeaderLossBeforeLocalSync$' \
  --output results/ci/quorum-put-recovery.json
```

## Steady load

The driver uses gRPC and a fresh local cluster for each sample.
Each workload uses 1,000 warm keys, 256-byte values, 200 warmup calls, and three repeats.
Both servers use durable logs and a snapshot interval of 1,000 entries.
The table shows medians at concurrency 8. CPU and peak RSS cover all server processes.
Linux also tests concurrency 1 and 32. Each raw report retains the client and resource samples.

| Host | Members | Reads % | Build | ops/s | p99 ms | CPU µs/op | RSS MiB |
| --- | ---: | ---: | --- | ---: | ---: | ---: | ---: |
| M4 | 1 | 0 | Baseline | 960 | 14.76 | 323 | 128.1 |
| M4 | 1 | 0 | Quorum Put | 985 | 13.86 | 283 | 135.3 |
| M4 | 1 | 0 | etcd | 900 | 22.28 | 283 | 47.0 |
| M4 | 1 | 50 | Baseline | 1023 | 14.34 | 275 | 73.4 |
| M4 | 1 | 50 | Quorum Put | 1024 | 14.74 | 273 | 73.6 |
| M4 | 1 | 50 | etcd | 999 | 22.11 | 280 | 43.8 |
| M4 | 1 | 100 | Baseline | 59966 | 0.32 | 26 | 32.7 |
| M4 | 1 | 100 | Quorum Put | 59532 | 0.33 | 26 | 32.8 |
| M4 | 1 | 100 | etcd | 56160 | 0.37 | 54 | 42.7 |
| M4 | 3 | 0 | Baseline | 229 | 52.14 | 2373 | 387.4 |
| M4 | 3 | 0 | Quorum Put | 226 | 54.06 | 2215 | 413.3 |
| M4 | 3 | 0 | etcd | 185 | 78.61 | 1545 | 187.9 |
| M4 | 3 | 50 | Baseline | 323 | 58.51 | 1727 | 224.8 |
| M4 | 3 | 50 | Quorum Put | 316 | 60.04 | 1622 | 225.4 |
| M4 | 3 | 50 | etcd | 236 | 96.39 | 1427 | 172.4 |
| M4 | 3 | 100 | Baseline | 27729 | 0.56 | 134 | 99.5 |
| M4 | 3 | 100 | Quorum Put | 27655 | 0.57 | 135 | 99.1 |
| M4 | 3 | 100 | etcd | 23730 | 0.67 | 241 | 172.9 |
| Linux x86 | 3 | 0 | Baseline | 3566 | 8.98 | 886 | 242.1 |
| Linux x86 | 3 | 0 | Quorum Put | 4218 | 7.48 | 716 | 274.1 |
| Linux x86 | 3 | 0 | etcd | 7091 | 2.83 | 382 | 170.3 |
| Linux x86 | 3 | 50 | Baseline | 5090 | 5.49 | 582 | 154.7 |
| Linux x86 | 3 | 50 | Quorum Put | 5558 | 3.55 | 522 | 157.6 |
| Linux x86 | 3 | 50 | etcd | 7465 | 2.72 | 376 | 159.1 |
| Linux x86 | 3 | 100 | Baseline | 10622 | 1.54 | 244 | 97.9 |
| Linux x86 | 3 | 100 | Quorum Put | 10660 | 1.53 | 244 | 97.9 |
| Linux x86 | 3 | 100 | etcd | 11694 | 1.69 | 227 | 160.2 |

On Linux, the service gained 18% write throughput and used 19% less CPU per write than its parent.
Peak write RSS rose from 242 to 274 MiB. Etcd retained a throughput and memory advantage.
At concurrency 32, the service reached 7,092 writes/s and etcd reached 13,013 writes/s.

On macOS, the service exceeded etcd throughput in the six cases in the table.
Three-member write throughput remained close to the parent. Peak write RSS exceeded etcd on both
member counts. All processes shared one disk, so the slow-leader experiment measures a different
condition from the normal load tests.

The Linux write profile collected 330 CPU samples across the three servers with zero lost samples.
System calls appeared in 24% of inclusive stacks. They include network writes and scheduling work.
These samples support measuring transport and wakeup costs. They cannot isolate a single cause.
The [Linux job] retains four profiles and the full comparison.
The [one-member Mac](macos-one.json), [three-member Mac](macos-three.json), and
[three-member Linux](linux-three.json) reports contain all 117 samples.

[Linux job]: https://github.com/siddontang/slatedb-etcd/actions/runs/44357392562

## Group consecutive Put proposals

Build `62d22ec` submits separate Put entries in one local Raft proposal.
The [matched Mac trials](macos-proposals.json) used three members, concurrency 8, and 2,500 writes.
The baseline includes the quorum reply and checkpoint changes above.

| Build | ops/s | p99 ms | CPU µs/op | RSS MiB |
| --- | ---: | ---: | ---: | ---: |
| Baseline | 230 | 51.98 | 2104 | 161.8 |
| Grouped proposals | 228 | 49.22 | 2084 | 161.9 |
| etcd | 192 | 82.06 | 1464 | 162.5 |

Median throughput stayed within 1% of the baseline on this disk. Median p99 fell by 5%.
The test uses a shorter write history than the first matrix. Compare RSS within each experiment.
The [proposal PR] runs the full Linux matrix on the same service change.

[proposal PR]: https://github.com/siddontang/slatedb-etcd/pull/7
