# Shared state mode compared with replicated mode

This report compares the two state modes on one host with a local S3 service. The raw records
are in `shared-state.jsonl`. `scripts/bench-shared-state.py` produced them, and
`scripts/run-shared-bench.sh` ran it under the machine-wide benchmark lock.

## Summary

- **Storage:** shared mode stores about one third of the bytes of replicated mode. It writes
  about one third of the S3 PUT requests, and two thirds fewer GET requests during writes.
- **Writes:** throughput is equal or a little higher in shared mode. The p99 latency of large
  writes is much lower, because only the leader flushes and compacts.
- **Reads:** shared mode reads are about 2.1 to 2.4 times slower, with a p99 of about 60 ms.
  After a checkpoint switch, reads go to the base checkpoint in S3 with a cold cache. This is
  the main open problem of this prototype.
- **Learner install:** 21 s in shared mode, 88 s in replicated mode, for the same data. Shared
  mode copies about 1.7 MiB instead of 741 MiB.
- **Failover:** the first write after a leader kill takes about 1.1 s in both modes.
- **Access policy:** members run with the S3 policies of the shared state guide, and MinIO denies
  the keys outside them.

## Method

- Host: Intel Core i7-6700K, 4 cores and 8 threads, 30 GiB RAM, Samsung MZ7LM1T9 SATA SSD,
  Linux 7.0.0. The CPU governor was `performance` during the run.
- Binary: `slatedb-etcd` release build at commit `ee2b1bb` (`shared-state-phase4`). The later
  commits on that branch change only tests. SHA-256 `73130619d0a1a187…`.
- S3: MinIO built from source, `github.com/minio/minio v0.0.0-20260212201848-7aac2a2c5b7c`, one
  drive on the same SSD. The MinIO download site no longer serves binaries. `mc` came from
  `github.com/minio/mc v0.0.0-20251106162529-77f82e18b540`.
- Cluster: three members on the loopback interface. Each member had two CPU threads: `0,4`,
  `1,5`, and `2,6`. The etcd v3.6.5 `benchmark` tool, `etcdctl`, and MinIO shared `3,7`.
- Both modes used S3 for state, with the same bucket settings and default flags. Shared mode
  added `--state-mode shared`, with `--view-max-age 10s` and `--view-retention off`.
- Each case started a new cluster in a new bucket, two times for each mode. The tables show
  medians of the two runs. The run took 20 minutes on 2026-11-24.
- Cases are a subset of the v3 harness cases, with 32-byte keys:

  | Case | Load |
  | --- | --- |
  | put-c16 | 16 clients, 30,000 puts of 256 B over 100,000 keys, to the leader |
  | put-c256 | 256 clients, 100,000 puts of 256 B over 100,000 keys, to the leader |
  | put-v4096 | 256 clients, 50,000 puts of 4 KiB over 10,000 keys, to the leader |
  | get-l-c64 | 64 clients, 200,000 linearizable random reads of 100,000 preloaded keys |
  | get-s-c64 | The same with serializable reads, on all members |
  | watch-latency | 100 streams with 10 watchers each, 2,000 puts at 200 per second |

- S3 requests and bytes come from `mc admin trace` for the bucket of each case. The MinIO
  Prometheus counters are cached, so they missed calls of short cases. "PUT-class" counts
  `PutObject`, multipart parts, and `CopyObject`. "GET-class" counts `GetObject` and
  `HeadObject`. The counts cover the load, a 15 s quiet time, and a `POST /checkpoint` on each
  member. The checkpoint makes the state durable in both modes, because replicated mode
  otherwise keeps small writes in its memtable.
- "Stored" is the size of the bucket after that, from `mc du`.
- RSS is the peak resident set size of each member, sampled every 250 ms during the load.
- Learner install: after 200,000 puts of 256 B and a checkpoint on each member, a learner joins
  with `--join`. The time runs from its start until its applied index reaches the leader's
  index at the time of the member add.
- Failover: after a burst of 20,000 puts, the leader gets `SIGKILL`. The time runs until an
  `etcdctl put` with a 500 ms timeout succeeds on the other members.
- Policy check: three voters and a learner ran with MinIO users that had the policies of
  `docs/shared-state-guide.md`. Then `mc cp` tried three keys outside those policies.

## Load results

Throughput, latency, and peak memory:

| Case | Mode | Ops/s | p50 ms | p99 ms | Leader RSS MiB | Follower RSS MiB |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| put-c16 | replicated | 10,035 | 1.53 | 3.18 | 137 | 137 |
| put-c16 | shared | 10,162 | 1.52 | 3.03 | 191 | 125 |
| put-c256 | replicated | 23,265 | 9.22 | 38.32 | 265 | 252 |
| put-c256 | shared | 25,328 | 9.52 | 21.23 | 428 | 294 |
| put-v4096 | replicated | 8,073 | 17.23 | 316.45 | 424 | 445 |
| put-v4096 | shared | 9,367 | 19.94 | 97.57 | 353 | 187 |
| get-l-c64 | replicated | 27,417 | 2.09 | 5.69 | 218 | 218 |
| get-l-c64 | shared | 11,536 | 1.89 | 58.00 | 185 | 156 |
| get-s-c64 | replicated | 30,484 | 1.68 | 5.73 | 218 | 212 |
| get-s-c64 | shared | 14,808 | 0.97 | 66.65 | 185 | 148 |
| watch-latency | replicated | 190 | 2.66 | 28.61 | 39 | 39 |
| watch-latency | shared | 198 | 1.79 | 19.97 | 45 | 39 |

S3 requests and stored bytes:

| Case | Mode | S3 PUT-class | S3 GET-class | S3 MiB in | Stored MiB |
| --- | --- | ---: | ---: | ---: | ---: |
| put-c16 | replicated | 18 | 5,466 | 83.7 | 83.3 |
| put-c16 | shared | 7 | 1,784 | 27.9 | 27.8 |
| put-c256 | replicated | 48 | 5,794 | 271.6 | 271.1 |
| put-c256 | shared | 18 | 1,908 | 90.6 | 90.4 |
| put-v4096 | replicated | 365 | 7,131 | 2,095.7 | 2,100.1 |
| put-v4096 | shared | 128 | 3,111 | 713.2 | 712.9 |
| get-l-c64 | replicated | 21 | 6,506 | 98.7 | 275.5 |
| get-l-c64 | shared | 0 | 18,095 | 1.5 | 91.9 |
| get-s-c64 | replicated | 21 | 6,348 | 98.7 | 275.5 |
| get-s-c64 | shared | 0 | 13,966 | 1.2 | 91.9 |
| watch-latency | replicated | 6 | 7,215 | 6.2 | 5.6 |
| watch-latency | shared | 6 | 2,430 | 2.1 | 1.9 |

The two runs of each case differ by less than 10% in throughput. The read cases include the
preload in "Stored", but not in the request counts. The watch-latency rate is the put rate of
the tool, not a capacity.

## Operations

| Mode | Stored MiB | Install s | GET-class | PUT-class | MiB in | Failover s |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| replicated | 551.2 | 87.8 | 102,536 | 146 | 741.3 | 1.18 |
| shared | 360.4 | 21.3 | 20,129 | 4 | 1.7 | 1.15 |

"Stored" is the bucket before the learner joins. The request columns are the S3 calls during the
learner install. "Failover" is the time to the first write after the leader kill.

The single runs were 88.1 s and 87.5 s, and 21.3 s and 21.2 s, for the learner. Failover took
1.03 s and 1.34 s in replicated mode, and 1.23 s and 1.07 s in shared mode.

## Access policy check

| Check | Result |
| --- | --- |
| Three voters start and serve with their own voter policies | Passed |
| A learner joins with the learner policy and reads a key | Passed |
| The learner writes under `shared/` | Denied |
| A voter writes the binding of another member | Denied |
| A voter writes under the state of another member | Denied |

## Observations

- Shared mode removes the copy of each SST on two members. The stored bytes and the PUT-class
  requests drop to about one third. The GET-class requests during writes drop, because only the
  leader compacts.
- The shared leader uses more memory than a replicated leader in put-c256: it holds the overlay
  and the writer memtable. Its followers use less memory than replicated followers in the large
  value case, because they do no flush or compaction.
- Shared reads are slower. After the preload, the checkpoint switch moves each member to a new
  base with an empty cache, and each read fetches SST blocks from S3. Replicated members read
  the blocks that their own writes left in cache. A warm base cache, or a local cache of the
  base SSTs, is needed before shared mode can serve read-heavy work.
- With retention off, the shared bucket keeps every view checkpoint. In the learner scenario it
  holds 360 MiB for data that a replicated member stores in about 184 MiB, because older
  checkpoints pin SSTs that compaction replaced.
- A shared learner reads its base from S3 on demand. It needs no copy, so it is ready four times
  faster.

## Caveats

- One host. The members, MinIO, and the client share CPUs and one SSD. MinIO on the loopback
  interface has a much lower latency than a remote S3 service. The read results would be worse
  with a remote service.
- MinIO came from a source build, not a release binary.
- Two runs for each case. The differences in this report are larger than the run variation, but
  small differences are not significant.
- The cases are short. Long runs with compaction and retention are not covered.
- The failover time includes the 500 ms client timeout and the election timeout.
