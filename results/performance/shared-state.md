# Shared state mode compared with replicated mode

This report compares the two state modes on one host with a local RustFS service. The raw
records are in `shared-state.jsonl`. `scripts/bench-shared-state.py` produced them, and
`scripts/run-shared-bench.sh` ran it under the machine-wide benchmark lock.

## Summary

- **Storage:** shared mode stores about one third of the bytes of replicated mode. It writes
  about 40% of the S3 PUT requests, and one third to one half of the GET requests.
- **Writes:** throughput is equal for small loads and about 20% higher in shared mode for large
  loads. The p99 latency of put-c256 is half, because only the leader flushes and compacts.
- **Reads:** shared mode reads are about 5% to 7% faster, with a lower p99. Each follower warms
  its cache for the SSTs of a new base. In these runs, reads caused almost no S3 traffic.
- **Follower downloads:** each follower downloads the current rows of the writes once, for that
  warm. In put-c256, the cluster-wide S3 response bytes were 70.4 MiB in shared mode, and less
  than 1 MiB in replicated mode.
- **Learner install:** 39 s in shared mode, 56 s in replicated mode, for the same data. Shared
  mode uploads nothing, and its cluster-wide S3 response bytes are 74 MiB. Replicated mode
  uploads 733 MiB.
- **Failover:** the first write after a leader kill takes 1.0 s to 1.6 s in both modes.
- **Access policy:** members run with the S3 policies of the shared state guide, and RustFS denies
  the keys outside them.

## Method

- Host: Intel Core i7-6700K, 4 cores and 8 threads, 30 GiB RAM, Samsung MZ7LM1T9 SATA SSD,
  Linux 7.0.0. The CPU governor was `performance` during the run.
- The benchmark used a `slatedb-etcd` release build of `ac8b19b`: `main` at `6041ace` with the
  shared-mode cache warm (SHA-256 prefix `4d4b81e11833d200`). Its source equals `82a76b7` of
  `perf/shared-read-cache`.
- S3: the RustFS 1.0.6 release of `scripts/rustfs_fixture.py`, `linux-x86_64-gnu`, binary SHA-256
  prefix `250be6671ba447e9`. One data directory on the same SSD.
- `scripts/s3-counter`, a Go reverse proxy, ran in front of RustFS. Members sent every S3 call
  through it. It counts the calls, request body bytes ("in"), and response body bytes ("out")
  of each bucket and operation. The counts are for all members together, not per member.
- Cluster: three members on the loopback interface. Each member had two CPU threads: `0,4`,
  `1,5`, and `2,6`. The etcd v3.6.5 `benchmark` tool, `etcdctl`, RustFS, and the proxy shared
  `3,7`.
- Both modes used S3 for state, with the same bucket settings and default flags. Shared mode
  added `--state-mode shared`, with `--view-max-age 10s` and `--view-retention off`.
- Each case started RustFS on an empty data directory and a new cluster, two times for each mode.
  The tables show medians of the two runs. The run took 25 minutes on 2026-11-24.
- Cases are a subset of the v3 harness cases, with 32-byte keys:

  | Case | Load |
  | --- | --- |
  | put-c16 | 16 clients, 30,000 puts of 256 B over 100,000 keys, to the leader |
  | put-c256 | 256 clients, 100,000 puts of 256 B over 100,000 keys, to the leader |
  | put-v4096 | 256 clients, 50,000 puts of 4 KiB over 10,000 keys, to the leader |
  | get-l-c64 | 64 clients, 200,000 linearizable random reads of 100,000 preloaded keys |
  | get-s-c64 | The same with serializable reads, on all members |
  | watch-latency | 100 streams with 10 watchers each, 2,000 puts at 200 per second |

- "PUT-class" counts `PutObject`, `UploadPart`, and `CopyObject`. "GET-class" counts `GetObject`
  and `HeadObject`. The counts cover the load, a 15 s quiet time, and a `POST /checkpoint` on each
  member. The checkpoint makes the state durable in both modes, because replicated mode
  otherwise keeps small writes in its memtable.
- The harness of these records read the counters twice between the load and the settle window:
  once to end the load and once to start the settle window. The S3 counts can miss calls made
  between these two reads. The number of missed calls is unknown. The harness now ends one
  window and starts the next with one read.
- "Stored" is the size of the bucket after that, from `ListObjectsV2`. A case fails when the
  checkpoint fails on any member. No case failed, so all 24 load runs and the four learner runs
  completed their checkpoints.
- RSS is the peak resident set size of each member, sampled every 250 ms. In these records the
  peak covers the load, the 15 s quiet time, and the checkpoint. The harness now keeps only the
  peak of the load.
- Learner install: after 200,000 puts of 256 B and a checkpoint on each member, a learner joins
  with `--join`. The time runs from its start until its applied index reaches the leader's
  index at the time of the member add.
- Failover: after a burst of 20,000 puts, the leader gets `SIGKILL`. The time runs until an
  `etcdctl put` with a 500 ms timeout succeeds on the other members.
- Policy check: three voters and a learner ran with RustFS users that had the policies of
  `docs/shared-state-guide.md`. The RustFS admin API created the users and canned policies. Then
  eight signed requests read or wrote one object each. Each probe expects one outcome: HTTP 200,
  or HTTP 403 with the S3 error code `AccessDenied`. Any other result fails the check.

## Load results

Throughput, latency, and peak memory:

Peak RSS covers the load, the quiet time, and the checkpoint.

| Case | Mode | Ops/s | p50 ms | p99 ms | Leader RSS MiB | Follower RSS MiB |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| put-c16 | replicated | 11,354 | 1.32 | 3.58 | 145 | 146 |
| put-c16 | shared | 11,237 | 1.34 | 3.21 | 195 | 140 |
| put-c256 | replicated | 20,560 | 8.93 | 48.56 | 278 | 269 |
| put-c256 | shared | 24,820 | 9.37 | 23.00 | 432 | 321 |
| put-v4096 | replicated | 6,039 | 26.23 | 126.86 | 549 | 557 |
| put-v4096 | shared | 7,112 | 25.32 | 109.59 | 541 | 389 |
| get-l-c64 | replicated | 26,977 | 2.12 | 6.05 | 228 | 223 |
| get-l-c64 | shared | 28,274 | 2.07 | 5.55 | 178 | 175 |
| get-s-c64 | replicated | 30,069 | 1.73 | 5.48 | 223 | 226 |
| get-s-c64 | shared | 32,285 | 1.65 | 4.71 | 176 | 174 |
| watch-latency | replicated | 186 | 2.75 | 30.01 | 39 | 40 |
| watch-latency | shared | 199 | 2.67 | 25.86 | 46 | 41 |

S3 requests and bytes of the whole cluster, and stored bytes:

| Case | Mode | S3 PUT-class | S3 GET-class | S3 MiB in | S3 MiB out | Stored MiB |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| put-c16 | replicated | 12 | 4,978 | 84.0 | 0.6 | 84.0 |
| put-c16 | shared | 5 | 1,598 | 27.8 | 21.4 | 27.9 |
| put-c256 | replicated | 36 | 5,432 | 271.5 | 0.7 | 271.5 |
| put-c256 | shared | 14 | 1,751 | 90.4 | 70.4 | 90.4 |
| put-v4096 | replicated | 268 | 6,024 | 1,953.4 | 1,091.6 | 1,928.4 |
| put-v4096 | shared | 106 | 3,012 | 712.8 | 985.4 | 712.8 |
| get-l-c64 | replicated | 15 | 6,122 | 98.6 | 0.7 | 275.8 |
| get-l-c64 | shared | 0 | 1,918 | 0.0 | 0.2 | 91.9 |
| get-s-c64 | replicated | 15 | 5,984 | 98.6 | 0.7 | 275.8 |
| get-s-c64 | shared | 0 | 1,845 | 0.0 | 0.2 | 91.9 |
| watch-latency | replicated | 6 | 6,812 | 5.8 | 0.8 | 5.8 |
| watch-latency | shared | 4 | 2,264 | 1.9 | 1.7 | 1.9 |

The two runs of each case differ by less than 6% in throughput. The read cases include the
preload in "Stored", but not in the request counts. The watch-latency rate is the put rate of
the tool, not a capacity.

## Operations

| Mode | Stored MiB | Install s | GET-class | PUT-class | MiB in | MiB out | Failover s |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| replicated | 551.9 | 55.6 | 40,664 | 136 | 732.6 | 819.5 | 1.51 |
| shared | 360.4 | 39.4 | 21,510 | 4 | 0.0 | 74.0 | 1.25 |

"Stored" is the bucket before the learner joins. The request columns are the S3 calls during the
learner install. "Failover" is the time to the first write after the leader kill.

The single runs were 56.8 s and 54.3 s, and 40.0 s and 38.8 s, for the learner. Failover took
1.43 s and 1.60 s in replicated mode, and 1.49 s and 1.02 s in shared mode.

## Access policy check

Three voters started and served with their own voter policies. A learner joined with the
learner policy and read a key. Then:

| Probe | Credential | Expected | Result |
| --- | --- | --- | --- |
| Read `shared/bootstrap.json` | Learner | Allowed | 200 |
| Write under `shared/` | Learner | Denied | 403 `AccessDenied` |
| Write under its own `member-<id>/` | Learner | Allowed | 200 |
| Read `mode.json` | Voter 1 | Allowed | 200 |
| Write under `member-1/` | Voter 1 | Allowed | 200 |
| Read `member-2/binding.json` | Voter 1 | Denied | 403 `AccessDenied` |
| Write `member-2/` | Voter 1 | Denied | 403 `AccessDenied` |
| Write `member-2/state/` | Voter 1 | Denied | 403 `AccessDenied` |

## Observations

- Shared mode removes the copy of each SST on two members. The stored bytes and the uploaded
  bytes drop to about one third. The GET-class requests drop to about one third, because only
  the leader compacts.
- A follower warms the index, filters, and current-row blocks of each new L0 SST before it switches
  its base. In each read case, the cluster-wide S3 response bytes were only 0.2 MiB, and shared
  mode was about 5% to 7% faster. A cold read can still go to S3 when a warm fails or reaches its
  500 ms limit.
- The warm costs downloads. Each follower reads about the current-row bytes of the writes once.
  Each warm reads a contiguous range of blocks with one GET, so it adds few requests.
- The shared leader uses more memory than a replicated leader in put-c256: it holds the overlay
  and the writer memtable. Its followers use less memory than replicated followers in the large
  value case, because they do no flush or compaction.
- With retention off, the shared bucket keeps every view checkpoint. In the learner scenario it
  holds 360 MiB for data that a replicated member stores in about 184 MiB, because older
  checkpoints pin SSTs that compaction replaced.
- A shared learner reads its base from S3 on demand. It copies no SST, so it is ready about 30%
  faster here.

## Caveats

- One host. The members, RustFS, the proxy, and the client share CPUs and one SSD. RustFS on the
  loopback interface has a much lower latency than a remote S3 service. Cold reads would cost
  more with a remote service.
- The proxy adds one local HTTP hop to every S3 call, in both modes.
- Earlier records of this report used MinIO. Their absolute values, such as the learner install
  times, differ from RustFS values. Compare modes only within one S3 service.
- Two runs for each case. The differences in this report are larger than the run variation, but
  small differences are not significant.
- The cases are short. Long runs with compaction and retention are not covered. After a
  compaction, both modes read cold compaction output.
- The failover time includes the 500 ms client timeout and the election timeout.
