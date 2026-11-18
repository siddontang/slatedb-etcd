# Linux three-member write profiles

Three local processes ran on one four-vCPU ARM64 Ubuntu guest with an ext4 disk.
The driver sent 10,000 ordinary puts with 256-byte values and 1,000 warm keys per sample.
It used 200 warmup requests and a checkpoint interval of 1,000 entries on each member.
The baseline used `9e85adc` with the probe from `b8ff6b3`. Both builds used release settings.

## Stage counters

Each cell shows the median of three per-run ratios. Each ratio sums the three member deltas.
The denominator is the batch count of the destination stage. Concurrency was eight.
Raft counts Ready batches. WAL counts Ready writes, including commit-only writes.
Apply counts batches that publish committed log entries. It counts prepared puts at publication.
Allocation totals include other work on that stage, such as saving a checkpoint on the WAL thread.

| Build | Stage | Entries/batch | Allocation calls/batch | Requested bytes/batch |
| --- | --- | ---: | ---: | ---: |
| Baseline | Raft | 0.90 | 10.89 | 3,411 |
| Candidate | Raft | 0.92 | 11.00 | 3,434 |
| Baseline | WAL | 1.71 | 8.14 | 82,876 |
| Candidate | WAL | 1.74 | 6.15 | 610 |
| Baseline | Apply | 2.60 | 365.29 | 60,941 |
| Candidate | Apply | 2.65 | 168.95 | 30,833 |

| Wake source and destination | Baseline wakes/batch | Candidate wakes/batch |
| --- | ---: | ---: |
| Raft to WAL | 0.98 | 0.98 |
| WAL to Raft | 0.51 | 0.52 |
| Raft to apply | 1.01 | 1.01 |
| Apply to Raft | 0.34 | 0.35 |

The probe counts calls to the destination task waker while the source stage runs.
The scheduler can coalesce wake calls. These counts do not measure kernel context switches.
The allocator counts allocation, zeroed allocation, and reallocation requests in the stage.
For a reallocation, it counts the requested new size. It excludes work on background tasks.
Requested bytes measure allocation traffic. The separate production runs measure peak RSS.
The probe reports process totals. Use one member per process.

Without a checkpoint in the measured interval, the baseline WAL requested 9,763 bytes per batch.
The baseline apply worker requested 23,468 bytes per batch in that experiment.
The candidate keeps full checkpoint files outside raft-engine and reuses small WAL encoding batches.
The apply worker caches current rows only after the SlateDB write succeeds.
Its 2,048 slots each hold at most 2,048 bytes of key and value, plus slot metadata.
The batch overlay takes precedence over the cache. A miss reads SlateDB.

Build the baseline and candidate with the same probe. Keep both executables.
Use `scripts/benchmark-etcd.py` with `--backends baseline service` for the stage measurements.
Use ordinary release builds for throughput, latency, CPU, and RSS comparisons with etcd.

```sh
cargo build --release --locked --features write-profile
python3 scripts/benchmark-etcd.py --binary /path/to/candidate --baseline-binary /path/to/base \
  --backends baseline service --requests 10000 --concurrency 8 32 --read-percent 0 \
  --warmup 200 --repeat 3 --output results/stages.json
python3 scripts/summarize-write-profile.py results/stages.json
```

## io_uring experiment

The same guest used Linux 6.8 and buffered files. Each batch used one write and one data sync.
The ring submitted a linked write and `IORING_FSYNC_DATASYNC` in one call at queue depth one.
It checked both completions. Both paths read back all data after the run.
Each trial used 100 warmup batches and 3,000 measured batches in a fresh preallocated file.

| Batch bytes | Direct batches/s | Ring batches/s | Direct p50 µs | Ring p50 µs |
| ---: | ---: | ---: | ---: | ---: |
| 1,024 | 11,683 | 10,257 | 57.05 | 68.34 |
| 4,096 | 5,161 | 4,732 | 162.47 | 176.93 |

Each cell shows the median of three trials. The 4 KiB tail latency varied between trials.
This experiment covers a single outstanding durable batch on a virtual disk.
Other disks, queue depths, and workloads need separate measurements.

Install the system liburing development package. Run the binary on the intended WAL file system.

```sh
cc -O2 -Wall -Wextra -Werror tests/performance/wal-sync.c -luring -o /tmp/wal-sync
for bytes in 1024 4096; do
  for mode in sync ring ring sync sync ring; do /tmp/wal-sync "$mode" "$bytes"; done
done
```

## Three-member WAL experiment

A separate Linux ARM64 test build used linked writes for a single synced raft-engine writer group.
It returned credit for the final sync only after both completions succeeded on the same descriptor.
Other writer groups, file rotation, directory syncs, and checkpoint files kept their normal paths.
The production executable kept the direct WAL system calls.

Five rounds ran 12,000 writes per sample. Each round shuffled the servers and concurrency levels.
CPU measures server microseconds per operation. RSS measures the peak for all three members.

| C | Server | Writes/s | p99 ms | CPU | RSS MiB |
| ---: | --- | ---: | ---: | ---: | ---: |
| 8 | Direct WAL | 6,477 | 6.13 | 314 | 294.7 |
| 8 | Linked ring | 6,224 | 6.33 | 332 | 269.5 |
| 8 | etcd v3.6.5 | 8,693 | 2.15 | 184 | 166.4 |
| 32 | Direct WAL | 11,120 | 23.72 | 183 | 250.8 |
| 32 | Linked ring | 11,015 | 21.91 | 187 | 258.6 |
| 32 | etcd v3.6.5 | 18,628 | 4.46 | 90 | 167.9 |

A separate 4,000-write trace counted 4,962 ring entry calls and 54 direct data sync calls.
The direct executable made zero ring entry calls and 5,036 direct data sync calls.
The linked build passed the process test for leader loss after an early quorum Put reply.
