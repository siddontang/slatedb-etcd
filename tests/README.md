# Test commands

Build the release binary first. Tests use the actual service and official etcd v3.6.5 client.
Go 1.25, Rust 1.91.1, and a protobuf compiler are required.
Jepsen also needs Java 17 or later and Leiningen.

```sh
scripts/cargo.sh build --locked --release
scripts/cargo.sh test --locked
python3 scripts/check-compat.py
python3 scripts/check-compat.py --tags upstream --output results/etcd-upstream.json
python3 scripts/harness.py test --output results/etcd-core-fault-tests.json
python3 scripts/check-jepsen.py --self-test
python3 scripts/check-robustness.py --jepsen
```

The last command runs seeds 1 to 4.
They cover partition, leader kill, leader pause, and full restart.
Run more campaigns with `--seeds 1,2,3,4,5,6,7,8`.
Each seed sets client choices. Operating system scheduling still changes process histories.
Use `SLATEDB_ETCD_BINARY` for direct Go runs, or `--binary` for Python runners.

The runners retain JSON reports and Go logs. Fault results stay in `tmp/test-results/robustness-N/`:

- `operations.json`: typed etcd model history, including unknown writes.
- `faults.json`: fault actions and monotonic times.
- `watches.json`: full event histories from all three replicas, including previous values.
- `history.html`: Porcupine history visualization.
- `jepsen.edn`: register history with invocation and completion events.
- `append.edn`: append transaction history for Elle.
- `result.json`: checker status, operation counts, and process data path.
- `jepsen-register-result.edn` and `jepsen-append-result.edn`: independent checker results.

Failed tests and fault campaigns retain process logs and data.
Set `SLATEDB_KEEP_DATA=1` to retain successful ordinary test data too.
Reports count skipped tests separately. Checker timeouts and unknown results fail the run.
See the [audit and capability gaps](upstream/README.md) before interpreting a passing report.

## Repeat a saved history

```sh
SLATEDB_REPLAY_HISTORY="$PWD/tmp/test-results/robustness-1/operations.json" \
  sh -c 'cd tests/compat && go test -tags robustness -run "^TestRobustnessReplay$" -count=1'
python3 scripts/check-jepsen.py tmp/test-results/robustness-1
```

`SLATEDB_CHECK_TIMEOUT=120s` extends the replay checker limit for diagnosis.
The normal fault checker limit is 30 seconds.
Set `LEIN_JAR` to a Leiningen standalone JAR if `lein` is not installed.
Set `JAVA_CMD` to a Java executable if the default runtime is unavailable.
The CI workflow pins Leiningen 2.11.2 and verifies its SHA-256.

## Deterministic event schedules

```sh
python3 scripts/check-dst.py --seeds 1,2,42,58,70
```

Seed 58 covers reordered appends before object-store fault injection.
The setup waits for a new applied write and checks that each PUT fault runs.
Seed 70 covers log-cache isolation across three engine instances on one thread.
See the [local Raft Engine fixes](../third_party/raft-engine/README.md) for dependency regressions.

The runner writes traces under `target/dst/`.
Direct Cargo tests accept `SLATEDB_DST_TRACE_DIR` to select another trace directory.
Repeat a failing seed with the same command. Each run also writes a partial `seed-N-last.json`.
The test drives real storage with a simulated transport and Raft clock.
It checks lease generations, Watch journals, auth revocation, and physical compaction after
recovery. It does not simulate lease expiry time.
The scheduler controls disk execution, completion delivery, and apply jobs.
A 1 KiB cache forces asynchronous reads. Each schedule checks all four log pool limits.
It injects read and write failure, restart, and late read completion after a term change.
It does not control operating system threads or SlateDB background scheduling.

## Raft durability and peer streams

Rust tests preserve entry, term, vote, and snapshot sync barriers.
A lost commit marker can recover from flushed state only when the local log covers that state.
Read tests cover bounded point batches, cancelled callers, apply fences, and stale process nonces.
DST includes point-read bursts and checks each acknowledged value after faults.
Reads can execute while apply is paused. A retained read view also spans a 16-Put group.
Restart checks require every Put revision and Watch event from that group.
State tests compare grouped puts with individual writes, including repeated keys, errors, and quota.
Native process hooks cover both sides of the combined state write.

Peer stream tests use real local sockets. They check message and byte limits, malformed frames,
cluster rejection, receipt timeout, reconnection, queued-message disposal, and snapshot results.
Snapshot replies must still wait for the storage result.
Ordinary receipts are not Raft acknowledgements.
DST controls Raft message delivery directly.
It does not simulate HTTP/2 framing or socket scheduling.
Process failover and TLS tests exercise the actual stream transport.

## API sequence fuzzing

```sh
cd tests/compat
go test -mod=readonly -run '^$' -fuzz '^FuzzKVSequence$' -fuzztime=60s -parallel=1
```

Each input creates a three-process cluster and an independent reference etcd.
The fuzzer compares bounded KV and Txn sequences, errors, revisions, and final state.
Go saves and reduces failing inputs under `testdata/fuzz/FuzzKVSequence/`.
Keep these inputs in version control. The default suite runs the fixed seed corpus.
This fuzzer is intentionally slower than an in-process parser fuzzer.

## Full capability probe

```sh
python3 scripts/check-compat.py --tags upstream --all-capabilities \
  --output results/etcd-all-capabilities.json
```

The adapter runs single-voter, three-voter, client TLS, and peer TLS configurations.
Both profile commands now run all imported capability tests without capability skips.
The three upstream Maintenance TODO helpers have local implementations.
The Downgrade helper checks authorization and the explicit unsupported binary-format result.
The standard CI job runs these profiles, faults, Jepsen controls, and 10 fuzz inputs.
The manual `extended` workflow option runs 100 DST seeds and 16 process fault seeds.

## Official client integration workflows

`TestOfficialClientWorkflows` runs the same cases against three service processes and real etcd
3.6.5.
It uses the unchanged official `client/v3/concurrency` package:

- Mutex: exclude competing owners, wait on Watch, and hand off after session revocation.
- Election: campaign, observe, proclaim, resign, and remove the leader through lease revocation.
- STM: transfer balances under three isolation modes and force conflict retries.

The STM modes are SerializableSnapshot, Serializable, and RepeatableReads.
STM uses one member endpoint because its historical reads are serializable.
Switching replicas can reach a member that has not applied the selected revision.
The official client uses round-robin routing with multiple endpoints.
These cases do not establish STM behavior across endpoint changes.
Mutex and election cases use all service endpoints.
ReadCommitted does not check read conflicts, so it cannot guarantee this transfer invariant.

These cases follow the official [mutex], [election], and [STM] APIs.
They are new integration tests, not copies of all upstream integration tests.
The [upstream source index](upstream/README.md) records the wider coverage boundary.

[mutex]: https://github.com/etcd-io/etcd/blob/v3.6.5/client/v3/concurrency/mutex.go
[election]: https://github.com/etcd-io/etcd/blob/v3.6.5/client/v3/concurrency/election.go
[STM]: https://github.com/etcd-io/etcd/blob/v3.6.5/client/v3/concurrency/stm.go

```sh
python3 scripts/check-compat.py --run '^TestOfficialClientWorkflows$' \
  --output tmp/test-results/client-workflows.json
```

`TestAdminCompatibility` checks Auth, Cluster, Maintenance, and physical Compact support.
`TestDifferentialHashKV` compares exact revision hashes with real etcd.
The documented native backup and binary downgrade limits remain explicit.
`TestFollowerReadIndexWaitsForLocalApply` guards the STM regression found by these workflows.
`TestBoundedLogCacheRecoversLaggingFollower` checks small caches with delayed and failed disk reads.

## Administration regressions

The administration tests compare HashKV and lease permission errors with real etcd.
They check certificate trust, missing client certificates, hostname errors, and forged identity
headers.
Member tests join a learner through a snapshot, promote it, transfer leadership, remove a voter,
and restart with old bootstrap addresses.
Snapshot tests verify chunk accounting, SHA-256, new-cluster restore, and stale-token rejection.
They reject damaged backups and existing restore directories.
DST pauses apply between permission revocation and a queued write, then restores the follower.

## Remote storage, native crash hooks, and downstream projects

See [remote storage](../docs/remote-storage.md) for S3 and disposable guest commands.
The [fault map](upstream/fault-mapping.md) accounts for all named gofail hooks at the pinned etcd
version. It distinguishes equivalent native boundaries, imported behavior, and remaining gaps.

```sh
python3 scripts/check-s3.py --output results/ci/s3.json
scripts/cargo.sh build --locked --release --features fault-injection
python3 scripts/check-compat.py --tags failpoints --run '^TestFailpointRecovery$' \
  --output results/ci/failpoints.json
python3 scripts/check-compat.py --suite downstream --output results/ci/downstream.json
```

Do not enable `fault-injection` in a deployment build.
The test-only file selector can exit a process.
Test credentials and guest disks are temporary. The drivers do not use existing database volumes.
