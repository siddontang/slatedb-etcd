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
python3 scripts/check-dst.py --seeds 1,2,42
```

Traces stay under `target/dst/`. Set `SLATEDB_DST_TRACE_DIR` to change this path.
Repeat a failing seed with the same command. Each run also writes a partial `seed-N-last.json`.
The test drives real storage with a simulated transport and Raft clock.
It checks lease generations and Watch journals after recovery. It does not simulate lease expiry
time.
The scheduler controls disk execution, completion delivery, and apply jobs.
A 1 KiB cache forces asynchronous reads. Each schedule checks all four log pool limits.
It injects read and write failure, restart, and late read completion after a term change.
It does not control operating system threads or SlateDB background scheduling.

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

This probe must fail for missing APIs. It does not convert unsupported features into success.
The standard CI job runs the supported profile, faults, Jepsen controls, and 10 fuzz inputs.
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

`TestCapabilityDifferences` checks four known gaps against real etcd.
Auth status, member listing, Maintenance status, and physical compaction return Unimplemented.
A passing gap control confirms a difference. It does not prove support for that capability.
`TestFollowerReadIndexWaitsForLocalApply` guards the STM regression found by these workflows.
`TestBoundedLogCacheRecoversLaggingFollower` checks small caches with delayed and failed disk reads.
