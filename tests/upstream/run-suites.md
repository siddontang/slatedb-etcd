# Run the upstream etcd suites

Use etcd v3.6.5, the same release as the compatibility contract. The checkout contains the full
upstream source, including test fixtures and benchmark programs. CI fetches the exact commit.
The repository keeps the source index and the process adapter under version control.

```sh
python3 scripts/fetch-test-references.py
cargo build --locked --release
python3 scripts/test_upstream.py
```

## Go suites

Run each suite from the project root. The default target is etcd.

```sh
for suite in unit integration proxy-integration go-bench fuzz; do
  python3 scripts/check-upstream-go.py "$suite" || exit 1
done
python3 scripts/check-upstream-go.py e2e --target service --require-complete
```

| Suite | Target | What runs |
| --- | --- | --- |
| `unit` | etcd | All modules with tests, including examples and robustness helpers |
| `integration` | etcd | In-process integration packages and common tests with the integration tag |
| `proxy-integration` | etcd | In-process integration with the upstream gRPC proxy |
| `common` | Both | The original common suite with the upstream process runner and etcdctl |
| `e2e` | Both | All process tests, including failover, recovery, CLI, and mixed versions |
| `proxy-e2e` | Both | Process tests with the upstream proxy build tag |
| `robustness` | Both | The original exploratory and regression campaigns |
| `examples` | Both | Each runnable upstream client example on a fresh three-member cluster |
| `go-bench` | etcd | All 73 Go benchmark functions, including storage and client recipes |
| `fuzz` | etcd | All three upstream fuzz targets, with a separate run for each target |

The source index has 1,602 tests, 73 benchmarks, 67 examples, three fuzz targets, and 26 TestMain
entry points. Some examples have no expected output. Go only compiles those examples.
Go tests that link the etcd server exercise the Go implementation.
The project's adapted common suite and differential fuzzer exercise the Rust server separately.

The process runner discovers names from the compiled test binary for the selected build tags.
Each top-level test has its own timeout. Use `--shards N --shard-index I` to split a suite.
Use `--run` to select a test or subtest, `--fuzztime` to set fuzz time, and `--benchtime` to set
microbenchmark duration. Each Go benchmark defaults to one iteration for a coverage check.
Use `go-bench --package PATH` to run one benchmark package. The path starts at the etcd root.

Three Go benchmarks in the client recipes package need a fixture correction in etcd v3.6.5.
The upstream leak check counts the Go benchmark coordinator as a leaked goroutine.
The benchmarks also pass a nil test handle to the cluster. The overlay supplies the benchmark
handle and reuses the queue cluster. The leak check excludes only the Go benchmark coordinator.
The mutex and queue operations and their assertions use the upstream code.

The fixture translates static loopback member addresses and bootstrap IDs for the Rust CLI.
It calls etcdctl endpoint health before returning a ready member. The Go overlay changes the
process fixture. Upstream tests, failpoints, workloads, assertions, and checkers keep their code.
The fixture reads the advertised etcd API version from the binary to select scenarios.
Unsupported flags return an error. A skip stays visible in the report.
The full workflow supplies etcd v3.5.20 for mixed-version tests and builds Go failpoints for etcd.
Supply `--last-release PATH` or `--lazyfs PATH` to select existing binaries on a local host.
The examples fixture changes the server endpoints. Each example retains its upstream assertion
and runs in its own process. The cluster uses a 200 ms Watch interval and permits member changes.
Use a disposable Linux host for full process campaigns. They stop members and mount filesystems.

The service still has gaps in AutoTLS, Unix sockets, gRPC proxy mode, rolling etcd upgrades,
and Go storage failpoints. The native Rust fault and DST suites cover other storage boundaries.
Use the [fault map](fault-mapping.md) to compare them.

## Official benchmark CLI

```sh
python3 scripts/benchmark-upstream.py
python3 scripts/benchmark-upstream.py --members 3 --profile stress
python3 scripts/benchmark-upstream.py --profile stress --case mixed-256
python3 scripts/benchmark-upstream.py --profile heatmap --case heatmap-1-8-256-32
```

The smoke profile runs every leaf command from the upstream benchmark binary.
Nine commands target each server through gRPC. They cover Put, Range, Txn Put, mixed Txn, STM,
Lease keepalive, Watch, Watch Get, and Watch latency. The tenth command runs `mvcc put` in the
Go storage engine. Each network case starts a fresh cluster and checks endpoint health.

The stress profile adds 256 concurrent mixed clients, 4 KiB writes, 1,000 Watch latency consumers,
and 100,000 watches. The heatmap profile uses all 392 workload cells from the upstream
`tools/rw-heatmaps/rw-benchmark.sh` script. Each cell preloads 65,536 keys and issues 200,000 Txns.
Use `--repeat 5` for five samples. Each sample starts a fresh cluster.
The historical `hack/benchmark/bench.sh` script targets the removed v2 HTTP API.
The complete checkout also contains the deprecated `tools/local-tester` bridge and fault driver.

Each report keeps the upstream histograms, operation errors, and raw output.
A timeout, missing section, missing successful operation, or RPC error fails the case.
The mixed Txn check reads both attempted counts. The Watch check counts event deliveries.
Use the [resource benchmark](../../docs/benchmarks.md) for process CPU and memory measurements.

## CI and results

Pull requests run the full CLI smoke profile and the source coverage check.
They also run the three recipe benchmarks, all runnable client examples, and four HashKV cases.
The weekly and manual **Full etcd suites** workflow runs all Go suites and load profiles
on one and three members. It also checks the upstream Prometheus alert rules, release images,
and archive checksums from both release mirrors. The heatmap runs on demand from the CLI.
CI stores the results as artifacts, including failures. Each process job requires zero skips.

Go runs write compressed JSON events, stderr, and summary records under `tmp/upstream-results`.
The CLI benchmark writes its summary and raw output under `tmp/upstream-benchmarks`.
Use a separate `--output` directory for each run you want to keep.

The 256-client workload reproduces a service limitation at the current baseline.
Linearizable prefix transactions can exhaust the pending ReadIndex slots. The benchmark reports
these reads as failures even when all writes succeed and the upstream driver exits successfully.
