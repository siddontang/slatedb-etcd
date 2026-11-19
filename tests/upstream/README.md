# Upstream test audit

The baseline is etcd v3.6.5 at `a0614505aff9b8ff469e9c59c2d979a5936d13f4`.
This matches the existing client and reference server. It is not the latest etcd release.
The Jepsen etcd source pin is `b47cc401f27bfb7c28afd9ac088b5f0dca4501b8`.

[etcd-inventory.json](etcd-inventory.json) indexes all 1,771 top-level test functions at this pin.
It includes fuzz tests, benchmarks, examples, and `TestMain` entry points. It excludes subtests.
Each entry has its source file, line, family, and integration status.
The index is an audit record. It does not mean that 1,771 tests passed against this service.

## Integration map

| Source family | Integration in this project |
| --- | --- |
| `tests/common` | Import all 85 functions; adapt the cluster and client interfaces. |
| `tests/integration` | Audit 493 functions; port KV, Txn, Watch, Lease, and recovery behavior. |
| `tests/e2e` | Audit 218 functions; use real local processes for failure and recovery. |
| `tests/robustness` | Use the original recorder, model, and Porcupine checker. |
| Server and client unit tests | Keep source references; use Rust tests for storage invariants. |
| Validation fuzz tests | Add an API sequence fuzzer with real etcd as the reference. |
| Benchmarks and examples | Index them; keep performance and correctness results separate. |
| Jepsen register and append | Check real fault histories with Knossos and Elle. |
| Antithesis | Use its invariant approach in local DST; no hosted Antithesis run is claimed. |

Go unit tests for bbolt, etcd WAL, and etcd server internals cannot test the Rust storage path.
Copying these files would still test etcd internals. Their source remains available for review.
The integration and e2e suites also depend on etcd flags, process controls, and maintenance RPCs.
The portable common suite uses this service through an adapter instead.

The importer keeps upstream license notices and changes the package and build tag.
It extracts the upstream client adapter without capability guards.
It applies [maintenance-auth.patch](maintenance-auth.patch) to three incomplete upstream helpers.
Those helpers test Snapshot, MoveLeader, and authorization before the native Downgrade format error.
The [JWT patch](jwt-fixture.patch) selects temporary RSA keys instead of committed key files.
[LICENSE.etcd](LICENSE.etcd) contains the upstream Apache 2.0 license.

## Capabilities

The adapter runs all imported capability tests. It covers single-voter and three-voter clusters,
client TLS, and peer TLS. TLS fixtures use fresh certificates and independent client and peer CAs.
Automatic certificate generation and mixed etcd binary versions are not implemented.

Reports retain all skip records. Parent tests can pass while their children skip.
Use `leaf_counts` for case counts and `passed` for the complete process outcome.
`complete` also requires zero skips. The `--all-capabilities` flag remains for command
compatibility.

| Capability | Coverage |
| --- | --- |
| Auth | Users, roles, permissions, tokens, recovery, and authenticated transactions |
| Cluster | Add, remove, update, learner promotion, leadership transfer, and recovery |
| Maintenance | Status, HashKV, native snapshot export and restore, defrag, and alarms |
| Deployment | Client TLS, peer TLS, certificate rejection, quota, and variable voter counts |
| Physical Compact | Flush completion, boundary reads, restart, and snapshot recovery |

Snapshot files use the native SlateDB state format, not bbolt.
The Downgrade RPC returns Unimplemented after permission checks. Its adapted test asserts that
limit.
The deprecated Hash RPC has native backend semantics. HashKV has differential etcd coverage.
These tests do not establish complete etcd compatibility or rolling binary upgrades.

## Fault and regression map

| Upstream failure or invariant | Current test |
| --- | --- |
| Leader loss and quorum recovery | Process SIGKILL, SIGSTOP, and peer partition campaigns |
| Uncertain write outcome | Original nondeterministic model and Jepsen `:info` events |
| Multi-key atomicity | Native etcd model plus Elle append transactions |
| Lost acknowledged writes | Final reads, model checks, restart tests, and DST invariants |
| Revision loss during compaction, issue 17780 | Compaction restart and snapshot recovery tests |
| Compacted revision in Txn, issue 18667 | Differential compaction tests against real etcd |
| Offline follower after log truncation | Snapshot transfer in process tests and DST |
| Snapshot persisted before state installation | Rust recovery test at that storage boundary |
| Delayed snapshot creation | Pinned snapshot tests with later writes and compaction |
| Watch order, previous values, and compaction | Differential tests and replica fault histories |
| Watch catch-up and progress | Paged replay, fragments, future revisions, and progress tests |
| Lease renewal versus stale expiry | State snapshot test and seeded Raft schedules |
| Lease quorum and leader failure | Renewal rejection, streamed renewal, and expiry tests |
| Watch issues 15271, 17247, 17529, 18089, 19179 | Exact upstream issue schedules remain unported |
| Defrag issues 14370 and 14685 | API tests pass; exact bbolt failure schedules remain unported |
| etcd gofail hooks around bbolt and WAL | See the complete [fault map](fault-mapping.md). |
| Disk exhaustion, corruption, and lost unsynced writes | Not yet covered by process SIGKILL |
| Membership replacement | Learner join, promotion, leader transfer, removal, and restart tests |
| Clock jumps | Not yet covered by the current service |

Peer partitions use the service's bidirectional message-drop endpoint.
The process campaigns do not emulate kernel routing, packet latency, or bandwidth limits.
Separate three-guest tests add kernel peer partitions, S3 outages, and VM crashes.
Each process campaign applies one fault. Seeds select partition, kill, pause, or full restart.
The workload continues during the fault and after recovery.
Use four consecutive seeds to include all four fault types.

The native history covers single-key operations, atomic pairs, revision guards, list appends, and
leases.
Lease workloads grant, attach keys, and revoke. They use long TTLs to exclude unrecorded expiry.
A separate Watch checker compares complete event histories on all three replicas after recovery.
It checks revision continuity, atomic delivery, progress, previous values, and final state.
The Watch checker is local code. The native etcd model still checks KV and Lease operations.
Unknown writes remain eligible to commit until the history ends.
No timeout or checker `Unknown` result counts as a pass.

The v3.6.5 recorder has different encodings for long Txn Put values and Range values.
The adapter calls upstream `ToValueOrHash` for Txn inputs to match Range encoding.
It does not change the checker or remove failed operations.
The normalization control test protects this adapter rule.

## Jepsen scope

The project uses Jepsen 0.3.15, Knossos 0.3.16, and Elle 0.2.8.
The Go driver records histories from real processes during real faults.
Jepsen checks register histories. Elle checks append transactions for strict serializability.
Append transactions use revision guards for both keys. Failed guards count as failed transactions.
Final reads run after recovery. Unknown writes use new logical process IDs for later operations.

An absent register key has a concrete sentinel in the checker.
Knossos otherwise treats a nil read as unknown and can miss a lost write.
Negative controls require rejection of old reads, lost writes, and partial transactions.
They also check both possible outcomes of an uncertain write.

This is a local fault driver with real Jepsen checkers.
It does not run the upstream Jepsen SSH deployment or its complete nemesis suite.
Jepsen Watch and lock workloads, membership, clock faults, bit flips, and lazyfs remain gaps.
The upstream source pin lets future adapters reuse those workloads without losing provenance.

## DST scope

`src/node/dst.rs` runs the real node, Raft storage, SlateDB apply, and snapshot code.
It replaces network transport with a message queue and advances a logical Raft clock.
A seed controls message order, drops, duplicates, and tick selection.
The schedule includes restart, partition, delayed apply, log truncation, and snapshot installation.
It also injects failed log writes and delays log execution and completion delivery.
Every seed runs twice. The test requires identical event traces.

The test checks one leader per term, stable committed entries, and valid apply boundaries.
It requires snapshot transfer and checks every acknowledged value on all replicas after recovery.
Writes carry a lease. A seeded prelude renews that lease and submits an obsolete expiry command.
The test compares the retained lease and complete Watch journal across all replicas.
It runs the same apply and log jobs through a manual scheduler.
It checks vote and append persistence barriers and reads after acknowledged writes.
Lease wall-clock expiry is outside this schedule; process tests cover it separately.
Partial traces remain available after an assertion failure.

This is deterministic event scheduling, not a deterministic operating system.
SlateDB background tasks and disk I/O remain real. Node drop is not a process crash.
The process tests cover SIGKILL separately.
Object-store delays, failures, and lost replies now have scheduled coverage.
Physical power loss and arbitrary OS or storage completion schedules remain gaps.

## Source review

- [etcd test entry points](https://github.com/etcd-io/etcd/blob/v3.6.5/Makefile)
- [Common suite](https://github.com/etcd-io/etcd/tree/v3.6.5/tests/common)
- [Robustness framework](https://github.com/etcd-io/etcd/tree/v3.6.5/tests/robustness)
- [Jepsen workloads and faults](https://github.com/jepsen-io/etcd)
- [etcd Antithesis report](https://etcd.io/blog/2025/autonomus_testing_with_antithesis/)

Fetch and verify the complete pinned source trees:

```sh
python3 scripts/fetch-test-references.py
python3 scripts/import-etcd-tests.py
```

The reference trees stay under the ignored `reference/` directory.
The common tests, source hashes, inventory, and license stay in version control.
