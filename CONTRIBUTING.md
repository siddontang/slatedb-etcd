# Contributing

Open a GitHub issue for a proposed change or a reproducible bug.
Use [private reporting](SECURITY.md) for security details.
Keep changes focused and state which compatibility behavior they add or change.

## Repository layout

| Path | Purpose |
| --- | --- |
| `src/grpc/` | etcd wire adapters, request identity, and streaming |
| `src/node/` | Raft ownership, log I/O, apply scheduling, and DST |
| `src/state/` | State-machine administration and native backup recovery |
| `src/mvcc.rs`, `src/lease.rs`, `src/watch.rs` | Data, lease, and event semantics |
| `proto/` | Adapted etcd protocol definitions |
| `tests/compat/` | Official-client, differential, imported, and process fault tests |
| `tests/compat/cmd/slatedb-snapshot/` | Native backup export with the official Go client |
| `tests/jepsen/` | Knossos and Elle history checks |
| `scripts/` | Build wrappers, test runners, and repository checks |
| `results/` | Selected validation and historical benchmark records |
| `docs/` | Setup, design, operations, and publication notes |

The [quick start](docs/getting-started.md) lists build prerequisites.
The [test guide](tests/README.md) documents extended and targeted checks.

## Change and verify

Use a topic branch. Keep secrets, generated keys, databases, and raw traces out of commits.
Put local test outputs under `tmp/`; promote only reviewed summaries into `results/`.

```sh
scripts/cargo.sh fmt --check
scripts/cargo.sh clippy --locked --all-targets -- -D warnings
scripts/cargo.sh test --locked
python3 scripts/check-public.py
python3 scripts/test_reporting.py
python3 scripts/check-secrets.py
```

The secret check requires Gitleaks 8.30.1. See [publication checks](docs/publication.md).
Run `gofmt` on changed Go files. Build the release binary before process tests.
Use a focused Go `-run` expression during development, then run the affected compatibility profile.
Add a regression test for a correctness fix. Keep the original model and error assertions intact.
Do not mark a failing behavior as supported by skipping its test.

Use concise English, active voice, and lines of at most 100 characters in prose and comments.
Keep sentences below 30 words. Follow `AGENTS.md` and the pull request template.
State the concrete behavior, validation results, and remaining limits.
Do not claim full etcd or Jepsen coverage from the imported common suite.

## Import upstream tests

```sh
python3 scripts/fetch-test-references.py
python3 scripts/import-etcd-tests.py
```

The importer requires the pinned etcd revision and preserves upstream license notices.
It applies the Maintenance adapter patch and replaces static JWT fixture paths with temporary keys.
A second import must produce no further diff.
Review the [inventory and integration map](tests/upstream/README.md) after changing an adapter.
Record source revisions for new copied code. Preserve its notices and license text.

## Reports

The report writers replace workstation roots with `<repo>`, `<home>`, and `<tmp>` labels.
They retain commands, binary and trace hashes, outcomes, seeds, and measurements.
Raw local logs can contain payloads and paths. Review them before attaching an issue or PR.
Never remove failed operations from a correctness history to obtain a passing result.

The CI checks all required test stages and prints JSON reports in its logs.
Artifact upload is optional because GitHub storage can be unavailable.
The repository uses Apache License 2.0; see [LICENSE](LICENSE) and [NOTICE](NOTICE).
