#!/usr/bin/env python3
"""Run the official etcd benchmark against fresh local clusters."""

import argparse
from contextlib import ExitStack
from dataclasses import dataclass
from fractions import Fraction
import json
from pathlib import Path
import re
import subprocess
import tempfile

from upstream_source import ROOT, INVENTORY, build_tool, checked_source, start_cluster


@dataclass(frozen=True)
class Case:
    name: str
    command: str
    expected: tuple
    clients: int = 8
    connections: int = 8
    preload: str = ""


def heatmap_cases():
    # Match the workload dimensions in tools/rw-heatmaps/rw-benchmark.sh.
    for ratio in ("1/128", "1/8", "1/4", "1/2", "2/1", "4/1", "8/1", "128/1"):
        for value_power in range(8, 15):
            size = 2**value_power
            preload = (f"put --sequential-keys --key-space-size 65536 --val-size {size} "
                       "--key-size 256 --total 65536")
            for client_power in range(5, 12):
                clients = 2**client_power
                name = f"heatmap-{ratio.replace('/', '-')}-{size}-{clients}"
                # The upstream shell script uses bc with four decimal places.
                value = int(Fraction(ratio) * 10000) / 10000
                command = f"txn-mixed --total 200000 --rw-ratio {value} --limit 100 --val-size {size}"
                yield Case(name, command, ("mixed", 200000), clients, clients, preload)


def cases(stress):
    writes = "put --total 200 --key-space-size 200 --sequential-keys"
    result = [
        Case("put", writes, (200,)),
        Case("range", "range A z --limit 100 --total 200", (200,), preload=writes),
        Case("txn-put", "txn-put --txn-ops 4 --key-space-size 64 --total 200", (200,)),
        Case("txn-mixed", "txn-mixed --total 512 --key-space-size 200 --limit 1000",
             ("mixed", 512), preload=writes),
        Case("stm", "stm --keys 32 --keys-per-txn 2 --total 200", (200,)),
        Case("lease-keepalive", "lease-keepalive --total 200", (200,)),
        Case("watch", "watch --streams 2 --watch-per-stream 8 --put-total 32", (16, 512)),
        Case("watch-get", "watch-get --streams 4 --watchers 128 --events 16", (None,)),
        Case("watch-latency", "watch-latency --streams 2 --watchers-per-stream 8 "
             "--put-total 32 --put-rate 100", (32, 512)),
        Case("mvcc-put", "mvcc put --total 200", (200,)),
    ]
    if stress:
        preload = "put --total 4000 --key-space-size 10000 --key-size 32 --val-size 256"
        result.extend([
            Case("mixed-256", "txn-mixed --total 8000 --key-space-size 10000 --key-size 32 "
                 "--val-size 256 --rw-ratio 1 --limit 1000", ("mixed", 8000), 256, 100, preload),
            Case("put-4k", "put --total 20000 --val-size 4096 --key-space-size 10000",
                 (20000,), 256, 100),
            Case("watch-1000", "watch-latency --streams 10 --watchers-per-stream 100 "
                 "--put-total 2000 --put-rate 200", (2000, 2000000), 10, 10),
            Case("watch-100k", "watch --streams 1000 --watch-per-stream 100 --put-total 1",
                 (100000, 100000), 100, 100),
        ])
    return result


def section(text):
    rate = re.search(r"Requests/sec:\s*(\S+)", text)
    bins = re.findall(r"(?m)^\s*\S+ \[(\d+)\]\s*\|", text)
    p99 = re.search(r"(?m)^\s*99% in (\S+) secs\.", text)
    errors = [{"count": int(n), "message": message}
              for n, message in re.findall(r"(?m)^\s*\[(\d+)\]\t(.+)$", text)]
    return {"successes": sum(map(int, bins)), "ops_per_second": float(rate[1]) if rate else None,
            "p99_seconds": float(p99[1]) if p99 else None, "errors": errors}


def parse_report(text, expected):
    # A mixed run can have an error distribution without any successful reads.
    totals = None
    if expected[0] == "mixed":
        groups = re.split(r"(?m)^Total (Read|Write) Ops: (\d+)$", text)[1:]
        labels, counts, blocks = groups[0::3], groups[1::3], groups[2::3]
        totals = list(map(int, counts))
        complete = labels == ["Read", "Write"] and sum(totals) == expected[1]
        reports = [dict(section(block), operation=label) for label, block in zip(labels, blocks)]
        expected = totals
    else:
        blocks = re.split(r"(?m)^Summary:\s*$", text)[1:]
        if not blocks and "Error distribution:" in text:
            blocks = [text]
        reports = [section(block) for block in blocks]
        complete = True
    complete &= len(expected) == len(reports) and all(
        r["successes"] == n if n is not None else r["successes"] > 0
        for r, n in zip(reports, expected))
    return {"sections": reports, "mixed_attempts": totals,
            "passed": complete and "Error distribution:" not in text}


def run_case(case, target, args, tools, source_revision, repeat):
    result = {"case": case.name, "target": target, "benchmark_revision": source_revision, "repeat": repeat, "passed": False,
              "clients": case.clients, "connections": case.connections, "command": case.command,
              "members": 0 if target == "etcd-mvcc" else args.members, "preload": case.preload,
              "snapshot_count": args.snapshot_count, "quota_bytes": args.quota_bytes}
    prefix = args.output / f"{target}-{case.name}-{repeat}"
    with tempfile.TemporaryDirectory() as temp, ExitStack() as stack:
        directory = Path(temp)
        command = [tools["benchmark"], "--precise", "--clients", str(case.clients),
                   "--conns", str(case.connections)]
        try:
            if target != "etcd-mvcc":
                endpoints = start_cluster(stack, directory, tools[target], target, args.members,
                                          tools["etcdctl"], args.snapshot_count, args.quota_bytes)
                command.extend(["--endpoints", endpoints])
            phases = []
            if case.preload:
                total = int(case.preload.split("--total ")[1].split()[0])
                phases.append(("preload", case.preload, (total,)))
            phases.append(("measure", case.command, case.expected))
            for phase, options, expected in phases:
                log_path = Path(f"{prefix}-{phase}.txt")
                with log_path.open("w") as log, Path(f"{prefix}-{phase}.stderr").open("w") as err:
                    run = subprocess.run([*command, *options.split()], cwd=directory, stdout=log,
                                         stderr=err, timeout=args.timeout)
                report = parse_report(log_path.read_text(), expected)
                result[phase] = dict(report, exit_code=run.returncode)
                if run.returncode or not report["passed"]:
                    return result
            result["passed"] = True
        except (OSError, RuntimeError, subprocess.TimeoutExpired) as error:
            result["error"] = str(error)
        finally:
            if not result["passed"]:
                for log in directory.glob("member-*.log"):
                    Path(f"{prefix}-{log.name}").write_bytes(log.read_bytes()[-16000:])
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", choices=["smoke", "stress", "heatmap"], default="smoke")
    parser.add_argument("--case", action="append", dest="selected")
    parser.add_argument("--source", type=Path, default=ROOT / "reference/etcd")
    parser.add_argument("--binary", type=Path, default=ROOT / "target/release/slatedb-etcd")
    parser.add_argument("--members", choices=[1, 3], type=int, default=1)
    parser.add_argument("--snapshot-count", type=int, default=1000)
    parser.add_argument("--timeout", type=int, default=600)
    parser.add_argument("--output", type=Path, default=ROOT / "tmp/upstream-benchmarks")
    parser.add_argument("--repeat", type=int, default=1)
    parser.add_argument("--quota-bytes", type=int)
    args = parser.parse_args()
    if args.repeat <= 0:
        parser.error("--repeat must be positive")
    if args.quota_bytes is None:
        args.quota_bytes = (20 if args.profile == "heatmap" else 2) * 1024**3
    source = checked_source(args.source)
    selected = list(heatmap_cases()) if args.profile == "heatmap" else cases(args.profile == "stress")
    if args.selected:
        if set(args.selected) - {c.name for c in selected}:
            parser.error("Unknown case in this profile")
        selected = [c for c in selected if c.name in args.selected]
    tools_dir = ROOT / "tmp/upstream-tools"
    tools = {name: build_tool(source, tools_dir, name) for name in ("benchmark", "etcdctl", "etcd")}
    tools["service"] = args.binary.resolve(strict=True)
    args.output.mkdir(parents=True, exist_ok=True)
    revision = json.loads(INVENTORY.read_text())["revision"]
    results = []
    for case in selected:
        for repeat in range(args.repeat):
            targets = ["service", "etcd"] if repeat % 2 == 0 else ["etcd", "service"]
            for target in (["etcd-mvcc"] if case.name == "mvcc-put" else targets):
                result = run_case(case, target, args, tools, revision, repeat)
                results.append(result)
                print(target, case.name, "pass" if result["passed"] else "FAIL", flush=True)
                (args.output / "summary.json").write_text(json.dumps(results, indent=2) + "\n")
    raise SystemExit(not all(r["passed"] for r in results))


if __name__ == "__main__":
    main()
