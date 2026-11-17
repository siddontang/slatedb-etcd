#!/usr/bin/env python3
"""Run the official etcd client tests and record their results."""

import argparse
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import subprocess

from reporting import write_report


def main():
    root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, default=root / "target/release/slatedb-etcd")
    parser.add_argument("--output", type=Path, default=root / "results/etcd-compatibility.json")
    parser.add_argument("--tags", default="", help="Go build tags, such as upstream or robustness")
    parser.add_argument("--run", default="", help="Go test name expression")
    parser.add_argument("--all-capabilities", action="store_true")
    args = parser.parse_args()
    binary = args.binary.resolve()
    command = ["go", "test", "-mod=readonly", "-json", "-count=1", "-timeout=30m"]
    if args.tags:
        command += ["-tags", args.tags]
    if args.run:
        command += ["-run", args.run]
    command += ["./..."]
    log = root / "tmp/test-results" / (args.output.stem + ".jsonl")
    log.parent.mkdir(parents=True, exist_ok=True)
    report = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
        "reference_etcd": "3.6.5",
        "client": "go.etcd.io/etcd/client/v3 v3.6.5",
        "go": subprocess.check_output(["go", "version"], text=True).strip(),
        "deployment": "three local service processes; one embedded reference etcd",
        "command": command,
        "profile": "all-capabilities" if args.all_capabilities else "supported-capabilities",
        "log": str(log),
        "tests": [],
    }
    process = subprocess.Popen(
        command, cwd=root / "tests/compat",
        env=dict(os.environ, SLATEDB_ETCD_BINARY=str(binary),
                 SLATEDB_UPSTREAM_ALL="1" if args.all_capabilities else "0"),
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
    )
    output = []
    messages = {}
    for line in process.stdout:
        output.append(line)
        try:
            event = json.loads(line)
        except ValueError:
            print(line, end="")
            continue
        name = event.get("Test")
        if name and event.get("Output"):
            messages.setdefault(name, []).append(event["Output"].strip())
        if event.get("Action") in ("pass", "fail", "skip") and event.get("Test"):
            result = {"name": name, "result": event["Action"], "seconds": event.get("Elapsed")}
            if event["Action"] != "pass":
                result["details"] = messages.get(name, [])
            report["tests"].append(result)
            if "/" not in event["Test"] or event["Action"] != "pass":
                print(event["Action"], event["Test"], flush=True)
    code = process.wait()
    log.write_text("".join(output))
    counts = Counter(test["result"] for test in report["tests"])
    report["counts"] = dict(counts)
    names = {test["name"] for test in report["tests"]}
    report["leaf_counts"] = dict(Counter(
        test["result"] for test in report["tests"]
        if not any(name.startswith(test["name"] + "/") for name in names)
    ))
    report["passed"] = code == 0 and counts["pass"] > 0
    report["complete"] = report["passed"] and counts["skip"] == 0
    args.output.parent.mkdir(parents=True, exist_ok=True)
    write_report(args.output, report)
    if not report["passed"]:
        print("".join(output))
    print("Report:", args.output)
    raise SystemExit(0 if report["passed"] else 1)


if __name__ == "__main__":
    main()
