#!/usr/bin/env python3
"""Run the official etcd client tests and record their results."""

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import subprocess


def main():
    root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, default=root / "target/release/slatedb-etcd")
    parser.add_argument("--output", type=Path, default=root / "results/etcd-compatibility.json")
    args = parser.parse_args()
    binary = args.binary.resolve()
    report = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
        "reference_etcd": "3.6.5",
        "client": "go.etcd.io/etcd/client/v3 v3.6.5",
        "go": subprocess.check_output(["go", "version"], text=True).strip(),
        "deployment": "three local service processes; one embedded reference etcd",
        "command": "go test -json -count=1 ./...",
        "tests": [],
    }
    process = subprocess.Popen(
        ["go", "test", "-json", "-count=1", "./..."], cwd=root / "tests/compat",
        env=dict(os.environ, SLATEDB_ETCD_BINARY=str(binary)),
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
    )
    output = []
    for line in process.stdout:
        output.append(line)
        try:
            event = json.loads(line)
        except ValueError:
            print(line, end="")
            continue
        if event.get("Action") in ("pass", "fail", "skip") and event.get("Test"):
            report["tests"].append({"name": event["Test"], "result": event["Action"],
                                    "seconds": event.get("Elapsed")})
            if "/" not in event["Test"] or event["Action"] != "pass":
                print(event["Action"], event["Test"], flush=True)
    code = process.wait()
    report["passed"] = code == 0 and bool(report["tests"])
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    if not report["passed"]:
        print("".join(output))
    print("Report:", args.output)
    raise SystemExit(0 if report["passed"] else 1)


if __name__ == "__main__":
    main()
