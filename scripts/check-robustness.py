#!/usr/bin/env python3
"""Run process fault histories and check them with the upstream etcd model."""

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time

from reporting import write_report

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seeds", default="1,2,3,4")
    parser.add_argument("--output", type=Path, default=ROOT / "results/robustness.json")
    parser.add_argument("--results-dir", type=Path, default=ROOT / "tmp/test-results")
    parser.add_argument("--binary", type=Path, default=ROOT / "target/release/slatedb-etcd")
    parser.add_argument("--jepsen", action="store_true")
    args = parser.parse_args()
    seeds = list(dict.fromkeys(int(seed) for seed in args.seeds.split(",")))
    binary, directory = args.binary.resolve(), args.results_dir.resolve()
    directory.mkdir(parents=True, exist_ok=True)
    report = {"created_at": datetime.now(timezone.utc).isoformat(), "passed": True,
              "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
              "jepsen_requested": args.jepsen, "runs": []}
    for seed in seeds:
        print(f"Checking process faults for seed {seed}", flush=True)
        started = time.time()
        folder = directory / f"robustness-{seed}"
        env = dict(os.environ, SLATEDB_TEST_RESULTS=str(directory), SLATEDB_TEST_SEED=str(seed))
        code = subprocess.run([sys.executable, str(ROOT / "scripts/check-compat.py"),
                               "--binary", str(binary), "--tags", "robustness",
                               "--run", "^TestRobustness($|CheckerControls|Recorder)",
                               "--output", str(directory / f"go-robustness-{seed}.json")],
                              env=env).returncode
        run = {"seed": seed, "passed": code == 0, "directory": str(folder)}
        result = folder / "result.json"
        if result.is_file() and result.stat().st_mtime >= started:
            run["model"] = json.loads(result.read_text())
        if args.jepsen and (folder / "append.edn").is_file() and result.is_file() and \
                result.stat().st_mtime >= started:
            check = subprocess.run([sys.executable, str(ROOT / "scripts/check-jepsen.py"),
                                    str(folder)]).returncode
            run["jepsen_passed"] = check == 0
            run["passed"] &= check == 0
        elif args.jepsen:
            run["jepsen_passed"] = False
            run["passed"] = False
        report["runs"].append(run)
        report["passed"] &= run["passed"]
        args.output.parent.mkdir(parents=True, exist_ok=True)
        write_report(args.output, report)
    print("Report:", args.output)
    raise SystemExit(0 if report["passed"] else 1)


if __name__ == "__main__":
    main()
