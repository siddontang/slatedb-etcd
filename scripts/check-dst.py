#!/usr/bin/env python3
"""Run deterministic event schedules and save trace hashes."""

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import subprocess
import time

from reporting import write_report

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seeds", default="1,2,42,58,70")
    parser.add_argument("--shared-seeds", default="1,2,3",
                        help="Seeds of the shared-state schedules. An empty value skips them.")
    parser.add_argument("--output", type=Path, default=ROOT / "results/dst.json")
    args = parser.parse_args()
    seeds = list(dict.fromkeys(int(seed) for seed in args.seeds.split(",")))
    shared = list(dict.fromkeys(int(seed) for seed in args.shared_seeds.split(",") if seed))
    if any(seed < 0 or seed >= 2**64 for seed in seeds + shared):
        parser.error("Seeds must be unsigned 64-bit integers")
    directory = ROOT / "target/dst"
    started = time.time()
    result = subprocess.run([str(ROOT / "scripts/cargo.sh"), "test", "--locked",
                             "seeded_fault_schedules_replay_identically", "--", "--nocapture"],
                            cwd=ROOT, env=dict(os.environ,
                                              SLATEDB_DST_SEEDS=",".join(map(str, seeds)),
                                              SLATEDB_DST_TRACE_DIR=str(directory)))
    passed = result.returncode == 0
    if shared:
        env = dict(os.environ, SLATEDB_DST_SHARED_SEEDS=",".join(map(str, shared)),
                   SLATEDB_DST_TRACE_DIR=str(directory))
        result = subprocess.run([str(ROOT / "scripts/cargo.sh"), "test", "--locked",
                                 "shared_store_schedules_replay_identically", "--", "--nocapture"],
                                cwd=ROOT, env=env)
        passed = passed and result.returncode == 0
    report = {"created_at": datetime.now(timezone.utc).isoformat(),
              "passed": passed, "replays_per_seed": 2, "seeds": []}
    runs = [(seed, f"seed-{seed}.json") for seed in seeds]
    runs += [(seed, f"shared-seed-{seed}.json") for seed in shared]
    for seed, name in runs:
        path = directory / name
        entry = {"seed": seed, "mode": "shared" if name.startswith("shared") else "replicated",
                 "trace": str(path)}
        if path.is_file() and path.stat().st_mtime >= started:
            data = path.read_bytes()
            entry.update(events=len(json.loads(data)), sha256=hashlib.sha256(data).hexdigest())
        else:
            report["passed"] = False
        report["seeds"].append(entry)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    write_report(args.output, report)
    raise SystemExit(0 if report["passed"] else 1)


if __name__ == "__main__":
    main()
