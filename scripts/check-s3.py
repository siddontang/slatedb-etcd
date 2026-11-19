#!/usr/bin/env python3
"""Run S3 recovery tests with a temporary RustFS server."""

import argparse
import json
from pathlib import Path
import subprocess
import sys

from reporting import write_report
from rustfs_fixture import local_rustfs

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=ROOT / "results/s3-recovery.json")
    parser.add_argument("--binary", type=Path, default=ROOT / "target/release/slatedb-etcd")
    parser.add_argument("--tags", default="s3")
    parser.add_argument("--run", default="^TestS3")
    parser.add_argument("--deployment", default="three local processes with a temporary S3 service")
    args = parser.parse_args()
    with local_rustfs() as (env, store):
        result = subprocess.run([sys.executable, str(ROOT / "scripts/check-compat.py"),
                                 "--tags", args.tags, "--run", args.run, "--binary", str(args.binary),
                                 "--output", str(args.output), "--deployment", args.deployment],
                                env=env, cwd=ROOT)
        if args.output.exists():
            report = json.loads(args.output.read_text())
            report["object_store"] = store
            write_report(args.output, report)
    raise SystemExit(result.returncode)


if __name__ == "__main__":
    main()
