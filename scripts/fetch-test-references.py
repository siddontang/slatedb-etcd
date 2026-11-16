#!/usr/bin/env python3
"""Fetch the exact upstream sources used by the test audit."""

from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[1]
SOURCES = [
    ("etcd", "https://github.com/etcd-io/etcd.git",
     "a0614505aff9b8ff469e9c59c2d979a5936d13f4"),
    ("jepsen-etcd", "https://github.com/jepsen-io/etcd.git",
     "b47cc401f27bfb7c28afd9ac088b5f0dca4501b8"),
]

for name, url, revision in SOURCES:
    path = ROOT / "reference" / name
    if not path.exists():
        path.parent.mkdir(exist_ok=True)
        subprocess.run(["git", "clone", "--filter=blob:none", "--no-checkout", url, str(path)],
                       check=True)
        subprocess.run(["git", "-C", str(path), "checkout", "--detach", revision], check=True)
    actual = subprocess.check_output(["git", "-C", str(path), "rev-parse", "HEAD"], text=True).strip()
    if actual != revision:
        raise SystemExit(f"{path}: expected {revision}; found {actual}. Keep local changes intact.")
    print(name, actual)
