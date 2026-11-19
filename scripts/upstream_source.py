"""Locate the pinned etcd checkout and build its tools."""

import json
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[1]
INVENTORY = ROOT / "tests/upstream/etcd-inventory.json"


def checked_source(path):
    source = path.resolve()
    revision = json.loads(INVENTORY.read_text())["revision"]
    head = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=source, text=True).strip()
    if head != revision:
        raise ValueError(f"Expected etcd {revision}, found {head}")
    return source


def build_tool(source, output, name):
    packages = {"etcd": ("server", "."), "etcdctl": ("etcdctl", "."),
                "etcdutl": ("etcdutl", "."), "benchmark": (".", "./tools/benchmark")}
    module, package = packages[name]
    output.mkdir(parents=True, exist_ok=True)
    path = output.resolve() / name
    subprocess.run(["go", "build", "-mod=readonly", "-o", path, package],
                   cwd=source / module, check=True)
    return path
