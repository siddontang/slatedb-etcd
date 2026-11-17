#!/usr/bin/env python3
"""Import the common suite and index every upstream Go test at a pinned revision."""

import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path
import re
import subprocess

PIN = "a0614505aff9b8ff469e9c59c2d979a5936d13f4"
ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=ROOT / "reference/etcd")
    args = parser.parse_args()
    source = args.source.resolve()
    revision = subprocess.check_output(["git", "-C", str(source), "rev-parse", "HEAD"],
                                       text=True).strip()
    if revision != PIN:
        raise SystemExit(f"Expected etcd v3.6.5 at {PIN}; found {revision}")
    target = ROOT / "tests/compat"
    copied = []
    for path in sorted((source / "tests/common").glob("*.go")):
        if path.name in {"e2e_test.go", "integration_test.go", "unit_test.go"}:
            continue
        content = path.read_text()
        name = "upstream_common_" + path.stem.removesuffix("_test") + "_test.go"
        (target / name).write_text("//go:build upstream\n\n" +
                                  content.replace("package common\n", "package compat\n", 1))
        copied.append({"source": str(path.relative_to(source)), "target": name,
                       "sha256": hashlib.sha256(path.read_bytes()).hexdigest()})
    path = source / "tests/framework/integration/integration.go"
    text = path.read_text()
    license_header = text[:text.index("package integration")]
    client = text[text.index("type integrationClient struct"):]
    client = client.replace("*clientv3.Client\n}", "*clientv3.Client\n\ttb testing.TB\n}", 1)
    imports = '''package compat

import (
    "context"
    "fmt"
    "strings"
    "testing"
    healthpb "google.golang.org/grpc/health/grpc_health_v1"
    clientv3 "go.etcd.io/etcd/client/v3"
    etcdctlcmd "go.etcd.io/etcd/etcdctl/v3/ctlv3/command"
    "go.etcd.io/etcd/tests/v3/framework/config"
)

'''
    (target / "upstream_client_test.go").write_text(
        "//go:build upstream\n\n" + license_header + imports + client)
    subprocess.run(["git", "apply", "--unidiff-zero",
                    str(ROOT / "tests/upstream/maintenance-auth.patch")],
                   cwd=ROOT, check=True)
    copied.append({"source": str(path.relative_to(source)), "target": "upstream_client_test.go",
                   "sha256": hashlib.sha256(path.read_bytes()).hexdigest()})
    tests = []
    imported_sources = {item["source"] for item in copied}
    for path in sorted(source.rglob("*_test.go")):
        text = path.read_text()
        rel = str(path.relative_to(source))
        family = "/".join(path.relative_to(source).parts[:2])
        for match in re.finditer(r"^func ((?:Test|Fuzz|Benchmark|Example)\w*)\s*\(", text, re.M):
            if rel in imported_sources:
                disposition = "imported-common-suite"
            elif rel.startswith("tests/robustness/model/"):
                disposition = "upstream-model-dependency"
            elif rel.startswith(("tests/e2e/", "tests/integration/")):
                disposition = "requires-etcd-server-or-process-adapter"
            else:
                disposition = "upstream-internal-test"
            tests.append({"file": rel, "line": text.count("\n", 0, match.start()) + 1,
                          "name": match[1], "family": family, "disposition": disposition})
    report = {"repository": "https://github.com/etcd-io/etcd", "tag": "v3.6.5",
              "revision": revision, "scope": "Top-level Go test, fuzz, benchmark, and example functions.",
              "imports": copied, "families": dict(sorted(Counter(t["family"] for t in tests).items())),
              "dispositions": dict(sorted(Counter(t["disposition"] for t in tests).items())),
              "tests": tests}
    destination = ROOT / "tests/upstream"
    destination.mkdir(exist_ok=True)
    (destination / "etcd-inventory.json").write_text(json.dumps(report, indent=2) + "\n")
    (destination / "LICENSE.etcd").write_bytes((source / "LICENSE").read_bytes())
    subprocess.run(["gofmt", "-w", *map(str, target.glob("upstream_*.go"))], check=True)
    print(f"Imported {len(copied)} files; indexed {len(tests)} upstream functions.")


if __name__ == "__main__":
    main()
