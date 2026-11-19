#!/usr/bin/env python3
"""Create three temporary Lima guests, test remote recovery, and delete the guests."""

import argparse
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess
import sys
import tarfile
import tempfile
import uuid

from reporting import write_report

ROOT = Path(__file__).resolve().parents[1]


def run(*args):
    return subprocess.check_output(list(args), text=True).strip()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--linux-binary", type=Path, help="Existing Linux ARM64 binary")
    parser.add_argument("--save-linux-binary", type=Path, help="Save the built binary for later runs")
    parser.add_argument("--reuse", help="Three disposable Lima guest names; delete them after this run")
    parser.add_argument("--output", type=Path, default=ROOT / "results/multihost.json")
    args = parser.parse_args()
    if platform.system() != "Darwin" or platform.machine() != "arm64":
        parser.error("This fixture requires Apple Silicon and Lima with the VZ driver")
    names = args.reuse.split(",") if args.reuse else [
        f"slatedb-s3-test-{uuid.uuid4().hex[:8]}-{i}" for i in range(1, 4)]
    if len(names) != 3 or len(set(names)) != 3 or any(
            not name.startswith("slatedb-s3-test-") for name in names):
        parser.error("Use three distinct disposable slatedb-s3-test-* guest names")
    owned = []
    report = {"passed": False, "deployment": "three Linux VMs and S3 on one physical host"}
    code = 1
    try:
        if args.reuse:
            existing = {json.loads(line)["name"] for line in run("limactl", "list", "--json").splitlines()}
            if not set(names) <= existing:
                raise RuntimeError("A requested disposable guest is missing")
            owned = names.copy()
        else:
            for i, name in enumerate(names):
                # Track the name before creation so a partial setup also gets removed.
                owned.append(name)
                subprocess.run(["limactl", "start", "--yes", "--name", name,
                    "--memory", "3" if i == 0 else "1.5", str(ROOT / "tests/deploy/lima.yaml")],
                    check=True)
        with tempfile.TemporaryDirectory(prefix="slatedb-host-test-") as temporary:
            directory = Path(temporary)
            binary = args.linux_binary
            if binary is None:
                source = directory / "source.tar.gz"
                with tarfile.open(source, "w:gz") as archive:
                    for name in ["Cargo.toml", "Cargo.lock", "rust-toolchain.toml", "build.rs", "src", "proto", "third_party"]:
                        archive.add(ROOT / name, arcname=name)
                build = directory / "build.sh"
                build.write_text('''#!/bin/sh
set -eu
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain 1.91.1
. "$HOME/.cargo/env"
mkdir -p ~/slatedb-build
cd ~/slatedb-build
tar -xzf ~/slatedb-source.tar.gz
CARGO_BUILD_JOBS=2 cargo build --locked --release
sync
''')
                run("limactl", "copy", str(source), names[0]+":slatedb-source.tar.gz")
                run("limactl", "copy", str(build), names[0]+":build.sh")
                subprocess.run(["limactl", "shell", names[0], "--", "sh", "build.sh"], check=True)
                binary = directory / "slatedb-etcd-linux"
                run("limactl", "copy", names[0]+":slatedb-build/target/release/slatedb-etcd", str(binary))
            if args.save_linux_binary:
                args.save_linux_binary.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(binary, args.save_linux_binary)
            kernels = [run("limactl", "shell", name, "--", "uname", "-sr") for name in names]
            env = dict(os.environ, SLATEDB_VM_NAMES=",".join(names),
                       SLATEDB_LINUX_BINARY=str(binary.resolve()))
            result = subprocess.run([sys.executable, str(ROOT / "scripts/check-s3.py"),
                "--tags", "multihost", "--run", "^TestThreeVirtualMachines$", "--binary", str(binary),
                "--deployment", report["deployment"], "--output", str(args.output)], cwd=ROOT, env=env)
            code = result.returncode
            if args.output.exists():
                report = json.loads(args.output.read_text())
            report.update(guest_kernels=kernels, physical_hosts=1, guest_count=3)
    finally:
        cleanup = []
        for name in owned:
            result = subprocess.run(["limactl", "delete", "--force", name])
            cleanup.append(result.returncode == 0)
        report["temporary_guests_removed"] = len(cleanup) == 3 and all(cleanup)
        report["passed"] = code == 0 and report.get("passed", False) and report["temporary_guests_removed"]
        args.output.parent.mkdir(parents=True, exist_ok=True)
        write_report(args.output, report)
    raise SystemExit(0 if report["passed"] else 1)


if __name__ == "__main__":
    main()
