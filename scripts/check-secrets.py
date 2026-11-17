#!/usr/bin/env python3
"""Scan fetched Git history and current publishable files with Gitleaks."""

import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[1]
VERSION = "8.30.1"


def git(*args):
    return subprocess.check_output(["git", *args], cwd=ROOT)


def scan(scanner, mode, path, folder, ignore, log_options=None, input_data=None):
    report = folder / (mode + ".json")
    config = folder / "config.toml"
    config.write_text("[extend]\nuseDefault = true\n")
    command = [scanner, mode] + ([] if mode == "stdin" else [str(path)])
    command += ["--config", str(config), "--redact=100",
               "--no-banner", "--no-color", "--ignore-gitleaks-allow",
               "--gitleaks-ignore-path", str(ignore), "--exit-code=10",
               "--report-format=json", "--report-path", str(report)]
    if log_options:
        command += ["--log-opts", log_options]
    result = subprocess.run(command, cwd=ROOT, input=input_data, capture_output=True, text=True)
    if result.returncode not in (0, 10) or not report.is_file():
        raise RuntimeError(f"Gitleaks {mode} failed with exit code {result.returncode}")
    findings = json.loads(report.read_text())
    if not isinstance(findings, list) or (result.returncode == 10 and not findings):
        raise RuntimeError("Gitleaks returned an invalid report")
    for item in findings:
        # Never print credential text, author metadata, or the matched source line.
        location = {key: item.get(key) for key in ["RuleID", "File", "StartLine", "Commit"]}
        source = Path(location["File"])
        if source.is_absolute():
            location["File"] = str(source.relative_to(path)) if source.is_relative_to(path) \
                else source.name
        print(json.dumps(location))
    return findings


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gitleaks", default=os.environ.get("GITLEAKS", "gitleaks"))
    args = parser.parse_args()
    scanner = shutil.which(args.gitleaks)
    if scanner is None:
        parser.error(f"Install Gitleaks {VERSION}, or set GITLEAKS to its executable")
    version = subprocess.check_output([scanner, "version"], text=True).strip()
    if version != VERSION:
        parser.error(f"Expected Gitleaks {VERSION}; found {version}")
    if git("rev-parse", "--is-shallow-repository").strip() == b"true":
        parser.error("Fetch the complete history before scanning")
    names = set(git("ls-files", "-z", "--cached", "--others", "--exclude-standard")
                .decode().split("\0")) - {""}
    with tempfile.TemporaryDirectory(prefix="slatedb-secret-scan-") as name:
        folder = Path(name)
        snapshot = folder / "files"
        snapshot.mkdir()
        count = 0
        for name in sorted(names):
            source = ROOT / name
            if source.is_symlink():
                raise RuntimeError(f"Review the publishable symlink before scanning: {name}")
            if source.is_file():
                target = snapshot / name
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(source, target)
                count += 1
        empty = folder / "empty.ignore"
        empty.write_text("")
        current = scan(scanner, "dir", snapshot, folder, empty)
        history = scan(scanner, "git", ROOT, folder, empty,
                       "--all --full-history")
        messages = git("log", "--all", "--format=%B")
        messages += git("for-each-ref", "--format=%(contents)", "refs/tags")
        metadata = scan(scanner, "stdin", ROOT, folder, empty,
                        input_data=messages.decode(errors="replace"))
    passed = not current and not history and not metadata
    print(json.dumps({"scanner": f"gitleaks {VERSION}", "passed": passed,
                      "current_files": count, "commits": int(git("rev-list", "--all", "--count")),
                      "current_findings": len(current), "history_findings": len(history),
                      "message_findings": len(metadata),
                      "historical_exceptions": 0}))
    raise SystemExit(0 if passed else 1)


if __name__ == "__main__":
    main()
