#!/usr/bin/env python3
"""Check fetched history for workstation paths and missing DCO sign-offs."""

import argparse
import json
from pathlib import Path
import re
import subprocess

ROOT = Path(__file__).resolve().parents[1]
SIGN_OFF_LINE = re.compile(rb"Signed-off-by: [^<>\n]+ <[^<>\s]+>")
HOME = re.compile(rb"(?<![\w./<>-])/(?:Users|home)/([^/\s\"'<>]+)")
TEMP = re.compile(rb"/(?:private/)?var/folders/[^/\s]+/[^/\s]+")
WINDOWS_HOME = re.compile(rb"[A-Za-z]:\\Users\\[^\\\s\"'<>]+", re.IGNORECASE)


def git(root, *args, data=None):
    return subprocess.check_output(["git", *args], cwd=root, input=data)


def path_errors(data, name=""):
    errors = []
    for match in HOME.finditer(data):
        if name == "scripts/test_reporting.py" and match[1] == b"example":
            continue
        errors.append("workstation home path")
    if TEMP.search(data) or WINDOWS_HOME.search(data):
        errors.append("workstation temporary or Windows path")
    if re.search(rb"(?:codex|plugin|skill)://", data):
        # The checker stores escaped patterns, not private application links.
        errors.append("private application link")
    return sorted(set(errors))


def commit_errors(root, raw):
    """Require a DCO sign-off on each commit that a person wrote.

    Merge commits and commits that GitHub creates, such as web merges, need no sign-off.
    """
    headers, message = raw.split(b"\n\n", 1)
    errors = path_errors(message)
    merge = len(re.findall(rb"(?m)^parent ", headers)) > 1
    github = re.search(rb"(?m)^committer .* <noreply@github\.com> ", headers) is not None
    trailers = git(root, "interpret-trailers", "--parse", data=message).splitlines()
    if not (merge or github) and not any(SIGN_OFF_LINE.fullmatch(t) for t in trailers):
        errors.append("missing Signed-off-by trailer")
    return errors


def audit(root, revisions=("--all",)):
    """Check the objects reachable from revisions, or from every fetched ref by default."""
    if git(root, "rev-parse", "--is-shallow-repository").strip() == b"true":
        raise RuntimeError("Fetch complete history before checking commit metadata")
    failures, counts = [], {"commits": 0, "blobs": 0, "tags": 0}
    rows = git(root, "rev-list", "--objects", *revisions, "--").splitlines()
    for row in rows:
        oid, _, name = row.partition(b" ")
        kind = git(root, "cat-file", "-t", oid.decode()).strip()
        if kind not in (b"commit", b"blob", b"tag"):
            continue
        data = git(root, "cat-file", kind.decode(), oid.decode())
        errors = []
        if kind == b"commit":
            counts["commits"] += 1
            errors = commit_errors(root, data)
        elif kind == b"blob":
            counts["blobs"] += 1
            errors = path_errors(data, name.decode())
        else:
            counts["tags"] += 1
            errors = path_errors(data)
        for error in errors:
            failures.append({"object": oid.decode(), "path": name.decode(), "error": error})
    return counts, failures


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=ROOT)
    parser.add_argument("--range", dest="revisions",
                        help="check only new commits, for example origin/main..HEAD")
    args = parser.parse_args()
    revisions = (args.revisions,) if args.revisions else ("--all",)
    counts, failures = audit(args.repo, revisions)
    for failure in failures:
        print(json.dumps(failure))
    print(json.dumps({**counts, "passed": not failures, "findings": len(failures)}))
    raise SystemExit(bool(failures))


if __name__ == "__main__":
    main()
