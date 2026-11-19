#!/usr/bin/env python3
"""Check fetched history for private metadata, local paths, and missing sign-offs."""

import argparse
import json
from pathlib import Path
import re
import subprocess

ROOT = Path(__file__).resolve().parents[1]
SIGN_OFF = b"Signed-off-by: siddontang <siddontang@gmail.com>"
# Maintainer sign-offs. These emails remain public by request.
MAINTAINER_SIGN_OFFS = (SIGN_OFF, b"Signed-off-by: nolouch <nolouch@gmail.com>")
# Maintainers who merge pull requests on GitHub with these emails as the author.
MAINTAINER_EMAILS = (b"nolouch@gmail.com",)
PUBLIC_EMAIL = re.compile(rb"(?:[\w.+-]+@users\.noreply\.github\.com|noreply@github\.com)\Z")
SIGN_OFF_LINE = re.compile(rb"Signed-off-by: [^<>\n]+ <([^<>\s]+)>")
EMAIL = re.compile(rb"[\w.+%-]+@[\w.-]+\.[A-Za-z]{2,}")
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


def sign_off_error(trailers, lines, author):
    """Accept one maintainer sign-off, or one sign-off by the author with a GitHub public email.

    A maintainer records approval of contributed commits with a signed-off merge commit.
    """
    if any(line in MAINTAINER_SIGN_OFFS for line in (*trailers, *lines)):
        in_trailers = [line for line in trailers if line in MAINTAINER_SIGN_OFFS]
        in_lines = [line for line in lines if line in MAINTAINER_SIGN_OFFS]
        if len(in_trailers) != 1 or in_lines != in_trailers:
            return "missing or repeated owner sign-off"
        return None
    sign_offs = [line for line in trailers if line.startswith(b"Signed-off-by:")]
    match = SIGN_OFF_LINE.fullmatch(sign_offs[0]) if len(sign_offs) == 1 else None
    if (match is None or lines.count(sign_offs[0]) != 1 or author is None
            or PUBLIC_EMAIL.fullmatch(match[1]) is None or match[1] != author):
        return "missing owner sign-off or author sign-off with a GitHub public email"
    return None


def web_merge(headers, emails):
    """Return whether a maintainer merged a pull request on GitHub.

    GitHub commits the merge, so the merge itself records the maintainer approval.
    """
    return (len(re.findall(rb"(?m)^parent ", headers)) >= 2
            and emails[b"author"] in MAINTAINER_EMAILS
            and emails[b"committer"] == b"noreply@github.com")


def commit_errors(root, raw):
    headers, message = raw.split(b"\n\n", 1)
    errors = path_errors(message)
    emails = {}
    for field in (b"author", b"committer"):
        match = re.search(rb"(?m)^" + field + rb" .* <([^<>]+)> ", headers)
        emails[field] = match[1] if match else None
        if match is None or (PUBLIC_EMAIL.fullmatch(match[1]) is None
                             and not (field == b"author" and match[1] in MAINTAINER_EMAILS)):
            errors.append(field.decode() + " must use a GitHub public email")
    trailers = git(root, "interpret-trailers", "--parse", data=message).splitlines()
    error = sign_off_error(trailers, message.splitlines(), emails[b"author"])
    if error and not web_merge(headers, emails):
        errors.append(error)
    lines = message.splitlines()
    remaining = b"\n".join(line for line in lines if line not in MAINTAINER_SIGN_OFFS)
    if any(PUBLIC_EMAIL.fullmatch(address) is None for address in EMAIL.findall(remaining)):
        errors.append("personal email outside the owner sign-off")
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
            match = re.search(rb"(?m)^tagger .* <([^<>]+)> ", data)
            if match and PUBLIC_EMAIL.fullmatch(match[1]) is None:
                errors.append("tagger must use a GitHub public email")
            message = data.partition(b"\n\n")[2]
            if any(PUBLIC_EMAIL.fullmatch(value) is None for value in EMAIL.findall(message)):
                errors.append("personal email in tag message")
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
