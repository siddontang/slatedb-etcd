#!/usr/bin/env python3
"""Check publication files, document links, and report privacy."""

from collections import Counter
from pathlib import Path
import re
import subprocess
from urllib.parse import unquote, urlsplit

ROOT = Path(__file__).resolve().parents[1]


def anchors(path):
    result, seen = set(), Counter()
    for line in path.read_text().splitlines():
        if not re.match(r"^#{1,6} ", line):
            continue
        value = re.sub(r"[^\w\- ]", "", line.lstrip("# ").lower()).replace(" ", "-")
        result.add(value if seen[value] == 0 else f"{value}-{seen[value]}")
        seen[value] += 1
    return result


def main():
    names = set(subprocess.check_output(
        ["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"],
        cwd=ROOT).decode().split("\0")) - {""}
    errors, links, documents = [], 0, 0
    for name in sorted(names):
        path = ROOT / name
        if path.is_symlink():
            errors.append(f"{name}: review the symlink before publication")
            continue
        if not path.is_file():
            continue
        if path.suffix in {".pem", ".key", ".p12", ".pfx", ".snapshot"} or \
                (path.name.startswith(".env") and path.name != ".env.example"):
            errors.append(f"{name}: credential or data file is not a publication input")
        if path.suffix != ".md" and not (name.startswith("results/") and path.suffix == ".json"):
            continue
        text = path.read_text()
        for number, line in enumerate(text.splitlines(), 1):
            if re.search(r"/(?:Users|home)/[^/\s\"'<>]+", line):
                errors.append(f"{name}:{number}: contains a workstation home path")
            if re.search(r"(?:codex|plugin|skill)://", line):
                errors.append(f"{name}:{number}: contains a private application link")
            if path.suffix == ".md" and name != "AGENTS.md" and len(line) > 100:
                errors.append(f"{name}:{number}: exceeds 100 columns")
        if path.suffix != ".md":
            continue
        documents += 1
        for match in re.finditer(r"\[[^\]]*\]\(([^\s)]+)\)", text):
            link = urlsplit(match[1])
            if link.scheme or link.netloc:
                continue
            target = (path.parent / unquote(link.path)).resolve() if link.path else path
            links += 1
            if not target.is_relative_to(ROOT) or not target.is_file():
                errors.append(f"{name}: missing local link target {match[1]}")
            elif link.fragment and target.suffix == ".md" and \
                    unquote(link.fragment) not in anchors(target):
                errors.append(f"{name}: missing section {match[1]}")
    for error in errors:
        print(error)
    print(f"Checked {documents} documents and {links} local links; {len(errors)} errors.")
    raise SystemExit(bool(errors))


if __name__ == "__main__":
    main()
