#!/usr/bin/env python3
"""Check fault histories with Jepsen and Elle. Requires Java 17+ and Leiningen."""

import argparse
import os
from pathlib import Path
import shutil
import subprocess

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("histories", nargs="*", type=Path, help="Robustness result directories")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    jar = os.environ.get("LEIN_JAR")
    if jar:
        lein = [os.environ.get("JAVA_CMD", "java"), "-cp", str(Path(jar).resolve()),
                "clojure.main", "-m", "leiningen.core.main"]
    elif shutil.which("lein"):
        lein = ["lein"]
    else:
        parser.error("Install Leiningen or set LEIN_JAR to its standalone JAR")
    if not args.self_test and not args.histories:
        parser.error("Provide result directories or --self-test")
    if args.self_test:
        subprocess.run(lein + ["test"], cwd=ROOT / "tests/jepsen", check=True)
    for workload, filename in [("register", "jepsen.edn"), ("append", "append.edn")]:
        paths = [(directory / filename).resolve() for directory in args.histories]
        for path in paths:
            if not path.is_file():
                parser.error(f"Missing history: {path}")
        if paths:
            subprocess.run(lein + ["run", workload, *map(str, paths)],
                           cwd=ROOT / "tests/jepsen", check=True)


if __name__ == "__main__":
    main()
