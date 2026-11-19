#!/usr/bin/env python3
"""Run the original etcd Go suites from the pinned source tree."""

import argparse
from contextlib import ExitStack
import gzip
import json
import os
import re
import shutil
from pathlib import Path
import subprocess
import tempfile

from upstream_source import ROOT, INVENTORY, build_tool, checked_source, start_cluster


def service_overlay(source, directory):
    replacements = {}
    for name in ("cluster.go", "etcd_process.go"):
        relative = Path("tests/framework/e2e") / name
        copy = directory / relative
        copy.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source / relative, copy)
        replacements[str(source / relative)] = str(copy)
    subprocess.run(["git", "apply", "--no-index", "--unidiff-zero",
                    ROOT / "tests/upstream/process-fixture.patch"],
                   cwd=directory, check=True)
    overlay = directory / "overlay.json"
    overlay.write_text(json.dumps({"Replace": replacements}))
    return overlay


def go_cases(source, suite, args):
    tests = json.loads(INVENTORY.read_text())["tests"]
    if suite in ("go-bench", "fuzz"):
        prefix = "Benchmark" if suite == "go-bench" else "Fuzz"
        targets = sorted({(str(Path(row["file"]).parent), row["name"] if prefix == "Fuzz" else ".")
                          for row in tests if row["name"].startswith(prefix)})
        for package, name in targets:
            flags = ["-bench=.", "-benchmem", "-benchtime=" + args.benchtime] if suite == "go-bench" \
                else ["-fuzz=^" + name + "$", "-fuzztime=" + args.fuzztime]
            yield package, ["-run=^$", *flags, "."]
    elif suite == "unit":
        modules = sorted({next(parent for parent in (source / row["file"]).parents
                               if (parent / "go.mod").is_file()) for row in tests})
        for module in modules:
            if module == source / "tests":
                # This module also holds long process suites. Run its helpers here.
                children = {Path(row["file"]).parent for row in tests
                            if row["file"].startswith("tests/robustness/")}
                children.discard(Path("tests/robustness"))
                yield "tests", ["./common", "./framework/...",
                                *["./" + str(p.relative_to("tests")) for p in sorted(children)]]
            else:
                yield str(module.relative_to(source)), ["./..."]
    elif suite == "integration":
        yield "tests", ["./integration/..."]
        yield "tests", ["-tags=integration", "./common"]
    elif suite == "proxy-integration":
        yield "tests", ["-tags=cluster_proxy", "./integration/..."]
    elif suite == "common":
        yield "tests", ["-tags=e2e", "./common"]
    elif suite == "proxy-e2e":
        yield "tests", ["-tags=cluster_proxy", "./e2e"]
    else:
        yield "tests", ["./" + suite]


def run_go(command, cwd, log_path, selector, suite, env=None):
    print(cwd.name, *command, flush=True)
    counts = {"pass": 0, "fail": 0, "skip": 0}
    samples = set()
    empty = False
    with gzip.open(log_path, "wt", compresslevel=1) as log, \
            log_path.with_suffix(".stderr").open("w") as errors, \
            subprocess.Popen(command, cwd=cwd, stdout=subprocess.PIPE, stderr=errors,
                             text=True, errors="replace", env=env) as process:
        for line in process.stdout:
            log.write(line)
            event = json.loads(line)
            output = event.get("Output", "")
            empty |= "testing: warning: no tests to run" in output
            if event.get("Test") and event["Action"] in counts:
                counts[event["Action"]] += 1
            match = re.match(r"^(Benchmark\S+?)(?:-\d+)?\s+\d+\s+", output)
            if match:
                samples.add(match[1])
            elif event.get("Test", "").startswith("Benchmark") and re.match(
                    r"^\s*\d+\s+[\d.]+ ns/op", output):
                # Go can split the benchmark name and its measurements across two JSON events.
                samples.add(event["Test"])
        status = process.wait()
    passed = status == 0
    if selector or suite == "fuzz":
        passed &= counts["pass"] > 0 and not empty
    if suite == "go-bench":
        passed &= bool(samples)
    return dict(counts, passed=passed, complete=passed and counts["skip"] == 0,
                exit_code=status, samples=sorted(samples),
                log=log_path.name)


def process_runs(source, module, flags, go_flags, binary_flags, directory, args):
    # Run each process test separately. A timeout must leave later tests eligible to run.
    build = directory / "suite.test"
    subprocess.run(["go", "test", "-c", "-o", build,
                    *[flag for flag in go_flags if flag.startswith("-overlay")], *flags],
                   cwd=source / module, check=True)
    package = next(flag for flag in flags if flag.startswith("./"))
    work = source / module / package
    fixture = binary_flags[1:]
    names = subprocess.check_output([build, "-test.list=^Test", *fixture], cwd=work, text=True)
    names = sorted(name for name in names.splitlines() if re.fullmatch(r"Test\w+", name))
    if not names:
        raise ValueError("No upstream process tests found")
    selected = names[args.shard_index::args.shards]
    if not selected:
        raise ValueError("This shard has no tests")
    for name in selected:
        command = ["go", "tool", "test2json", "-t", "-p", package, build, "-test.v=test2json",
                   "-test.timeout=" + args.timeout, "-test.run=^" + name + "$", *fixture]
        yield name, command, work


def run_examples(source, args):
    package = source / "tests/integration/clientv3/examples"
    summary = args.output / f"{args.target}-examples-summary.jsonl"
    summary.write_text("")
    passed = True
    with tempfile.TemporaryDirectory() as temp:
        directory = Path(temp).resolve()
        ctl = build_tool(source, directory, "etcdctl")
        binary = (args.binary.resolve(strict=True) if args.target == "service"
                  else build_tool(source, directory, "etcd"))
        fixture = package / "main_test.go"
        original = "return lazyCluster.EndpointsGRPC()"
        text = fixture.read_text()
        if text.count(original) != 1:
            raise ValueError("Unexpected upstream client example fixture")
        copy = directory / fixture.name
        text = text.replace('import (', 'import (\n	"strings"', 1)
        copy.write_text(text.replace(original, 'return strings.Split(os.Getenv("TEST_URLS"), ",")'))
        overlay = directory / "overlay.json"
        overlay.write_text(json.dumps({"Replace": {str(fixture): str(copy)}}))
        executable = directory / "examples.test"
        subprocess.run(["go", "test", "-c", "-overlay=" + str(overlay), "-o", executable, "."],
                       cwd=package, check=True)
        selector = args.run or "^Example"
        listing = subprocess.check_output([executable, "-test.list=" + selector], cwd=package,
                                          text=True)
        names = [name for name in listing.splitlines() if re.fullmatch(r"Example\w*", name)]
        if not names:
            raise ValueError("No runnable client examples matched the selector")
        for name in names:
            # An example can change membership or call log.Fatal. Isolate both kinds of change.
            with tempfile.TemporaryDirectory() as data, ExitStack() as stack:
                endpoints = start_cluster(stack, Path(data), binary, args.target, 3, ctl, 1000,
                                          2 * 1024**3, ["--strict-reconfig-check=false",
                                                         "--watch-progress-notify-interval=200ms"])
                env = dict(os.environ, TEST_URLS=endpoints)
                command = ["go", "tool", "test2json", "-t", executable, "-test.v=test2json",
                           "-test.run=^" + name + "$", "-test.timeout=" + args.timeout]
                log = args.output / f"{args.target}-{name}.jsonl.gz"
                result = run_go(command, package, log, name, "examples", env)
            result.update(target=args.target, suite="examples", case=name)
            with summary.open("a") as output:
                output.write(json.dumps(result) + "\n")
            print(name, result, flush=True)
            passed &= result["complete" if args.require_complete else "passed"]
    return passed


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("suite", choices=["unit", "integration", "common", "e2e", "robustness",
                                          "proxy-integration", "proxy-e2e", "go-bench", "fuzz",
                                          "examples"])
    parser.add_argument("--target", choices=["etcd", "service"], default="etcd")
    parser.add_argument("--source", type=Path, default=ROOT / "reference/etcd")
    parser.add_argument("--binary", type=Path, default=ROOT / "target/release/slatedb-etcd")
    parser.add_argument("--run", help="Go test name or subtest expression")
    parser.add_argument("--timeout", default="45m", help="Time limit per Go package")
    parser.add_argument("--benchtime", default="1x")
    parser.add_argument("--fuzztime", default="10s")
    parser.add_argument("--output", type=Path, default=ROOT / "tmp/upstream-results")
    parser.add_argument("--last-release", type=Path)
    parser.add_argument("--lazyfs", type=Path)
    parser.add_argument("--require-complete", action="store_true", help="Fail on upstream skips")
    parser.add_argument("--shards", type=int, default=1)
    parser.add_argument("--shard-index", type=int, default=0)
    args = parser.parse_args()
    process_suite = args.suite in ("common", "e2e", "robustness", "proxy-e2e")
    if args.shard_index < 0 or args.shard_index >= args.shards:
        parser.error("Shard index must be at least zero and less than the shard count")
    if args.shards != 1 and (not process_suite or args.run):
        parser.error("Shards apply to a complete process suite")
    if args.target == "service" and not process_suite and args.suite != "examples":
        parser.error("This suite links the Go implementation. Use --target etcd.")
    if args.run and args.suite in ("go-bench", "fuzz"):
        parser.error("--run selects tests, not fuzz targets or benchmarks")
    source = checked_source(args.source)
    args.output.mkdir(parents=True, exist_ok=True)
    if args.suite == "examples":
        raise SystemExit(not run_examples(source, args))
    summary = args.output / f"{args.target}-{args.suite}-{args.shard_index}-summary.jsonl"
    summary.write_text("")
    passed = True
    with tempfile.TemporaryDirectory() as temp:
        directory = Path(temp).resolve()
        go_flags = ["-count=1", "-json", "-p=1", "-timeout=" + args.timeout]
        binary_flags = []
        if process_suite:
            binaries = directory / "bin"
            for name in ("etcdctl", "etcdutl"):
                build_tool(source, binaries, name)
            if args.target == "service":
                (binaries / "slatedb-etcd").symlink_to(args.binary.resolve(strict=True))
                (binaries / "etcd").symlink_to(ROOT / "tests/upstream/service-process.py")
                go_flags.append("-overlay=" + str(service_overlay(source, directory)))
            elif (source / "bin/etcd").is_file():
                (binaries / "etcd").symlink_to(source / "bin/etcd")
            else:
                build_tool(source, binaries, "etcd")
            for name, option in (("etcd-last-release", args.last_release), ("lazyfs", args.lazyfs)):
                path = option or source / "bin" / name
                if option or path.is_file():
                    (binaries / name).symlink_to(path.resolve(strict=True))
            binary_flags = ["-args", "-bin-dir", str(binaries)]
        if args.run:
            go_flags += ["-run", args.run]
        for index, (module, flags) in enumerate(go_cases(source, args.suite, args)):
            if process_suite and not args.run:
                runs = process_runs(source, module, flags, go_flags, binary_flags, directory, args)
            else:
                command = ["go", "test", *go_flags, *flags, *binary_flags]
                runs = [(str(index), command, source / module)]
            for label, command, cwd in runs:
                name = f"{args.target}-{args.suite}-{label}"
                log_path = args.output / (name + ".jsonl.gz")
                result = run_go(command, cwd, log_path, args.run, args.suite)
                if args.suite == "go-bench":
                    tests = json.loads(INVENTORY.read_text())["tests"]
                    expected = {t["name"] for t in tests if t["name"].startswith("Benchmark")
                                and str(Path(t["file"]).parent) == module}
                    measured = {name.split("/")[0] for name in result["samples"]}
                    result["missing_benchmarks"] = sorted(expected - measured)
                    if result["missing_benchmarks"]:
                        result["passed"] = result["complete"] = False
                result.update(target=args.target, suite=args.suite, case=label, module=module)
                with summary.open("a") as log:
                    log.write(json.dumps(result) + "\n")
                passed &= result["complete" if args.require_complete else "passed"]
                print(name, result, flush=True)
    raise SystemExit(not passed)


if __name__ == "__main__":
    main()
