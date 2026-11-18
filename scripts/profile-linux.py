#!/usr/bin/env python3
"""Sample Linux server CPU stacks after the benchmark warmup barrier."""

import argparse
from contextlib import contextmanager
import hashlib
import importlib.util
import os
from pathlib import Path
import platform
import re
import select
import signal
import subprocess

from reporting import write_report

spec = importlib.util.spec_from_file_location(
    "benchmark", Path(__file__).with_name("benchmark-etcd.py"))
benchmark = importlib.util.module_from_spec(spec)
spec.loader.exec_module(benchmark)


def control(process, command, writer, reader):
    if process.poll() is not None:
        raise RuntimeError("perf exited before the measurement barrier")
    os.write(writer, (command + "\n").encode())
    if (not select.select([reader], [], [], 10)[0]
            or os.read(reader, 1024).rstrip(b"\0") != b"ack\n"):
        raise RuntimeError("perf did not acknowledge " + command)


@contextmanager
def record(processes, output, frequency):
    control_read, control_write = os.pipe()
    ack_read, ack_write = os.pipe()
    process = None
    try:
        with (output / "perf.log").open("w") as log:
            process = subprocess.Popen([
                "perf", "record", "--event", "cpu-clock", "--freq", str(frequency),
                "--call-graph", "dwarf,16384", "--delay=-1",
                "--control", f"fd:{control_read},{ack_write}",
                "--pid", ",".join(str(p.pid) for p in processes),
                "--output", str(output / "perf.data"),
            ], pass_fds=(control_read, ack_write), stdout=log, stderr=log)
            control(process, "enable", control_write, ack_read)
            try:
                yield
            finally:
                control(process, "disable", control_write, ack_read)
                process.send_signal(signal.SIGINT)
                code = process.wait(timeout=30)
                if code not in (0, -signal.SIGINT):
                    raise RuntimeError("perf recording failed; see perf.log")
    finally:
        if process and process.poll() is None:
            process.kill()
            process.wait()
        for fd in (control_read, control_write, ack_read, ack_write):
            os.close(fd)


def validate_profile(text):
    samples = re.search(r"# Samples: ([0-9.]+[KMG]?)\s+of event 'cpu-clock'", text)
    lost = re.search(r"# Total Lost Samples: ([0-9]+)", text)
    symbols = re.findall(r"^\s+[0-9.]+%\s+\[\.\] ([^\n]+)", text, re.MULTILINE)
    if not samples or float(samples[1].rstrip("KMG")) <= 0:
        raise RuntimeError("perf captured no CPU samples")
    if not any(not symbol.strip().startswith("0x") for symbol in symbols):
        raise RuntimeError("perf captured no resolved user symbols")
    if not lost or int(lost[1]) != 0:
        raise RuntimeError("perf lost CPU samples")
    return {"samples_display": samples[1], "lost_samples": int(lost[1])}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--backend", choices=["service", "etcd"], default="service")
    parser.add_argument("--read-percent", type=int, choices=[0, 100], required=True)
    parser.add_argument("--requests", type=int, default=20000)
    parser.add_argument("--concurrency", type=int, default=8)
    parser.add_argument("--keys", type=int, default=1000)
    parser.add_argument("--value-size", type=int, default=256)
    parser.add_argument("--warmup", type=int, default=200)
    parser.add_argument("--snapshot-entries", type=int, default=1000)
    parser.add_argument("--frequency", type=int, default=99)
    parser.add_argument("--skip-inline", action="store_true",
                        help="Skip slow inline expansion; retain resolved flat stacks")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if platform.system() != "Linux":
        parser.error("CPU stack sampling requires Linux and perf")
    if min(args.requests, args.concurrency, args.keys, args.value_size,
           args.snapshot_entries, args.frequency) < 1 or args.warmup < 0:
        parser.error("Counts must be positive; warmup can be zero")
    args.output.mkdir(parents=True, exist_ok=True)
    report = {"passed": False, "profiled": True, "platform": platform.system(),
              "architecture": platform.machine(), "frequency_hz": args.frequency,
              "event": "cpu-clock", "stack_bytes": 16384,
              "inline_symbols": not args.skip_inline,
              "scope": "three servers; measured phase only; no client samples",
              "limits": "Sampling changes runtime cost. Use separate runs for performance results."}
    report_path = args.output / "report.json"
    write_report(report_path, report)
    tools = benchmark.ROOT / "tmp/benchmark-tools"
    tools.mkdir(parents=True, exist_ok=True)
    subprocess.run(["go", "build", "-mod=readonly", "-o", str(tools / "benchmark"),
                    "./cmd/benchmark"], cwd=benchmark.ROOT / "tests/compat", check=True)
    binary = args.binary.resolve()
    report["binary_sha256"] = hashlib.sha256(binary.read_bytes()).hexdigest()
    report["perf_version"] = subprocess.check_output(["perf", "version"], text=True).strip()
    args.read_requests, args.seed, args.timeout_seconds = None, 42, 600
    args.quota_bytes = 2 * 1024**3
    args.s3_bucket, args.s3_endpoint, args.s3_allow_http = None, None, False
    sample = benchmark.run_sample(
        args, tools, binary, args.backend, args.concurrency, args.read_percent, 0,
        observer=lambda processes: record(processes, args.output, args.frequency))
    report["sample"] = sample
    if sample["passed"]:
        views = [("self.txt", "--no-children", "--no-inline", "none"),
                 ("stacks.txt", "--children", "--no-inline", "graph")]
        if not args.skip_inline:
            views.append(("inline.txt", "--children", "--inline", "none"))
        for name, children, inline, graph in views:
            with (args.output / name).open("w") as output:
                subprocess.run(["perf", "report", "--stdio", inline, children,
                                "--sort", "symbol", "--call-graph", graph,
                                "--percent-limit", "0.5", "--input",
                                str(args.output / "perf.data")],
                               stdout=output, check=True)
        report["sampling"] = validate_profile((args.output / "self.txt").read_text())
        report["passed"] = True
    write_report(report_path, report)
    print("Profile:", report_path)
    raise SystemExit(0 if report["passed"] else 1)


if __name__ == "__main__":
    main()
