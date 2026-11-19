#!/usr/bin/env python3
"""Compare services with one gRPC workload and separate resource phases."""

import argparse
from contextlib import nullcontext
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import platform
import random
import socket
import subprocess
import tempfile
import threading
import time
import urllib.request

from reporting import write_report
from rustfs_fixture import local_rustfs

ROOT = Path(__file__).resolve().parents[1]


def addresses(count):
    listeners = []
    try:
        for _ in range(count):
            listener = socket.socket()
            listeners.append(listener)
            listener.bind(("127.0.0.1", 0))
        return ["127.0.0.1:" + str(listener.getsockname()[1]) for listener in listeners]
    finally:
        for listener in listeners:
            listener.close()


def parse_cpu(value):
    days, clock = value.split("-", 1) if "-" in value else ("0", value)
    seconds = 0.0
    for part in clock.split(":"):
        seconds = seconds * 60 + float(part)
    return int(days) * 86400 + seconds


def resources(processes):
    if platform.system() == "Linux":
        ticks = os.sysconf("SC_CLK_TCK")
        page = os.sysconf("SC_PAGE_SIZE")
        rows = []
        for process in processes:
            # The command name can contain spaces and parentheses.
            fields = Path(f"/proc/{process.pid}/stat").read_text().rsplit(")", 1)[1].split()
            rows.append(((int(fields[11]) + int(fields[12])) / ticks, int(fields[21]) * page))
        return rows
    result = subprocess.run(["ps", "-o", "pid=,time=,rss=", "-p",
                             ",".join(str(p.pid) for p in processes)],
                            capture_output=True, text=True, check=True)
    rows = {int(pid): (parse_cpu(cpu), int(rss)*1024)
            for pid, cpu, rss in (line.split() for line in result.stdout.splitlines())}
    if len(rows) != len(processes):
        raise RuntimeError("a benchmark process exited during resource sampling")
    return [rows[p.pid] for p in processes]


def linux_io(processes):
    rows = []
    for process in processes:
        lines = Path(f"/proc/{process.pid}/io").read_text().splitlines()
        counters = dict(line.split(": ") for line in lines)
        rows.append((int(counters["read_bytes"]), int(counters["write_bytes"])))
    return rows


def wait_file(path, processes, deadline):
    while not path.exists():
        if any(p.poll() is not None for p in processes):
            raise RuntimeError("a benchmark process exited before " + path.name)
        if time.monotonic() >= deadline:
            raise TimeoutError("benchmark deadline exceeded before " + path.name)
        time.sleep(0.005)


def measured_resources(before, after, memory, members, seconds, successes):
    if len(before) != members + 1 or len(after) != members + 1:
        raise ValueError("Resource samples must contain all servers and one client")
    cpu = [max(0, end[0] - start[0]) for start, end in zip(before, after)]
    server_cpu = sum(cpu[:members])
    return dict(member_resources=[
                    dict(member=i + 1, cpu_seconds=cpu[i], peak_rss_bytes=max(row[i] for row in memory))
                    for i in range(members)],
                process_cpu_seconds=server_cpu, client_cpu_seconds=cpu[members],
                average_server_cpu_cores=server_cpu / seconds,
                server_cpu_microseconds_per_success=server_cpu * 1e6 / max(1, successes),
                peak_process_rss_bytes=max(sum(row[:members]) for row in memory),
                peak_client_rss_bytes=max(row[members] for row in memory))


def state_counters(peers):
    counters = []
    for address in peers:
        with urllib.request.urlopen("http://" + address + "/status", timeout=10) as response:
            status = json.load(response)
        member = {key: status.get(key) for key in ("applied_index", "state_write_batches")}
        if "write_profile" in status:
            member["write_profile"] = status["write_profile"]
        counters.append(member)
    return counters


def run_sample(args, tools, binary, kind, concurrency, reads, repeat, observer=None):
    with tempfile.TemporaryDirectory(prefix="slatedb-bench-") as directory:
        root = Path(directory)
        members = getattr(args, "members", 3)
        ports = addresses(members * 2)
        clients, peers = ports[:members], ports[members:]
        processes, logs = [], []
        driver = None
        stop = threading.Event()
        sampler = None
        memory, resource_errors = [], []
        initial = ",".join(f"node{i+1}=http://{peer}" for i, peer in enumerate(peers))
        sample = {"passed": False, "backend": kind, "members": members, "concurrency": concurrency,
                  "read_percent": reads, "repeat": repeat}
        try:
            for i in range(members):
                data = root / f"node{i+1}"
                if kind == "etcd":
                    command = [str(binary), "--name", f"node{i+1}", "--data-dir", str(data),
                               "--listen-client-urls", "http://"+clients[i],
                               "--advertise-client-urls", "http://"+clients[i],
                               "--listen-peer-urls", "http://"+peers[i],
                               "--initial-advertise-peer-urls", "http://"+peers[i],
                               "--initial-cluster", initial, "--snapshot-count", str(args.snapshot_entries),
                               "--quota-backend-bytes", str(args.quota_bytes), "--log-level", "error"]
                else:
                    command = [str(binary), "--id", str(i+1), "--dir", str(data),
                               "--listen", clients[i], "--peer-listen", peers[i],
                               "--cluster-id", root.name, "--snapshot-every", str(args.snapshot_entries),
                               "--quota-backend-bytes", str(args.quota_bytes)]
                    workers = getattr(args, "service_runtime_workers", None)
                    if kind == "service" and workers is not None:
                        command += ["--runtime-workers", str(workers)]
                    if args.s3_bucket:
                        command += ["--s3-bucket", args.s3_bucket]
                    if args.s3_endpoint:
                        command += ["--s3-endpoint", args.s3_endpoint]
                    if args.s3_allow_http:
                        command += ["--s3-allow-http"]
                    for j in range(members):
                        command += ["--peer", f"{j+1}={peers[j]}", "--client-peer", f"{j+1}={clients[j]}"]
                log = open(root / f"node{i+1}.log", "w")
                logs.append(log)
                env = getattr(args, "service_env", None) if kind != "etcd" else None
                processes.append(subprocess.Popen(command, stdout=log, stderr=log, env=env))
            log = open(root / "client.log", "w")
            logs.append(log)
            result_path = root / "sample.json"
            driver = subprocess.Popen([str(tools / "benchmark"), "--endpoints", ",".join(clients),
                "--requests", str(args.read_requests if reads == 100 and args.read_requests else args.requests), "--keys", str(args.keys), "--value-size", str(args.value_size),
                "--concurrency", str(concurrency), "--read-percent", str(reads), "--warmup", str(args.warmup),
                "--compact-every", str(getattr(args, "compact_every", 0)),
                "--retain-revisions", str(getattr(args, "retain_revisions", 1000)),
                "--value-pattern", getattr(args, "value_pattern", "repeat"),
                "--rate", str(getattr(args, "rate", 0)),
                "--seed", str(args.seed+repeat), "--control-dir", str(root), "--output", str(result_path)],
                stdout=log, stderr=log)
            all_processes = processes + [driver]
            deadline = time.monotonic() + args.timeout_seconds
            wait_file(root / "ready", all_processes, deadline)
            if kind != "etcd":
                sample["state_counters_before"] = state_counters(peers)
            # Give both backends one idle second after preload and warmup.
            time.sleep(1)
            with observer(processes) if observer else nullcontext():
                linux = platform.system() == "Linux"
                if linux:
                    io_before = linux_io(processes)
                before = resources(all_processes)
                sample["startup_preload_warmup_server_cpu_seconds"] = sum(row[0] for row in before[:members])
                sample["server_rss_after_warmup_bytes"] = sum(row[1] for row in before[:members])
                memory.append([row[1] for row in before])

                def sample_memory():
                    while not stop.wait(0.1):
                        try:
                            memory.append([row[1] for row in resources(all_processes)])
                        except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
                            resource_errors.append(str(error))
                            return

                sampler = threading.Thread(target=sample_memory)
                sampler.start()
                (root / "start").touch()
                wait_file(root / "done", all_processes, deadline)
                after = resources(all_processes)
                if linux:
                    io_after = linux_io(processes)
                    sample["member_storage_io_bytes"] = [
                        {"member": i + 1, "read": end[0] - start[0], "write": end[1] - start[1]}
                        for i, (start, end) in enumerate(zip(io_before, io_after))]
                memory.append([row[1] for row in after])
                stop.set()
                sampler.join()
                (root / "finish").touch()
                code = driver.wait(timeout=10)
            if kind != "etcd":
                sample["state_counters_after"] = state_counters(peers)
            sample.update(json.loads(result_path.read_text()))
            sample["passed"] &= code == 0 and not resource_errors and all(p.poll() is None for p in processes)
            sample.update(measured_resources(before, after, memory, members,
                                             sample["seconds"], sample["successes"]))
            sample.update(resource_samples=len(memory), resource_errors=resource_errors,
                          allocated_data_bytes=sum(p.stat().st_blocks*512
                              for p in root.glob("node*/**/*") if p.is_file()))
        except (RuntimeError, OSError, ValueError, subprocess.SubprocessError) as error:
            sample.update(passed=False, error=str(error))
        finally:
            stop.set()
            if sampler:
                sampler.join()
            for process in processes + ([driver] if driver else []):
                if process.poll() is None:
                    process.kill()
                process.wait()
            for log in logs:
                log.close()
            if not sample["passed"]:
                sample["logs"] = {p.name: p.read_text(errors="replace")[-4000:] for p in root.glob("*.log")}
        return sample


def workload_cases(kinds, concurrency, reads, baseline_concurrency=None):
    return [(kind, count, percent) for kind in kinds for count in concurrency for percent in reads
            if kind != "baseline" or baseline_concurrency is None or count in baseline_concurrency]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--members", type=int, choices=[1, 3], default=3)
    parser.add_argument("--service-runtime-workers", type=int,
                        help="Override the current service's async worker count")
    parser.add_argument("--requests", type=int, default=10000)
    parser.add_argument("--read-requests", type=int, help="Separate request count for read-only samples")
    parser.add_argument("--rate", type=int, default=0, help="Target operations per second; zero runs at capacity")
    parser.add_argument("--concurrency", type=int, nargs="+", default=[1, 8, 32])
    parser.add_argument("--read-percent", type=int, nargs="+", default=[0, 50, 100])
    parser.add_argument("--keys", type=int, default=1000)
    parser.add_argument("--compact-every", type=int, default=0)
    parser.add_argument("--retain-revisions", type=int, default=1000)
    parser.add_argument("--value-pattern", choices=("repeat", "binary", "metadata"), default="repeat")
    parser.add_argument("--value-size", type=int, default=256)
    parser.add_argument("--warmup", type=int, default=100)
    parser.add_argument("--repeat", type=int, default=1)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--snapshot-entries", type=int, default=1000)
    parser.add_argument("--quota-bytes", type=int, default=2*1024**3)
    parser.add_argument("--output", type=Path, default=ROOT / "results/benchmark-etcd.json")
    parser.add_argument("--binary", type=Path, default=ROOT / "target/release/slatedb-etcd")
    parser.add_argument("--baseline-binary", type=Path)
    parser.add_argument("--baseline-concurrency", type=int, nargs="+",
                        help="Restrict baseline cases to these selected concurrency levels")
    parser.add_argument("--backends", nargs="+", choices=["baseline", "service", "etcd"])
    parser.add_argument("--rustfs", action="store_true", help="Start a local RustFS test server")
    parser.add_argument("--s3-bucket")
    parser.add_argument("--s3-endpoint")
    parser.add_argument("--s3-allow-http", action="store_true")
    parser.add_argument("--timeout-seconds", type=int, default=600)
    args = parser.parse_args()
    if min(args.requests, args.keys, args.value_size, args.timeout_seconds, args.repeat,
           args.snapshot_entries, args.quota_bytes, *args.concurrency) < 1 or args.warmup < 0:
        parser.error("Counts, sizes, quotas, and timeouts must be positive; warmup can be zero")
    if args.compact_every < 0 or args.retain_revisions < 0:
        parser.error("Compaction count and retention must be nonnegative")
    if args.value_pattern == "metadata" and args.value_size < 256:
        parser.error("Metadata values require at least 256 bytes")
    if args.rate < 0:
        parser.error("Rate must be nonnegative")
    if args.service_runtime_workers is not None and args.service_runtime_workers < 1:
        parser.error("Service runtime workers must be positive")
    if args.read_requests is not None and args.read_requests < 1:
        parser.error("Read request count must be positive")
    if args.seed < 0 or args.seed+args.repeat > 2**64-1:
        parser.error("Seeds must fit in an unsigned 64-bit integer")
    if any(value < 0 or value > 100 for value in args.read_percent):
        parser.error("Read percentages must be between zero and 100")
    kinds = args.backends or (["baseline", "service", "etcd"] if args.baseline_binary else ["service", "etcd"])
    if "baseline" in kinds and not args.baseline_binary:
        parser.error("The baseline backend requires --baseline-binary")
    if len(set(kinds)) != len(kinds):
        parser.error("Backend names must be distinct")
    if args.baseline_concurrency is not None and (
            "baseline" not in kinds or not set(args.baseline_concurrency) <= set(args.concurrency)):
        parser.error("Baseline concurrency must select measured levels and requires a baseline")
    if args.rustfs and (args.s3_bucket or args.s3_endpoint or args.s3_allow_http):
        parser.error("--rustfs supplies its own bucket, endpoint, and HTTP settings")
    if args.rustfs and kinds == ["etcd"]:
        parser.error("--rustfs requires a service or baseline backend")
    with local_rustfs() if args.rustfs else nullcontext() as fixture:
        store = None
        if fixture:
            args.service_env, store = fixture
            args.s3_bucket = args.service_env["SLATEDB_S3_TEST_BUCKET"]
            args.s3_endpoint = args.service_env["SLATEDB_S3_TEST_ENDPOINT"]
            args.s3_allow_http = True
        benchmark(args, kinds, store)


def benchmark(args, kinds, store):
    cases = workload_cases(kinds, args.concurrency, args.read_percent, args.baseline_concurrency)
    expected = len(cases)*args.repeat
    report = {"schema_version": 2, "passed": False, "complete": False, "samples": [],
              "created_at": datetime.now(timezone.utc).isoformat(), "expected_samples": expected,
              "platform": platform.system(), "architecture": platform.machine(), "logical_cpus": os.cpu_count(),
              "members": args.members, "target_ops_per_second": args.rate,
              "service_runtime_workers": args.service_runtime_workers,
              "baseline_concurrency": args.baseline_concurrency,
              "settle_seconds_after_warmup": 1,
              "deployment": f"{args.members} processes per backend; shared host and disk; loopback plaintext",
              "reference_etcd": "v3.6.5", "state_backend": "s3" if args.s3_bucket else "local",
              "raft_snapshot_entries": args.snapshot_entries, "quota_bytes": args.quota_bytes,
              "order_seed": args.seed, "binaries": {},
              "resource_phases": "CPU deltas and peak RSS cover only the measured phase; retained preload memory remains",
              "limits": "bounded concurrency; warm keys; host page cache is not cleared; RSS is not unique physical memory; "
                        "ps CPU precision and controller barriers affect short samples; no capacity or cross-AZ claim"}
    if store:
        report["object_store"] = store
    report["host_memory_bytes"] = os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES")
    if platform.system() == "Darwin":
        report["cpu_model"] = subprocess.check_output(
            ["sysctl", "-n", "machdep.cpu.brand_string"], text=True).strip()
    else:
        report["cpu_model"] = platform.processor()
    report["kernel"] = platform.release()
    write_report(args.output, report)
    tools = ROOT / "tmp/benchmark-tools"
    tools.mkdir(parents=True, exist_ok=True)
    for binary, package in [("etcd", "reference-etcd"), ("benchmark", "benchmark")]:
        subprocess.run(["go", "build", "-mod=readonly", "-o", str(tools / binary),
                        "./cmd/" + package], cwd=ROOT / "tests/compat", check=True)
    binaries = {"service": args.binary.resolve(), "etcd": tools / "etcd"}
    if args.baseline_binary:
        binaries["baseline"] = args.baseline_binary.resolve()
    for kind in kinds:
        report["binaries"][kind] = hashlib.sha256(binaries[kind].read_bytes()).hexdigest()
    for repeat in range(args.repeat):
        shuffled = cases.copy()
        random.Random(args.seed+repeat).shuffle(shuffled)
        for kind, concurrency, reads in shuffled:
            sample = run_sample(args, tools, binaries[kind], kind, concurrency, reads, repeat)
            report["samples"].append(sample)
            report["complete"] = len(report["samples"]) == expected
            report["passed"] = report["complete"] and all(s["passed"] for s in report["samples"])
            write_report(args.output, report)
            print(kind, repeat, concurrency, reads, sample.get("successful_ops_per_second"), flush=True)
    raise SystemExit(0 if report["passed"] else 1)


if __name__ == "__main__":
    main()
