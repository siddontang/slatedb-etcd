#!/usr/bin/env python3
"""Compare three service processes with three pinned etcd processes through the same gRPC driver."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import socket
import subprocess
import tempfile
import threading
import time

from reporting import write_report

ROOT = Path(__file__).resolve().parents[1]


def address():
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return "127.0.0.1:" + str(listener.getsockname()[1])


def cpu_seconds(processes):
    output = subprocess.run(["ps", "-o", "time=", "-p",
        ",".join(str(p.pid) for p in processes)], capture_output=True, text=True).stdout
    total = 0.0
    for value in output.split():
        days, clock = value.split("-", 1) if "-" in value else ("0", value)
        seconds = 0.0
        for part in clock.split(":"):
            seconds = seconds * 60 + float(part)
        total += int(days) * 86400 + seconds
    return total


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--requests", type=int, default=10000)
    parser.add_argument("--concurrency", type=int, nargs="+", default=[1, 8, 32])
    parser.add_argument("--read-percent", type=int, nargs="+", default=[0, 50, 100])
    parser.add_argument("--keys", type=int, default=1000)
    parser.add_argument("--value-size", type=int, default=256)
    parser.add_argument("--output", type=Path, default=ROOT / "results/benchmark-etcd.json")
    parser.add_argument("--binary", type=Path, default=ROOT / "target/release/slatedb-etcd")
    parser.add_argument("--s3-bucket")
    parser.add_argument("--s3-endpoint")
    parser.add_argument("--s3-allow-http", action="store_true")
    parser.add_argument("--timeout-seconds", type=int, default=600)
    args = parser.parse_args()
    if min(args.requests, args.keys, args.value_size, args.timeout_seconds, *args.concurrency) < 1:
        parser.error("Counts, sizes, and timeouts must be positive")
    if any(value < 0 or value > 100 for value in args.read_percent):
        parser.error("Read percentages must be between zero and 100")
    tools = ROOT / "tmp/benchmark-tools"
    tools.mkdir(parents=True, exist_ok=True)
    for binary, package in [("etcd", "reference-etcd"), ("benchmark", "benchmark")]:
        subprocess.run(["go", "build", "-mod=readonly", "-o", str(tools / binary),
                        "./cmd/" + package], cwd=ROOT / "tests/compat", check=True)
    report = {"passed": False, "samples": [], "platform": platform.system(),
              "architecture": platform.machine(), "logical_cpus": os.cpu_count(),
              "deployment": "three processes per backend; shared host and disk; loopback plaintext",
              "reference_etcd": "v3.6.5", "state_backend": "s3" if args.s3_bucket else "local", "raft_snapshot_entries": 1000,
              "quota_bytes": 2 * 1024**3,
              "limits": "closed-loop client; warmup excluded; RSS includes allocator and page caches; "
                        "no cross-AZ or capacity claim", "binaries": {}}
    report["resource_phases"] = "RSS, CPU, and disk samples include preload and measured operations"
    expected_samples = 2 * len(args.concurrency) * len(args.read_percent)
    report["expected_samples"] = expected_samples
    successful = True
    binaries = {"service": args.binary.resolve(), "etcd": tools / "etcd"}
    for kind, binary in binaries.items():
        report["binaries"][kind] = hashlib.sha256(binary.read_bytes()).hexdigest()
    for kind, concurrency, reads in [(kind, concurrency, reads) for kind in binaries
                                     for concurrency in args.concurrency for reads in args.read_percent]:
        binary = binaries[kind]
        with tempfile.TemporaryDirectory(prefix="slatedb-bench-") as directory:
            root = Path(directory)
            clients, peers = [address() for _ in range(3)], [address() for _ in range(3)]
            processes, logs = [], []
            initial = ",".join(f"node{i+1}=http://{peer}" for i, peer in enumerate(peers))
            try:
                for i in range(3):
                    data = root / f"node{i+1}"
                    if kind == "etcd":
                        command = [str(binary), "--name", f"node{i+1}", "--data-dir", str(data),
                                   "--listen-client-urls", "http://"+clients[i],
                                   "--advertise-client-urls", "http://"+clients[i],
                                   "--listen-peer-urls", "http://"+peers[i],
                                   "--initial-advertise-peer-urls", "http://"+peers[i],
                                   "--initial-cluster", initial, "--snapshot-count", "1000",
                                   "--quota-backend-bytes", str(2*1024**3), "--log-level", "error"]
                    else:
                        command = [str(binary), "--id", str(i+1), "--dir", str(data),
                                   "--listen", clients[i], "--cluster-id", root.name, "--snapshot-every", "1000",
                                   "--quota-backend-bytes", str(2*1024**3)]
                        if args.s3_bucket:
                            command += ["--s3-bucket", args.s3_bucket]
                        if args.s3_endpoint:
                            command += ["--s3-endpoint", args.s3_endpoint]
                        if args.s3_allow_http:
                            command += ["--s3-allow-http"]
                        for j, endpoint in enumerate(clients):
                            command += ["--peer", f"{j+1}={endpoint}"]
                    log = open(root / f"node{i+1}.log", "w")
                    logs.append(log)
                    processes.append(subprocess.Popen(command, stdout=log, stderr=log))
                time.sleep(3)
                stop, memory = threading.Event(), []
                def sample_memory():
                    while not stop.is_set():
                        result = subprocess.run(["ps", "-o", "rss=", "-p",
                                                 ",".join(str(p.pid) for p in processes)],
                                                capture_output=True, text=True)
                        if result.returncode == 0:
                            memory.append(sum(int(n) for n in result.stdout.split())*1024)
                        stop.wait(0.1)
                sampler = threading.Thread(target=sample_memory)
                sampler.start()
                cpu_before = cpu_seconds(processes)
                result_path = root / "sample.json"
                try:
                    result = subprocess.run([str(tools / "benchmark"), "--endpoints",
                        ",".join(clients), "--requests", str(args.requests),
                        "--keys", str(args.keys), "--value-size", str(args.value_size),
                        "--concurrency", str(concurrency), "--read-percent", str(reads),
                        "--output", str(result_path)], capture_output=True, text=True,
                        timeout=args.timeout_seconds)
                except subprocess.TimeoutExpired:
                    result = subprocess.CompletedProcess([], 1, "", "benchmark deadline exceeded")
                finally:
                    stop.set()
                    sampler.join()
                sample = json.loads(result_path.read_text()) if result_path.exists() else {
                    "passed": False, "error": result.stderr[-2000:]}
                sample.update(backend=kind, process_cpu_seconds=cpu_seconds(processes)-cpu_before,
                              peak_process_rss_bytes=max(memory, default=0),
                              allocated_data_bytes=sum(p.stat().st_blocks*512
                                  for p in root.glob("node*/**/*") if p.is_file()))
                report["samples"].append(sample)
                successful &= result.returncode == 0 and sample["passed"]
                report["passed"] = successful and len(report["samples"]) == expected_samples
                write_report(args.output, report)
                print(kind, concurrency, reads, sample.get("successful_ops_per_second"), flush=True)
            finally:
                for process in processes:
                    process.kill()
                    process.wait()
                for log in logs:
                    log.close()
    write_report(args.output, report)
    raise SystemExit(0 if report["passed"] else 1)


if __name__ == "__main__":
    main()
