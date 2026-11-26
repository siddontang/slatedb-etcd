#!/usr/bin/env python3
"""Run process-crash checks or a closed-loop, local three-process benchmark."""

import argparse
import concurrent.futures
import http.client
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import re
import socket
import statistics
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from urllib.parse import quote

from reporting import write_report


class ApiFailure(Exception):
    def __init__(self, status, data):
        self.status, self.data = status, data
        super().__init__(f"HTTP {status}: {data}")


def request(port, path, data=None, timeout=12, connection=None):
    owned = connection is None
    conn = connection or http.client.HTTPConnection("127.0.0.1", port, timeout=timeout)
    try:
        body = None if data is None else json.dumps(data).encode()
        conn.request("GET" if body is None else "POST", path, body,
                     {"Content-Type": "application/json"})
        response = conn.getresponse()
        raw = response.read()
        try:
            value = json.loads(raw)
        except ValueError:
            value = raw.decode(errors="replace")
        if response.status >= 300:
            raise ApiFailure(response.status, value)
        return value
    finally:
        if owned:
            conn.close()


def eventually(check, description, timeout=30):
    deadline = time.monotonic() + timeout
    last_error = None
    while time.monotonic() < deadline:
        try:
            value = check()
            if value:
                return value
        except (OSError, ApiFailure, AssertionError, http.client.HTTPException) as error:
            last_error = error
        time.sleep(0.05)
    raise AssertionError(f"Timed out: {description}; last error: {last_error}")


class Cluster:
    def __init__(self, binary, root, snapshot_every, extra=()):
        self.binary, self.root, self.snapshot_every = binary, root, snapshot_every
        self.extra = list(extra)
        sockets = [socket.socket() for _ in range(3)]
        for sock in sockets:
            sock.bind(("127.0.0.1", 0))
        self.ports = {i + 1: sock.getsockname()[1] for i, sock in enumerate(sockets)}
        for sock in sockets:
            sock.close()
        self.processes, self.logs = {}, {}
        self.cluster_id = f"poc-{root.name}"

    def start(self, node):
        assert node not in self.processes
        log = (self.root / f"node-{node}.log").open("ab")
        command = [str(self.binary), "--id", str(node), "--listen",
                   f"127.0.0.1:{self.ports[node]}", "--dir", str(self.root / f"node-{node}"),
                   "--cluster-id", self.cluster_id, "--snapshot-every", str(self.snapshot_every)]
        command.extend(self.extra)
        for peer, port in self.ports.items():
            command.extend(["--peer", f"{peer}=127.0.0.1:{port}"])
        env = dict(os.environ, RUST_LOG="slatedb_etcd=info,slatedb=warn", RUST_BACKTRACE="1")
        self.processes[node] = subprocess.Popen(command, stdout=log, stderr=log, env=env)
        self.logs[node] = log

    def stop(self, node):
        process = self.processes.pop(node)
        process.kill()
        process.wait(timeout=10)
        self.logs.pop(node).close()

    def close(self):
        for node in list(self.processes):
            self.stop(node)

    def status(self, node):
        process = self.processes[node]
        assert process.poll() is None, f"Node {node} exited: {(self.root / f'node-{node}.log').read_text()[-6000:]}"
        return request(self.ports[node], "/status", timeout=3)

    def leader(self, nodes=None):
        nodes = list(nodes or self.processes)

        def find():
            statuses = [self.status(node) for node in nodes]
            leaders = [s for s in statuses if s["role"] == "Leader"]
            if len(leaders) == 1 and all(s["leader_id"] == leaders[0]["id"] for s in statuses):
                return leaders[0]["id"]
            return None

        return eventually(find, f"one leader among {nodes}")

    def submit(self, node, operations):
        return request(self.ports[node], "/kv", {"operations": operations})

    def put(self, node, key, value):
        return self.submit(node, [{"op": "put", "key": key, "value": value}])

    def get(self, node, key):
        return self.submit(node, [{"op": "get", "key": key}])["values"][0]

    def local(self, node, key):
        return request(self.ports[node], "/debug/value?key=" + quote(key, safe=""))["value"]

    def converged(self, key, value, nodes=None):
        nodes = list(nodes or self.processes)
        eventually(lambda: all(self.local(node, key) == value for node in nodes), f"replicated {key}")

    def checkpoint(self, node):
        return request(self.ports[node], "/checkpoint", {})

    def partition(self, isolated, enabled):
        others = [node for node in self.processes if node != isolated]
        for node in self.processes:
            blocked = (others if node == isolated else [isolated]) if enabled else []
            request(self.ports[node], "/debug/partition", {"blocked": blocked})


def run_tests(cluster, checks):
    def passed(name, **details):
        checks.append({"name": name, "passed": True, **details})
        print(json.dumps(checks[-1]), flush=True)

    for node in cluster.ports:
        cluster.start(node)
    leader = cluster.leader()
    reply = cluster.submit(leader, [
        {"op": "put", "key": "atomic/a", "value": "first"},
        {"op": "get", "key": "atomic/a"},
        {"op": "put", "key": "atomic/b", "value": "second"},
    ])
    assert reply["values"] == [None, "first", None]
    cluster.converged("atomic/a", "first")
    cluster.converged("atomic/b", "second")
    passed("election_and_atomic_write", leader=leader, index=reply["index"])

    # Kill the leader immediately after an acknowledged write, without a SlateDB flush.
    cluster.put(leader, "ack-before-crash", "must-survive")
    started = time.perf_counter()
    cluster.stop(leader)
    new_leader = cluster.leader()
    assert cluster.get(new_leader, "ack-before-crash") == "must-survive"
    cluster.put(new_leader, "after-failover", "available")
    failover_ms = (time.perf_counter() - started) * 1000
    cluster.start(leader)
    cluster.converged("after-failover", "available")
    passed("leader_sigkill_and_follower_rejoin", old_leader=leader, new_leader=new_leader,
           failover_to_success_ms=failover_ms)

    leader = cluster.leader()
    cluster.put(leader, "all-crash", "durable-majority")
    for node in list(cluster.processes):
        cluster.stop(node)
    for node in cluster.ports:
        cluster.start(node)
    leader = cluster.leader()
    assert cluster.get(leader, "all-crash") == "durable-majority"
    cluster.converged("atomic/b", "second")
    passed("all_processes_sigkill_and_wal_replay",
           recovered_from={node: cluster.status(node)["recovered_from_index"] for node in cluster.ports})

    # Keep one follower offline while the other two replace many log files with a small snapshot.
    lagging = next(node for node in cluster.ports if node != leader)
    cluster.stop(lagging)
    for index in range(512):
        cluster.put(leader, f"gc/{index % 16}", f"{index}:" + "x" * 4096)
    cluster.put(leader, "snapshot-marker", "caught-up")
    before = cluster.status(leader)
    before["log"] = request(cluster.ports[leader], "/debug/log-stats")
    after = cluster.checkpoint(leader)
    after["log"] = request(cluster.ports[leader], "/debug/log-stats")
    assert after["snapshot_index"] > 0
    assert after["first_log_index"] == after["snapshot_index"] + 1
    removed = set(before["log"]["files"]) - set(after["log"]["files"])
    assert removed, f"No physical Raft files reclaimed: {before['log']} -> {after['log']}"
    assert after["log"]["bytes"] < before["log"]["bytes"]
    for node in cluster.processes:
        cluster.checkpoint(node)
    cluster.start(lagging)
    cluster.converged("snapshot-marker", "caught-up")
    eventually(lambda: cluster.status(lagging)["installed_snapshots"] > 0, "snapshot installation")
    assert cluster.local(lagging, "gc/15") == "511:" + "x" * 4096
    passed("log_compaction_physical_gc_and_snapshot_catchup", lagging_node=lagging,
           snapshot_index=after["snapshot_index"], reclaimed_files=len(removed),
           log_bytes_before=before["log"]["bytes"], log_bytes_after=after["log"]["bytes"])

    # Recover again after the original log prefix no longer exists.
    for node in cluster.processes:
        cluster.checkpoint(node)
    for node in list(cluster.processes):
        cluster.stop(node)
    for node in cluster.ports:
        cluster.start(node)
    leader = cluster.leader()
    assert cluster.get(leader, "snapshot-marker") == "caught-up"
    cluster.converged("gc/15", "511:" + "x" * 4096)
    passed("restart_after_log_gc")

    # An isolated leader can append locally, but it must not acknowledge the write.
    isolated = leader
    cluster.partition(isolated, True)
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        attempt = pool.submit(cluster.put, isolated, "minority-only", "must-not-commit")
        majority = [node for node in cluster.ports if node != isolated]
        leader = cluster.leader(majority)
        cluster.put(leader, "majority-only", "committed")
        try:
            attempt.result(timeout=15)
            raise AssertionError("Minority write returned success")
        except ApiFailure as error:
            assert error.status in (409, 503)
    cluster.partition(isolated, False)
    cluster.converged("majority-only", "committed")
    assert cluster.get(cluster.leader(), "minority-only") is None
    cluster.converged("minority-only", None)
    passed("partition_rejects_minority_and_replaces_uncommitted_suffix")

    leader = cluster.leader()
    cluster.submit(leader, [{"op": "delete", "key": "atomic/a"}])
    assert cluster.get(leader, "atomic/a") is None
    cluster.converged("atomic/a", None)
    passed("replicated_delete_and_linearizable_read")

    # Hold snapshot creation longer than the election timeout while writes and heartbeats continue.
    leader = cluster.leader()
    term = cluster.status(leader)["term"]
    request(cluster.ports[leader], "/debug/snapshot-delay", {"delay_ms": 2500})
    cluster.put(leader, "before-delayed-snapshot", "pinned")
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        checkpoint = pool.submit(cluster.checkpoint, leader)
        eventually(lambda: cluster.status(leader)["checkpoint_running"], "background snapshot started")
        started = time.monotonic()
        writes = 0
        while time.monotonic() - started < 2:
            cluster.put(leader, "during-delayed-snapshot", str(writes))
            writes += 1
            time.sleep(0.01)
        checkpoint.result(timeout=10)
    request(cluster.ports[leader], "/debug/snapshot-delay", {"delay_ms": 0})
    assert cluster.leader() == leader and cluster.status(leader)["term"] == term
    passed("slow_background_snapshot_keeps_heartbeats_and_writes", writes_during_snapshot=writes)
    return {"checks": checks, "final_status": [cluster.status(node) for node in cluster.ports]}


def percentile(values, quantile):
    return sorted(values)[max(0, math.ceil(len(values) * quantile) - 1)]


def benchmark(cluster, counts, concurrency_levels, value_size, reports):
    for node in cluster.ports:
        cluster.start(node)
    leader = cluster.leader()
    payload = "x" * value_size
    for i in range(100):
        cluster.put(leader, f"warmup/{i}", payload)
    for concurrency in concurrency_levels:
        statuses_before = [cluster.status(node) for node in cluster.ports]

        def worker(worker_id):
            conn = http.client.HTTPConnection("127.0.0.1", cluster.ports[leader], timeout=12)
            latencies, errors = [], []
            try:
                for i in range(worker_id, counts, concurrency):
                    body = {"operations": [{"op": "put", "key": f"bench/{concurrency}/{i}", "value": payload}]}
                    start = time.perf_counter_ns()
                    try:
                        request(cluster.ports[leader], "/kv", body, connection=conn)
                        latencies.append((time.perf_counter_ns() - start) / 1_000_000)
                    except Exception as error:
                        errors.append(str(error))
                        conn.close()
            finally:
                conn.close()
            return latencies, errors

        start = time.perf_counter()
        with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as pool:
            results = list(pool.map(worker, range(concurrency)))
        elapsed = time.perf_counter() - start
        latencies = [latency for values, _ in results for latency in values]
        errors = [error for _, items in results for error in items]
        assert latencies, f"No successful requests: {errors[:3]}"
        report = {
            "concurrency": concurrency, "requests": counts, "successful": len(latencies),
            "failed": len(errors), "errors": errors[:10], "value_bytes": value_size,
            "elapsed_seconds": elapsed, "successful_requests_per_second": len(latencies) / elapsed,
            "latency_ms": {"mean": statistics.mean(latencies), "p50": percentile(latencies, .5),
                           "p95": percentile(latencies, .95), "p99": percentile(latencies, .99),
                           "max": max(latencies)},
            "snapshot_every": cluster.snapshot_every,
            "sync_writes_delta": {
                node: cluster.status(node)["sync_writes"] - statuses_before[node - 1]["sync_writes"]
                for node in cluster.ports
            },
        }
        print(json.dumps(report), flush=True)
        reports.append(report)
        assert not errors, f"Benchmark had {len(errors)} failures"
        cluster.converged(f"bench/{concurrency}/{counts - 1}", payload)
    return {"benchmarks": reports}


def log_summary(root):
    result = {}
    for path in sorted(root.glob("node-*.log")):
        content = path.read_text(errors="replace")
        snapshots = re.findall(r"Snapshot saved and log compacted node=\d+ index=(\d+)", content)
        changes = re.findall(
            r"Raft state changed node=(\d+) role=(\w+) term=(\d+) leader=(\d+)", content)
        result[path.stem] = {
            "completed_snapshots": len(snapshots),
            "last_snapshot_index": int(snapshots[-1]) if snapshots else None,
            "raft_state_changes": [
                {"role": role, "term": int(term), "leader_id": int(leader)}
                for _, role, term, leader in changes
            ],
        }
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=["test", "bench"])
    parser.add_argument("--binary", type=Path, default=Path(__file__).resolve().parents[1] / "target/release/slatedb-etcd")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--requests", type=int, default=2000)
    parser.add_argument("--concurrency", type=int, nargs="+", default=[1, 16, 64])
    parser.add_argument("--value-size", type=int, default=256)
    parser.add_argument("--snapshot-every", type=int, default=0)
    args = parser.parse_args()
    assert args.requests > 0 and all(c > 0 for c in args.concurrency)
    root = Path(tempfile.mkdtemp(prefix="slatedb-raft-poc-"))
    print(f"Run directory: {root}", flush=True)
    # In tests, small log files let the log truncation check reclaim whole files.
    # Only tests use the fault routes; bench mode must also run binaries that predate the flag.
    extra = (["--log-file-bytes", str(256 * 1024), "--enable-fault-controls"]
             if args.mode == "test" else [])
    cluster = Cluster(args.binary.resolve(), root, args.snapshot_every, extra)
    report = {"mode": args.mode, "host": platform.platform(), "machine": platform.machine(),
              "created_at": datetime.now(timezone.utc).isoformat(), "logical_cpus": os.cpu_count(),
              "binary_sha256": hashlib.sha256(cluster.binary.read_bytes()).hexdigest(),
              "python": sys.version, "run_directory": str(root), "binary": str(cluster.binary),
              "deployment": "three processes on one host and disk; loopback HTTP; local object store",
              "durability": "raft-engine synchronous writes; process crashes tested, power loss not tested"}
    # Retain completed checks and measurements if a later assertion fails.
    report["checks" if args.mode == "test" else "benchmarks"] = []
    try:
        report.update(run_tests(cluster, report["checks"]) if args.mode == "test" else
                      benchmark(cluster, args.requests, args.concurrency, args.value_size,
                                report["benchmarks"]))
        report["passed"] = True
    except Exception as error:
        report.update(passed=False, error=str(error))
        for node in cluster.ports:
            log = root / f"node-{node}.log"
            if log.exists():
                print(f"Node {node} log tail:\n{log.read_text()[-4000:]}", file=sys.stderr)
        raise
    finally:
        cluster.close()
        report["node_log_summary"] = log_summary(root)
        destination = args.output or root / "report.json"
        destination.parent.mkdir(parents=True, exist_ok=True)
        write_report(destination, report)
        print(f"Report: {destination}", flush=True)


if __name__ == "__main__":
    main()
