"""Locate the pinned etcd checkout and build its tools."""

from contextlib import ExitStack
import json
from pathlib import Path
import socket
import subprocess
import time

from rustfs_fixture import shared_args, shared_store, unique_prefix

ROOT = Path(__file__).resolve().parents[1]
INVENTORY = ROOT / "tests/upstream/etcd-inventory.json"


def checked_source(path):
    source = path.resolve()
    revision = json.loads(INVENTORY.read_text())["revision"]
    head = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=source, text=True).strip()
    if head != revision:
        raise ValueError(f"Expected etcd {revision}, found {head}")
    return source


def build_tool(source, output, name):
    packages = {"etcd": ("server", "."), "etcdctl": ("etcdctl", "."),
                "etcdutl": ("etcdutl", "."), "benchmark": (".", "./tools/benchmark")}
    module, package = packages[name]
    output.mkdir(parents=True, exist_ok=True)
    path = output.resolve() / name
    subprocess.run(["go", "build", "-mod=readonly", "-o", path, package],
                   cwd=source / module, check=True)
    return path


def stop(process):
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()


def start_cluster(stack, directory, executable, target, members, ctl, snapshot, quota, extra=(),
                  shared=True):
    with ExitStack() as reservations:
        ports = []
        for _ in range(2 * members):
            sock = reservations.enter_context(socket.socket())
            sock.bind(("127.0.0.1", 0))
            ports.append(f"127.0.0.1:{sock.getsockname()[1]}")
    clients, peers = ports[:members], ports[members:]
    cluster = ",".join(f"m{i}=http://{addr}" for i, addr in enumerate(peers))
    store = []
    if target != "etcd" and members > 1 and shared:
        # Several members run in shared state mode on RustFS. An outer `shared_store` supplies one
        # server for all clusters of a run.
        stack.enter_context(shared_store())
        store = shared_args(unique_prefix("upstream"))
    processes = []
    for i, (client, peer) in enumerate(zip(clients, peers)):
        data = directory / f"data-{i}"
        if target == "etcd":
            argv = ["--name", f"m{i}", "--data-dir", data, "--listen-client-urls", "http://"+client,
                    "--advertise-client-urls", "http://"+client, "--listen-peer-urls", "http://"+peer,
                    "--initial-advertise-peer-urls", "http://"+peer, "--initial-cluster", cluster,
                    "--snapshot-count", str(snapshot), "--log-level", "error"]
        else:
            argv = ["--id", str(i+1), "--dir", data, "--listen", client, "--peer-listen", peer,
                    "--snapshot-every", str(snapshot)]
            for j in range(members):
                argv.extend(["--peer", f"{j+1}={peers[j]}", "--client-peer", f"{j+1}={clients[j]}"])
        argv.extend(["--quota-backend-bytes", str(quota), *store, *extra])
        log = stack.enter_context((directory / f"member-{i}.log").open("w"))
        process = subprocess.Popen([executable, *argv], stdout=log, stderr=log)
        stack.callback(stop, process)
        processes.append(process)
    endpoints = ",".join(clients)
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline and all(p.poll() is None for p in processes):
        check = subprocess.run([ctl, "--endpoints", endpoints, "--command-timeout=1s", "endpoint",
                                "health"], capture_output=True, timeout=10)
        if check.returncode == 0:
            return endpoints
        time.sleep(0.1)
    raise RuntimeError("Cluster did not become healthy")
