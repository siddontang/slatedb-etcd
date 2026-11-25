#!/usr/bin/env python3
"""Compare replicated and shared state mode on three local members with one local RustFS.

The RustFS server is the pinned release of scripts/rustfs_fixture.py. Members reach it through
scripts/s3-counter, a proxy that counts the S3 requests and bytes of each bucket.

Each case starts a new RustFS data directory and a new cluster. The script records the etcd
benchmark result, the S3 requests and bytes, and the peak RSS of each member. The `ops` scenario
measures the time from a leader kill to the first successful write, and the time that a new
learner needs to reach the leader's index. The `policy` check runs members with the S3 policies
of docs/shared-state-guide.md and checks that RustFS denies keys outside them.

The caller holds the machine-wide benchmark lock. See scripts/run-shared-bench.sh.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import threading
import time
import urllib.error
import urllib.request
import xml.etree.ElementTree as ElementTree

import rustfs_fixture

GiB = 1024**3
ROOT_USER, ROOT_PASSWORD = "benchroot", "benchroot-secret"
SERVER_CPUS = ["0,4", "1,5", "2,6", "3,7"]
CLIENT_CPUS = "3,7"
CLUSTER = "bench"
K, V = "32", "256"

CASES = {
    "put-c16": ["--conns", "16", "--clients", "16", "put", "--key-size", K, "--val-size", V,
                "--key-space-size", "100000", "--total", "30000"],
    "put-c256": ["--conns", "100", "--clients", "256", "put", "--key-size", K, "--val-size", V,
                 "--key-space-size", "100000", "--total", "100000"],
    "put-v4096": ["--conns", "100", "--clients", "256", "put", "--key-size", K, "--val-size",
                  "4096", "--key-space-size", "10000", "--total", "50000"],
    "get-l-c64": ["--conns", "64", "--clients", "64", "range-rand", "--key-size", K,
                  "--key-space-size", "100000", "--consistency", "l", "--total", "200000"],
    "get-s-c64": ["--conns", "64", "--clients", "64", "range-rand", "--key-size", K,
                  "--key-space-size", "100000", "--consistency", "s", "--total", "200000"],
    "watch-latency": ["--conns", "100", "--clients", "100", "watch-latency", "--streams", "100",
                      "--watchers-per-stream", "10", "--put-rate", "200", "--put-total", "2000",
                      "--val-size", V],
}
PRELOAD = {"get-l-c64": 100000, "get-s-c64": 100000}


def run(cmd, env=None, check=True, timeout=600, **kw):
    r = subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=timeout, **kw)
    if check and r.returncode != 0:
        raise RuntimeError(f"{' '.join(map(str, cmd))} failed:\n{r.stdout[-1500:]}{r.stderr[-1500:]}")
    return r


class S3:
    """A RustFS server behind the counting proxy. Each case restarts RustFS on empty data."""

    def __init__(self, work, go, port=9100):
        self.work, self.port = work, port
        self.url = f"http://127.0.0.1:{port}"
        self.direct = f"http://127.0.0.1:{port + 1}"
        tools = work / "tools"
        tools.mkdir(parents=True, exist_ok=True)
        self.binary = rustfs_fixture.install(tools)
        self.version = rustfs_fixture.VERSION
        self.sha256 = rustfs_fixture.sha256(self.binary)
        source = Path(__file__).resolve().parent / "s3-counter" / "main.go"
        self.counter = tools / "s3-counter"
        run([go, "build", "-o", str(self.counter), str(source)],
            env=dict(os.environ, CGO_ENABLED="0"))
        self.server = None
        self.proxy = subprocess.Popen(
            ["taskset", "-c", CLIENT_CPUS, str(self.counter), "-listen", f"127.0.0.1:{port}",
             "-target", self.direct],
            stdout=open(work / "s3-counter.log", "ab"), stderr=subprocess.STDOUT)

    def fresh(self, bucket):
        """Restart RustFS on an empty data directory, and create `bucket`."""
        self.stop_server()
        data = self.work / "rustfs-data"
        shutil.rmtree(data, ignore_errors=True)
        data.mkdir(parents=True)
        env = {k: v for k, v in os.environ.items() if not k.startswith("RUSTFS_")}
        env.update(RUSTFS_ACCESS_KEY=ROOT_USER, RUSTFS_SECRET_KEY=ROOT_PASSWORD,
                   RUSTFS_CONSOLE_ENABLE="false")
        self.server = subprocess.Popen(
            ["taskset", "-c", CLIENT_CPUS, str(self.binary), "server", "--address",
             f"127.0.0.1:{self.port + 1}", str(data)],
            stdout=open(self.work / "rustfs.log", "ab"), stderr=subprocess.STDOUT, env=env,
            cwd=self.work)
        rustfs_fixture.wait_for_bucket(self.server, self.direct, bucket, ROOT_USER,
                                       ROOT_PASSWORD)

    def stop_server(self):
        if self.server is not None:
            self.server.send_signal(signal.SIGTERM)
            try:
                self.server.wait(30)
            except subprocess.TimeoutExpired:
                self.server.kill()
                self.server.wait()
            self.server = None

    def stop(self):
        self.stop_server()
        self.proxy.terminate()
        self.proxy.wait(10)

    def counts(self):
        """The counts since the proxy started."""
        return json.loads(urllib.request.urlopen(f"{self.url}/__counts", timeout=10).read())

    def request(self, method, path, access=ROOT_USER, secret=ROOT_PASSWORD, query=None,
                body=b""):
        """A signed request to RustFS, not counted. Return (status, body)."""
        try:
            with rustfs_fixture.signed_request(self.direct, method, path, access, secret,
                                               query=query, body=body) as response:
                return response.status, response.read()
        except urllib.error.HTTPError as error:
            with error:
                return error.code, error.read()

    def admin(self, operation, query, body):
        status, reply = self.request("PUT", f"/rustfs/admin/v3/{operation}", query=query,
                                     body=body)
        if status != 200:
            raise RuntimeError(f"{operation} failed with {status}: {reply[:300]!r}")

    def usage(self, bucket):
        """The bytes and objects in `bucket`, from ListObjectsV2."""
        size = objects = 0
        query = {"list-type": "2"}
        while True:
            status, body = self.request("GET", f"/{bucket}", query=query)
            if status != 200:
                raise RuntimeError(f"ListObjectsV2 failed with {status}: {body[:300]!r}")
            root = ElementTree.fromstring(body)
            ns = root.tag.split("}")[0] + "}" if root.tag.startswith("{") else ""
            for item in root.iter(f"{ns}Contents"):
                size += int(item.find(f"{ns}Size").text)
                objects += 1
            token = root.find(f"{ns}NextContinuationToken")
            if root.findtext(f"{ns}IsTruncated") != "true" or token is None:
                return {"bytes": size, "objects": objects}
            query = {"list-type": "2", "continuation-token": token.text}


class Cluster:
    def __init__(self, args, mode, bucket, work, members=3, credentials=None):
        self.args, self.mode, self.bucket, self.work = args, mode, bucket, work
        self.ids = list(range(1, members + 1))
        self.ports = {i: 26000 + i * 10 for i in self.ids}
        self.procs, self.credentials = {}, credentials or {}
        self.samples = {}

    def endpoint(self, i):
        return f"127.0.0.1:{self.ports[i]}"

    def eps(self, ids=None):
        return ",".join(self.endpoint(i) for i in (ids or self.ids) if i in self.procs)

    def command(self, i, join=False):
        port = self.ports[i]
        cmd = [str(self.args.binary), "--id", str(i), "--dir", str(self.work / f"m{i}"),
               "--listen", f"127.0.0.1:{port}", "--peer-listen", f"127.0.0.1:{port + 1}",
               "--cluster-id", CLUSTER, "--quota-backend-bytes", str(8 * GiB),
               "--s3-bucket", self.bucket, "--s3-endpoint", self.args.s3_url,
               "--s3-allow-http", "--s3-prefix", "bench"]
        for j in self.ports:
            cmd += ["--peer", f"{j}=127.0.0.1:{self.ports[j] + 1}",
                    "--client-peer", f"{j}=http://127.0.0.1:{self.ports[j]}"]
        if self.mode == "shared":
            cmd += ["--state-mode", "shared"]
        if join:
            cmd += ["--join"]
        return cmd

    def env(self, i):
        user, secret = self.credentials.get(i, (ROOT_USER, ROOT_PASSWORD))
        return dict(os.environ, AWS_ACCESS_KEY_ID=user, AWS_SECRET_ACCESS_KEY=secret,
                    AWS_REGION="us-east-1", RUST_LOG="warn")

    def start_member(self, i, join=False):
        log = open(self.work / f"m{i}.log", "ab")
        cpus = SERVER_CPUS[(i - 1) % len(SERVER_CPUS)] if i <= 3 else SERVER_CPUS[3]
        self.procs[i] = subprocess.Popen(["taskset", "-c", cpus] + self.command(i, join),
                                         stdout=log, stderr=subprocess.STDOUT, env=self.env(i))

    def start(self):
        for i in self.ids:
            (self.work / f"m{i}").mkdir(parents=True, exist_ok=True)
            self.start_member(i)
        self.wait_ready()
        self.sampler = threading.Thread(target=self.sample, daemon=True)
        self.sampling = True
        self.sampler.start()
        return self

    def wait_ready(self, timeout=120):
        deadline = time.time() + timeout
        while time.time() < deadline:
            for i, p in self.procs.items():
                if p.poll() is not None:
                    raise RuntimeError(f"member {i} exited rc={p.returncode}")
            if self.ctl("put", "__ready", "1", check=False).returncode == 0:
                return
            time.sleep(0.5)
        raise RuntimeError("cluster not ready")

    def ctl(self, *args, eps=None, check=True, timeout="2s"):
        return run([str(self.args.etcdctl), "--endpoints", eps or self.eps(),
                    f"--command-timeout={timeout}", *args], check=check)

    def status(self):
        r = self.ctl("endpoint", "status", "-w", "json", check=False)
        try:
            return json.loads(r.stdout)
        except json.JSONDecodeError:
            return []

    def leader(self):
        for s in self.status():
            if s["Status"]["header"]["member_id"] == s["Status"]["leader"]:
                port = int(s["Endpoint"].rsplit(":", 1)[1])
                return next(i for i in self.ports if self.ports[i] == port)
        raise RuntimeError("no leader")

    def add_learner(self, port):
        """Add a learner at `port`. The health check needs recent peer traffic, so retry."""
        deadline = time.time() + 60
        while True:
            r = self.ctl("member", "add", "n4", "--learner",
                         f"--peer-urls=http://127.0.0.1:{port + 1}", "-w", "json", check=False)
            if r.returncode == 0:
                return json.loads(r.stdout)["member"]["ID"]
            if time.time() > deadline:
                raise RuntimeError(f"member add failed: {r.stderr[-500:]}")
            time.sleep(1)

    def sample(self):
        while self.sampling:
            for i, p in list(self.procs.items()):
                try:
                    text = Path(f"/proc/{p.pid}/status").read_text()
                    rss = int(re.search(r"VmRSS:\s+(\d+)", text).group(1)) * 1024
                    self.samples[i] = max(self.samples.get(i, 0), rss)
                except (OSError, AttributeError):
                    pass
            time.sleep(0.25)

    def stop(self):
        self.sampling = False
        for p in self.procs.values():
            if p.poll() is None:
                p.send_signal(signal.SIGTERM)
        for p in self.procs.values():
            try:
                p.wait(30)
            except subprocess.TimeoutExpired:
                p.kill()


def bench(args, eps, tool_args):
    cmd = ["taskset", "-c", CLIENT_CPUS, str(args.benchmark), "--endpoints", eps,
           "--precise"] + tool_args
    started = time.time()
    r = run(cmd, timeout=3600)
    out = {"wall_s": time.time() - started}
    for key, name in [("Requests/sec", "rps"), ("Average", "avg_s"), ("Slowest", "max_s")]:
        m = re.search(rf"{re.escape(key)}:\s*([0-9.eE+-]+)", r.stdout)
        if m:
            out[name] = float(m.group(1))
    for m in re.finditer(r"\s([0-9.]+)% in ([0-9.eE+-]+) secs", r.stdout):
        out[f"p{m.group(1)}_s"] = float(m.group(2))
    out["tail"] = r.stdout[-1500:]
    return out


class Trace:
    """Count the S3 calls of one bucket in consecutive intervals: {api: [calls, bytes in, bytes
    out]}. Bytes in are request bodies, and bytes out are response bodies.

    One counter snapshot ends an interval and starts the next, so no call falls between two
    intervals. The proxy counts a call when it ends.
    """

    def __init__(self, s3, bucket):
        self.s3, self.bucket = s3, bucket
        self.mark = self.counts()

    def counts(self):
        return self.s3.counts().get(self.bucket, {})

    def next(self, wait=0.0):
        """End the interval after `wait` seconds, start the next one, and return the counts of
        the interval that ended. The last interval waits for calls in flight."""
        time.sleep(wait)
        end = self.counts()
        zero = [0, 0, 0]
        ended = {api: [a - b for a, b in zip(value, self.mark.get(api, zero))]
                 for api, value in end.items() if value != self.mark.get(api)}
        self.mark = end
        return ended


def checkpoint_all(cluster):
    """POST /checkpoint on each member: each flushes, or waits for a covering base. With a
    separate peer listener, the diagnostic routes are on that listener. A failure on any member
    fails the measurement, because the stored bytes then miss that member's state."""
    for i in cluster.procs:
        try:
            urllib.request.urlopen(urllib.request.Request(
                f"http://127.0.0.1:{cluster.ports[i] + 1}/checkpoint", data=b"{}",
                headers={"Content-Type": "application/json"}), timeout=60).read()
        except Exception as error:  # noqa: BLE001
            raise RuntimeError(f"checkpoint on member {i} failed: {error}") from error


def fresh(s3, args, name):
    bucket = re.sub(r"[^a-z0-9-]", "-", name.lower())[:60]
    s3.fresh(bucket)
    work = args.work / name
    shutil.rmtree(work, ignore_errors=True)
    work.mkdir(parents=True)
    return bucket, work


def run_case(s3, args, mode, case, repeat):
    bucket, work = fresh(s3, args, f"{case}-{mode}-{repeat}")
    cluster = Cluster(args, mode, bucket, work).start()
    try:
        leader = cluster.leader()
        if case in PRELOAD:
            bench(args, cluster.endpoint(leader),
                  ["--conns", "64", "--clients", "256", "put", "--sequential-keys",
                   "--key-space-size", str(PRELOAD[case]), "--total", str(PRELOAD[case]),
                   "--key-size", K, "--val-size", V])
            time.sleep(args.quiet)
        cluster.samples = {}
        trace = Trace(s3, bucket)
        eps = cluster.endpoint(leader) if case.startswith("put") else cluster.eps()
        result = bench(args, eps, CASES[case])
        # The peaks of the load only. The quiet time and the checkpoint come after it.
        peaks = dict(cluster.samples)
        result["s3_load"] = trace.next()
        # Then make the state durable in both modes, and measure the stored bytes.
        time.sleep(args.quiet)
        checkpoint_all(cluster)
        result["s3_settle"] = trace.next(wait=1)
        result["s3_usage"] = s3.usage(bucket)
        result["rss"] = {("leader" if i == leader else f"follower-{i}"): rss
                         for i, rss in peaks.items()}
        result["rss_interval"] = "load"
    finally:
        cluster.stop()
    return result


def run_ops(s3, args, mode, repeat):
    """Learner install and failover after a preload."""
    bucket, work = fresh(s3, args, f"ops-{mode}-{repeat}")
    cluster = Cluster(args, mode, bucket, work).start()
    out = {}
    try:
        leader = cluster.leader()
        bench(args, cluster.endpoint(leader),
              ["--conns", "64", "--clients", "256", "put", "--sequential-keys",
               "--key-space-size", "200000", "--total", "200000", "--key-size", K,
               "--val-size", V])
        time.sleep(args.quiet)
        # A learner joins. It needs a Raft snapshot: replicated mode copies SSTs, shared mode
        # names a checkpoint.
        port = 26040
        learner = cluster.add_learner(port)
        target = max(s["Status"]["raftIndex"] for s in cluster.status())
        cluster.ports[learner] = port
        (work / f"m{learner}").mkdir()
        checkpoint_all(cluster)
        out["s3_usage_before_learner"] = s3.usage(bucket)
        trace = Trace(s3, bucket)
        started = time.time()
        cluster.start_member(learner, join=True)
        while True:
            r = cluster.ctl("endpoint", "status", "-w", "json", eps=cluster.endpoint(learner),
                            check=False)
            try:
                status = json.loads(r.stdout) or []
                if status and status[0]["Status"]["raftAppliedIndex"] >= target:
                    break
            except (json.JSONDecodeError, IndexError, KeyError, TypeError):
                pass
            if time.time() - started > 300:
                raise RuntimeError("the learner did not catch up")
            time.sleep(0.05)
        out["learner_install_s"] = time.time() - started
        out["learner_s3"] = trace.next(wait=1)
        out["learner_rss"] = cluster.samples.get(learner)
        cluster.procs.pop(learner).kill()
        # Failover: a burst of writes, then SIGKILL of the leader with no further pause.
        bench(args, cluster.endpoint(leader),
              ["--conns", "16", "--clients", "64", "put", "--key-size", K, "--val-size", V,
               "--key-space-size", "100000", "--total", "20000"])
        cluster.procs[leader].kill()
        killed = time.time()
        others = [i for i in cluster.ids if i != leader]
        while True:
            r = cluster.ctl("put", "after-failover", "1", eps=cluster.eps(others), check=False,
                            timeout="500ms")
            if r.returncode == 0:
                break
            if time.time() - killed > 120:
                raise RuntimeError("no write after the failover")
        out["failover_first_write_s"] = time.time() - killed
    finally:
        cluster.stop()
    return out


def cluster_prefix():
    return f"bench/{hashlib.sha256(CLUSTER.encode()).hexdigest()}"


def policy(bucket, prefix, role, member):
    """The S3 policy of the shared state guide for one role."""
    arn = f"arn:aws:s3:::{bucket}"
    cluster = f"{arn}/{prefix}"
    statements = [
        {"Effect": "Allow", "Action": ["s3:ListBucket"], "Resource": [arn],
         "Condition": {"StringLike": {"s3:prefix": [f"{prefix}/*", f"{prefix}"]}}},
        {"Effect": "Allow", "Action": ["s3:GetObject"],
         "Resource": [f"{cluster}/mode.json", f"{cluster}/member-*/owner.json"]},
        {"Effect": "Allow", "Action": ["s3:GetObject", "s3:PutObject"],
         "Resource": [f"{cluster}/member-{member}/*"]},
    ]
    if role == "voter":
        statements += [
            {"Effect": "Allow", "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
             "Resource": [f"{cluster}/shared/*"]},
            {"Effect": "Allow", "Action": ["s3:PutObject"], "Resource": [f"{cluster}/mode.json"]},
        ]
    elif role == "learner":
        statements += [{"Effect": "Allow", "Action": ["s3:GetObject"],
                        "Resource": [f"{cluster}/shared/*"]}]
    return {"Version": "2012-10-17", "Statement": statements}


def user(s3, bucket, prefix, role, member):
    """Create a RustFS user with the guide's policy for `role`. RustFS 1.0.6 takes the admin
    request bodies as plain JSON."""
    name = f"{role}-{member}"
    secret = f"{name}-secret-key"
    s3.admin("add-canned-policy", {"name": name},
             json.dumps(policy(bucket, prefix, role, member)).encode())
    s3.admin("add-user", {"accessKey": name},
             json.dumps({"secretKey": secret, "status": "enabled"}).encode())
    s3.admin("set-user-or-group-policy",
             {"policyName": name, "userOrGroup": name, "isGroup": "false"}, b"")
    return name, secret


def run_policy(s3, args):
    """Run three voters and a learner with the guide's policies."""
    bucket, work = fresh(s3, args, "policy-check")
    prefix = cluster_prefix()
    credentials = {i: user(s3, bucket, prefix, "voter", i) for i in (1, 2, 3)}
    cluster = Cluster(args, "shared", bucket, work, credentials=credentials).start()
    out = {"voters_ready": True}
    try:
        cluster.ctl("put", "policy", "checked")
        port = 26040
        learner = cluster.add_learner(port)
        cluster.ports[learner] = port
        cluster.credentials[learner] = user(s3, bucket, prefix, "learner", learner)
        (work / f"m{learner}").mkdir()
        cluster.start_member(learner, join=True)
        deadline = time.time() + 120
        value = None
        while time.time() < deadline:
            r = cluster.ctl("get", "policy", "--consistency=s", "--print-value-only",
                            eps=cluster.endpoint(learner), check=False)
            if r.returncode == 0 and r.stdout.strip():
                value = r.stdout.strip()
                break
            time.sleep(0.5)
        out["learner_reads"] = value == "checked"
        # Each probe has one expected outcome: the guide's policy allows it, or RustFS denies it
        # with AccessDenied. Any other result fails the check.
        voter = credentials[1]
        probes = [
            ("learner_reads_shared", cluster.credentials[learner], "read",
             "shared/bootstrap.json", "allowed"),
            ("learner_writes_shared", cluster.credentials[learner], "write", "shared/denied",
             "denied"),
            ("learner_writes_own_binding_prefix", cluster.credentials[learner], "write",
             f"member-{learner}/probe", "allowed"),
            ("voter_reads_mode", voter, "read", "mode.json", "allowed"),
            ("voter_writes_own_prefix", voter, "write", "member-1/probe", "allowed"),
            ("voter_reads_other_binding", voter, "read", "member-2/binding.json", "denied"),
            ("voter_writes_other_binding", voter, "write", "member-2/denied", "denied"),
            ("voter_writes_other_state", voter, "write", "member-2/state/denied", "denied"),
        ]
        results, passed = {}, True
        for name, (user_name, secret), action, key, expected in probes:
            method, body = ("GET", b"") if action == "read" else ("PUT", b"x")
            status, reply = s3.request(method, f"/{bucket}/{prefix}/{key}", user_name, secret,
                                       body=body)
            code = re.search(rb"<Code>(\w+)</Code>", reply)
            code = code.group(1).decode() if code else None
            if status == 200:
                outcome = "allowed"
            elif status == 403 and code == "AccessDenied":
                outcome = "denied"
            else:
                outcome = "error"
            results[name] = {"action": action, "key": key, "expected": expected,
                             "outcome": outcome, "status": status, "code": code}
            passed &= outcome == expected
        out["probes"] = results
        out["passed"] = passed and out["learner_reads"]
    finally:
        cluster.stop()
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--binary", type=str, required=True)
    ap.add_argument("--go", default="go", help="Go toolchain that builds scripts/s3-counter")
    ap.add_argument("--benchmark", type=str, required=True, help="etcd benchmark tool")
    ap.add_argument("--etcdctl", type=str, required=True)
    ap.add_argument("--work", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--cases", nargs="*", default=list(CASES))
    ap.add_argument("--modes", nargs="*", default=["replicated", "shared"])
    ap.add_argument("--repeat", type=int, default=2)
    ap.add_argument("--quiet", type=float, default=15.0,
                    help="seconds after a load before S3 usage is measured")
    ap.add_argument("--ops", action="store_true")
    ap.add_argument("--policy", action="store_true")
    args = ap.parse_args()
    # RustFS runs in the work directory, so relative paths would resolve from there.
    for name in ["work", "out"]:
        setattr(args, name, getattr(args, name).resolve())
    # A bare executable name, such as `etcdctl`, is found through PATH.
    for name in ["binary", "benchmark", "etcdctl"]:
        value = getattr(args, name)
        if os.sep not in value:
            found = shutil.which(value)
            if found is None:
                ap.error(f"--{name}: {value} is not on PATH")
            value = found
        setattr(args, name, Path(value).resolve())
    args.work.mkdir(parents=True, exist_ok=True)
    s3 = S3(args.work, args.go)
    args.s3_url = s3.url
    try:
        with open(args.out, "a") as out:
            def record(entry):
                entry["time"] = time.strftime("%Y-%m-%dT%H:%M:%S")
                out.write(json.dumps(entry) + "\n")
                out.flush()
                print(json.dumps({k: v for k, v in entry.items() if k != "tail"})[:600],
                      flush=True)
            record({"kind": "s3", "name": "RustFS", "version": s3.version,
                    "binary_sha256": s3.sha256})
            if args.policy:
                checked = run_policy(s3, args)
                record({"kind": "policy", **checked})
                if not checked["passed"]:
                    raise SystemExit("the policy check failed")
            for repeat in range(args.repeat):
                for case in args.cases:
                    for mode in args.modes:
                        entry = {"kind": "case", "case": case, "mode": mode, "repeat": repeat}
                        try:
                            entry.update(run_case(s3, args, mode, case, repeat))
                        except Exception as error:  # noqa: BLE001
                            # An invalid case keeps no measurement.
                            entry["error"] = str(error)
                        record(entry)
                if args.ops:
                    for mode in args.modes:
                        record({"kind": "ops", "mode": mode, "repeat": repeat,
                                **run_ops(s3, args, mode, repeat)})
    finally:
        s3.stop()


if __name__ == "__main__":
    main()
