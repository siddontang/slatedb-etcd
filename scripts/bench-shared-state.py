#!/usr/bin/env python3
"""Compare replicated and shared state mode on three local members with one local MinIO.

Each case starts a new cluster in a new bucket. The script records the etcd benchmark result,
the S3 requests and bytes from MinIO, and the peak RSS of each member. The `ops` scenario
measures the time from a leader kill to the first successful write, and the time that a new
learner needs to reach the leader's index. The `policy` check runs members with the S3
policies of docs/shared-state-guide.md and checks that MinIO denies keys outside them.

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
import urllib.request

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


class Minio:
    def __init__(self, tools, work, port=9100):
        self.tools, self.work, self.port = tools, work, port
        self.url = f"http://127.0.0.1:{port}"
        self.env = dict(os.environ, MC_CONFIG_DIR=str(work / "mc"))

    def start(self):
        data = self.work / "minio-data"
        shutil.rmtree(data, ignore_errors=True)
        data.mkdir(parents=True)
        env = dict(os.environ, MINIO_ROOT_USER=ROOT_USER, MINIO_ROOT_PASSWORD=ROOT_PASSWORD,
                   MINIO_PROMETHEUS_AUTH_TYPE="public", MINIO_BROWSER="off")
        log = open(self.work / "minio.log", "ab")
        self.proc = subprocess.Popen(
            ["taskset", "-c", CLIENT_CPUS, str(self.tools / "minio"), "server", str(data),
             "--address", f"127.0.0.1:{self.port}", "--quiet"],
            stdout=log, stderr=subprocess.STDOUT, env=env)
        for _ in range(100):
            if self.mc("alias", "set", "local", self.url, ROOT_USER, ROOT_PASSWORD,
                       check=False).returncode == 0 and \
                    self.mc("ls", "local", check=False).returncode == 0:
                return self
            time.sleep(0.2)
        raise RuntimeError("MinIO did not start")

    def stop(self):
        self.proc.send_signal(signal.SIGTERM)
        self.proc.wait(30)

    def mc(self, *args, check=True):
        return run([str(self.tools / "mc"), *args], env=self.env, check=check)

    def trace(self, path):
        """Start `mc admin trace`. MinIO metrics are cached, so the trace counts each call."""
        return subprocess.Popen([str(self.tools / "mc"), "admin", "trace", "--json", "local"],
                                stdout=open(path, "w"), stderr=subprocess.DEVNULL,
                                env=self.env)

    def usage(self, bucket):
        r = self.mc("du", "--json", f"local/{bucket}")
        line = json.loads(r.stdout.strip().splitlines()[-1])
        return {"bytes": line.get("size", 0), "objects": line.get("objects", 0)}


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
               "--s3-bucket", self.bucket, "--s3-endpoint", self.args.minio_url,
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


def traced(path, bucket):
    """S3 calls and bytes for `bucket` in a trace: {api: [calls, bytes in, bytes out]}."""
    out = {}
    for line in Path(path).read_text().splitlines():
        try:
            call = json.loads(line)
        except json.JSONDecodeError:
            continue
        if f"/{bucket}" not in call.get("path", "") and \
                call.get("callStats", {}).get("bucket") != bucket:
            continue
        api = call.get("api") or call.get("funcName", "unknown")
        stats = call.get("callStats", {})
        entry = out.setdefault(api.removeprefix("s3."), [0, 0, 0])
        entry[0] += 1
        entry[1] += stats.get("rx", 0)
        entry[2] += stats.get("tx", 0)
    return out


class Trace:
    """Count the S3 calls of one bucket between start and stop."""

    def __init__(self, minio, work, bucket):
        self.path, self.bucket = work / "trace.jsonl", bucket
        self.proc = minio.trace(self.path)
        time.sleep(1)

    def stop(self):
        time.sleep(1)
        self.proc.terminate()
        self.proc.wait(10)
        return traced(self.path, self.bucket)


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


def fresh(minio, args, name):
    bucket = re.sub(r"[^a-z0-9-]", "-", name.lower())[:60]
    minio.mc("rb", "--force", f"local/{bucket}", check=False)
    minio.mc("mb", f"local/{bucket}")
    work = args.work / name
    shutil.rmtree(work, ignore_errors=True)
    work.mkdir(parents=True)
    return bucket, work


def run_case(minio, args, mode, case, repeat):
    bucket, work = fresh(minio, args, f"{case}-{mode}-{repeat}")
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
        trace = Trace(minio, work, bucket)
        eps = cluster.endpoint(leader) if case.startswith("put") else cluster.eps()
        result = bench(args, eps, CASES[case])
        result["s3_load"] = trace.stop()
        # Then make the state durable in both modes, and measure the stored bytes.
        trace = Trace(minio, work, bucket)
        time.sleep(args.quiet)
        checkpoint_all(cluster)
        result["s3_settle"] = trace.stop()
        result["s3_usage"] = minio.usage(bucket)
        result["rss"] = {("leader" if i == leader else f"follower-{i}"): rss
                         for i, rss in cluster.samples.items()}
    finally:
        cluster.stop()
    minio.mc("rb", "--force", f"local/{bucket}", check=False)
    return result


def run_ops(minio, args, mode, repeat):
    """Learner install and failover after a preload."""
    bucket, work = fresh(minio, args, f"ops-{mode}-{repeat}")
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
        out["s3_usage_before_learner"] = minio.usage(bucket)
        trace = Trace(minio, work, bucket)
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
        out["learner_s3"] = trace.stop()
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
    minio.mc("rb", "--force", f"local/{bucket}", check=False)
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


def user(minio, work, bucket, prefix, role, member):
    name = f"{role}-{member}"
    secret = f"{name}-secret-key"
    path = work / f"{name}.json"
    path.write_text(json.dumps(policy(bucket, prefix, role, member)))
    minio.mc("admin", "policy", "create", "local", name, str(path))
    minio.mc("admin", "user", "add", "local", name, secret)
    minio.mc("admin", "policy", "attach", "local", name, "--user", name)
    return name, secret


def run_policy(minio, args):
    """Run three voters and a learner with the guide's policies."""
    bucket, work = fresh(minio, args, "policy-check")
    prefix = cluster_prefix()
    credentials = {i: user(minio, work, bucket, prefix, "voter", i) for i in (1, 2, 3)}
    cluster = Cluster(args, "shared", bucket, work, credentials=credentials).start()
    out = {"voters_ready": True}
    try:
        cluster.ctl("put", "policy", "checked")
        port = 26040
        learner = cluster.add_learner(port)
        cluster.ports[learner] = port
        cluster.credentials[learner] = user(minio, work, bucket, prefix, "learner", learner)
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
        # Each probe has one expected outcome: the guide's policy allows it, or MinIO denies it
        # with AccessDenied. Any other result fails the check.
        probe = work / "probe"
        probe.write_text("x")
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
            minio.mc("alias", "set", name, minio.url, user_name, secret)
            target = f"{name}/{bucket}/{prefix}/{key}"
            # `--debug` prints the S3 response, so the check reads the exact error code.
            if action == "read":
                r = minio.mc("--debug", "cat", target, check=False)
            else:
                r = minio.mc("--debug", "cp", str(probe), target, check=False)
            text = r.stdout + r.stderr
            # mc also probes the bucket, and MinIO denies that. Only the errors of the probed
            # object count.
            codes = sorted({
                re.search(r"<Code>(\w+)</Code>", error).group(1)
                for error in re.findall(r"<Error>.*?</Error>", text, re.S)
                if f"<Key>{prefix}/{key}</Key>" in error
            })
            # A HEAD response has no body, so a denied stat shows only its 403 status. The same
            # credentials pass the allowed probes, so a 403 there is a policy denial.
            statuses, request = [], None
            for line in text.splitlines():
                m = re.search(r"\b(GET|HEAD|PUT|POST) (/\S+) HTTP/1\.1", line)
                if m:
                    request = (m.group(1), m.group(2))
                m = re.search(r"HTTP/1\.1 (\d{3})", line)
                if m and request and request[1].split("?")[0].endswith(f"/{prefix}/{key}"):
                    statuses.append((request[0], int(m.group(1))))
                    request = None
            if not codes and ("HEAD", 403) in statuses:
                codes = ["AccessDenied"]
            if r.returncode == 0 and not codes:
                outcome = "allowed"
            elif r.returncode != 0 and codes == ["AccessDenied"]:
                outcome = "denied"
            else:
                outcome = "error"
            results[name] = {"action": action, "key": key, "expected": expected,
                             "outcome": outcome, "object_errors": codes,
                             "object_statuses": statuses}
            passed &= outcome == expected
        out["probes"] = results
        out["passed"] = passed and out["learner_reads"]
    finally:
        cluster.stop()
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--binary", type=Path, required=True)
    ap.add_argument("--tools", type=Path, required=True, help="directory with minio and mc")
    ap.add_argument("--benchmark", type=Path, required=True, help="etcd benchmark tool")
    ap.add_argument("--etcdctl", type=Path, required=True)
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
    args.work.mkdir(parents=True, exist_ok=True)
    minio = Minio(args.tools, args.work).start()
    args.minio_url = minio.url
    try:
        with open(args.out, "a") as out:
            def record(entry):
                entry["time"] = time.strftime("%Y-%m-%dT%H:%M:%S")
                out.write(json.dumps(entry) + "\n")
                out.flush()
                print(json.dumps({k: v for k, v in entry.items() if k != "tail"})[:600],
                      flush=True)
            if args.policy:
                checked = run_policy(minio, args)
                record({"kind": "policy", **checked})
                if not checked["passed"]:
                    raise SystemExit("the policy check failed")
            for repeat in range(args.repeat):
                for case in args.cases:
                    for mode in args.modes:
                        entry = {"kind": "case", "case": case, "mode": mode, "repeat": repeat}
                        try:
                            entry.update(run_case(minio, args, mode, case, repeat))
                        except Exception as error:  # noqa: BLE001
                            # An invalid case keeps no measurement.
                            entry["error"] = str(error)
                        record(entry)
                if args.ops:
                    for mode in args.modes:
                        record({"kind": "ops", "mode": mode, "repeat": repeat,
                                **run_ops(minio, args, mode, repeat)})
    finally:
        minio.stop()


if __name__ == "__main__":
    main()
