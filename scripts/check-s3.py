#!/usr/bin/env python3
"""Run S3 recovery tests with a temporary MinIO service and fresh credentials."""

import argparse
from datetime import datetime, timezone
import hashlib
import hmac
import json
import os
from pathlib import Path
import secrets
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request

from reporting import write_report

ROOT = Path(__file__).resolve().parents[1]
MINIO_REVISION = "9e49d5e7a648f00e26f2246f4dc28e6b07f8c84a"


def create_bucket(endpoint, bucket, access, secret):
    """Sign the bucket creation request with AWS Signature Version 4."""
    now = datetime.now(timezone.utc)
    date, timestamp = now.strftime("%Y%m%d"), now.strftime("%Y%m%dT%H%M%SZ")
    host = endpoint.split("://", 1)[1]
    digest = hashlib.sha256(b"").hexdigest()
    headers = f"host:{host}\nx-amz-content-sha256:{digest}\nx-amz-date:{timestamp}\n"
    names = "host;x-amz-content-sha256;x-amz-date"
    canonical = f"PUT\n/{bucket}\n\n{headers}\n{names}\n{digest}"
    scope = f"{date}/us-east-1/s3/aws4_request"
    string = f"AWS4-HMAC-SHA256\n{timestamp}\n{scope}\n"
    string += hashlib.sha256(canonical.encode()).hexdigest()
    key = ("AWS4" + secret).encode()
    for part in [date, "us-east-1", "s3", "aws4_request"]:
        key = hmac.new(key, part.encode(), hashlib.sha256).digest()
    signature = hmac.new(key, string.encode(), hashlib.sha256).hexdigest()
    request = urllib.request.Request(endpoint + "/" + bucket, data=b"", method="PUT", headers={
        "x-amz-date": timestamp, "x-amz-content-sha256": digest,
        "Authorization": f"AWS4-HMAC-SHA256 Credential={access}/{scope}, "
                         f"SignedHeaders={names}, Signature={signature}",
    })
    with urllib.request.urlopen(request, timeout=10) as response:
        assert response.status == 200


def wait_for_bucket(process, endpoint, bucket, access, secret):
    """Start tests after the local S3 service accepts a signed write."""
    deadline = time.monotonic() + 20
    while process.poll() is None:
        try:
            create_bucket(endpoint, bucket, access, secret)
            return
        except urllib.error.HTTPError as error:
            if error.code != 503:
                raise
            error.close()
        except urllib.error.URLError as error:
            if not isinstance(error.reason, ConnectionRefusedError):
                raise
        if time.monotonic() >= deadline:
            raise RuntimeError("MinIO did not accept the test bucket before the deadline")
        time.sleep(0.1)
    raise RuntimeError("MinIO exited before it accepted the test bucket")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=ROOT / "results/s3-recovery.json")
    parser.add_argument("--binary", type=Path, default=ROOT / "target/release/slatedb-etcd")
    parser.add_argument("--tags", default="s3")
    parser.add_argument("--run", default="^TestS3")
    parser.add_argument("--deployment", default="three local processes with a temporary S3 service")
    args = parser.parse_args()
    tools = ROOT / "tmp/s3-tools"
    binary = Path(os.environ.get("MINIO_BINARY", tools / "minio"))
    if not binary.exists():
        tools.mkdir(parents=True, exist_ok=True)
        subprocess.run(["go", "install", f"github.com/minio/minio@{MINIO_REVISION}"],
                       env=dict(os.environ, GOBIN=str(tools)), check=True)
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    access, secret = secrets.token_hex(12), secrets.token_hex(32)
    endpoint, bucket = f"http://127.0.0.1:{port}", "slatedb-recovery"
    with tempfile.TemporaryDirectory(prefix="slatedb-s3-") as directory:
        with open(Path(directory) / "minio.log", "w+") as log:
            process = subprocess.Popen([str(binary), "server", directory + "/data",
                                        "--address", f"127.0.0.1:{port}"], stdout=log, stderr=log,
                                       env=dict(os.environ, MINIO_ROOT_USER=access,
                                                MINIO_ROOT_PASSWORD=secret, MINIO_BROWSER="off"))
            try:
                wait_for_bucket(process, endpoint, bucket, access, secret)
                env = dict(os.environ, AWS_ACCESS_KEY_ID=access, AWS_SECRET_ACCESS_KEY=secret,
                           SLATEDB_S3_TEST_ENDPOINT=endpoint, SLATEDB_S3_TEST_BUCKET=bucket)
                env.pop("AWS_SESSION_TOKEN", None)
                result = subprocess.run([sys.executable, str(ROOT / "scripts/check-compat.py"),
                                         "--tags", args.tags, "--run", args.run, "--binary", str(args.binary),
                                         "--output", str(args.output), "--deployment", args.deployment], env=env, cwd=ROOT)
            finally:
                process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
            if args.output.exists():
                report = json.loads(args.output.read_text())
                report["object_store"] = {"name": "MinIO", "source_revision": MINIO_REVISION,
                                          "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest()}
                write_report(args.output, report)
            raise SystemExit(result.returncode)


if __name__ == "__main__":
    main()
