#!/usr/bin/env python3
"""Start a pinned RustFS server for local S3 tests, benchmarks, and clusters of several members.

A cluster of several members runs in shared state mode on RustFS, as production runs on S3.
`python3 scripts/rustfs_fixture.py -- COMMAND...` runs a command with one server, for example a
direct `go test` run. Without a command, the server runs until Ctrl-C. `--env-file` writes its
settings as shell exports for other terminals. The server deletes its objects when it stops.
"""

import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import hmac
import os
from pathlib import Path
import platform
import secrets
import shlex
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
import zipfile

from reporting import write_pending

ROOT = Path(__file__).resolve().parents[1]
# The server environment that `shared_store` exports, and that nested runners reuse.
STORE_ENV = ("SLATEDB_S3_TEST_ENDPOINT", "SLATEDB_S3_TEST_BUCKET", "AWS_ACCESS_KEY_ID",
             "AWS_SECRET_ACCESS_KEY")
VERSION = "1.0.6"
PACKAGES = {
    ("Darwin", "arm64"): ("macos-aarch64",
                          "e0bf7e8aeee45490034576fde6513377db81844d8c40e24c57269e9bb274882f"),
    ("Linux", "x86_64"): ("linux-x86_64-gnu",
                          "4f0bf3fecbe861467427ff506aee22c302cfd5ac8b7dcbde71714f8e5e680dea"),
    ("Linux", "aarch64"): ("linux-aarch64-gnu",
                           "fde8485aa2580ad774ba8588b0b51b68790ff28677fb27024ea68d8ab2be8e00"),
}


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def install(directory):
    system = platform.system(), platform.machine()
    if system not in PACKAGES:
        raise RuntimeError(f"No pinned RustFS package for {system}")
    target, checksum = PACKAGES[system]
    name = f"rustfs-{target}-v{VERSION}.zip"
    cache = ROOT / "tmp/s3-tools"
    cache.mkdir(parents=True, exist_ok=True)
    archive = cache / name
    if not archive.is_file() or sha256(archive) != checksum:
        url = f"https://github.com/rustfs/rustfs/releases/download/{VERSION}/{name}"
        with tempfile.TemporaryDirectory(dir=cache) as staging:
            download = Path(staging) / name
            subprocess.run(["curl", "--fail", "--location", "--silent", "--show-error",
                            "--retry", "3", "--output", str(download), url], check=True)
            if sha256(download) != checksum:
                raise ValueError("RustFS archive checksum does not match the pinned release")
            download.replace(archive)
    binary = directory / "rustfs"
    with zipfile.ZipFile(archive) as package:
        with package.open("rustfs") as source, binary.open("wb") as dest:
            shutil.copyfileobj(source, dest)
    binary.chmod(0o700)
    return binary


def signed_request(endpoint, method, path, access, secret, query=None, body=b"", timeout=10):
    """Send one request signed with AWS Signature Version 4, and return the response.

    Raise `urllib.error.HTTPError` for an error status, as `urllib.request.urlopen` does.
    """
    now = datetime.now(timezone.utc)
    date, timestamp = now.strftime("%Y%m%d"), now.strftime("%Y%m%dT%H%M%SZ")
    host = endpoint.split("://", 1)[1]
    quote = lambda value, safe="": urllib.parse.quote(str(value), safe="-_.~" + safe)
    path = quote(path, "/")
    params = "&".join(f"{quote(k)}={quote(v)}" for k, v in sorted((query or {}).items()))
    digest = hashlib.sha256(body).hexdigest()
    headers = f"host:{host}\nx-amz-content-sha256:{digest}\nx-amz-date:{timestamp}\n"
    names = "host;x-amz-content-sha256;x-amz-date"
    canonical = f"{method}\n{path}\n{params}\n{headers}\n{names}\n{digest}"
    scope = f"{date}/us-east-1/s3/aws4_request"
    string = f"AWS4-HMAC-SHA256\n{timestamp}\n{scope}\n"
    string += hashlib.sha256(canonical.encode()).hexdigest()
    key = ("AWS4" + secret).encode()
    for part in [date, "us-east-1", "s3", "aws4_request"]:
        key = hmac.new(key, part.encode(), hashlib.sha256).digest()
    signature = hmac.new(key, string.encode(), hashlib.sha256).hexdigest()
    url = endpoint + path + (f"?{params}" if params else "")
    request = urllib.request.Request(url, data=body if method in ("PUT", "POST") else None,
                                     method=method, headers={
        "x-amz-date": timestamp, "x-amz-content-sha256": digest,
        "Authorization": f"AWS4-HMAC-SHA256 Credential={access}/{scope}, "
                         f"SignedHeaders={names}, Signature={signature}",
    })
    return urllib.request.urlopen(request, timeout=timeout)


def create_bucket(endpoint, bucket, access, secret):
    """Create `bucket` with a signed request."""
    with signed_request(endpoint, "PUT", "/" + bucket, access, secret) as response:
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
            raise RuntimeError("RustFS did not accept the test bucket before the deadline")
        time.sleep(0.1)
    raise RuntimeError(f"RustFS exited with status {process.returncode} before bucket creation")


@contextmanager
def local_rustfs():
    access, secret = secrets.token_hex(12), secrets.token_hex(32)
    with tempfile.TemporaryDirectory(prefix="slatedb-rustfs-") as temporary:
        directory = Path(temporary)
        binary = install(directory)
        data = directory / "data"
        data.mkdir()
        settings = {key: value for key, value in os.environ.items() if not key.startswith("RUSTFS_")}
        settings.update(RUSTFS_ACCESS_KEY=access, RUSTFS_SECRET_KEY=secret,
                        RUSTFS_CONSOLE_ENABLE="false")
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            address = f"127.0.0.1:{listener.getsockname()[1]}"
        endpoint, bucket = "http://" + address, "slatedb-test"
        with (directory / "server.log").open("w") as log:
            process = subprocess.Popen([str(binary), "server", "--address", address, str(data)],
                                       stdout=log, stderr=log, env=settings, cwd=directory)
            try:
                wait_for_bucket(process, endpoint, bucket, access, secret)
                env = dict(os.environ, AWS_ACCESS_KEY_ID=access, AWS_SECRET_ACCESS_KEY=secret,
                           AWS_EC2_METADATA_DISABLED="true", SLATEDB_S3_TEST_ENDPOINT=endpoint,
                           SLATEDB_S3_TEST_BUCKET=bucket)
                env.pop("AWS_SESSION_TOKEN", None)
                yield env, {"name": "RustFS", "version": VERSION, "binary_sha256": sha256(binary)}
            finally:
                process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()


@contextmanager
def shared_store(report=None, value=None):
    """Supply one RustFS server to the clusters of several members in this process and its children.

    If the environment already names a server, for example from an outer runner, use that server.
    Otherwise start one and export its settings to `os.environ` until the context ends. Yield the
    store description for reports.

    With a `report` path, first replace that report with the failed `value`, so a failed start
    leaves no earlier passed report.
    """
    if report is not None:
        write_pending(report, value or {})
    if all(os.environ.get(name) for name in STORE_ENV):
        yield {"name": "RustFS", "version": VERSION, "reused": True}
        return
    with local_rustfs() as (env, store):
        saved = {name: os.environ.get(name) for name in (*STORE_ENV, "AWS_EC2_METADATA_DISABLED",
                                                         "AWS_SESSION_TOKEN")}
        os.environ.pop("AWS_SESSION_TOKEN", None)
        os.environ.update({name: env[name] for name in (*STORE_ENV, "AWS_EC2_METADATA_DISABLED")})
        try:
            yield store
        finally:
            for name, value in saved.items():
                if value is None:
                    os.environ.pop(name, None)
                else:
                    os.environ[name] = value


def unique_prefix(kind):
    """Return a new S3 prefix for one cluster, so that clusters on one server do not collide."""
    return f"{kind}/{secrets.token_hex(8)}"


def shared_args(prefix, env=None):
    """Return the flags that start one member in shared state mode on the store of `env`.

    All members of one cluster use the same `prefix`, also after a restart. Without
    `SLATEDB_S3_TEST_ENDPOINT`, the member uses the AWS endpoint of the bucket.
    Each Python launcher builds the flags here. The Go tests build the same flags in
    `tests/compat/compat_test.go` (`storeFlags`) and `tests/downstream/storage_test.go`.
    """
    env = os.environ if env is None else env
    endpoint, bucket = env.get("SLATEDB_S3_TEST_ENDPOINT"), env.get("SLATEDB_S3_TEST_BUCKET")
    if not bucket:
        raise RuntimeError("A cluster of several members needs RustFS. Use shared_store, or run "
                           "the command through scripts/rustfs_fixture.py")
    flags = ["--state-mode", "shared", "--s3-bucket", bucket, "--s3-prefix", prefix]
    if endpoint:
        flags += ["--s3-endpoint", endpoint]
        if endpoint.startswith("http://"):
            flags.append("--s3-allow-http")
    return flags



def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--env-file", type=Path, help="Write the server settings as shell exports")
    parser.add_argument("command", nargs="*", help="Command to run with the server environment")
    args = parser.parse_args()
    # Stop the server on SIGTERM too.
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(143))
    with shared_store():
        try:
            serve(args)
        finally:
            # The settings name a server that no longer runs.
            if args.env_file:
                args.env_file.unlink(missing_ok=True)


def serve(args):
    if args.env_file:
        names = (*STORE_ENV, "AWS_EC2_METADATA_DISABLED")
        args.env_file.parent.mkdir(parents=True, exist_ok=True)
        args.env_file.touch(mode=0o600)
        args.env_file.chmod(0o600)
        # A reused server needs only the four STORE_ENV settings.
        values = dict({"AWS_EC2_METADATA_DISABLED": "true"}, **os.environ)
        args.env_file.write_text("".join(f"export {name}={shlex.quote(values[name])}\n"
                                         for name in names))
        print("Server settings:", args.env_file, flush=True)
    if args.command:
        raise SystemExit(subprocess.run(args.command).returncode)
    print("RustFS runs at", os.environ["SLATEDB_S3_TEST_ENDPOINT"], "until Ctrl-C", flush=True)
    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
