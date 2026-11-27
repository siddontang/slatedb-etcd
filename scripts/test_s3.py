#!/usr/bin/env python3
"""Check the RustFS fixture, its store settings, and the reports of the launchers that use it."""

import importlib.util
from io import BytesIO
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch
from urllib.error import HTTPError, URLError

import rustfs_fixture as s3

SCRIPTS = Path(__file__).resolve().parent


def load(name):
    spec = importlib.util.spec_from_file_location(name.replace("-", "_"), SCRIPTS / (name + ".py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def without_store():
    """Return the environment without the settings of a reusable server."""
    return {k: v for k, v in os.environ.items() if k not in s3.STORE_ENV}


class BucketReadiness(unittest.TestCase):
    def test_refusal_and_unavailable_then_ready(self):
        process = Mock()
        process.poll.return_value = None
        responses = [URLError(ConnectionRefusedError()), HTTPError("", 503, "", {}, BytesIO()), None]
        with patch.object(s3, "create_bucket", side_effect=responses) as create:
            with patch.object(s3.time, "sleep"):
                s3.wait_for_bucket(process, "endpoint", "bucket", "key", "secret")
        self.assertEqual(create.call_count, 3)

    def test_failed_download_names_the_step_and_retries_each_error(self):
        with tempfile.TemporaryDirectory() as directory:
            failure = s3.subprocess.CalledProcessError(22, "curl")
            with patch.object(s3, "ROOT", Path(directory)), \
                    patch.object(s3.platform, "system", return_value="Linux"), \
                    patch.object(s3.platform, "machine", return_value="x86_64"), \
                    patch.object(s3.subprocess, "run", side_effect=failure) as download:
                with self.assertRaisesRegex(RuntimeError, "RustFS download failed: curl exit 22"):
                    s3.install(Path(directory))
        command = download.call_args.args[0]
        self.assertIn("--retry-all-errors", command)
        self.assertEqual(command[command.index("--retry") + 1], "8")

    def test_failed_signature_stops_startup(self):
        process = Mock()
        process.poll.return_value = None
        with patch.object(s3, "create_bucket", side_effect=HTTPError("", 403, "", {}, BytesIO())):
            with self.assertRaises(HTTPError) as error:
                s3.wait_for_bucket(process, "endpoint", "bucket", "key", "secret")
        self.assertEqual(error.exception.code, 403)


class SharedStore(unittest.TestCase):
    SERVER = {"SLATEDB_S3_TEST_ENDPOINT": "http://127.0.0.1:9", "SLATEDB_S3_TEST_BUCKET": "bucket",
              "AWS_ACCESS_KEY_ID": "key", "AWS_SECRET_ACCESS_KEY": "secret"}

    def test_nested_runner_reuses_the_server(self):
        with patch.dict(s3.os.environ, self.SERVER), patch.object(s3, "local_rustfs") as start:
            with s3.shared_store() as store:
                self.assertTrue(store["reused"])
        start.assert_not_called()

    def test_started_server_settings_end_with_the_context(self):
        env = dict(self.SERVER, AWS_EC2_METADATA_DISABLED="true")
        fixture = Mock()
        fixture.__enter__ = Mock(return_value=(env, {"name": "RustFS"}))
        fixture.__exit__ = Mock(return_value=False)
        with patch.dict(s3.os.environ, {"AWS_SESSION_TOKEN": "old"}, clear=True), \
                patch.object(s3, "local_rustfs", return_value=fixture):
            with s3.shared_store():
                self.assertEqual(s3.os.environ["SLATEDB_S3_TEST_BUCKET"], "bucket")
                self.assertNotIn("AWS_SESSION_TOKEN", s3.os.environ)
            self.assertEqual(dict(s3.os.environ), {"AWS_SESSION_TOKEN": "old"})

    def test_member_flags_select_shared_mode_and_the_prefix(self):
        first, second = s3.unique_prefix("test"), s3.unique_prefix("test")
        self.assertNotEqual(first, second)
        flags = s3.shared_args(first, self.SERVER)
        self.assertEqual(flags[:2], ["--state-mode", "shared"])
        self.assertEqual(flags[flags.index("--s3-prefix") + 1], first)
        self.assertIn("--s3-allow-http", flags)
        secure = s3.shared_args(first, dict(self.SERVER, SLATEDB_S3_TEST_ENDPOINT="https://s3"))
        self.assertNotIn("--s3-allow-http", secure)
        aws = s3.shared_args(first, {"SLATEDB_S3_TEST_BUCKET": "bucket"})
        self.assertNotIn("--s3-endpoint", aws)
        with self.assertRaises(RuntimeError):
            s3.shared_args(first, {})

    def test_env_file_of_a_reused_server_needs_only_the_four_settings(self):
        with tempfile.TemporaryDirectory() as directory:
            settings, copy = Path(directory) / "rustfs.env", Path(directory) / "copy"
            child = f"import shutil; shutil.copy({str(settings)!r}, {str(copy)!r})"
            argv = ["rustfs_fixture.py", "--env-file", str(settings), "--", sys.executable, "-c",
                    child]
            server = dict(without_store(), **self.SERVER)
            server.pop("AWS_EC2_METADATA_DISABLED", None)
            with patch.dict(s3.os.environ, server, clear=True), patch("sys.argv", argv), \
                    patch.object(s3, "local_rustfs") as start:
                with self.assertRaises(SystemExit) as end:
                    s3.main()
            start.assert_not_called()
            self.assertEqual(end.exception.code, 0)
            self.assertIn("export AWS_EC2_METADATA_DISABLED=true\n", copy.read_text())
            self.assertIn("export SLATEDB_S3_TEST_BUCKET=bucket\n", copy.read_text())
            self.assertFalse(settings.exists())



class StaleReports(unittest.TestCase):
    """A failed RustFS start must replace an earlier passed report."""

    def run_launcher(self, name, argv, module=None):
        module = module or load(name)
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "report.json"
            binary = Path(directory) / "slatedb-etcd"
            binary.write_bytes(b"binary")
            output.write_text('{"passed": true, "complete": true}')
            argv = [name, *[str(arg).format(output=output, binary=binary) for arg in argv]]
            failure = RuntimeError("RustFS exited with status 1 before bucket creation")
            with patch.dict(os.environ, without_store(), clear=True), patch("sys.argv", argv), \
                    patch.object(s3, "local_rustfs", side_effect=failure), \
                    patch.object(tempfile, "mkdtemp", return_value=directory):
                with self.assertRaises(RuntimeError):
                    module.main()
            report = json.loads(output.read_text())
            self.assertFalse(report["passed"], name)
            self.assertTrue(report["pending"], name)

    def test_launchers_write_a_failed_report_before_rustfs_starts(self):
        compat = load("check-compat")
        compat.subprocess = Mock(check_output=Mock(return_value="go version test"))
        self.run_launcher("check-compat", ["--binary", "{binary}", "--output", "{output}"],
                          module=compat)
        self.run_launcher("check-robustness", ["--binary", "{binary}", "--output", "{output}"])
        self.run_launcher("harness", ["bench", "--binary", "{binary}", "--output", "{output}"])
        check = load("check-s3")
        check.local_rustfs = Mock(side_effect=RuntimeError("RustFS exited"))
        self.run_launcher("check-s3", ["--binary", "{binary}", "--output", "{output}"],
                          module=check)


class StartAction(unittest.TestCase):
    """The CI action names the failure even when the fixture logs nothing."""

    def start_script(self):
        lines = (SCRIPTS.parent / ".github/actions/rustfs/action.yml").read_text().splitlines()
        start = lines.index("    - name: Start the RustFS server")
        body = start + lines[start:].index("      run: |") + 1
        script = []
        for line in lines[body:]:
            if line.startswith("    - name:"):
                break
            script.append(line[8:])
        return "\n".join(script)

    def test_a_silent_failed_start_emits_an_error(self):
        with tempfile.TemporaryDirectory() as directory:
            fake = Path(directory) / "bin"
            fake.mkdir()
            python = fake / "python3"
            python.write_text("#!/bin/sh\nexit 1\n")
            python.chmod(0o755)
            env = dict(os.environ, RUNNER_TEMP=directory,
                       PATH=f"{fake}{os.pathsep}{os.environ['PATH']}")
            result = subprocess.run(["bash", "--noprofile", "--norc", "-eo", "pipefail", "-c",
                                     self.start_script()], env=env, cwd=SCRIPTS.parent, capture_output=True,
                                    text=True, timeout=60)
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn("::error title=RustFS::RustFS did not start in its last step: none",
                      result.stdout)


if __name__ == "__main__":
    unittest.main()
