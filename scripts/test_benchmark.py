"""Check resource parsing and failure handling in the benchmark controller."""

from contextlib import contextmanager
import importlib.util
from pathlib import Path
from types import SimpleNamespace
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import rustfs_fixture

spec = importlib.util.spec_from_file_location("benchmark", Path(__file__).with_name("benchmark-etcd.py"))
benchmark = importlib.util.module_from_spec(spec)
spec.loader.exec_module(benchmark)
spec = importlib.util.spec_from_file_location(
    "summary", Path(__file__).with_name("summarize-benchmark.py"))
summary = importlib.util.module_from_spec(spec)
spec.loader.exec_module(summary)


class BenchmarkTests(unittest.TestCase):
    def test_server_resources_exclude_the_client_for_each_cluster_size(self):
        for members in (1, 3):
            before = [(2, 200)] * (members + 1)
            after = [(3, 300)] * members + [(12, 900)]
            memory = [[row[1] for row in values] for values in (before, after)]
            measured = benchmark.measured_resources(before, after, memory, members, 2, 100)
            self.assertEqual(measured["process_cpu_seconds"], members)
            self.assertEqual(measured["member_resources"], [
                dict(member=i + 1, cpu_seconds=1, peak_rss_bytes=300) for i in range(members)])
            self.assertEqual(measured["client_cpu_seconds"], 10)
            self.assertEqual(measured["peak_process_rss_bytes"], members * 300)
            self.assertEqual(measured["peak_client_rss_bytes"], 900)

    def test_baseline_subset_keeps_all_reference_and_service_cases(self):
        cases = benchmark.workload_cases(["baseline", "service", "etcd"], [1, 8, 32],
                                         [0, 50, 100], [8])
        self.assertEqual(len(cases), 21)
        self.assertEqual(len(set(cases)), 21)
        for kind in ["service", "etcd"]:
            self.assertEqual(sum(case[0] == kind for case in cases), 9)
        self.assertEqual([case for case in cases if case[0] == "baseline"],
                         [("baseline", 8, percent) for percent in [0, 50, 100]])

    def test_cluster_addresses_are_distinct(self):
        ports = benchmark.addresses(6)
        self.assertEqual(len(ports), len(set(ports)))

    def test_cpu_formats_and_process_order(self):
        self.assertEqual(benchmark.parse_cpu("03:04.50"), 184.5)
        self.assertEqual(benchmark.parse_cpu("02:03:04"), 7384)
        self.assertEqual(benchmark.parse_cpu("1-02:03:04"), 93784)
        processes = [SimpleNamespace(pid=10), SimpleNamespace(pid=20)]
        result = SimpleNamespace(stdout="20 00:01.20 100\n10 02:03 200\n")
        with patch.object(benchmark.platform, "system", return_value="Darwin"), \
             patch.object(benchmark.subprocess, "run", return_value=result):
            self.assertEqual(benchmark.resources(processes), [(123, 204800), (1.2, 102400)])

    def test_linux_subsecond_cpu_and_command_parentheses(self):
        fields = ["0"] * 22
        fields[0], fields[11], fields[12], fields[21] = "S", "3", "2", "7"
        row = "10 (a command) with spaces) " + " ".join(fields)
        with patch.object(benchmark.platform, "system", return_value="Linux"), \
             patch.object(benchmark.os, "sysconf", side_effect=[100, 4096]), \
             patch.object(Path, "read_text", return_value=row):
            self.assertEqual(benchmark.resources([SimpleNamespace(pid=10)]), [(0.05, 28672)])

    def test_linux_storage_io_uses_bytes_charged_by_the_kernel(self):
        sample = "rchar: 9000\nwchar: 8000\nread_bytes: 4096\nwrite_bytes: 8192\n"
        with patch.object(Path, "read_text", return_value=sample):
            self.assertEqual(benchmark.linux_io([SimpleNamespace(pid=5)]), [(4096, 8192)])

    def test_missing_process_does_not_become_zero_usage(self):
        result = SimpleNamespace(stdout="10 00:01 100\n")
        with patch.object(benchmark.platform, "system", return_value="Darwin"), \
             patch.object(benchmark.subprocess, "run", return_value=result):
            with self.assertRaises(RuntimeError):
                benchmark.resources([SimpleNamespace(pid=10), SimpleNamespace(pid=20)])

    def test_dead_process_cannot_pass_ready_gate(self):
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaises(RuntimeError):
                benchmark.wait_file(Path(directory) / "ready",
                                    [SimpleNamespace(poll=lambda: 1)], float("inf"))

    def test_partial_and_failed_reports_have_no_summary(self):
        for report in [{}, {"schema_version": 2, "passed": True, "complete": False},
                       {"schema_version": 2, "passed": False, "complete": True}]:
            with self.assertRaises(ValueError):
                summary.summarize(report)

    def test_summary_keeps_repeat_variation_and_rejects_duplicates(self):
        rows = []
        for repeat, throughput in enumerate([10, 100, 30]):
            rows.append(dict(backend="service", concurrency=8, read_percent=50,
                             keys=10, value_bytes=256, requests=100, successes=100,
                             errors={}, passed=True, repeat=repeat,
                             successful_ops_per_second=throughput,
                             latency_ms={"p50": 1, "p99": 5},
                             server_cpu_microseconds_per_success=20,
                             average_server_cpu_cores=1, peak_process_rss_bytes=1024**2,
                             client_cpu_seconds=0.1))
        report = dict(schema_version=2, complete=True, passed=True, expected_samples=3,
                      samples=rows)
        group = summary.summarize(report)["groups"][0]
        self.assertEqual(group["metrics"]["ops_per_second"],
                         {"median": 30, "min": 10, "max": 100})
        rows[-1] = rows[0]
        with self.assertRaises(ValueError):
            summary.summarize(report)

    def test_summary_separates_one_and_three_members(self):
        sample = dict(backend="service", concurrency=8, read_percent=100,
                      keys=10, value_bytes=256, requests=100, successes=100,
                      errors={}, passed=True, repeat=0, successful_ops_per_second=100,
                      latency_ms={"p50": 1, "p99": 5}, server_cpu_microseconds_per_success=20,
                      average_server_cpu_cores=1, peak_process_rss_bytes=1024**2,
                      client_cpu_seconds=0.1)
        report = dict(schema_version=2, complete=True, passed=True, expected_samples=2,
                      samples=[dict(sample, members=1), dict(sample, members=3)])
        groups = summary.summarize(report)["groups"]
        self.assertEqual({group["members"] for group in groups}, {1, 3})

    def test_failed_build_replaces_old_passed_report(self):
        import json
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "report.json"
            output.write_text('{"passed": true, "complete": true}')
            # The etcd backend alone needs no RustFS, so the build is the first step that fails.
            with patch("sys.argv", ["benchmark", "--backends", "etcd", "--output", str(output)]), \
                 patch.object(benchmark.subprocess, "run",
                              side_effect=subprocess.CalledProcessError(1, "go")), \
                 patch.object(benchmark.subprocess, "check_output", return_value="test cpu"):
                with self.assertRaises(subprocess.CalledProcessError):
                    benchmark.main()
            report = json.loads(output.read_text())
            self.assertFalse(report["passed"])
            self.assertFalse(report["complete"])
            self.assertEqual(report["samples"], [])

    def run_failed_start(self, argv):
        import json
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "report.json"
            output.write_text('{"passed": true, "complete": true}')
            environment = {k: v for k, v in benchmark.os.environ.items()
                           if k not in rustfs_fixture.STORE_ENV}
            failure = RuntimeError("RustFS exited with status 1 before bucket creation")
            with patch.dict(benchmark.os.environ, environment, clear=True), \
                 patch("sys.argv", ["benchmark", *argv, "--output", str(output)]), \
                 patch.object(rustfs_fixture, "local_rustfs", side_effect=failure) as start, \
                 patch.object(benchmark.subprocess, "run",
                              side_effect=subprocess.CalledProcessError(1, "go")), \
                 patch.object(benchmark.subprocess, "check_output", return_value="test cpu"):
                with self.assertRaises((RuntimeError, subprocess.CalledProcessError)):
                    benchmark.main()
            return json.loads(output.read_text()), start

    def test_failed_rustfs_start_replaces_old_passed_report(self):
        report, start = self.run_failed_start([])
        start.assert_called_once()
        self.assertFalse(report["passed"])
        self.assertTrue(report["pending"])

    def test_etcd_alone_starts_no_rustfs(self):
        report, start = self.run_failed_start(["--backends", "etcd"])
        start.assert_not_called()
        self.assertFalse(report["passed"])

    def test_s3_options_require_a_bucket(self):
        for argv in (["--s3-endpoint", "http://127.0.0.1:9"], ["--s3-allow-http"]):
            with patch("sys.argv", ["benchmark", *argv]), \
                 patch.object(rustfs_fixture, "local_rustfs") as start, \
                 patch("sys.stderr"):
                with self.assertRaises(SystemExit) as end:
                    benchmark.main()
            self.assertEqual(end.exception.code, 2)
            start.assert_not_called()

    def service_args(self, args):
        """Return the arguments of the first service member. Start no process."""
        commands = []

        def start(command, **_):
            commands.append(command)
            raise OSError("no process in this test")

        defaults = dict(members=3, snapshot_entries=10, quota_bytes=1, read_requests=None,
                        requests=1, keys=1, value_size=1, warmup=0, seed=0, timeout_seconds=1)
        with patch.object(benchmark.subprocess, "Popen", side_effect=start):
            sample = benchmark.run_sample(SimpleNamespace(**dict(defaults, **vars(args))),
                                          Path("tools"), Path("slatedb-etcd"), "service", 1, 0, 0)
        self.assertIn("no process", sample["error"])
        return commands[0]

    def test_http_opt_in_follows_the_caller_or_the_fixture(self):
        caller = dict(s3_bucket="bucket", s3_endpoint="http://127.0.0.1:9")
        command = self.service_args(SimpleNamespace(**caller, s3_allow_http=False))
        self.assertIn("--s3-endpoint", command)
        self.assertNotIn("--s3-allow-http", command)
        command = self.service_args(SimpleNamespace(**caller, s3_allow_http=True))
        self.assertIn("--s3-allow-http", command)
        server = {"SLATEDB_S3_TEST_ENDPOINT": "http://127.0.0.1:9",
                  "SLATEDB_S3_TEST_BUCKET": "bucket", "AWS_ACCESS_KEY_ID": "key",
                  "AWS_SECRET_ACCESS_KEY": "secret", "AWS_EC2_METADATA_DISABLED": "true"}
        @contextmanager
        def own():
            yield server, {"name": "RustFS"}

        environment = {k: v for k, v in benchmark.os.environ.items()
                       if k not in rustfs_fixture.STORE_ENV}
        with tempfile.TemporaryDirectory() as directory:
            report = Path(directory) / "report.json"
            # The fixture starts its own server, or reuses the exported one of a CI step.
            for exported in ({}, server):
                args = SimpleNamespace(members=3, rustfs=False, s3_bucket=None, s3_endpoint=None,
                                       s3_allow_http=False)
                with patch.dict(benchmark.os.environ, dict(environment, **exported), clear=True), \
                     patch.object(rustfs_fixture, "local_rustfs", side_effect=own) as start:
                    with benchmark.object_store(args, ["service"], report, {}):
                        command = self.service_args(args)
                self.assertEqual(start.called, not exported)
                self.assertIn("--s3-allow-http", command)
                self.assertIn("--s3-bucket", command)


if __name__ == "__main__":
    unittest.main()
