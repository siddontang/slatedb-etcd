"""Check resource parsing and failure handling in the benchmark controller."""

import importlib.util
from pathlib import Path
from types import SimpleNamespace
import subprocess
import tempfile
import unittest
from unittest.mock import patch

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
            self.assertEqual(measured["client_cpu_seconds"], 10)
            self.assertEqual(measured["peak_process_rss_bytes"], members * 300)
            self.assertEqual(measured["peak_client_rss_bytes"], 900)

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

    def test_failed_build_replaces_old_passed_report(self):
        import json
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "report.json"
            output.write_text('{"passed": true, "complete": true}')
            with patch("sys.argv", ["benchmark", "--output", str(output)]), \
                 patch.object(benchmark.subprocess, "run",
                              side_effect=subprocess.CalledProcessError(1, "go")), \
                 patch.object(benchmark.subprocess, "check_output", return_value="test cpu"):
                with self.assertRaises(subprocess.CalledProcessError):
                    benchmark.main()
            report = json.loads(output.read_text())
            self.assertFalse(report["passed"])
            self.assertFalse(report["complete"])
            self.assertEqual(report["samples"], [])


if __name__ == "__main__":
    unittest.main()
