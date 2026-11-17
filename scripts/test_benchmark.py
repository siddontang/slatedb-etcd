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


class BenchmarkTests(unittest.TestCase):
    def test_cpu_formats_and_process_order(self):
        self.assertEqual(benchmark.parse_cpu("03:04.50"), 184.5)
        self.assertEqual(benchmark.parse_cpu("02:03:04"), 7384)
        self.assertEqual(benchmark.parse_cpu("1-02:03:04"), 93784)
        processes = [SimpleNamespace(pid=10), SimpleNamespace(pid=20)]
        result = SimpleNamespace(stdout="20 00:01.20 100\n10 02:03 200\n")
        with patch.object(benchmark.subprocess, "run", return_value=result):
            self.assertEqual(benchmark.resources(processes), [(123, 204800), (1.2, 102400)])

    def test_missing_process_does_not_become_zero_usage(self):
        result = SimpleNamespace(stdout="10 00:01 100\n")
        with patch.object(benchmark.subprocess, "run", return_value=result):
            with self.assertRaises(RuntimeError):
                benchmark.resources([SimpleNamespace(pid=10), SimpleNamespace(pid=20)])

    def test_dead_process_cannot_pass_ready_gate(self):
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaises(RuntimeError):
                benchmark.wait_file(Path(directory) / "ready",
                                    [SimpleNamespace(poll=lambda: 1)], float("inf"))

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
