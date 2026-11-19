"""Check upstream workload coverage and result accounting."""

import importlib.util
import json
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

from upstream_source import INVENTORY, ROOT, checked_source


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


bench = load("official_bench", ROOT / "scripts/benchmark-upstream.py")
runner = load("official_go", ROOT / "scripts/check-upstream-go.py")
adapter = load("fixture", ROOT / "tests/upstream/service-process.py")
SECTION = "Summary:\n  Requests/sec: 8\n  0.1 [8]\t|x\n  99% in 0.001 secs.\n"


class BenchResults(unittest.TestCase):
    def test_mixed_checks_both_reports(self):
        report = "Total Read Ops: 8\n" + SECTION + "Total Write Ops: 8\n" + SECTION
        self.assertTrue(bench.parse_report(report, ("mixed", 16))["passed"])
        partial = report.replace("Total Read Ops: 8", "Total Read Ops: 12")
        self.assertFalse(bench.parse_report(partial, ("mixed", 20))["passed"])
        error = "Error distribution:\n  [4]\trpc error: code = Unavailable\n"
        result = bench.parse_report(partial.replace("Total Write", error + "Total Write"),
                                    ("mixed", 20))
        self.assertFalse(result["passed"])
        self.assertEqual(result["sections"][0]["errors"][0]["count"], 4)

    def test_watch_requires_both_histograms(self):
        self.assertFalse(bench.parse_report(SECTION, (8, 8))["passed"])
        self.assertFalse(bench.parse_report(SECTION + SECTION, (8, 16))["passed"])
        self.assertTrue(bench.parse_report(SECTION + SECTION, (8, 8))["passed"])

    def test_no_success_and_partial_result_fail(self):
        for output in ("", "Error distribution:\n  [8]\tfailure\n", SECTION):
            self.assertFalse(bench.parse_report(output, (16,))["passed"])

    def test_all_failed_reads_keep_the_error(self):
        report = ("Total Read Ops: 8\nError distribution:\n  [8]\trpc error\n"
                  "Total Write Ops: 8\n" + SECTION)
        result = bench.parse_report(report, ("mixed", 16))
        self.assertFalse(result["passed"])
        reads, writes = result["sections"]
        self.assertEqual((reads["operation"], reads["successes"], reads["errors"][0]["count"]),
                         ("Read", 0, 8))
        self.assertEqual(writes["successes"], 8)

    def test_heatmap_unique_cells(self):
        cells = list(bench.heatmap_cases())
        self.assertEqual(len({c.name for c in cells}), 392)

    def test_fixture_rejects_unknown_semantics(self):
        with self.assertRaises(ValueError):
            adapter.local_url("unix://localhost:21379")
        with self.assertRaises(ValueError):
            adapter.local_url("http://local.invalid:21379")


class GoResults(unittest.TestCase):
    def test_selection_across_packages(self):
        events = [
            {"Action": "output", "Package": "empty", "Output": "testing: warning: no tests to run"},
            {"Action": "run", "Package": "kv", "Test": "TestKV"},
            {"Action": "pass", "Package": "kv", "Test": "TestKV"},
        ]
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            path = root / "events"
            for package, matched in (("other", True), ("kv", False)):
                events[0]["Package"] = package
                path.write_text("".join(json.dumps(event) + "\n" for event in events))
                result = runner.run_go(["cat", path], root, root / "log.gz", "TestKV", "unit")
                self.assertEqual(result["passed"], matched)


class SourceCoverage(unittest.TestCase):
    def test_suite_matches_pinned_checkout(self):
        source = checked_source(ROOT / "reference/etcd")
        files = subprocess.check_output(["git", "ls-files", "*_test.go"], cwd=source,
                                        text=True).splitlines()
        actual = {(f, name) for f in files for name in re.findall(
            r"^func ((?:Test|Fuzz|Benchmark|Example)\w*)\s*\(", (source / f).read_text(), re.M)}
        inventory = json.loads(INVENTORY.read_text())["tests"]
        self.assertEqual(actual, {(row["file"], row["name"]) for row in inventory})
        cli = build_commands(source)
        self.assertEqual(cli, {case.command.split(" --")[0].split()[0]
                               if case.name != "mvcc-put" else "mvcc put"
                               for case in bench.cases(False)})
        options = mock.Mock(benchtime="1x", fuzztime="1x")
        packages = {package for package, _ in runner.go_cases(source, "go-bench", options)}
        self.assertEqual(packages, {str(Path(f).parent) for f, n in actual
                                    if n.startswith("Benchmark")})


def build_commands(source):
    commands = set()
    for path in (source / "tools/benchmark/cmd").glob("*.go"):
        code = path.read_text()
        if "RootCmd.AddCommand(" in code and path.name != "mvcc.go":
            commands.add(re.search(r'Use:\s+"([^ "]+)', code)[1])
        if "mvccCmd.AddCommand(" in code:
            commands.add("mvcc " + re.search(r'Use:\s+"([^ "]+)', code)[1])
    return commands


if __name__ == "__main__":
    unittest.main()
