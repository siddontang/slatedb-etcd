"""Check path removal without changing test evidence."""

import copy
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from reporting import ROOT, portable


class ReportTests(unittest.TestCase):
    def test_nested_paths_and_embedded_failure_text(self):
        report = {"passed": False, "hash": "a" * 64, "count": 387,
                  "runs": [{"path": str(ROOT / "tmp/run"),
                            "trace": str(Path(tempfile.gettempdir()) / "trace.json"),
                            "error": "failed at /home/example/project/src.rs:12"}]}
        before = copy.deepcopy(report)
        result = portable(report)
        self.assertEqual(report, before)
        self.assertFalse(result["passed"])
        self.assertEqual(result["hash"], report["hash"])
        self.assertEqual(result["count"], 387)
        self.assertEqual(result["runs"][0]["path"], "<repo>/tmp/run")
        self.assertEqual(result["runs"][0]["trace"], "<tmp>/trace.json")
        self.assertEqual(result["runs"][0]["error"], "failed at <home>/project/src.rs:12")

    def test_other_macos_temporary_roots_are_removed(self):
        root = "/var/" + "folders/xx/example/T"
        self.assertEqual(portable(root + "/trace.json"), "<tmp>/trace.json")
        self.assertEqual(portable("/private" + root + "/trace.json"), "<tmp>/trace.json")

    def test_relative_paths_and_public_urls_remain(self):
        values = ["results/dst.json", "https://github.com/etcd-io/etcd", None, 0, True]
        self.assertEqual(portable(values), values)

    def test_linux_temp_root_does_not_rewrite_path_components(self):
        with patch("reporting.tempfile.gettempdir", return_value="/tmp"):
            values = [str(ROOT / "tmp/run"), "<repo>/tmp/run", "other/tmp/run",
                      "https://example.com/tmp/run", "trace=/tmp/run"]
            expected = ["<repo>/tmp/run", "<repo>/tmp/run", "other/tmp/run",
                        "https://example.com/tmp/run", "trace=<tmp>/run"]
            self.assertEqual(portable(values), expected)
            self.assertEqual(portable(expected), expected)


if __name__ == "__main__":
    unittest.main()
