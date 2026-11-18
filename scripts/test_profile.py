"""Reject incomplete CPU profiles and handle both perf ACK formats."""

import importlib.util
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("profile_linux",
                                             Path(__file__).with_name("profile-linux.py"))
profile = importlib.util.module_from_spec(spec)
spec.loader.exec_module(profile)


class ProfileTests(unittest.TestCase):
    def test_control_acknowledgments(self):
        for ack in [b"ack\n", b"ack\n\0"]:
            with patch.object(profile.os, "write") as write, \
                 patch.object(profile.os, "read", return_value=ack), \
                 patch.object(profile.select, "select", return_value=([2], [], [])):
                profile.control(SimpleNamespace(poll=lambda: None), "enable", 1, 2)
                write.assert_called_once_with(1, b"enable\n")

    def test_dead_or_unresponsive_profiler_fails(self):
        with self.assertRaises(RuntimeError):
            profile.control(SimpleNamespace(poll=lambda: 1), "enable", 1, 2)
        for ready, ack in [([], b""), ([2], b"invalid")]:
            with patch.object(profile.os, "write"), \
                 patch.object(profile.os, "read", return_value=ack), \
                 patch.object(profile.select, "select", return_value=(ready, [], [])):
                with self.assertRaises(RuntimeError):
                    profile.control(SimpleNamespace(poll=lambda: None), "enable", 1, 2)

    def test_missing_lost_or_unresolved_samples_fail(self):
        valid = "# Total Lost Samples: 0\n# Samples: 3K of event 'cpu-clock'\n 1.20% [.] server\n"
        self.assertEqual(profile.validate_profile(valid)["samples_display"], "3K")
        for text in ["", valid.replace("3K", "0"), valid.replace("server", "0x1234"),
                     valid.replace("Lost Samples: 0", "Lost Samples: 1")]:
            with self.assertRaises(RuntimeError):
                profile.validate_profile(text)


if __name__ == "__main__":
    unittest.main()
