"""Require the scanner to reject new private keys, including at the old fixture path."""

import importlib.util
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

SPEC = importlib.util.spec_from_file_location("secret_scan", Path(__file__).with_name("check-secrets.py"))
SCAN = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(SCAN)


class SecretScanTests(unittest.TestCase):
    def test_new_key_cannot_use_historical_exception(self):
        scanner = shutil.which(os.environ.get("GITLEAKS", "gitleaks"))
        self.assertIsNotNone(scanner, "Gitleaks is required for this negative control")
        with tempfile.TemporaryDirectory(prefix="slatedb-secret-control-") as directory:
            root = Path(directory)
            repository = root / "repo"
            key = repository / SCAN.FIXTURE
            key.parent.mkdir(parents=True)
            generated = subprocess.check_output(["openssl", "genrsa", "2048"],
                                                stderr=subprocess.DEVNULL)
            key.write_bytes(generated)
            key.chmod(0o600)
            ignore = repository / ".gitleaksignore"
            shutil.copyfile(SCAN.ROOT / ".gitleaksignore", ignore)
            empty = root / "empty.ignore"
            empty.write_text("")
            findings = SCAN.scan(scanner, "dir", repository, root, empty)
            self.assertTrue(any(item["RuleID"] == "private-key" for item in findings))
            subprocess.run(["git", "init", "-q", str(repository)], check=True)
            subprocess.run(["git", "-C", str(repository), "add", "."], check=True)
            subprocess.run(["git", "-C", str(repository), "-c", "user.name=Scan Test",
                            "-c", "user.email=scan@example.invalid", "-c", "commit.gpgsign=false",
                            "commit", "-qm", "Add a temporary negative control"], check=True)
            findings = SCAN.scan(scanner, "git", repository, root, ignore, "--all --full-history")
            self.assertTrue(any(item["RuleID"] == "private-key" for item in findings))


if __name__ == "__main__":
    unittest.main()
