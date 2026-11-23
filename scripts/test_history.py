"""Check that history validation rejects workstation paths and unsigned commits."""

import importlib.util
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

SPEC = importlib.util.spec_from_file_location("history", Path(__file__).with_name("check-history.py"))
HISTORY = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(HISTORY)
MISSING = "missing Signed-off-by trailer"
SIGN_OFF = b"Signed-off-by: tester <tester@example.com>"
GITHUB = dict(os.environ, GIT_COMMITTER_NAME="GitHub", GIT_COMMITTER_EMAIL="noreply@github.com")


class HistoryTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix="slatedb-history-control-")
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.git("init", "-q")
        self.git("config", "user.name", "tester")
        self.git("config", "user.email", "tester@example.com")
        self.git("config", "commit.gpgsign", "false")

    def git(self, *args, data=None, env=None):
        return subprocess.check_output(["git", *args], cwd=self.root, input=data, env=env,
                                       stderr=subprocess.DEVNULL)

    def commit(self, message=b"Change\n\n" + SIGN_OFF + b"\n", env=None):
        self.git("add", ".")
        self.git("commit", "-q", "--allow-empty", "--file=-", data=message, env=env)

    def errors(self, *revisions):
        return [item["error"] for item in HISTORY.audit(self.root, revisions or ("--all",))[1]]

    def test_signed_commit_with_any_email_passes(self):
        self.commit()
        self.assertEqual(self.errors(), [])

    def test_missing_sign_off_fails(self):
        self.commit(b"Missing sign-off\n")
        self.assertIn(MISSING, self.errors())

    def test_sign_off_in_body_only_fails(self):
        self.commit(SIGN_OFF + b"\n\nLater prose\n")
        self.assertIn(MISSING, self.errors())

    def test_github_squash_merge_without_sign_off_passes(self):
        self.commit(b"Squashed (#1)\n", env=GITHUB)
        self.assertEqual(self.errors(), [])

    def test_merge_commit_without_sign_off_passes(self):
        self.commit()
        self.git("switch", "-q", "-c", "feature")
        self.commit()
        self.git("switch", "-q", "-")
        self.commit()
        self.git("merge", "-q", "--no-ff", "-m", "Merge feature", "feature")
        self.assertEqual(self.errors(), [])

    def test_range_checks_only_new_commits(self):
        self.commit(b"Old commit without sign-off\n")
        base = self.git("rev-parse", "HEAD").strip().decode()
        self.commit()
        self.assertEqual(self.errors(base + "..HEAD"), [])
        self.assertIn(MISSING, self.errors())

    def test_removed_home_path_still_fails(self):
        path = self.root / "old-report.txt"
        path.write_text("/" + "Users/" + "private-person/project\n")
        self.commit()
        path.unlink()
        self.commit()
        self.assertIn("workstation home path", self.errors())

    def test_non_default_branch_is_checked(self):
        self.commit()
        first = self.git("rev-parse", "HEAD").decode().strip()
        self.git("checkout", "-qb", "old-branch")
        self.commit(b"Unsigned old branch\n")
        self.git("checkout", "-q", first)
        self.assertIn(MISSING, self.errors())

    def test_temporary_and_windows_paths_fail(self):
        values = ["/var/" + "folders/xx/example/T/trace",
                  "C:" + chr(92) + "Users" + chr(92) + "example"]
        for value in values:
            self.assertTrue(HISTORY.path_errors(value.encode()))

    def test_tag_with_home_path_fails(self):
        self.commit()
        self.git("tag", "-a", "path-tag", "-m", "/" + "Users/" + "private-person/trace")
        self.assertIn("workstation home path", self.errors())


if __name__ == "__main__":
    unittest.main()
