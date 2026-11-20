"""Check that history validation rejects deleted data and invalid commit metadata."""

import importlib.util
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

SPEC = importlib.util.spec_from_file_location("history", Path(__file__).with_name("check-history.py"))
HISTORY = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(HISTORY)
MISSING = "missing owner sign-off or author sign-off with a GitHub public email"
CONTRIBUTOR_EMAIL = "1+contributor@users.noreply.github.com"
CONTRIBUTOR_SIGN_OFF = b"Signed-off-by: contributor <" + CONTRIBUTOR_EMAIL.encode() + b">"


class HistoryTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix="slatedb-history-control-")
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.git("init", "-q")
        self.git("config", "user.name", "siddontang")
        self.git("config", "user.email", "1080370+siddontang@users.noreply.github.com")
        self.git("config", "commit.gpgsign", "false")

    def git(self, *args, data=None):
        return subprocess.check_output(["git", *args], cwd=self.root, input=data,
                                       stderr=subprocess.DEVNULL)

    def commit(self, message=None):
        if message is None:
            message = b"Test metadata\n\n" + HISTORY.SIGN_OFF + b"\n"
        self.git("add", ".")
        self.git("commit", "-q", "--allow-empty", "--file=-", data=message)

    def contributor(self):
        self.git("config", "user.name", "contributor")
        self.git("config", "user.email", CONTRIBUTOR_EMAIL)

    def errors(self):
        return [item["error"] for item in HISTORY.audit(self.root)[1]]

    def test_public_identity_and_owner_sign_off_pass(self):
        self.commit()
        self.assertEqual(self.errors(), [])

    def test_missing_sign_off_fails(self):
        self.commit(b"Missing sign-off\n")
        self.assertIn(MISSING, self.errors())

    def test_maintainer_sign_off_passes(self):
        self.commit(b"Maintainer change\n\n" + HISTORY.MAINTAINER_SIGN_OFFS[1] + b"\n")
        self.assertEqual(self.errors(), [])

    def test_two_maintainer_sign_offs_fail(self):
        self.commit(b"Twice\n\n" + b"\n".join(HISTORY.MAINTAINER_SIGN_OFFS) + b"\n")
        self.assertIn("missing or repeated owner sign-off", self.errors())

    def web_merge(self, email):
        self.commit()
        self.git("switch", "-q", "-c", "feature")
        self.commit(b"Feature\n\n" + HISTORY.SIGN_OFF + b"\n")
        self.git("switch", "-q", "-")
        self.commit(b"Main\n\n" + HISTORY.SIGN_OFF + b"\n")
        env = dict(os.environ, GIT_AUTHOR_NAME="ShuNing", GIT_AUTHOR_EMAIL=email,
                   GIT_COMMITTER_NAME="GitHub", GIT_COMMITTER_EMAIL="noreply@github.com")
        subprocess.check_output(["git", "merge", "-q", "--no-ff", "-m", "Merge pull request #1",
                                 "feature"], cwd=self.root, env=env, stderr=subprocess.DEVNULL)
        parents = self.git("rev-list", "--parents", "-1", "HEAD").split()
        self.assertEqual(len(parents), 3)

    def test_maintainer_web_merge_passes(self):
        self.web_merge("nolouch@gmail.com")
        self.assertEqual(self.errors(), [])

    def squash_merge(self, email):
        message = b"Squashed (#1)\n\n" + b"\n\n".join([HISTORY.MAINTAINER_SIGN_OFFS[1]] * 3) + b"\n"
        env = dict(os.environ, GIT_AUTHOR_NAME="ShuNing", GIT_AUTHOR_EMAIL=email,
                   GIT_COMMITTER_NAME="GitHub", GIT_COMMITTER_EMAIL="noreply@github.com")
        subprocess.check_output(["git", "commit", "-q", "--allow-empty", "--file=-"],
                                cwd=self.root, env=env, input=message, stderr=subprocess.DEVNULL)

    def test_maintainer_squash_merge_with_repeated_sign_offs_passes(self):
        self.squash_merge("nolouch@gmail.com")
        self.assertEqual(self.errors(), [])

    def test_other_squash_merge_with_private_email_fails(self):
        self.squash_merge("someone@example.com")
        self.assertIn("author must use a GitHub public email", self.errors())

    def test_maintainer_local_commit_with_repeated_sign_offs_fails(self):
        self.commit(b"Twice\n\n" + b"\n".join([HISTORY.MAINTAINER_SIGN_OFFS[1]] * 2) + b"\n")
        self.assertIn("missing or repeated owner sign-off", self.errors())

    def test_other_web_merge_with_private_email_fails(self):
        self.web_merge("someone@example.com")
        errors = self.errors()
        self.assertIn("author must use a GitHub public email", errors)
        self.assertIn(MISSING, errors)

    def test_body_only_owner_sign_off_fails(self):
        self.commit(HISTORY.SIGN_OFF + b"\n\nLater prose\n")
        self.assertIn("missing or repeated owner sign-off", self.errors())

    def test_range_checks_only_new_commits(self):
        self.commit(b"Old commit without sign-off\n")
        base = self.git("rev-parse", "HEAD").strip().decode()
        self.commit()
        counts, failures = HISTORY.audit(self.root, (base + "..HEAD",))
        self.assertEqual((counts["commits"], failures), (1, []))
        self.assertIn(MISSING, self.errors())

    def test_range_rejects_new_commit_without_sign_off(self):
        self.commit()
        base = self.git("rev-parse", "HEAD").strip().decode()
        self.commit(b"New commit without sign-off\n")
        failures = HISTORY.audit(self.root, (base + "..HEAD",))[1]
        self.assertIn(MISSING, [item["error"] for item in failures])

    def test_contributor_sign_off_with_public_email_passes(self):
        self.contributor()
        self.commit(b"Contributed change\n\n" + CONTRIBUTOR_SIGN_OFF + b"\n")
        self.assertEqual(self.errors(), [])

    def test_contributor_sign_off_must_match_author_once(self):
        other = b"Signed-off-by: other <2+other@users.noreply.github.com>"
        messages = [
            b"Wrong person\n\n" + other + b"\n",
            b"Twice\n\n" + CONTRIBUTOR_SIGN_OFF + b"\n" + other + b"\n",
            CONTRIBUTOR_SIGN_OFF + b"\n\nBody only\n",
        ]
        for message in messages:
            with self.subTest(message=message):
                self.contributor()
                self.commit(message)
                self.assertIn(MISSING, self.errors())

    def test_contributor_sign_off_with_private_email_fails(self):
        self.contributor()
        self.commit(b"Private\n\nSigned-off-by: contributor <person@example.invalid>\n")
        errors = self.errors()
        self.assertIn(MISSING, errors)
        self.assertIn("personal email outside the owner sign-off", errors)

    def test_private_author_and_message_email_fail(self):
        self.git("config", "user.email", "person@example.invalid")
        self.commit(b"Contact person@example.invalid\n\n" + HISTORY.SIGN_OFF + b"\n")
        errors = self.errors()
        self.assertIn("author must use a GitHub public email", errors)
        self.assertIn("committer must use a GitHub public email", errors)
        self.assertIn("personal email outside the owner sign-off", errors)

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

    def test_private_tag_message_fails(self):
        self.commit()
        self.git("tag", "-a", "message-tag", "-m", "Contact person@example.invalid")
        self.assertIn("personal email in tag message", self.errors())

    def test_private_tag_identity_fails(self):
        self.commit()
        self.git("config", "user.email", "person@example.invalid")
        self.git("tag", "-a", "old-tag", "-m", "Test tag")
        self.assertIn("tagger must use a GitHub public email", self.errors())


if __name__ == "__main__":
    unittest.main()
