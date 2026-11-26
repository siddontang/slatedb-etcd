#!/usr/bin/env python3
"""Check the RustFS fixture and its store settings."""

from io import BytesIO
import unittest
from unittest.mock import Mock, patch
from urllib.error import HTTPError, URLError

import rustfs_fixture as s3


class BucketReadiness(unittest.TestCase):
    def test_refusal_and_unavailable_then_ready(self):
        process = Mock()
        process.poll.return_value = None
        responses = [URLError(ConnectionRefusedError()), HTTPError("", 503, "", {}, BytesIO()), None]
        with patch.object(s3, "create_bucket", side_effect=responses) as create:
            with patch.object(s3.time, "sleep"):
                s3.wait_for_bucket(process, "endpoint", "bucket", "key", "secret")
        self.assertEqual(create.call_count, 3)

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

    def test_prefix_deletion_lists_and_deletes_until_no_key_remains(self):
        listing = (b'<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
                   b'<Contents><Key>p/a</Key></Contents><Contents><Key>p/b&amp;c</Key></Contents>'
                   b'</ListBucketResult>')
        empty = b'<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"/>'
        done = b'<DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"/>'
        calls = []

        def answer(endpoint, method, path, access, secret, query=None, body=b"", **options):
            calls.append((method, query, body, options.get("headers")))
            reply = {0: listing, 1: done}.get(len(calls) - 1, empty)
            response = Mock()
            response.__enter__ = Mock(return_value=Mock(read=Mock(return_value=reply)))
            response.__exit__ = Mock(return_value=False)
            return response

        with patch.object(s3, "signed_request", side_effect=answer):
            self.assertEqual(s3.delete_prefix("p", self.SERVER), 2)
        self.assertEqual(calls[0][1]["prefix"], "p/")
        self.assertEqual(calls[1][0], "POST")
        self.assertIn(b"<Key>p/b&amp;c</Key>", calls[1][2])
        self.assertIn("Content-MD5", calls[1][3])
        self.assertEqual(len(calls), 3)



if __name__ == "__main__":
    unittest.main()
