#!/usr/bin/env python3
"""Check S3 startup retries before the recovery tests."""

import importlib.util
from pathlib import Path
import unittest
from unittest.mock import Mock, patch
from urllib.error import HTTPError, URLError


spec = importlib.util.spec_from_file_location("s3", Path(__file__).with_name("check-s3.py"))
s3 = importlib.util.module_from_spec(spec)
spec.loader.exec_module(s3)


class BucketReadiness(unittest.TestCase):
    def test_refusal_and_unavailable_then_ready(self):
        process = Mock()
        process.poll.return_value = None
        responses = [URLError(ConnectionRefusedError()), HTTPError("", 503, "", {}, None), None]
        with patch.object(s3, "create_bucket", side_effect=responses) as create:
            with patch.object(s3.time, "sleep"):
                s3.wait_for_bucket(process, "endpoint", "bucket", "key", "secret")
        self.assertEqual(create.call_count, 3)

    def test_failed_signature_stops_startup(self):
        process = Mock()
        process.poll.return_value = None
        with patch.object(s3, "create_bucket", side_effect=HTTPError("", 403, "", {}, None)):
            with self.assertRaises(HTTPError) as error:
                s3.wait_for_bucket(process, "endpoint", "bucket", "key", "secret")
        self.assertEqual(error.exception.code, 403)


if __name__ == "__main__":
    unittest.main()
