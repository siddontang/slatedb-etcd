#!/usr/bin/env python3
"""Check S3 startup retries before the recovery tests."""

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


if __name__ == "__main__":
    unittest.main()
