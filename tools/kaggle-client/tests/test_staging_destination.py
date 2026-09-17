"""Regression coverage for the fixed staging storage destination allowlist."""
import importlib.util
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[3]
SPEC = importlib.util.spec_from_file_location("relay_staging_destination", ROOT / "internal/provider/kaggle/staging.py")
staging = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(staging)


class StagingDestinationTests(unittest.TestCase):
    def test_googleapis_upload_destination_is_path_scoped(self):
        accepted = (
            "https://storage.googleapis.com/bucket/object?signature=fixture",
            "https://www.googleapis.com/upload/storage/v1/b/kaggle-data-sets/o?uploadType=resumable&upload_id=fixture",
            "https://www.googleapis.com:443/upload/storage/v1/b/kaggle-data-sets/o?uploadType=resumable&upload_id=fixture",
        )
        for url in accepted:
            with self.subTest(url=url):
                self.assertEqual(staging.signed_url(url), url)

        rejected = (
            "http://www.googleapis.com/upload/storage/v1/b/kaggle-data-sets/o?uploadType=resumable",
            "https://www.googleapis.com/storage/v1/b/kaggle-data-sets/o",
            "https://www.googleapis.com/upload/drive/v3/files",
            "https://www.googleapis.com/upload/storage/v1/b",
            "https://www.googleapis.com.evil.invalid/upload/storage/v1/b/kaggle-data-sets/o",
            "https://user:secret@www.googleapis.com/upload/storage/v1/b/kaggle-data-sets/o",
            "https://www.googleapis.com:444/upload/storage/v1/b/kaggle-data-sets/o",
            "https://www.googleapis.com/upload/storage/v1/b/kaggle-data-sets/o#fragment",
        )
        for url in rejected:
            with self.subTest(url=url):
                with self.assertRaises(ValueError):
                    staging.signed_url(url)


if __name__ == "__main__":
    unittest.main()
