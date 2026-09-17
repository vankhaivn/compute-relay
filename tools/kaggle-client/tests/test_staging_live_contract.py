"""Regressions for live Kaggle staging metadata and post-create visibility."""
import importlib.util
from types import SimpleNamespace
import unittest
from unittest import mock

import requests
from test_preflight import ROOT

SPEC = importlib.util.spec_from_file_location(
    "relay_staging_live_contract", ROOT / "internal/provider/kaggle/staging.py")
staging = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(staging)


def request(license_name="other"):
    return dict(owner="fixture_user", slug="crs-" + "a" * 40,
                marker_sha256="b" * 64, license=license_name)


def http_error(status):
    response = requests.Response()
    response.status_code = status
    response._content = (b'{"error":{"code":403,"message":"Permission '
                         b"'datasets.get' was denied\",\"status\":\"PERMISSION_DENIED\"}}")
    response._content_consumed = True
    return requests.HTTPError(response=response)


class StagingLiveContractTests(unittest.TestCase):
    def test_provider_license_display_names_are_mapped_exactly(self):
        for requested, returned in (("other", "Other (specified in description)"),
                                    ("unknown", "Unknown")):
            with self.subTest(requested=requested):
                r = request(requested)
                meta = SimpleNamespace(id=123, ref=r["owner"] + "/" + r["slug"],
                                       current_version_number=1,
                                       description=staging.description(r), license_name=returned)
                self.assertEqual(staging.matching_dataset(meta, r), 123)
        r = request("other")
        meta = SimpleNamespace(id=123, ref=r["owner"] + "/" + r["slug"],
                               current_version_number=1,
                               description=staging.description(r), license_name="Other")
        with self.assertRaises(staging.IdentityError):
            staging.matching_dataset(meta, r)

    @unittest.skipUnless(importlib.util.find_spec("kagglesdk"), "pinned SDK is exercised in client CI")
    def test_post_create_missing_permission_is_pending_not_absence(self):
        r = request()
        with mock.patch.object(staging, "dataset_call", side_effect=http_error(403)):
            self.assertEqual(staging.observe(None, None, r), staging.empty_result("pending"))
        with mock.patch.object(staging, "dataset_call", side_effect=http_error(404)):
            with self.assertRaises(requests.HTTPError):
                staging.observe(None, None, r)


if __name__ == "__main__":
    unittest.main()
