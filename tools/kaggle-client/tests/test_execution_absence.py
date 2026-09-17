"""Regression coverage for Kaggle's live missing-kernel 403 contract."""
import importlib.util
import json
import os
import unittest
from unittest import mock

import requests
from test_execution import bridge, kernel_fixture, request_fixture
from test_preflight import response


def missing_payload(permission="kernels.get", status="PERMISSION_DENIED"):
    return json.dumps({"error": {
        "code": 403,
        "message": f"Permission '{permission}' was denied",
        "status": status,
    }}).encode()


def http_error(code, payload=b""):
    r = requests.Response()
    r.status_code = code
    r._content = payload
    r._content_consumed = True
    return requests.HTTPError(response=r)


class ExecutionAbsenceTests(unittest.TestCase):
    def test_kernel_absence_classifier_is_narrow(self):
        self.assertTrue(bridge.missing_kernel_error(http_error(404)))
        self.assertTrue(bridge.missing_kernel_error(http_error(403, missing_payload())))
        self.assertFalse(bridge.missing_kernel_error(http_error(403, missing_payload("kernels.list"))))
        self.assertFalse(bridge.missing_kernel_error(http_error(403, missing_payload(status="OTHER"))))
        self.assertFalse(bridge.missing_kernel_error(http_error(403, b"not-json")))
        self.assertFalse(bridge.missing_kernel_error(http_error(503, missing_payload())))

    @unittest.skipUnless(importlib.util.find_spec("kagglesdk"), "pinned SDK is exercised in client CI")
    def test_missing_permission_enters_one_submit_but_other_403_stays_closed(self):
        r = request_fixture()
        kernel = [None]
        saves = [0]
        permission = ["kernels.get"]

        def exchange(adapter, request, **kwargs):
            self.assertEqual(kwargs["proxies"], {})
            self.assertTrue(kwargs["verify"])
            operation = request.url.removeprefix(bridge.ROOT)
            body = json.loads(request.body)
            if operation == bridge.OPERATIONS["auth"]:
                return response(json.dumps({"active": True, "username": r["owner"]}).encode())
            if operation == bridge.OPERATIONS["get"]:
                if kernel[0] is None:
                    return response(missing_payload(permission[0]), 403)
                return response(json.dumps(kernel[0]).encode())
            if operation == bridge.OPERATIONS["save"]:
                saves[0] += 1
                self.assertEqual(saves[0], 1)
                kernel[0] = kernel_fixture(r)
                return response(json.dumps({
                    "ref": r["owner"] + "/" + r["slug"],
                    "versionNumber": 1,
                    "kernelId": 42,
                }).encode())
            if operation == bridge.OPERATIONS["status"]:
                return response(b'{"status":"COMPLETE"}')
            self.fail("unexpected operation " + operation + " body=" + repr(body))

        def send(adapter, request, **kwargs):
            return exchange(adapter, request, **kwargs)

        patches = (
            mock.patch.object(requests.adapters.HTTPAdapter, "send", send),
            mock.patch.dict(os.environ, {"KAGGLE_API_TOKEN": "AMBIENT_CANARY", "HTTPS_PROXY": "http://blocked.invalid"}),
            mock.patch("kagglesdk.kaggle_http_client.get_access_token_from_env", side_effect=AssertionError("ambient auth")),
            mock.patch("kagglesdk.kaggle_http_client._get_apikey_creds", side_effect=AssertionError("legacy auth")),
        )
        with patches[0], patches[1], patches[2], patches[3]:
            read = dict(r, source="")
            self.assertEqual(bridge.operate(bridge.validate_request(read, "reconcile"), "SYNTHETIC_TOKEN", "reconcile"),
                             bridge.outcome("not_found"))
            self.assertEqual(saves[0], 0)

            permission[0] = "kernels.list"
            self.assertEqual(bridge.operate(bridge.validate_request(r, "submit"), "SYNTHETIC_TOKEN", "submit")["status"],
                             "rejected")
            self.assertEqual(saves[0], 0)

            permission[0] = "kernels.get"
            result = bridge.operate(bridge.validate_request(r, "submit"), "SYNTHETIC_TOKEN", "submit")
            self.assertEqual(result["status"], "found")
            self.assertEqual(result["kernel_id"], "42")
            self.assertEqual(saves[0], 1)


if __name__ == "__main__":
    unittest.main()
