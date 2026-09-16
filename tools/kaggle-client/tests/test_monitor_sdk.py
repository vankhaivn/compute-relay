"""Actual pinned SDK types with HTTP replaced; no provider credentials/network."""
import base64
import importlib.util
import json
import os
import unittest
from unittest import mock

import requests
from test_execution import bridge as execution, kernel_fixture, request_fixture
from test_monitor_protocol import monitor, quota_fixture
from test_preflight import response


@unittest.skipUnless(importlib.util.find_spec("kagglesdk"), "real SDK runs in locked client CI")
class MonitorPinnedSDKTests(unittest.TestCase):
    def setUp(self):
        monitor.core = execution
        self.execution = request_fixture()
        self.kernel = kernel_fixture(self.execution)
        self.quota = quota_fixture()
        self.log = {"files": [], "log": "one\nSYNTHETIC_TOKEN\nthree", "nextPageToken": "artifact-page-not-a-log-cursor"}
        self.status = {"status": "COMPLETE"}
        self.calls = []
        self.wrong_account = False
        self.auth_status = 200
        self.fault = ""
        self.log_seen = False

    def exchange(self, adapter, request, **settings):
        self.assertEqual(request.method, "POST")
        self.assertEqual(settings["proxies"], {})
        self.assertTrue(settings["verify"])
        self.assertTrue(settings["stream"])
        self.assertEqual(settings["timeout"], (5, 10))
        self.assertEqual(request.headers["Authorization"], "Bearer SYNTHETIC_TOKEN")
        operation = request.url.removeprefix(execution.ROOT)
        body = json.loads(request.body)
        self.calls.append((operation, body))
        if operation == execution.OPERATIONS["auth"]:
            self.assertEqual(body["token"], "SYNTHETIC_TOKEN")
            raw = {"active": True, "username": "other_user" if self.wrong_account else self.execution["owner"]}
            return response(json.dumps(raw).encode(), self.auth_status)
        if operation == execution.OPERATIONS["quota"]:
            self.assertEqual(body, {})
            if self.fault == "quota-http":
                return response(b'{"message":"SYNTHETIC_TOKEN"}', 503)
            if self.fault == "quota-duplicate":
                return response(b'{"gpuQuota":{},"gpuQuota":{}}')
            return response(json.dumps(self.quota).encode())
        if operation == execution.OPERATIONS["get"]:
            self.assertEqual(body["userName"], self.execution["owner"])
            self.assertEqual(body["kernelSlug"], self.execution["slug"])
            self.assertIn(body.get("versionLabel", ""), ("", "1"))
            if self.log_seen and self.fault == "final-read":
                return response(b'{}', 503)
            return response(json.dumps(self.kernel).encode())
        if operation == execution.OPERATIONS["status"]:
            self.assertEqual(body["versionLabel"], "1")
            if self.fault == "status-http":
                return response(b'{}', 503)
            return response(json.dumps(self.status).encode())
        if operation == execution.OPERATIONS["logs"]:
            self.assertEqual(body["versionLabel"], "1")
            self.assertEqual(body["pageSize"], 1)
            self.assertNotIn("pageToken", body)
            self.log_seen = True
            if self.fault == "source":
                self.kernel["blob"]["source"] += "# changed"
            elif self.fault == "id":
                self.kernel["metadata"]["id"] = 43
            elif self.fault == "version":
                self.kernel["metadata"]["currentVersionNumber"] = 2
            elif self.fault == "privacy":
                self.kernel["metadata"]["isPrivate"] = False
            elif self.fault == "log-http":
                return response(b'{"message":"SYNTHETIC_TOKEN"}', 503)
            elif self.fault == "log-redirect":
                return response(b"do-not-read" * 100, 307, {"Location": "https://must-not-follow.invalid"})
            elif self.fault == "log-late":
                r = response()
                def chunks(**kwargs):
                    yield json.dumps(self.log).encode()
                    raise OSError("SYNTHETIC_TOKEN")
                r.iter_content = chunks
                return r
            return response(json.dumps(self.log).encode())
        self.fail("unexpected network or mutation operation: " + operation)

    def invoke(self, mode):
        target = dict(self.execution, source="", kernel_id="42") if mode == "logs" else None
        request = dict(protocol=1, owner=self.execution["owner"], execution=target)
        def send(adapter, r, **settings):
            return self.exchange(adapter, r, **settings)
        with mock.patch.object(requests.adapters.HTTPAdapter, "send", send), \
             mock.patch.dict(os.environ, {"KAGGLE_API_TOKEN": "AMBIENT_CANARY", "HTTPS_PROXY": "http://blocked.invalid"}), \
             mock.patch("kagglesdk.kaggle_http_client.get_access_token_from_env", side_effect=AssertionError("ambient auth")), \
             mock.patch("kagglesdk.kaggle_http_client._get_apikey_creds", side_effect=AssertionError("legacy auth")):
            result = monitor.operate(monitor.validate_request(request, mode), "SYNTHETIC_TOKEN", mode)
        self.assertNotIn("SYNTHETIC_TOKEN", json.dumps(result))
        self.assertFalse(any("Save" in op or "Cancel" in op or "Delete" in op for op, _ in self.calls))
        return result

    def test_exact_quota_fields_survive_sdk_microsecond_rounding(self):
        result = self.invoke("quota")
        self.assertEqual(result["status"], "known")
        self.assertEqual(result["limit_ns"], "19999999999")
        self.assertEqual(result["used_ns"], "2000000001")
        self.assertEqual(result["reserved_ns"], "3000000001")
        self.assertEqual(len(self.calls), 2)
        for key in ("timeReserved", "isPayToScaleEnabled"):
            self.quota = quota_fixture()
            del self.quota["gpuQuota"][key]
            self.assertEqual(self.invoke("quota")["status"], "unknown")
        self.quota = quota_fixture()
        self.quota["gpuQuota"]["isPayToScaleEnabled"] = True
        self.assertEqual(self.invoke("quota")["status"], "unknown")
        self.quota["gpuQuota"].update(isPayToScaleEnabled=False, timeUsed="-1s")
        self.assertEqual(self.invoke("quota")["status"], "unavailable")

    def test_account_and_transport_failures_never_return_cached_quota(self):
        for fault in ("wrong-account", "auth", "quota-http", "quota-duplicate"):
            with self.subTest(fault=fault):
                self.calls = []
                self.wrong_account = fault == "wrong-account"
                self.auth_status = 401 if fault == "auth" else 200
                self.fault = fault
                result = self.invoke("quota")
                self.assertEqual(result, monitor.empty("unavailable", "read_unavailable"))
                self.assertEqual(len(self.calls), 1 if fault in ("wrong-account", "auth") else 2)

    def test_logs_use_version_and_recheck_identity_without_artifact_download(self):
        result = self.invoke("logs")
        self.assertEqual(base64.b64decode(result["text_b64"]), b"one\n[REDACTED]\nthree")
        self.assertEqual(result["availability"], "after_completion")
        self.assertFalse(result["truncated"])
        self.assertNotIn("artifact-page", json.dumps(result))
        self.assertEqual(len(self.calls), 6)
        self.assertEqual([body.get("versionLabel", "") for op, body in self.calls if op == execution.OPERATIONS["get"]], ["", "1", ""])
        for status in ({"status": "RUNNING"}, {}, {"status": "FUTURE_STATE"}):
            self.status = status
            self.assertEqual(self.invoke("logs")["availability"], "delayed")
        self.fault = "status-http"
        self.assertEqual(self.invoke("logs")["availability"], "delayed")

    def test_changed_identity_after_log_read_discards_all_bytes(self):
        for fault in ("source", "id", "version", "privacy"):
            with self.subTest(fault=fault):
                self.kernel = kernel_fixture(self.execution)
                self.fault = fault
                result = self.invoke("logs")
                self.assertEqual(result, monitor.empty("invalid", "identity_mismatch"))
        self.kernel = kernel_fixture(self.execution)
        self.fault = "final-read"
        self.log_seen = False
        self.assertEqual(self.invoke("logs"), monitor.empty("unavailable", "read_unavailable"))

    def test_missing_empty_and_bounded_logs_are_different(self):
        for raw in ({"files": []}, {"files": [], "log": None}):
            self.log = raw
            self.assertEqual(self.invoke("logs")["status"], "unavailable")
        self.log = {"files": [], "log": ""}
        result = self.invoke("logs")
        self.assertEqual(result["status"], "logs")
        self.assertEqual(result["text_b64"], "")
        self.log = {"files": [], "log": "界" * 30000}
        result = self.invoke("logs")
        self.assertTrue(result["truncated"])
        self.assertLessEqual(len(base64.b64decode(result["text_b64"])), 65536)
        self.assertEqual(base64.b64decode(result["text_b64"]).decode(), "界" * (65536 // 3))

    def test_failed_redirected_or_late_log_read_returns_no_prefix(self):
        for fault in ("log-http", "log-redirect", "log-late"):
            with self.subTest(fault=fault):
                self.fault = fault
                result = self.invoke("logs")
                self.assertEqual(result, monitor.empty("unavailable", "read_unavailable"))
