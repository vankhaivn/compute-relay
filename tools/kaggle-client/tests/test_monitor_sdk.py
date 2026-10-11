"""Actual pinned SDK types with HTTP replaced; no provider credentials/network."""
import base64
import importlib.util
import json
import io
import os
from types import SimpleNamespace
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
        self.log = [{"data": "one\nSYNTHETIC_TOKEN\nthree\n", "stream_name": "stdout", "time": "2026-10-11T00:00:00Z"}]
        self.sse = True
        self.status = {"status": "COMPLETE"}
        self.calls = []
        self.wrong_account = False
        self.auth_status = 200
        self.fault = ""
        self.log_seen = False

    def exchange(self, adapter, request, **settings):
        if request.method == "GET":
            self.assertEqual(request.url, "https://api.kaggle.com/v1/kernels/logs/stream/" + self.execution["owner"] + "/" + self.execution["slug"])
            self.assertEqual(settings, dict(timeout=(3, 0.5), stream=True, proxies={}, verify=True, cert=None))
            self.assertEqual(request.headers["Authorization"], "Bearer SYNTHETIC_TOKEN")
            self.assertEqual(request.headers["Accept"], "text/event-stream, */*")
            self.assertNotIn("Content-Type", request.headers)
            self.assertIsNone(request.body)
            self.assertEqual(adapter.max_retries.total, 0)
            self.calls.append(("logs-stream", {}))
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
                return response(b"do-not-read", 307, {"Location": "https://must-not-follow.invalid"})
            elif self.fault == "log-connect-timeout":
                raise requests.exceptions.ConnectTimeout("SYNTHETIC_TOKEN")
            elif self.fault == "log-real-header-timeout":
                return self.real_adapter_send(adapter, request, **settings)
            elif self.fault.startswith("log-status-"):
                return response(b'SYNTHETIC_TOKEN', int(self.fault.removeprefix("log-status-")))
            elif self.fault == "log-unknown":
                raise ValueError("SYNTHETIC_TOKEN")
            body = (b"".join(b"data: " + json.dumps(event).encode() + b"\n\n" for event in self.log)
                    if self.sse else json.dumps(self.log).encode())
            r = response(body, headers={"Content-Type": "text/event-stream" if self.sse else "application/json"})
            if self.fault.startswith("log-body-"):
                from urllib3.exceptions import ReadTimeoutError, ProtocolError
                from urllib3.response import HTTPResponse
                faults = {
                    "log-body-requests-timeout": requests.exceptions.ReadTimeout("SYNTHETIC_TOKEN"),
                    "log-body-urllib3-timeout": ReadTimeoutError(None, "/synthetic", "SYNTHETIC_TOKEN"),
                    "log-body-protocol-error": ProtocolError("SYNTHETIC_TOKEN"),
                    "log-body-os-error": OSError("SYNTHETIC_TOKEN"),
                    "log-body-invalid": ValueError("SYNTHETIC_TOKEN"),
                    "log-body-socket-timeout": TimeoutError("SYNTHETIC_TOKEN"),
                }
                class Body(io.BytesIO):
                    reads = 0
                    def read(inner, size=-1):
                        self.assertGreater(size, 0)
                        self.assertLessEqual(size, monitor.MAX_INPUT_BYTES + 1)
                        inner.reads += 1
                        raise faults[self.fault]
                self.raw_body = Body()
                r.headers["Content-Type"] = "application/json"
                r.raw = (HTTPResponse(body=self.raw_body, headers=r.headers, status=200,
                                      preload_content=False, decode_content=False)
                         if self.fault == "log-body-socket-timeout" else self.raw_body)
            if self.fault in ("log-idle", "log-late", "log-malformed"):
                class Stream(io.BytesIO):
                    reads = 0
                    def read1(inner, size):
                        inner.reads += 1
                        if inner.reads == 1 and self.fault != "log-idle":
                            return super(Stream, inner).read1(size)
                        if self.fault == "log-malformed":
                            return b'data: {bad}\n\n'
                        from urllib3.exceptions import ReadTimeoutError
                        raise ReadTimeoutError(None, request.url, "SYNTHETIC_TOKEN")
                r.raw = Stream(body)
            return r
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
            self.assertNotIn("versionLabel", body)
            if self.fault == "first-read" or self.log_seen and self.fault == "final-read":
                return response(b'{}', 503)
            return response(json.dumps(self.kernel).encode())
        if operation == execution.OPERATIONS["status"]:
            self.assertEqual(body["userName"], self.execution["owner"])
            self.assertEqual(body["kernelSlug"], self.execution["slug"])
            self.assertNotIn("versionLabel", body)
            if self.fault == "status-http":
                return response(b'{}', 503)
            return response(json.dumps(self.status).encode())
        self.fail("unexpected network or mutation operation: " + operation)

    def invoke(self, mode, **pagination):
        self.log_seen = False
        target = dict(self.execution, source="", kernel_id="42") if mode == "logs" else None
        request = dict(protocol=1, owner=self.execution["owner"], execution=target, **pagination)
        transport_failures = []
        def send(adapter, r, **settings):
            try:
                return self.exchange(adapter, r, **settings)
            except AssertionError as exc:
                transport_failures.append(str(exc))
                raise
        with mock.patch.object(requests.adapters.HTTPAdapter, "send", send), \
             mock.patch.dict(os.environ, {"KAGGLE_API_TOKEN": "AMBIENT_CANARY", "HTTPS_PROXY": "http://blocked.invalid"}), \
             mock.patch("kagglesdk.kaggle_http_client.get_access_token_from_env", side_effect=AssertionError("ambient auth")) as ambient_auth, \
             mock.patch("kagglesdk.kaggle_http_client._get_apikey_creds", side_effect=AssertionError("legacy auth")) as legacy_auth:
            result = monitor.operate(monitor.validate_request(request, mode), "SYNTHETIC_TOKEN", mode)
        ambient_auth.assert_not_called()
        legacy_auth.assert_not_called()
        self.assertEqual(transport_failures, [], "wire assertion failed inside fail-closed monitor")
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
            observed = self.invoke("quota")
            if key == "isPayToScaleEnabled":
                self.assertEqual(observed, result)  # implicit scalar false, same raw durations
            else:
                self.assertEqual(observed["status"], "unknown")  # absent message is not zero
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

    def test_logs_use_current_kernel_and_recheck_identity_without_artifact_download(self):
        result = self.invoke("logs")
        self.assertEqual(base64.b64decode(result["text_b64"]), b"one\n[REDACTED]\nthree\n")
        self.assertEqual(result["availability"], "after_completion")
        self.assertFalse(result["truncated"])
        self.assertNotIn("artifact-page", json.dumps(result))
        self.assertEqual(len(self.calls), 5)
        self.assertTrue(all("versionLabel" not in body for op, body in self.calls if op == execution.OPERATIONS["get"]))
        for status in ({"status": "RUNNING"}, {}, {"status": "FUTURE_STATE"}):
            self.status = status
            self.assertEqual(self.invoke("logs")["availability"], "live")
        self.fault = "status-http"
        self.assertEqual(self.invoke("logs")["availability"], "live")

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
        self.assertEqual(self.invoke("logs"), monitor.empty("unavailable", "kernel_read_failed"))

    def test_missing_empty_and_bounded_logs_are_different(self):
        self.sse = False
        for raw in ({}, {"data": None}):
            self.log = raw
            self.assertEqual(self.invoke("logs"), monitor.empty("unavailable", "stream_format_invalid"))
        self.log = []
        result = self.invoke("logs")
        self.assertEqual((result["status"], result["availability"], result["text_b64"]), ("logs", "after_completion", ""))
        self.sse = True
        self.log = [{"data": "a" * 40000 + "\n"}, {"data": "b" * 40000 + "\n"}]
        result = self.invoke("logs")
        self.assertTrue(result["truncated"])
        self.assertEqual(base64.b64decode(result["text_b64"]), b"a" * 40000 + b"\n")
        continuation = self.invoke("logs", log_offset=result["offset"], log_prefix=result["prefix"])
        self.assertEqual(base64.b64decode(continuation["text_b64"]), b"b" * 40000 + b"\n")
        self.log = [{"data": "a" * (monitor.MAX_EVENT_BYTES + 1)}]
        self.assertEqual(self.invoke("logs"), monitor.empty("unavailable", "stream_format_invalid"))

    def test_replay_continuation_appends_and_identity_checks_wrap_each_page(self):
        self.status = {"status": "RUNNING"}
        self.log = [{"data": "one\n"}, {"data": "two\n"}]
        first = self.invoke("logs", log_limit=1)
        self.assertEqual(base64.b64decode(first["text_b64"]), b"one\n")
        self.log.append({"data": "three\n"})
        second = self.invoke("logs", log_limit=1, log_offset=first["offset"], log_prefix=first["prefix"])
        self.assertEqual(base64.b64decode(second["text_b64"]), b"two\n")
        self.assertEqual([op for op, _ in self.calls], [execution.OPERATIONS["auth"], execution.OPERATIONS["get"], execution.OPERATIONS["status"], "logs-stream", execution.OPERATIONS["get"]] * 2)
        self.log[0] = {"data": "bad\n"}
        self.assertEqual(self.invoke("logs", log_offset=first["offset"], log_prefix=first["prefix"]), monitor.empty("reset", "log_changed"))

    def test_failed_redirected_or_malformed_log_read_returns_no_prefix(self):
        for fault, reason in (("log-http", "stream_http_5xx"), ("log-redirect", "stream_redirect"),
                              ("log-connect-timeout", "stream_timeout"), ("log-malformed", "stream_format_invalid")):
            with self.subTest(fault=fault):
                self.fault = fault
                result = self.invoke("logs")
                self.assertEqual(result, monitor.empty("unavailable", reason))

    def test_idle_and_interrupted_sse_preserve_verified_replay_without_completion(self):
        self.status = {"status": "RUNNING"}
        self.fault = "log-idle"
        result = self.invoke("logs")
        self.assertEqual((result["status"], result["availability"], result["text_b64"]), ("logs", "live", ""))
        self.status = {"status": "COMPLETE"}
        self.assertEqual(self.invoke("logs")["availability"], "delayed")
        self.fault = "log-late"
        self.status = {"status": "RUNNING"}
        result = self.invoke("logs")
        self.assertEqual(base64.b64decode(result["text_b64"]), b"one\n[REDACTED]\nthree\n")
        self.assertEqual(result["availability"], "live")
        self.assertEqual(self.calls[-1][0], execution.OPERATIONS["get"])

    def test_identity_is_checked_before_any_stream_read(self):
        for field, value in (("id", 43), ("currentVersionNumber", 2), ("isPrivate", False)):
            with self.subTest(field=field):
                self.calls = []
                self.kernel = kernel_fixture(self.execution)
                self.kernel["metadata"][field] = value
                self.assertEqual(self.invoke("logs"), monitor.empty("invalid", "identity_mismatch"))
                self.assertEqual([op for op, _ in self.calls], [execution.OPERATIONS["auth"], execution.OPERATIONS["get"]])
        for wrong_account, auth_status in ((True, 200), (False, 401)):
            self.kernel = kernel_fixture(self.execution)
            self.calls = []
            self.wrong_account, self.auth_status = wrong_account, auth_status
            self.assertEqual(self.invoke("logs"), monitor.empty("unavailable", "auth_read_failed"))
            self.assertEqual([op for op, _ in self.calls], [execution.OPERATIONS["auth"]])

    def test_terminal_json_blob_replays_pages_and_distinguishes_exhaustion(self):
        self.sse = False
        self.log = [{"data": "one\n"}, {"data": "two\n"}]
        first = self.invoke("logs", log_limit=1)
        self.assertEqual((base64.b64decode(first["text_b64"]), first["availability"]), (b"one\n", "after_completion"))
        second = self.invoke("logs", log_limit=1, log_offset=first["offset"], log_prefix=first["prefix"])
        self.assertEqual(base64.b64decode(second["text_b64"]), b"two\n")
        empty = self.invoke("logs", log_limit=1, log_offset=second["offset"], log_prefix=second["prefix"])
        self.assertEqual((empty["status"], empty["availability"], empty["text_b64"], empty["offset"], empty["prefix"]),
                         ("logs", "after_completion", "", second["offset"], second["prefix"]))
        self.assertEqual(sum(op == "logs-stream" for op, _ in self.calls), 3)
        self.assertEqual(sum(op == execution.OPERATIONS["get"] for op, _ in self.calls), 6)

    def test_real_adapter_header_timeout_reports_fixed_reason_without_response(self):
        from urllib3.exceptions import ReadTimeoutError
        self.real_adapter_send = requests.adapters.HTTPAdapter.send
        self.fault = "log-real-header-timeout"
        pool = SimpleNamespace(urlopen=mock.Mock(side_effect=ReadTimeoutError(None, "/synthetic", "SYNTHETIC_TOKEN")))
        with mock.patch.object(requests.adapters.HTTPAdapter, "get_connection_with_tls_context", return_value=pool), \
             mock.patch.object(requests.adapters.HTTPAdapter, "build_response") as build_response:
            result = self.invoke("logs")
        self.assertEqual(result, monitor.empty("unavailable", "stream_timeout"))
        build_response.assert_not_called()
        self.assertEqual(pool.urlopen.call_count, 1)
        settings = pool.urlopen.call_args.kwargs
        self.assertEqual((settings["timeout"].connect_timeout, settings["timeout"].read_timeout), (3, 0.5))
        self.assertEqual(settings["retries"].total, 0)
        self.assertFalse(settings["redirect"])
        self.assertFalse(settings["preload_content"])

    def test_log_failure_http_classes_and_stages_are_fixed_and_secret_free(self):
        for status, reason in ((401,"stream_http_401"),(403,"stream_http_403"),(404,"stream_http_404"),
                               (429,"stream_http_429"),(418,"stream_http_4xx"),(500,"stream_http_5xx"),
                               (302,"stream_redirect"),(307,"stream_redirect"),(204,"stream_http_other")):
            with self.subTest(status=status):
                self.fault = "log-status-" + str(status)
                self.assertEqual(self.invoke("logs"), monitor.empty("unavailable", reason))
        for fault, reason in (("first-read", "kernel_read_failed"), ("final-read", "kernel_read_failed"),
                              ("log-unknown", "read_unavailable")):
            self.fault = fault
            self.assertEqual(self.invoke("logs"), monitor.empty("unavailable", reason))
        self.fault = ""
        self.wrong_account = True
        self.assertEqual(self.invoke("logs"), monitor.empty("unavailable", "auth_read_failed"))

    def test_unreached_replay_prefix_is_diagnostic_not_completion(self):
        self.fault = "log-idle"
        self.status = {"status":"RUNNING"}
        result = self.invoke("logs", log_offset=4, log_prefix="a" * 64)
        self.assertEqual(result, monitor.empty("unavailable", "replay_unavailable"))

    def test_completed_body_transport_failures_are_not_malformed_log_format(self):
        for fault, reason in (("log-body-requests-timeout", "stream_timeout"),
                              ("log-body-urllib3-timeout", "stream_timeout"),
                              ("log-body-socket-timeout", "stream_timeout"),
                              ("log-body-protocol-error", "read_unavailable"),
                              ("log-body-os-error", "read_unavailable"),
                              ("log-body-invalid", "stream_format_invalid")):
            with self.subTest(fault=fault):
                self.fault = fault
                self.calls = []
                result = self.invoke("logs")
                self.assertEqual(result, monitor.empty("unavailable", reason))
                self.assertEqual(self.raw_body.reads, 1)
                self.assertEqual(self.calls[-1][0], "logs-stream")
                self.assertTrue(self.raw_body.closed)
