"""Exact monitor pure protocol tests; no SDK import or network needed."""
import base64
import hashlib
from types import SimpleNamespace
import importlib.util
import io
import json
from pathlib import Path
import subprocess
import sys
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[3]
SPEC = importlib.util.spec_from_file_location("monitor_bridge", ROOT / "internal/provider/kaggle/monitor.py")
monitor = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(monitor)
CORE_SPEC = importlib.util.spec_from_file_location("monitor_core", ROOT / "internal/provider/kaggle/execution.py")
monitor.core = importlib.util.module_from_spec(CORE_SPEC)
CORE_SPEC.loader.exec_module(monitor.core)


def log_response(events, sse=True):
    body = (b"".join(b"data: " + json.dumps(event).encode() + b"\n\n" for event in events)
            if sse else json.dumps(events).encode())
    return SimpleNamespace(headers={"Content-Type": "text/event-stream" if sse else "application/json"}, raw=io.BytesIO(body))


def quota_fixture():
    return {"gpuQuota": {"isPayToScaleEnabled": False, "totalTimeAllowed": "19.999999999s",
                         "timeUsed": "2.000000001s", "timeReserved": "3.000000001s"}}


class MonitorProtocolTests(unittest.TestCase):
    def test_duration_preserves_nanos_and_rejects_invalid_values(self):
        for raw, want in (("0s", 0), ("0.000000001s", 1), ("1.5s", 1500000000),
                          ("19.999999999s", 19999999999), ("31622400s", 31622400000000000)):
            self.assertEqual(monitor.duration_ns(raw), want)
        for raw in (None, True, 1, "", "1", "-1s", "+1s", "01s", "1e3s", "1.s", "1.1234567890s",
                    "1s\n", "1ss", "31622400.000000001s", "999999999999999s"):
            with self.subTest(raw=raw):
                with self.assertRaises(ValueError):
                    monitor.duration_ns(raw)

    def test_quota_missing_duration_messages_do_not_inherit_zero_defaults(self):
        expected = monitor.quota_result(quota_fixture())
        self.assertEqual((expected["limit_ns"], expected["used_ns"], expected["reserved_ns"]),
                         ("19999999999", "2000000001", "3000000001"))
        for field in ("totalTimeAllowed", "timeUsed", "timeReserved"):
            for remove in (False, True):
                for omit_flag in (False, True):
                    raw = quota_fixture()
                    if omit_flag:
                        del raw["gpuQuota"]["isPayToScaleEnabled"]
                    if remove:
                        del raw["gpuQuota"][field]
                    else:
                        raw["gpuQuota"][field] = None
                    result = monitor.quota_result(raw)
                    self.assertEqual(result["status"], "unknown")
                    self.assertEqual(result["limit_ns"] + result["used_ns"] + result["reserved_ns"], "")
        for flag in (True, None, "false", "true", "", 0, 1, 0.0, [], {}):
            raw = quota_fixture()
            raw["gpuQuota"]["isPayToScaleEnabled"] = flag
            self.assertEqual(monitor.quota_result(raw), monitor.empty("unknown", "paid_or_unknown"))
        for raw in ({}, {"gpuQuota": None}, {"gpuQuota": {}}):
            self.assertEqual(monitor.quota_result(raw)["status"], "unknown")
        with self.assertRaises(ValueError):
            monitor.quota_result({"gpuQuota": []})

    def test_omitted_billing_bool_uses_its_schema_default_not_a_quota_default(self):
        # BUG-RUN-20260917-01-01: representative non-secret operator wire shape.
        raw = {"gpuQuota": {"totalTimeAllowed": "108000s", "timeUsed": "0s",
                            "timeReserved": "0s", "minimumTimeAllowed": "108000s",
                            "hasEverRun": True}}
        result = monitor.quota_result(raw)
        self.assertEqual(result, dict(monitor.empty("known", "none"),
                                      limit_ns="108000000000000", used_ns="0", reserved_ns="0"))
        raw["gpuQuota"]["isPayToScaleEnabled"] = False
        self.assertEqual(monitor.quota_result(raw), result)
        fractional = quota_fixture()
        expected = monitor.quota_result(fractional)
        del fractional["gpuQuota"]["isPayToScaleEnabled"]
        self.assertEqual(monitor.quota_result(fractional), expected)
        for name in ("totalTimeAllowed", "timeUsed", "timeReserved"):
            fractional["gpuQuota"][name] = "0s"
        zero = monitor.quota_result(fractional)
        self.assertEqual((zero["status"], zero["limit_ns"], zero["used_ns"], zero["reserved_ns"]),
                         ("known", "0", "0", "0"))
        fractional["gpuQuota"]["timeReserved"] = "-1s"
        with self.assertRaises(ValueError):
            monitor.quota_result(fractional)

    def test_logs_redact_before_bounding_and_preserve_explicit_availability(self):
        for sse in (True, False):
            result = monitor.stream_result(log_response([{"data": "first\r\nSYNTHETIC_"},
                                                        {"data": "TOKEN\rlast"}], sse), {}, "RUNNING", "SYNTHETIC_TOKEN")
            self.assertEqual(base64.b64decode(result["text_b64"]), b"first\n[REDACTED]\nlast\n")
            self.assertEqual(result["availability"], "live")
            self.assertFalse(result["truncated"])
            self.assertTrue(result["replay"])
        for state in ("COMPLETE", "ERROR"):
            result = monitor.stream_result(log_response([], False), {}, state, "SYNTHETIC_TOKEN")
            self.assertEqual(result["status"], "logs")
            self.assertEqual(result["availability"], "after_completion")
            self.assertEqual(result["text_b64"], "")
        for event in ({}, {"data": None}, {"data": []}, {"data": 1}, {"data": True}, {"data": "\ud800"}):
            with self.subTest(event=event):
                with self.assertRaises((ValueError, UnicodeError)):
                    monitor.stream_result(log_response([event]), {}, "COMPLETE", "x")

    def test_incremental_pages_keep_prefix_and_detect_changed_or_reset_replay(self):
        events = [{"data": "one\n"}, {"data": "two\n"}]
        first = monitor.stream_result(log_response(events), {"log_limit": 1}, "RUNNING", "x")
        self.assertEqual(base64.b64decode(first["text_b64"]), b"one\n")
        request = dict(log_limit=1, log_offset=first["offset"], log_prefix=first["prefix"])
        second = monitor.stream_result(log_response(events + [{"data": "three\n"}]), request, "RUNNING", "x")
        self.assertEqual(base64.b64decode(second["text_b64"]), b"two\n")
        request.update(log_offset=second["offset"], log_prefix=second["prefix"])
        idle = monitor.stream_result(log_response(events), request, "RUNNING", "x")
        self.assertEqual((idle["offset"], idle["prefix"], idle["text_b64"]),
                         (second["offset"], second["prefix"], ""))
        self.assertEqual(idle["availability"], "live")
        reset = monitor.stream_result(log_response([]), request, "RUNNING", "x")
        self.assertEqual(reset, monitor.empty("reset", "log_changed"))
        with self.assertRaises(monitor.core.IdentityMismatch):
            monitor.stream_result(log_response([{"data": "bad\n"}, {"data": "two\n"}]), request, "RUNNING", "x")
        with self.assertRaises(monitor.core.IdentityMismatch):
            monitor.stream_result(log_response(events), dict(log_offset=1, log_prefix=hashlib.sha256(b"o").hexdigest()), "RUNNING", "x")

    def test_page_bytes_are_bounded_without_skipping_the_next_line(self):
        events = [{"data": "a" * 40000 + "\n"}, {"data": "b" * 40000 + "\n"}]
        result = monitor.stream_result(log_response(events), {}, "RUNNING", "x")
        self.assertTrue(result["truncated"])
        self.assertEqual(base64.b64decode(result["text_b64"]), b"a" * 40000 + b"\n")
        self.assertEqual(result["offset"], 40001)
        next_page = monitor.stream_result(log_response(events), dict(log_offset=result["offset"], log_prefix=result["prefix"]), "RUNNING", "x")
        self.assertEqual(base64.b64decode(next_page["text_b64"]), b"b" * 40000 + b"\n")
        with self.assertRaises(ValueError):
            monitor.stream_result(log_response([{"data": "a" * (monitor.MAX_EVENT_BYTES + 1)}]), {}, "RUNNING", "x")
        for wire in (b"data: {bad}\n\n", b"data: {}\n\n", b"foreign: payload\n", b"data: {\"data\":\"x\"}"):
            with self.subTest(wire=wire):
                with self.assertRaises(ValueError):
                    monitor.stream_result(SimpleNamespace(headers={"Content-Type": "text/event-stream"}, raw=io.BytesIO(wire)), {}, "RUNNING", "x")

    def test_idle_stream_or_sentinel_is_never_completion_evidence(self):
        class Idle:
            def read(self, size):
                raise TimeoutError("SYNTHETIC_TOKEN")
        for state, availability in (("RUNNING", "live"), ("COMPLETE", "delayed")):
            result = monitor.stream_result(SimpleNamespace(headers={"Content-Type": "text/event-stream"}, raw=Idle()), {}, state, "SYNTHETIC_TOKEN")
            self.assertEqual((result["status"], result["availability"], result["text_b64"]), ("logs", availability, ""))
        result = monitor.stream_result(SimpleNamespace(headers={"Content-Type": "text/event-stream"}, raw=io.BytesIO(b"data: END_OF_LOG\n\n")), {}, "RUNNING", "x")
        self.assertEqual(result["availability"], "live")

    def test_quota_request_and_local_pins_reject_before_credentials(self):
        good = {"protocol": 1, "owner": "fixture_user", "execution": None}
        self.assertEqual(monitor.validate_request(good.copy(), "quota"), good)
        for key, value in (("protocol", True), ("owner", "foreign/path"), ("execution", {}), ("token", "secret")):
            with self.assertRaises(ValueError):
                monitor.validate_request(dict(good, **{key: value}), "quota")
        with self.assertRaises(ValueError):
            monitor.validate_request(good, "submit")
        with mock.patch.object(monitor.sys, "argv", ["helper", "quota"]), \
             mock.patch.object(monitor.sys, "version_info", (3, 13)), \
             mock.patch.object(monitor.sys, "stdin") as stdin:
            self.assertEqual(monitor.main(), 2)
            stdin.buffer.readline.assert_not_called()

    def test_independent_watchdog_and_exception_redaction(self):
        source = (ROOT / "internal/provider/kaggle/monitor.py").read_text(encoding="utf-8")
        for mode, budget in (("quota", 25), ("logs", 9)):
            script = ("import time\nns={'__name__':'fixture'}\nexec(" + repr(source) + ",ns)\n"
                      "ns['sys'].argv=['helper'," + repr(mode) + "]\ntimer=ns['threading'].Timer\n"
                      "def bounded(seconds, callback):\n assert seconds==" + str(budget) + "\n return timer(0.05,callback)\n"
                      "ns['threading'].Timer=bounded\nns['main']=lambda:time.sleep(60)\nns['bounded_main']()\n")
            result = subprocess.run([sys.executable, "-I", "-c", script], input=b"", capture_output=True, timeout=5, check=False)
            self.assertEqual(result.returncode, 3)
            self.assertEqual(result.stdout + result.stderr, b"")
        import contextlib
        out = io.StringIO()
        with mock.patch.object(monitor, "main", side_effect=ValueError("SYNTHETIC_TOKEN")), contextlib.redirect_stdout(out):
            self.assertEqual(monitor.bounded_main(), 2)
        self.assertEqual(out.getvalue(), "")

    def test_failure_reason_allowlist_never_reflects_exception_canaries(self):
        for reason in monitor.LOG_FAILURE_REASONS:
            failure = monitor.LogUnavailable(reason)
            self.assertEqual((failure.reason, str(failure)), (reason, reason))
        for reason in ("SYNTHETIC_TOKEN", "stream_timeout: SYNTHETIC_TOKEN", None, 401):
            failure = monitor.LogUnavailable(reason)
            self.assertEqual((failure.reason, str(failure)), ("read_unavailable", "read_unavailable"))
