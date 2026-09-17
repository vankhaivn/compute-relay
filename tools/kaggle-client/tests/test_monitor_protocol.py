"""Exact monitor pure protocol tests; no SDK import or network needed."""
import base64
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
        result = monitor.log_result({"log": "first\r\nSYNTHETIC_TOKEN\rlast"}, "RUNNING", "SYNTHETIC_TOKEN")
        raw = base64.b64decode(result["text_b64"])
        self.assertEqual(raw, b"first\n[REDACTED]\nlast")
        self.assertEqual(result["availability"], "delayed")
        self.assertFalse(result["truncated"])
        for state in ("COMPLETE", "ERROR"):
            empty = monitor.log_result({"log": ""}, state, "SYNTHETIC_TOKEN")
            self.assertEqual(empty["status"], "logs")
            self.assertEqual(empty["availability"], "after_completion")
            self.assertEqual(empty["text_b64"], "")
        for raw in ({}, {"log": None}):
            self.assertEqual(monitor.log_result(raw, "COMPLETE", "x")["status"], "unavailable")
        for value in ([], 1, True, "\ud800"):
            with self.assertRaises((ValueError, UnicodeError)):
                monitor.log_result({"log": value}, "COMPLETE", "x")
        large = monitor.log_result({"log": "界" * 30000}, "UNKNOWN", "SYNTHETIC_TOKEN")
        data = base64.b64decode(large["text_b64"])
        self.assertTrue(large["truncated"])
        self.assertLessEqual(len(data), monitor.MAX_LOG_BYTES)
        self.assertEqual(data.decode("utf-8"), "界" * (65536 // 3))
        self.assertEqual(large["availability"], "delayed")

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
        script = ("import time\nns={'__name__':'fixture'}\nexec(" + repr(source) + ",ns)\n"
                  "timer=ns['threading'].Timer\n"
                  "def bounded(seconds, callback):\n assert seconds==25\n return timer(0.05,callback)\n"
                  "ns['threading'].Timer=bounded\nns['main']=lambda:time.sleep(60)\nns['bounded_main']()\n")
        result = subprocess.run([sys.executable, "-I", "-c", script], input=b"", capture_output=True, timeout=5, check=False)
        self.assertEqual(result.returncode, 3)
        self.assertEqual(result.stdout + result.stderr, b"")
        import contextlib
        out = io.StringIO()
        with mock.patch.object(monitor, "main", side_effect=ValueError("SYNTHETIC_TOKEN")), contextlib.redirect_stdout(out):
            self.assertEqual(monitor.bounded_main(), 2)
        self.assertEqual(out.getvalue(), "")
