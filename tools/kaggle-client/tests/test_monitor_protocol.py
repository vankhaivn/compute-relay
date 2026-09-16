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

    def test_quota_missing_fields_do_not_inherit_free_or_zero_defaults(self):
        expected = monitor.quota_result(quota_fixture())
        self.assertEqual((expected["limit_ns"], expected["used_ns"], expected["reserved_ns"]),
                         ("19999999999", "2000000001", "3000000001"))
        for field in ("isPayToScaleEnabled", "totalTimeAllowed", "timeUsed", "timeReserved"):
            for remove in (False, True):
                raw = quota_fixture()
                if remove:
                    del raw["gpuQuota"][field]
                else:
                    raw["gpuQuota"][field] = None
                result = monitor.quota_result(raw)
                self.assertEqual(result["status"], "unknown")
                self.assertEqual(result["limit_ns"] + result["used_ns"] + result["reserved_ns"], "")
        for flag in (True, "false", 0):
            raw = quota_fixture()
            raw["gpuQuota"]["isPayToScaleEnabled"] = flag
            self.assertEqual(monitor.quota_result(raw)["reason"], "paid_or_unknown")
        self.assertEqual(monitor.quota_result({})["status"], "unknown")
        with self.assertRaises(ValueError):
            monitor.quota_result({"gpuQuota": []})

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
        self.assertEqual(data.decode("utf-8"), "界" * (monitor.MAX_LOG_BYTES // 3))
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
