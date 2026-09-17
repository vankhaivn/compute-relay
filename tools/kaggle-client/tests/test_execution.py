"""Actual pinned SDK, synthetic HTTP only. No kernel source is executed locally."""
import hashlib
import importlib.util
import io
import json
import os
import subprocess
import sys
from types import SimpleNamespace
import unittest
from unittest import mock

import requests
from test_preflight import ROOT, response

SPEC = importlib.util.spec_from_file_location("execution_bridge", ROOT / "internal/provider/kaggle/execution.py")
bridge = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(bridge)


def request_fixture():
    source = "# SYNTHETIC_SOURCE_ONLY_DO_NOT_EXECUTE\n"
    return dict(protocol=1, owner="fixture_user", slug="cre-" + "a" * 40, source=source,
                source_sha256=hashlib.sha256(source.encode()).hexdigest(),
                dataset="fixture_user/crs-" + "b" * 40, wall_seconds=10,
                internet=False, gpu=False, machine_shape="", kernel_id="")


def kernel_fixture(r, kernel_id=42):
    return {"metadata": {
        "id": kernel_id, "ref": r["owner"] + "/" + r["slug"],
        "author": "Fixture Display Name", "slug": r["slug"], "language": "python", "kernelType": "script",
        "currentVersionNumber": 1, "isPrivate": True, "enableGpu": r["gpu"],
        "enableInternet": r["internet"], "enableTpu": False,
        "datasetDataSources": [r["dataset"]], "kernelDataSources": [],
        "competitionDataSources": [], "modelDataSources": [], "machineShape": r["machine_shape"],
    }, "blob": {"source": r["source"], "language": "python", "kernelType": "script", "slug": r["slug"]}}


class ExecutionProtocolTests(unittest.TestCase):
    def test_request_is_closed_and_cannot_upgrade_resources(self):
        r = request_fixture()
        self.assertEqual(bridge.validate_request(r.copy(), "submit"), r)
        for key, value in (("protocol", True), ("owner", "foreign/path"), ("slug", "other"),
                           ("source_sha256", "0" * 64), ("wall_seconds", 0),
                           ("internet", 1), ("gpu", True), ("machine_shape", "NvidiaTeslaT4"),
                           ("dataset", "foreign/crs-" + "b" * 40), ("kernel_id", "42"),
                           ("extra", "secret")):
            with self.subTest(key=key):
                with self.assertRaises(ValueError):
                    bridge.validate_request(dict(r, **{key: value}), "submit")
        for raw in (b'{"x":1,"x":2}', b'{"x":NaN}', b'{"x":"\xff"}', b'[]', b'{} {}'):
            with self.assertRaises((ValueError, UnicodeError)):
                bridge.strict_json(raw)
        for identity in (True, 0, -1, 1.0, "01", "-1", str(1 << 63)):
            with self.assertRaises(ValueError):
                bridge.identifier(identity)

    def test_status_absence_and_cancellation_are_not_default_success(self):
        for raw in ({}, {"status": None}, {"status": True}, {"status": 1.0},
                    {"status": "new-provider-value"}, {"status": 7}, {"status": -1}):
            self.assertEqual(bridge.normalize_status(raw), "UNKNOWN")
        for number, name in enumerate(bridge.STATES):
            self.assertEqual(bridge.normalize_status({"status": number}), name)
            self.assertEqual(bridge.normalize_status({"status": name.lower()}), name)

    def test_raw_identity_fields_cannot_inherit_sdk_defaults(self):
        r = request_fixture()
        good = kernel_fixture(r)
        self.assertEqual(bridge.check_kernel(good, r), "42")
        good["metadata"]["author"] = "Another Human Display Name"
        self.assertEqual(bridge.check_kernel(good, r), "42")
        for field in ("id", "ref", "slug", "currentVersionNumber", "isPrivate", "enableGpu", "enableTpu", "enableInternet", "datasetDataSources"):
            with self.subTest(field=field):
                raw = kernel_fixture(r)
                del raw["metadata"][field]
                with self.assertRaises(bridge.IdentityMismatch):
                    bridge.check_kernel(raw, r)
        for kind in ("version", "public", "source", "replacement", "extra-input"):
            raw = kernel_fixture(r)
            if kind == "version":
                raw["metadata"]["currentVersionNumber"] = 2
            elif kind == "public":
                raw["metadata"]["isPrivate"] = False
            elif kind == "source":
                raw["blob"]["source"] += "# changed\n"
            elif kind == "extra-input":
                raw["metadata"]["datasetDataSources"].append("foreign/data")
            with self.assertRaises(bridge.IdentityMismatch):
                bridge.check_kernel(raw, r, "99" if kind == "replacement" else "42")

    def test_guard_blocks_redirect_body_extra_calls_and_read_mutations(self):
        session = requests.Session()
        self.addCleanup(session.close)
        guard = bridge.Guard(SimpleNamespace(http_client=lambda: SimpleNamespace(_session=session)), "observe")
        call = requests.Request("POST", bridge.ROOT + bridge.OPERATIONS["get"]).prepare()
        r = response(b"x" * 100000, 307, {"Location": "https://must-not-follow.invalid"})
        r.raw = mock.Mock(wraps=r.raw)
        with mock.patch.object(requests.adapters.HTTPAdapter, "send", return_value=r) as transport:
            with self.assertRaises(ValueError):
                guard.call("get", lambda _: session.send(call), None)
            r.raw.read.assert_not_called()
            self.assertEqual(transport.call_count, 1)
            self.assertEqual(transport.call_args.kwargs["proxies"], {})
            self.assertTrue(transport.call_args.kwargs["verify"])
            with self.assertRaises(ValueError):
                session.send(call)
            with self.assertRaises(ValueError):
                guard.call("save", lambda _: session.send(call), None)
            self.assertEqual(transport.call_count, 1)
        self.assertFalse(session.trust_env)
        self.assertEqual(session.get_adapter(call.url).max_retries.total, 0)

    def test_guard_bounds_response_before_sdk_parsing(self):
        for kind in ("size", "declared", "duplicate", "late"):
            session = requests.Session()
            self.addCleanup(session.close)
            guard = bridge.Guard(SimpleNamespace(http_client=lambda: SimpleNamespace(_session=session)), "reconcile")
            r = response(b"x" * (bridge.MAX_REQUEST + 1) if kind == "size" else b"{}")
            if kind == "declared":
                r.headers["Content-Length"] = str(bridge.MAX_REQUEST + 1)
            if kind == "duplicate":
                r = response(b'{"metadata":{},"metadata":{}}')
            if kind == "late":
                def chunks(**kwargs):
                    yield b"{}"
                    raise OSError("SYNTHETIC_TOKEN")
                r.iter_content = chunks
            with mock.patch.object(requests.adapters.HTTPAdapter, "send", return_value=r):
                with self.assertRaises((ValueError, OSError)):
                    guard.call("get", lambda _: session.send(requests.Request("POST", bridge.ROOT + bridge.OPERATIONS["get"]).prepare()), None)

    def test_local_pin_failure_precedes_stdin_and_standalone_watchdog(self):
        poison = SimpleNamespace(buffer=mock.Mock())
        with mock.patch.object(bridge.sys, "argv", ["helper", "submit", "10"]), \
             mock.patch.object(bridge.sys, "version_info", (3, 13)), \
             mock.patch.object(bridge.sys, "stdin", poison):
            self.assertEqual(bridge.main(), 2)
            poison.buffer.readline.assert_not_called()
        source = (ROOT / "internal/provider/kaggle/execution.py").read_text(encoding="utf-8")
        script = ("import sys,time\nns={'__name__':'fixture'}\nexec(" + repr(source) + ",ns)\n"
                  "sys.argv=['helper','observe','1']\nns['main']=lambda:time.sleep(60)\nns['bounded_main']()\n")
        result = subprocess.run([sys.executable, "-I", "-c", script], input=b"", capture_output=True, timeout=5, check=False)
        self.assertEqual(result.returncode, 3)
        self.assertEqual(result.stdout + result.stderr, b"")


@unittest.skipUnless(importlib.util.find_spec("kagglesdk"), "pinned SDK is exercised in client CI")
class ExecutionPinnedSDKTests(unittest.TestCase):
    def setUp(self):
        self.assertEqual(importlib.metadata.version("kagglesdk"), "0.1.35")
        self.r = request_fixture()
        self.kernel = None
        self.calls = []
        self.saves = 0
        self.save_fault = ""
        self.get_fault = ""
        self.status_reply = {"status": "COMPLETE"}
        self.wrong_account = False
        self.pay = False
        self.change_during_status = False

    def exchange(self, adapter, request, **kwargs):
        self.assertEqual(request.method, "POST")
        self.assertEqual(kwargs["timeout"], (5, 10))
        self.assertEqual(kwargs["proxies"], {})
        self.assertTrue(kwargs["verify"])
        self.assertEqual(request.headers["Authorization"], "Bearer SYNTHETIC_TOKEN")
        operation = request.url.removeprefix(bridge.ROOT)
        body = json.loads(request.body)
        self.calls.append((operation, body))
        if operation == bridge.OPERATIONS["auth"]:
            self.assertEqual(body["token"], "SYNTHETIC_TOKEN")
            return response(json.dumps({"active": True, "username": "other_user" if self.wrong_account else self.r["owner"]}).encode())
        if operation == bridge.OPERATIONS["quota"]:
            return response(json.dumps({"gpuQuota": {"isPayToScaleEnabled": self.pay}}).encode())
        if operation == bridge.OPERATIONS["get"]:
            self.assertEqual(body["userName"], self.r["owner"])
            self.assertEqual(body["kernelSlug"], self.r["slug"])
            self.assertIn(body.get("versionLabel", ""), ("", "1"))
            if self.get_fault == "error-body":
                return response(b'{"error":"not found"}')
            if self.get_fault == "unreachable":
                return response(b'{}', 503)
            if self.kernel is None:
                return response(b'{}', 404)
            return response(json.dumps(self.kernel).encode())
        if operation == bridge.OPERATIONS["save"]:
            self.saves += 1
            self.assertEqual(body["slug"], self.r["owner"] + "/" + self.r["slug"])
            self.assertEqual(body["text"], self.r["source"])
            self.assertTrue(body["isPrivate"])
            self.assertEqual(body["sessionTimeoutSeconds"], self.r["wall_seconds"])
            self.assertEqual(body["datasetDataSources"], [self.r["dataset"]])
            self.assertNotIn("SYNTHETIC_TOKEN", body["text"])
            self.assertFalse(body.get("enableTpu", False))
            self.assertFalse(body.get("id", 0))
            self.kernel = kernel_fixture(self.r)
            if self.save_fault == "lost":
                raise requests.exceptions.Timeout("SYNTHETIC_TOKEN")
            if self.save_fault == "http":
                return response(b'{"error":"SYNTHETIC_TOKEN"}', 500)
            receipt = {"ref": body["slug"], "versionNumber": 1, "kernelId": 42}
            if self.save_fault == "error":
                receipt["error"] = "SYNTHETIC_TOKEN"
            if self.save_fault == "version":
                receipt["versionNumber"] = 2
            if self.save_fault == "invalid-input":
                receipt["invalidDatasetSources"] = ["bad/source"]
            return response(json.dumps(receipt).encode())
        if operation == bridge.OPERATIONS["status"]:
            self.assertEqual(body["userName"], self.r["owner"])
            self.assertEqual(body["kernelSlug"], self.r["slug"])
            self.assertNotIn("versionLabel", body)
            if self.change_during_status:
                self.kernel["metadata"]["currentVersionNumber"] = 2
            return response(json.dumps(self.status_reply).encode())
        self.fail("unexpected provider operation")

    def invoke(self, mode):
        r = dict(self.r)
        if mode != "submit":
            r["source"] = ""
        if mode == "observe":
            r["kernel_id"] = "42"
        def send(adapter, request, **kwargs):
            return self.exchange(adapter, request, **kwargs)
        with mock.patch.object(requests.adapters.HTTPAdapter, "send", send), \
             mock.patch.dict(os.environ, {"KAGGLE_API_TOKEN": "AMBIENT_CANARY", "HTTPS_PROXY": "http://blocked.invalid"}), \
             mock.patch("kagglesdk.kaggle_http_client.get_access_token_from_env", side_effect=AssertionError("ambient auth")), \
             mock.patch("kagglesdk.kaggle_http_client._get_apikey_creds", side_effect=AssertionError("legacy auth")):
            result = bridge.operate(bridge.validate_request(r, mode), "SYNTHETIC_TOKEN", mode)
        self.assertNotIn("SYNTHETIC_TOKEN", json.dumps(result))
        return result

    def test_private_save_readback_and_explicit_version_observation(self):
        result = self.invoke("submit")
        self.assertEqual(result["status"], "found")
        self.assertEqual(result["kernel_id"], "42")
        self.assertEqual(result["raw_state"], "COMPLETE")
        self.assertEqual(self.saves, 1)
        self.assertEqual(self.invoke("reconcile")["status"], "found")
        self.assertEqual(self.invoke("observe")["status"], "found")
        self.assertEqual(self.invoke("submit")["status"], "found")  # exact existing source is never updated
        self.assertEqual(self.saves, 1)

    def test_lost_or_error_save_is_unknown_then_reconciled_without_resave(self):
        for fault in ("lost", "http", "error", "version", "invalid-input"):
            with self.subTest(fault=fault):
                self.kernel, self.saves, self.save_fault = None, 0, fault
                self.assertEqual(self.invoke("submit")["status"], "unknown")
                self.assertEqual(self.saves, 1)
                self.save_fault = ""
                self.assertEqual(self.invoke("reconcile")["status"], "found")
                self.assertEqual(self.saves, 1)

    def test_only_actual_absence_can_enter_new_save_and_reads_never_create(self):
        for mode in ("observe", "reconcile"):
            self.assertEqual(self.invoke(mode)["status"], "not_found")
        self.assertEqual(self.saves, 0)
        self.get_fault = "error-body"
        self.assertEqual(self.invoke("submit")["status"], "invalid")
        self.get_fault = "unreachable"
        self.assertEqual(self.invoke("submit")["status"], "rejected")
        self.assertEqual(self.saves, 0)
        self.get_fault, self.wrong_account = "", True
        self.assertEqual(self.invoke("submit")["status"], "rejected")
        self.assertEqual(self.saves, 0)

    def test_missing_and_new_status_cannot_inherit_default_queued(self):
        self.kernel = kernel_fixture(self.r)
        for value in ({}, {"status": None}, {"status": "FUTURE_STATE"}, {"status": True}, {"status": 99}):
            self.status_reply = value
            result = self.invoke("observe")
            self.assertEqual(result["status"], "found")
            self.assertEqual(result["raw_state"], "UNKNOWN")
        for name in bridge.STATES:
            self.status_reply = {"status": name}
            result = self.invoke("observe")
            self.assertEqual(result["status"], "found")
            self.assertEqual(result["raw_state"], name)
        self.assertEqual(self.saves, 0)

    def test_manual_source_version_privacy_and_id_changes_fail_closed(self):
        for fault in ("source", "version", "public", "id", "missing-private", "during-poll"):
            with self.subTest(fault=fault):
                self.kernel = kernel_fixture(self.r)
                self.change_during_status = fault == "during-poll"
                if fault == "source":
                    self.kernel["blob"]["source"] += "# modified"
                if fault == "version":
                    self.kernel["metadata"]["currentVersionNumber"] = 2
                if fault == "public":
                    self.kernel["metadata"]["isPrivate"] = False
                if fault == "id":
                    self.kernel["metadata"]["id"] = 43
                if fault == "missing-private":
                    del self.kernel["metadata"]["isPrivate"]
                self.assertEqual(self.invoke("observe")["status"], "invalid")
                self.assertEqual(self.saves, 0)

    def test_gpu_selection_is_explicit_and_pay_to_scale_blocks_save(self):
        self.r.update(gpu=True, machine_shape="NvidiaTeslaT4")
        for pay in (True, None):
            self.pay = pay
            self.assertEqual(self.invoke("submit")["status"], "rejected")
            self.assertEqual(self.saves, 0)
        self.pay = False
        self.assertEqual(self.invoke("submit")["status"], "found")
        save_body = next(body for op, body in self.calls if op == bridge.OPERATIONS["save"])
        self.assertEqual(save_body["machineShape"], "NvidiaTeslaT4")
        self.assertTrue(save_body["enableGpu"])
        self.assertEqual(self.saves, 1)
