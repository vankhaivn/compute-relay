"""Pinned SDK cleanup wire tests; all provider traffic is synthetic."""
import copy
import hashlib
import importlib.util
import io
import json
import unittest
from types import SimpleNamespace
from unittest import mock

import requests
from test_execution import bridge as core, request_fixture, kernel_fixture
from test_preflight import ROOT, response

SPEC = importlib.util.spec_from_file_location("cleanup_bridge", ROOT / "internal/provider/kaggle/cleanup.py")
bridge = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(bridge)
bridge.core = core


class CleanupSDKTests(unittest.TestCase):
    def setUp(self):
        execution = request_fixture()
        self.r = dict(protocol=1, owner=execution["owner"], slug=execution["slug"], kind="kernel", id="42",
                      sha256=execution["source_sha256"], accept_name_based_delete=True)
        self.kernel = kernel_fixture(execution)
        self.marker = b'{"original":"marker"}'
        self.calls, self.bodies, self.deletes = [], [], 0
        self.state, self.fault, self.changed = "COMPLETE", "", False
        self.meta = None

    def dataset(self):
        self.r.update(kind="dataset", slug="crs-" + "b" * 40, sha256=hashlib.sha256(self.marker).hexdigest())
        self.meta = dict(id=42, ref=self.r["owner"] + "/" + self.r["slug"], currentVersionNumber=1, isPrivate=True,
                         description="Compute Relay staging " + self.r["sha256"] + ". Existing licenses remain applicable.")

    def exchange(self, adapter, request, **kwargs):
        self.assertEqual(kwargs["proxies"], {})
        self.assertTrue(kwargs["verify"])
        self.assertEqual(adapter.max_retries.total, 0)
        if request.method == "GET":
            self.assertNotIn("Authorization", request.headers)
            self.assertNotIn("Cookie", request.headers)
            return response(self.marker, headers={"Content-Type": "application/octet-stream"})
        self.assertEqual(request.method, "POST")
        self.assertEqual(request.headers.get("Authorization"), "Bearer SYNTHETIC_TOKEN")
        operation = request.url.removeprefix(bridge.API)
        self.assertIn(operation, bridge.READS | bridge.DELETES)
        self.calls.append(operation)
        body = json.loads(request.body)
        self.bodies.append(body)
        if operation == bridge.AUTH:
            return response(json.dumps(dict(active=True, username="foreign" if self.fault == "account" else self.r["owner"])).encode())
        if operation.endswith(("GetKernel", "GetDataset")):
            if self.fault == "403" or self.deletes and self.fault == "post403":
                return response(b'{"error":{"code":403,"status":"PERMISSION_DENIED","message":"Permission denied"}}', status=403)
            if self.fault == "404" or self.deletes and self.fault != "still_present":
                return response(status=404)
            raw = copy.deepcopy(self.kernel if self.r["kind"] == "kernel" else self.meta)
            if self.changed:
                (raw["metadata"] if self.r["kind"] == "kernel" else raw)["id"] = 43
            if self.fault == "second_read":
                self.changed = True
            return response(json.dumps(raw).encode())
        if operation.endswith("GetKernelSessionStatus"):
            self.assertNotIn("versionLabel", body)
            return response(json.dumps(dict(status=self.state)).encode())
        if operation.endswith("DownloadDataset"):
            self.assertEqual(body, dict(ownerSlug=self.r["owner"], datasetSlug=self.r["slug"], datasetVersionNumber=1, fileName="relay-stage.bin", raw=True))
            if self.fault == "redirect":
                return response(status=302, headers={"Location": "https://storage.googleapis.com/fixture?sig=synthetic"})
            if self.fault == "foreign_redirect":
                return response(status=302, headers={"Location": "https://evil.invalid/fixture"})
            return response(self.marker, headers={"Content-Type": "application/octet-stream"})
        if operation in bridge.DELETES:
            expected = dict(userName=self.r["owner"], kernelSlug=self.r["slug"]) if self.r["kind"] == "kernel" else dict(ownerSlug=self.r["owner"], datasetSlug=self.r["slug"])
            self.assertEqual(body, expected)
            self.deletes += 1
            if self.fault == "lost_ack":
                raise requests.ConnectionError("SYNTHETIC_TOKEN")
            if self.fault == "202":
                return response(status=202)
            if self.fault == "delete_error":
                return response(b'{"errorMessage":"SYNTHETIC_TOKEN"}' if self.r["kind"] == "kernel" else b'{"error":"SYNTHETIC_TOKEN"}')
            return response()
        self.fail("unhandled operation")

    def run_bridge(self, mode="apply"):
        with mock.patch.object(requests.adapters.HTTPAdapter, "send", autospec=True, side_effect=self.exchange):
            value = bridge.operate(self.r, "SYNTHETIC_TOKEN", mode)
        self.assertNotIn("SYNTHETIC_TOKEN", json.dumps(value))
        return value["status"]

    def test_delete_both_kinds_checks_original_and_confirms_absence(self):
        for kind in ("kernel", "dataset"):
            with self.subTest(kind=kind):
                self.setUp()
                if kind == "dataset": self.dataset()
                self.assertEqual(self.run_bridge(), "deleted")
                self.assertEqual(self.deletes, 1)
                self.assertEqual(sum(call.endswith(("GetKernel", "GetDataset")) for call in self.calls), 3)

    def test_preview_recovery_and_no_acceptance_never_delete(self):
        for kind in ("kernel", "dataset"):
            for mode, expected in (("preview", "would_delete"), ("recover", "unknown")):
                self.setUp()
                if kind == "dataset": self.dataset()
                self.assertEqual(self.run_bridge(mode), expected)
                self.assertEqual(self.deletes, 0)
        self.r["accept_name_based_delete"] = False
        with self.assertRaises(ValueError): self.run_bridge()
        self.assertEqual(self.run_bridge("preview"), "would_delete")

    def test_uncertain_delete_is_not_reissued_by_recovery(self):
        for kind in ("kernel", "dataset"):
            for fault in ("lost_ack", "202", "delete_error", "still_present"):
                with self.subTest(kind=kind, fault=fault):
                    self.setUp()
                    if kind == "dataset": self.dataset()
                    self.fault = fault
                    self.assertEqual(self.run_bridge(), "unknown")
                    self.assertEqual(self.deletes, 1)
                    self.run_bridge("recover")
                    self.assertEqual(self.deletes, 1)

    def test_completed_delete_survives_provider_missing_name_403(self):
        for kind in ("kernel", "dataset"):
            with self.subTest(kind=kind):
                self.setUp()
                if kind == "dataset": self.dataset()
                self.fault = "post403"
                self.assertEqual(self.run_bridge(), "deleted")
                self.assertEqual(self.deletes, 1)
                # Without the received completion response, the same 403 is
                # ambiguous. A fresh recovery process must not repeat deletion.
                self.assertEqual(self.run_bridge("recover"), "unknown")
                self.assertEqual(self.deletes, 1)

    def test_predelete_replacement_access_and_activity_are_retained(self):
        for fault, expected in (("account", "identity_mismatch"), ("second_read", "identity_mismatch"), ("403", "unknown"), ("404", "absent")):
            for kind in ("kernel", "dataset"):
                self.setUp()
                if kind == "dataset": self.dataset()
                self.fault = fault
                self.assertEqual(self.run_bridge(), expected, (kind, fault))
                self.assertEqual(self.deletes, 0)
        for state in ("RUNNING", "QUEUED", "CANCEL_ACKNOWLEDGED", "unknown", None, True):
            self.setUp()
            self.state = state
            self.assertEqual(self.run_bridge(), "unknown" if state in ("unknown", True, None) else "active", state)
            self.assertEqual(self.deletes, 0)

    def test_raw_identity_never_inherits_defaults(self):
        for kind in ("kernel", "dataset"):
            for field, value in (("id", None), ("id", True), ("id", 43), ("ref", "foreign/name"), ("currentVersionNumber", 2), ("currentVersionNumber", True), ("currentVersionNumber", None), ("isPrivate", False), ("isPrivate", None)):
                self.setUp()
                if kind == "dataset": self.dataset()
                target = self.kernel["metadata"] if kind == "kernel" else self.meta
                target[field] = value
                self.assertEqual(self.run_bridge(), "identity_mismatch", (kind, field, value))
                self.assertEqual(self.deletes, 0)
        self.setUp()
        self.kernel["blob"]["source"] += "#foreign"
        self.assertEqual(self.run_bridge(), "identity_mismatch")
        self.setUp(); self.dataset(); self.marker += b"foreign"
        self.assertEqual(self.run_bridge(), "identity_mismatch")
        self.assertEqual(self.deletes, 0)

    def test_signed_marker_download_keeps_token_off_storage(self):
        self.dataset(); self.fault = "redirect"
        self.assertEqual(self.run_bridge(), "deleted")
        self.setUp(); self.dataset(); self.fault = "foreign_redirect"
        self.assertEqual(self.run_bridge(), "unknown")
        self.assertEqual(self.deletes, 0)

    def test_transport_blocks_hidden_retry_and_unrelated_mutations(self):
        session = requests.Session()
        self.addCleanup(session.close)
        guard = bridge.Guard(SimpleNamespace(http_client=lambda: SimpleNamespace(_session=session)), "SYNTHETIC_TOKEN", "recover")
        self.addCleanup(guard.close)
        with mock.patch.object(requests.adapters.HTTPAdapter, "send") as send:
            for operation in bridge.DELETES | {bridge.KERNEL + "SaveKernel", bridge.DATASET + "CreateDataset"}:
                with self.assertRaises(ValueError): guard.call(operation, lambda _: None, None)
            with self.assertRaises(ValueError): guard.send(requests.Request("POST", bridge.API + bridge.AUTH).prepare())
            send.assert_not_called()

    def test_local_pins_precede_secret_read_and_marker_size_is_bounded(self):
        source = mock.Mock(readline=mock.Mock(side_effect=AssertionError("secret read")))
        with mock.patch.object(bridge.importlib.metadata, "version", return_value="wrong"), mock.patch.object(bridge.sys, "stdin", SimpleNamespace(buffer=source)):
            self.assertEqual(bridge.main(), bridge.result("unknown"))
        for data, headers in ((b"a" * 65537, {}), (b"a", {"Content-Length": "2"}), (b"a", {"Content-Encoding": "gzip"})):
            with self.assertRaises(ValueError): bridge.marker_bytes(response(data, headers=headers))
