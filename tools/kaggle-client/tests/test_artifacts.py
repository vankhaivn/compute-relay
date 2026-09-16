"""Pinned SDK output/file operations with mocked HTTP; no provider effects."""
import hashlib
import importlib.util
import io
import json
import os
import unittest
from unittest import mock
from urllib.parse import quote, unquote, urlsplit

import requests
from test_artifact_contract import ROOT, contract, fixture
from test_execution import bridge as core, request_fixture, kernel_fixture
from test_preflight import response

SPEC = importlib.util.spec_from_file_location("artifact_bridge", ROOT / "internal/provider/kaggle/artifacts.py")
bridge = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(bridge)
bridge.core, bridge.contract = core, contract


def artifact_fixture():
    identity, outputs, manifest, _ = fixture()
    execution = request_fixture()
    execution.update(source="", kernel_id="42")
    return dict(protocol=1, execution=execution, identity=identity, outputs=outputs,
                max_files=contract.MAX_FILES, max_bytes=contract.MAX_BYTES, target=None), manifest


class ArtifactTransportTests(unittest.TestCase):
    def test_guard_cannot_authorize_mutations_or_unarmed_requests(self):
        from types import SimpleNamespace
        session = requests.Session()
        self.addCleanup(session.close)
        guard = bridge.Guard(SimpleNamespace(http_client=lambda: SimpleNamespace(_session=session)), "SYNTHETIC_TOKEN")
        self.addCleanup(guard.close)
        with mock.patch.object(requests.adapters.HTTPAdapter, "send") as send:
            for name in ("SaveKernel", "CancelKernelSession", "DeleteKernel", "DownloadKernelOutputZip"):
                with self.assertRaises(ValueError):
                    guard.call(bridge.KERNEL + name, lambda _: None, None)
            with self.assertRaises(ValueError):
                guard.send(requests.Request("POST", bridge.API + bridge.DOWNLOAD).prepare())
            send.assert_not_called()
        self.assertFalse(session.trust_env)
        self.assertEqual(guard.cloud.max_retries.total, 0)

    def test_bounded_eof_encoding_and_request_validation(self):
        for payload, headers in ((b"abcd", {"Content-Length": "3"}), (b"a", {"Content-Length": "2"}),
                                 (b"", {"Content-Encoding": "gzip"}), (b"", {"Content-Length": "-1"})):
            r = response(payload, headers=headers)
            with self.assertRaises(ValueError):
                list(bridge.chunks(r, 3))
        r, _ = artifact_fixture()
        self.assertEqual(bridge.validate_request(r, "catalog"), r)
        for key, value in (("protocol", True), ("target", {}), ("max_files", 0), ("max_bytes", 1 << 40)):
            with self.assertRaises(ValueError):
                bridge.validate_request(dict(r, **{key: value}), "catalog")
        for path in ("code/main.py", "scratch/private.bin", "outputs/foreign.txt"):
            r["target"] = contract.entry(path, 0, hashlib.sha256(b"").hexdigest())
            with self.assertRaises(ValueError):
                bridge.validate_request(r, "fetch")


@unittest.skipUnless(importlib.util.find_spec("kagglesdk"), "pinned SDK runs in offline client CI")
class ArtifactPinnedSDKTests(unittest.TestCase):
    def setUp(self):
        self.assertEqual(importlib.metadata.version("kagglesdk"), "0.1.35")
        self.r, self.manifest = artifact_fixture()
        self.kernel = kernel_fixture(request_fixture())
        self.data = {contract.MANIFEST: json.dumps(self.manifest).encode(), "outputs/answer.txt": b"yes",
                     "control/stdout.log": b"payload log\n", "control/environment.json": b"{}",
                     "scratch/private.bin": b"NEVER_DOWNLOAD", "code/main.py": b"NEVER_DOWNLOAD"}
        self.calls, self.downloads, self.cloud_calls = [], [], []
        self.list_fault = ""
        self.read_fault = ""
        self.change_after = ""
        self.wrong_account = False
        self.status = "COMPLETE"
        self.redirect = False
        self.target_path = "outputs/answer.txt"
        self.listed = [contract.PREFIX + path for path in self.data]

    def exchange(self, adapter, request, **kwargs):
        self.assertTrue(kwargs["stream"])
        self.assertTrue(kwargs["verify"])
        self.assertEqual(kwargs["proxies"], {})
        self.assertEqual(kwargs["timeout"], (5, 30))
        if request.url.startswith("https://storage.googleapis.com/"):
            self.assertEqual(request.method, "GET")
            self.assertNotIn("Authorization", request.headers)
            self.assertNotIn("Cookie", request.headers)
            path = unquote(urlsplit(request.url).path[len("/fixture/"):])
            self.cloud_calls.append(path)
            return self.raw_response(path)
        self.assertEqual(request.method, "POST")
        self.assertEqual(request.headers["Authorization"], "Bearer SYNTHETIC_TOKEN")
        op = request.url.removeprefix(bridge.API)
        body = json.loads(request.body)
        self.calls.append((op, body))
        self.assertIn(op, bridge.ALLOWED)
        if op == bridge.AUTH:
            return response(json.dumps(dict(active=True, username="other_user" if self.wrong_account else self.r["execution"]["owner"])).encode())
        if op == bridge.KERNEL + "GetKernel":
            self.assertEqual(body["userName"], self.r["execution"]["owner"])
            self.assertEqual(body["kernelSlug"], self.r["execution"]["slug"])
            if self.target_path in self.downloads and self.change_after:
                if self.change_after == "source":
                    self.kernel["blob"]["source"] += "# changed"
                elif self.change_after == "privacy":
                    self.kernel["metadata"]["isPrivate"] = False
                elif self.change_after == "id":
                    self.kernel["metadata"]["id"] = 43
            return response(json.dumps(self.kernel).encode())
        if op == bridge.KERNEL + "GetKernelSessionStatus":
            self.assertEqual(body["versionLabel"], "1")
            return response(json.dumps(dict(status=self.status)).encode())
        if op == bridge.KERNEL + "ListKernelSessionOutput":
            self.assertEqual(body["versionLabel"], "1")
            self.assertEqual(body["pageSize"], 100)
            cursor = body.get("pageToken", "")
            offset = int(cursor) if cursor else 0
            names = self.listed[offset:offset + 2]
            files = [{"fileName": name, "url": "https://untrusted.invalid/DO_NOT_FOLLOW"} for name in names]
            next_cursor = str(offset + 2) if offset + 2 < len(self.listed) else ""
            if self.list_fault == "cycle":
                next_cursor = "2"
            if self.list_fault == "duplicate" and offset:
                files[0]["fileName"] = self.listed[0]
            if self.list_fault == "unsafe":
                files[0]["fileName"] = "../outside"
            if self.list_fault == "missing-field":
                return response(b"{}")
            return response(json.dumps(dict(files=files, nextPageToken=next_cursor)).encode())
        if op == bridge.DOWNLOAD:
            self.assertEqual(body["versionNumber"], 1)
            self.assertEqual(body["ownerSlug"], self.r["execution"]["owner"])
            self.assertEqual(body["kernelSlug"], self.r["execution"]["slug"])
            physical = body["filePath"]
            self.assertTrue(physical.startswith(contract.PREFIX))
            path = physical[len(contract.PREFIX):]
            self.assertNotIn(path.split("/", 1)[0], ("scratch", "code", "inputs"))
            self.downloads.append(path)
            if self.redirect:
                r = response(b"NEVER_READ_REDIRECT_BODY", 302, {"Location": "https://storage.googleapis.com/fixture/" + quote(path, safe="")})
                r.raw = mock.Mock(wraps=r.raw)
                r.raw.read.side_effect = AssertionError("redirect body consumed")
                return r
            return self.raw_response(path)
        self.fail("unexpected operation")

    def raw_response(self, path):
        data = self.data[path]
        fault = self.read_fault if path == self.target_path else ""
        headers = {"Content-Type": "application/octet-stream"}
        if fault == "short":
            data = data[:-1]
        elif fault == "wrong":
            data = b"NO!" if len(data) == 3 else b"?" * len(data)
        elif fault == "overflow":
            data += b"x"
        elif fault == "encoding":
            headers["Content-Encoding"] = "gzip"
        r = response(data, headers=headers)
        if fault == "late":
            def chunks(**kwargs):
                yield data
                raise OSError("SYNTHETIC_TOKEN")
            r.iter_content = chunks
        if fault == "close":
            r.close = mock.Mock(side_effect=OSError("SYNTHETIC_TOKEN"))
        return r

    def invoke(self, mode="catalog", sink=None):
        def send(adapter, request, **kwargs):
            return self.exchange(adapter, request, **kwargs)
        if sink is None:
            sink = io.BytesIO()
        with mock.patch.object(requests.adapters.HTTPAdapter, "send", send), \
             mock.patch.dict(os.environ, {"KAGGLE_API_TOKEN": "AMBIENT_CANARY", "HTTPS_PROXY": "http://blocked.invalid"}), \
             mock.patch("kagglesdk.kaggle_http_client.get_access_token_from_env", side_effect=AssertionError("ambient auth")), \
             mock.patch("kagglesdk.kaggle_http_client._get_apikey_creds", side_effect=AssertionError("legacy auth")):
            return bridge.operate(bridge.validate_request(self.r, mode), "SYNTHETIC_TOKEN", mode, sink)

    def pin(self, path):
        self.r["target"] = contract.entry(path, len(self.data[path]), hashlib.sha256(self.data[path]).hexdigest())

    def test_all_pages_manifest_first_and_selected_control_only(self):
        result = self.invoke()
        self.assertEqual(self.downloads, [contract.MANIFEST, "control/stdout.log", "control/environment.json"])
        self.assertEqual(len([x for x in self.calls if x[0].endswith("ListKernelSessionOutput")]), 3)
        self.assertEqual([x["path"] for x in result["files"]], sorted([contract.MANIFEST, "outputs/answer.txt", "control/stdout.log", "control/environment.json"]))
        self.assertNotIn("untrusted.invalid", json.dumps(result))
        self.assertEqual(result["files"][-1]["sha256"], hashlib.sha256(b"yes").hexdigest())

    def test_stream_uses_exact_file_version_and_credential_free_signed_storage(self):
        self.redirect = True
        for path in (contract.MANIFEST, "outputs/answer.txt", "control/stdout.log"):
            self.pin(path)
            sink = io.BytesIO()
            self.assertIsNone(self.invoke("fetch", sink))
            self.assertEqual(sink.getvalue(), self.data[path])
        self.assertIn("outputs/answer.txt", self.cloud_calls)
        self.assertTrue(all(op in bridge.ALLOWED for op, _ in self.calls))

    def test_pagination_identity_and_missing_results_never_authorize_bytes(self):
        for fault in ("cycle", "duplicate", "unsafe", "missing-field"):
            self.list_fault = fault
            with self.assertRaises(Exception):
                self.invoke()
            self.assertEqual(self.downloads, [])
        self.list_fault = ""
        self.manifest["attempt_nonce"] = "foreign"
        self.data[contract.MANIFEST] = json.dumps(self.manifest).encode()
        with self.assertRaises(ValueError):
            self.invoke()
        self.assertEqual(self.downloads, [contract.MANIFEST])
        self.downloads.clear()
        self.listed.remove(contract.PREFIX + contract.MANIFEST)
        with self.assertRaises(ValueError):
            self.invoke()
        self.assertEqual(self.downloads, [])

    def test_full_bytes_followed_by_digest_eof_close_or_identity_error_fail(self):
        self.pin("outputs/answer.txt")
        for fault in ("short", "wrong", "overflow", "encoding", "late", "close"):
            self.read_fault = fault
            with self.subTest(fault=fault), self.assertRaises(Exception):
                self.invoke("fetch", io.BytesIO())
        self.read_fault = ""
        for fault in ("source", "privacy", "id"):
            self.kernel = kernel_fixture(request_fixture())
            self.downloads.clear()
            self.change_after = fault
            sink = io.BytesIO()
            with self.assertRaises(core.IdentityMismatch):
                self.invoke("fetch", sink)
            self.assertEqual(sink.getvalue(), b"yes")

    def test_nonterminal_wrong_account_and_changed_pin_stop_without_fallback(self):
        for status in ("RUNNING", "QUEUED", "FUTURE_STATE", None):
            self.status = status
            with self.assertRaises(ValueError):
                self.invoke()
            self.assertEqual(self.downloads, [])
        self.status, self.wrong_account = "COMPLETE", True
        with self.assertRaises(ValueError):
            self.invoke()
        self.assertEqual(self.downloads, [])
        self.wrong_account = False
        self.pin("outputs/answer.txt")
        self.r["target"]["sha256"] = "0" * 64
        with self.assertRaises(ValueError):
            self.invoke("fetch")
        self.assertNotIn("outputs/answer.txt", self.downloads)

    def test_large_payload_is_streamed_in_bounded_chunks(self):
        payload = b"abcdef01" * (2 << 20)
        self.data["outputs/answer.txt"] = payload
        self.r["outputs"][0]["max_bytes"] = 0
        self.manifest["artifacts"][0].update(bytes=len(payload), sha256=hashlib.sha256(payload).hexdigest())
        self.data[contract.MANIFEST] = json.dumps(self.manifest).encode()
        self.pin("outputs/answer.txt")
        class Sink:
            size = 0
            largest = 0
            h = hashlib.sha256()
            def write(inner, data):
                inner.size += len(data)
                inner.largest = max(inner.largest, len(data))
                inner.h.update(data)
                return len(data)
        sink = Sink()
        self.invoke("fetch", sink)
        self.assertEqual(sink.size, 16 << 20)
        self.assertLessEqual(sink.largest, 65536)
        self.assertEqual(sink.h.hexdigest(), self.r["target"]["sha256"])
