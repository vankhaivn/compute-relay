"""Offline checks for the exact Go-embedded helper, including the real pinned SDK."""
import importlib.util
import io
import json
import os
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest
from unittest import mock

import requests

ROOT = Path(__file__).resolve().parents[3]
SPEC = importlib.util.spec_from_file_location(
    "compute_relay_preflight", ROOT / "internal/provider/kaggle/preflight.py")
bridge = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(bridge)


def response(payload=b"{}", status=200, headers=None):
    r = requests.Response()
    r.status_code = status
    r.headers = {"Content-Type": "application/json", **(headers or {})}
    r.raw = io.BytesIO(payload)
    return r


class PreflightTransportTests(unittest.TestCase):
    def session(self):
        session = requests.Session()
        self.addCleanup(session.close)
        bridge.harden_session(SimpleNamespace(http_client=lambda: SimpleNamespace(_session=session)))
        return session

    def test_no_proxy_retry_redirect_or_unbounded_body(self):
        session = self.session()
        self.assertFalse(session.trust_env)
        self.assertEqual(session.get_adapter(bridge.URLS[0]).max_retries.total, 0)
        call = requests.Request("POST", bridge.URLS[0]).prepare()
        for kind in ("pass", "redirect", "large", "declared", "duplicate", "invalid-utf8", "late-error"):
            with self.subTest(kind=kind):
                session = self.session()
                r = response()
                if kind == "redirect":
                    r = response(b"x" * 100000, 307, {"Location": "https://must-not-follow.invalid"})
                    r.raw = mock.Mock(wraps=r.raw)
                if kind == "large":
                    r = response(b"x" * (bridge.MAX_RESPONSE + 1))
                if kind == "declared":
                    r = response(headers={"Content-Length": str(bridge.MAX_RESPONSE + 1)})
                if kind == "duplicate":
                    r = response(b'{"active":false,"active":true}')
                if kind == "invalid-utf8":
                    r = response(b'{"name":"\xff"}')
                if kind == "late-error":
                    def chunks(*args, **kwargs):
                        yield b"{}"
                        raise OSError("SYNTHETIC_TOKEN")
                    r.iter_content = chunks
                with mock.patch.object(requests.adapters.HTTPAdapter, "send", return_value=r) as send:
                    if kind == "pass":
                        self.assertEqual(session.send(call).json(), {})
                    else:
                        with self.assertRaises((ValueError, OSError)):
                            session.send(call)
                    self.assertEqual(send.call_count, 1)
                    settings = send.call_args.kwargs
                    self.assertEqual(settings["proxies"], {})
                    self.assertTrue(settings["verify"])
                    self.assertTrue(settings["stream"])
                    self.assertEqual(settings["timeout"], (5, 10))
                    if kind == "redirect":
                        r.raw.read.assert_not_called()

    def test_only_two_exact_ordered_read_requests(self):
        session = self.session()
        with mock.patch.object(requests.adapters.HTTPAdapter, "send", side_effect=lambda *a, **k: response()) as send:
            for url in bridge.URLS:
                session.send(requests.Request("POST", url).prepare())
            for url in (bridge.URLS[1], bridge.URLS[0], "https://other.invalid"):
                with self.assertRaises(ValueError):
                    session.send(requests.Request("POST", url).prepare())
            self.assertEqual(send.call_count, 2)
        for method, url in (("GET", bridge.URLS[0]), ("POST", bridge.URLS[1]),
                            ("POST", bridge.URLS[0] + "?token=x"), ("POST", "http://api.kaggle.com")):
            with mock.patch.object(requests.adapters.HTTPAdapter, "send") as send:
                with self.assertRaises(ValueError):
                    self.session().send(requests.Request(method, url).prepare())
                send.assert_not_called()

    def test_pins_before_stdin_or_sdk(self):
        poison = SimpleNamespace(buffer=mock.Mock())
        poison.buffer.read.side_effect = AssertionError("read secret during local/mismatch")
        with mock.patch.object(bridge.sys, "argv", ["helper", "read_only", "fixture_user"]), \
             mock.patch.object(bridge.sys, "stdin", poison), \
             mock.patch.object(bridge.sys, "version_info", (3, 13)), \
             mock.patch.object(bridge, "read_only") as sdk, \
             context_stdout() as out:
            self.assertEqual(bridge.main(), 0)
            self.assertEqual(json.loads(out.getvalue())["local"], "version_mismatch")
            sdk.assert_not_called()
        with mock.patch.object(bridge.sys, "version_info", (3, 11)), \
             mock.patch.object(bridge.importlib.metadata, "version", side_effect=lambda n: {"kaggle": "2.2.4", "kagglesdk": "0.1.35"}[n]):
            self.assertEqual(bridge.local_check("local"), bridge.baseline("local"))
        with mock.patch.object(bridge.sys, "version_info", (3, 11)), \
             mock.patch.object(bridge.importlib.metadata, "version", side_effect=RuntimeError("SYNTHETIC_TOKEN")):
            self.assertEqual(bridge.local_check("local")["problem"], "unavailable")

    def test_invalid_tokens_never_call_sdk(self):
        for token in (b"", b"x\n", b"x y", b"\xff", b"x" * (bridge.MAX_TOKEN + 1)):
            with mock.patch.object(bridge.sys, "argv", ["helper", "read_only", "fixture_user"]), \
                 mock.patch.object(bridge.sys, "stdin", SimpleNamespace(buffer=io.BytesIO(token))), \
                 mock.patch.object(bridge, "local_check", return_value=bridge.baseline("read_only")), \
                 mock.patch.object(bridge, "read_only") as sdk, context_stdout() as out:
                self.assertEqual(bridge.main(), 0)
                self.assertEqual(json.loads(out.getvalue())["problem"], "credential_unavailable")
                sdk.assert_not_called()

    def test_exception_text_never_reaches_protocol(self):
        with mock.patch.object(bridge.sys, "argv", ["helper", "read_only", "fixture_user"]), \
             mock.patch.object(bridge.sys, "stdin", SimpleNamespace(buffer=io.BytesIO(b"SYNTHETIC_TOKEN"))), \
             mock.patch.object(bridge, "local_check", return_value=bridge.baseline("read_only")), \
             mock.patch.object(bridge, "read_only", side_effect=ValueError("SYNTHETIC_TOKEN")), context_stdout() as out:
            self.assertEqual(bridge.main(), 0)
            self.assertNotIn("SYNTHETIC_TOKEN", out.getvalue())
            self.assertEqual(json.loads(out.getvalue())["authentication"], "unavailable")


def context_stdout():
    import contextlib
    @contextlib.contextmanager
    def capture():
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            yield out
    return capture()


@unittest.skipUnless(importlib.util.find_spec("kagglesdk"), "requires pinned SDK; transport tests still run")
class PinnedSDKPreflightTests(unittest.TestCase):
    def test_exact_versions_and_real_sdk_protocol_without_network(self):
        self.assertEqual(bridge.local_check("local")["local"], "ready")
        for kind in ("success", "wrong-account", "inactive", "missing-name", "auth-error", "quota-error", "redirect"):
            with self.subTest(kind=kind):
                calls = []
                def exchange(adapter, request, **kwargs):
                    calls.append(request)
                    self.assertEqual(request.headers["Authorization"], "Bearer SYNTHETIC_TOKEN")
                    if len(calls) == 1:
                        self.assertEqual(request.url, bridge.URLS[0])
                        self.assertEqual(json.loads(request.body)["token"], "SYNTHETIC_TOKEN")
                        if kind == "auth-error":
                            return response(b'{"message":"SYNTHETIC_TOKEN"}', 401)
                        if kind == "redirect":
                            return response(b"", 307, {"Location": "https://do-not-follow.invalid"})
                        name = "other_user" if kind == "wrong-account" else "fixture_user"
                        if kind == "missing-name":
                            name = ""
                        return response(json.dumps({"active": kind != "inactive", "username": name}).encode())
                    self.assertEqual(request.url, bridge.URLS[1])
                    return response(status=503 if kind == "quota-error" else 200)
                with mock.patch.object(requests.adapters.HTTPAdapter, "send", exchange), \
                     mock.patch.dict(os.environ, {"KAGGLE_API_TOKEN": "AMBIENT_CANARY", "HTTPS_PROXY": "http://blocked.invalid"}), \
                     mock.patch("kagglesdk.kaggle_http_client.get_access_token_from_env", side_effect=AssertionError("ambient auth")), \
                     mock.patch("kagglesdk.kaggle_http_client._get_apikey_creds", side_effect=AssertionError("legacy auth")):
                    result = bridge.read_only("SYNTHETIC_TOKEN", "fixture_user")
                self.assertFalse(result["batch_ready"])
                self.assertNotIn("SYNTHETIC_TOKEN", json.dumps(result))
                self.assertNotIn("other_user", json.dumps(result))
                expected = {"success": "none", "wrong-account": "account_mismatch", "inactive": "credential_rejected",
                            "missing-name": "invalid_response", "auth-error": "credential_rejected",
                            "quota-error": "quota_unavailable", "redirect": "provider_unavailable"}[kind]
                self.assertEqual(result["problem"], expected)
                self.assertEqual(len(calls), 2 if kind in ("success", "quota-error") else 1)
