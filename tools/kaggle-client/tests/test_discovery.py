"""Managed account discovery through the real pinned SDK and synthetic HTTP only."""
import contextlib
import importlib.util
import io
import json
import os
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest import mock

import requests

from test_preflight import bridge as preflight, response


def load(name, filename):
    path = Path(__file__).resolve().parents[3] / "internal/provider/kaggle" / filename
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


discovery = load("compute_relay_discovery", "discovery.py")
discovery.preflight = preflight
discovery.quota_decoder = load("compute_relay_discovery_quota", "monitor.py")


class DiscoveryProtocolTests(unittest.TestCase):
    def invoke(self, token):
        output = io.StringIO()
        with mock.patch.object(discovery.sys, "argv", ["helper"]), \
             mock.patch.object(discovery.sys, "stdin", SimpleNamespace(buffer=io.BytesIO(token))), \
             contextlib.redirect_stdout(output):
            self.assertEqual(discovery.bounded_main(), 0)
        return json.loads(output.getvalue())

    def test_all_pins_checked_before_stdin_or_sdk(self):
        for python, client, sdk in (((3, 12), "2.2.4", "0.1.35"),
                                    ((3, 11), "2.2.5", "0.1.35"),
                                    ((3, 11), "2.2.4", "0.1.36")):
            with self.subTest(python=python, client=client, sdk=sdk):
                poison = SimpleNamespace(buffer=mock.Mock())
                poison.buffer.read.side_effect = AssertionError("credential read with mismatched pins")
                with mock.patch.object(discovery.sys, "argv", ["helper"]), \
                     mock.patch.object(discovery.sys, "stdin", poison), \
                     mock.patch.object(preflight.sys, "version_info", python), \
                     mock.patch.object(preflight.importlib.metadata, "version",
                                       side_effect=lambda name: {"kaggle": client, "kagglesdk": sdk}[name]), \
                     mock.patch.object(discovery, "discover") as call:
                    self.assertEqual(discovery.main(), discovery.empty("unavailable"))
                    call.assert_not_called()

    def test_invalid_tokens_never_create_client(self):
        for token in (b"", b"x\n", b"x y", b"\xff", b"x" * 8193):
            with mock.patch.object(preflight, "local_check", return_value={"local": "ready"}), \
                 mock.patch.object(discovery, "discover") as call:
                self.assertEqual(self.invoke(token), discovery.empty("credential_rejected"))
                call.assert_not_called()

    def test_raw_error_and_diagnostics_never_reach_protocol(self):
        def poison(token):
            print(token)
            raise RuntimeError(token)
        with mock.patch.object(preflight, "local_check", return_value={"local": "ready"}), \
             mock.patch.object(discovery, "discover", poison):
            result = self.invoke(b"SYNTHETIC_TOKEN")
        self.assertEqual(result, discovery.empty("unavailable"))
        self.assertNotIn("SYNTHETIC_TOKEN", json.dumps(result))


@unittest.skipUnless(importlib.util.find_spec("kagglesdk"), "requires locked official SDK")
class DiscoveryPinnedSDKTests(unittest.TestCase):
    def setUp(self):
        self.calls = []
        self.identity = {"active": True, "username": "fixture_user"}
        self.quota = {"gpuQuota": {"totalTimeAllowed": "19.999999999s",
                                  "timeUsed": "2.000000001s", "timeReserved": "3.000000001s"}}
        self.auth_status = 200
        self.quota_status = 200
        self.token = "SYNTHETIC_TOKEN"
        self.auth_raw = None
        self.quota_raw = None

    def exchange(self, adapter, request, **settings):
        self.calls.append(request.url)
        self.assertLessEqual(len(self.calls), 2)
        self.assertEqual(request.method, "POST")
        self.assertEqual(request.url, preflight.URLS[len(self.calls) - 1])
        self.assertEqual(request.headers["Authorization"], "Bearer " + self.token)
        self.assertEqual(settings, dict(timeout=(5, 10), stream=True, proxies={}, verify=True, cert=None))
        if len(self.calls) == 1:
            self.assertEqual(json.loads(request.body), {"token": self.token})
            return response(self.auth_raw if self.auth_raw is not None else json.dumps(self.identity).encode(), self.auth_status)
        self.assertEqual(json.loads(request.body), {})
        return response(self.quota_raw if self.quota_raw is not None else json.dumps(self.quota).encode(), self.quota_status)

    def invoke(self):
        self.calls = []
        self.assertEqual(preflight.local_check("local")["local"], "ready")
        def send(adapter, request, **settings):
            return self.exchange(adapter, request, **settings)
        with mock.patch.object(requests.adapters.HTTPAdapter, "send", send), \
             mock.patch.dict(os.environ, {"KAGGLE_API_TOKEN": "AMBIENT_CANARY", "HTTPS_PROXY": "http://blocked.invalid"}), \
             mock.patch("kagglesdk.kaggle_http_client.get_access_token_from_env", side_effect=AssertionError("ambient auth")), \
             mock.patch("kagglesdk.kaggle_http_client._get_apikey_creds", side_effect=AssertionError("legacy auth")):
            result = discovery.discover(self.token)
        self.assertNotIn(self.token, json.dumps(result))
        self.assertNotIn("AMBIENT_CANARY", json.dumps(result))
        return result

    def test_same_account_discovery_for_distinct_tokens_and_exact_quota(self):
        first = self.invoke()
        self.assertEqual(first["status"], "verified")
        self.assertEqual(first["account"], "fixture_user")
        self.assertEqual((first["limit_ns"], first["used_ns"], first["reserved_ns"]),
                         ("19999999999", "2000000001", "3000000001"))
        self.assertEqual(len(self.calls), 2)
        self.token = "SYNTHETIC_REPLACEMENT"
        self.assertEqual(self.invoke(), first)

    def test_rejected_or_missing_account_never_reads_quota(self):
        for identity, want in (({"active": False, "username": "fixture_user"}, "credential_rejected"),
                               ({"username": "fixture_user"}, "credential_rejected"),
                               ({"active": True}, "invalid_response")):
            self.identity = identity
            self.assertEqual(self.invoke(), discovery.empty(want))
            self.assertEqual(len(self.calls), 1)
        for account in ("", "Fixture_User", "a", "../other", "fixture_user\n", "fixturé", "a" * 51):
            self.identity = {"active": True, "username": account}
            self.assertEqual(self.invoke(), discovery.empty("invalid_response"))
            self.assertEqual(len(self.calls), 1)
        self.token = "synthetic_token"
        self.identity = {"active": True, "username": self.token}
        self.assertEqual(self.invoke(), discovery.empty("invalid_response"))

    def test_auth_failure_and_malformed_identity_return_no_binding(self):
        for status, want in ((401, "credential_rejected"), (403, "unavailable"), (503, "unavailable")):
            self.auth_status = status
            self.auth_raw = b'{"message":"SYNTHETIC_TOKEN"}'
            self.assertEqual(self.invoke(), discovery.empty(want))
            self.assertEqual(len(self.calls), 1)
        self.auth_status = 200
        for raw in (b'{"active":true,"active":true,"username":"fixture_user"}',
                    b'{"active":1,"username":"fixture_user"}', b'[]', b'{"active":true,"username":null}'):
            self.auth_raw = raw
            self.assertEqual(self.invoke(), discovery.empty("invalid_response"))
            self.assertEqual(len(self.calls), 1)

    def test_unknown_unavailable_and_known_zero_quota_preserve_verified_account(self):
        for quota, want in (({}, "unknown"), ({"gpuQuota": {}}, "unknown"),
                            ({"gpuQuota": {"isPayToScaleEnabled": True}}, "unknown"),
                            ({"gpuQuota": {"totalTimeAllowed": "10s", "timeUsed": "0s"}}, "unknown"),
                            ({"gpuQuota": {"totalTimeAllowed": "0s", "timeUsed": "0s", "timeReserved": "0s"}}, "known")):
            self.quota = quota
            result = self.invoke()
            self.assertEqual((result["status"], result["account"], result["quota_status"]),
                             ("verified", "fixture_user", want))
            self.assertEqual(len(self.calls), 2)
            if want == "unknown":
                self.assertEqual((result["limit_ns"], result["used_ns"], result["reserved_ns"]), ("", "", ""))
            else:
                self.assertEqual((result["limit_ns"], result["used_ns"], result["reserved_ns"]), ("0", "0", "0"))
        for status, raw in ((503, b'{"message":"SYNTHETIC_TOKEN"}'),
                            (200, b'{"gpuQuota":{},"gpuQuota":{}}'),
                            (200, b'{"gpuQuota":{"totalTimeAllowed":"-1s","timeUsed":"0s","timeReserved":"0s"}}')):
            self.quota_status, self.quota_raw = status, raw
            result = self.invoke()
            self.assertEqual((result["status"], result["account"], result["quota_status"]),
                             ("verified", "fixture_user", "unavailable"))
            self.assertEqual((result["limit_ns"], result["used_ns"], result["reserved_ns"]), ("", "", ""))


if __name__ == "__main__":
    unittest.main()
