"""Public client intent/recovery tests with a fake HTTP boundary, no providers."""

import argparse
import base64
import hashlib
import io
import json
from pathlib import Path
import sys
import tempfile
import time
import unittest
from unittest import mock
from urllib.parse import parse_qs, urlsplit

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import controller as c
import fetch_runtime as fetch
from workload import Failure


class Input:
    def __init__(self, value):
        self.buffer = io.BytesIO(value)

    def isatty(self):
        return False


class FakeAPI:
    base, workspace = "/v1/workspaces/example", "example"

    def __init__(self):
        self.calls, self.effects = [], {}
        self.ready = {"attempt_id": "att_original", "state": "ready", "revision": "a" * 64, "secrets": {"status": "missing"}}
        self.job = {"active_attempt_id": "att_original", "state": {"execution": "failed", "remote_activity": "inactive", "result": "available"}}
        self.lose_start = False
        self.logs_unavailable = False
        self.artifacts = []

    def request(self, method, route, body=None, key=None, **_kwargs):
        self.calls.append((method, route, body, key))
        if method == "GET":
            if route.endswith("/readiness"):
                return self.ready
            if "/logs?" in route:
                if self.logs_unavailable:
                    raise Failure("api_status_503")
                return {"attempt_id": "att_original", "availability": "live", "lines": [], "next_cursor": "old-cursor"}
            if "/artifacts?" in route:
                return {"next_cursor": "", "artifacts": self.artifacts}
            if "/operations/" in route:
                return {"status": "succeeded", "effect": "remote_deleted"}
            return self.job
        if route.endswith("/workload-secrets"):
            self.ready["secrets"]["status"] = "available"
            self.effects.setdefault(key, {"authorization_id": "secret_original", "status": "granted"})
            return self.effects[key]
        if route.endswith("/start"):
            self.effects.setdefault(key, {"authorization_id": "start_original", "status": "granted"})
            if self.lose_start:
                self.lose_start = False
                raise Failure("request_uncertain_use_observe")
            return self.effects[key]
        if route.endswith("/retry"):
            return {"new_attempt_id": "att_continuation", "status": "succeeded"}
        if route.endswith("/cleanup"):
            return {"links": {"self": self.base + "/operations/op_cleanup"}}
        raise AssertionError("unexpected request")


class PagedLogAPI(FakeAPI):
    def __init__(self, count, availability="after_completion"):
        super().__init__()
        self.records = [{"schema_version": 1, "event": "progress", "elapsed_ms": index * 10000,
                         "frames": index * 300, "output_time_us": index * 10000000} for index in range(count)]
        self.availability = availability
        self.log_calls = 0

    def request(self, method, route, *args, **kwargs):
        if method == "GET" and "/logs?" in route:
            self.log_calls += 1
            query = parse_qs(urlsplit(route).query)
            offset = int(query.get("cursor", ["0"])[0])
            end = min(offset + int(query["limit"][0]), len(self.records))
            cursor = str(end) if end < len(self.records) or self.availability == "live" else ""
            return {"attempt_id": "att_original", "availability": self.availability,
                    "lines": [json.dumps(record) for record in self.records[offset:end]], "next_cursor": cursor}
        return super().request(method, route, *args, **kwargs)


class ControllerTests(unittest.TestCase):
    def fixture(self, directory):
        session = c.Session(Path(directory))
        session.state.update(job_id="job_original", attempt_id="att_original", plan={
            "end_at": c.timestamp(time.time() + 600), "wall_seconds": 600,
            "staged_bytes": 100, "runtime_expanded_bytes": 100})
        session.save()
        return session

    def test_lost_start_ack_replays_original_intent_without_secret_resupply(self):
        with tempfile.TemporaryDirectory() as directory:
            session, api = self.fixture(directory), FakeAPI()
            api.lose_start = True
            canary = b"rtmps://receiver.invalid/live?pass=synthetic-write-only"
            args = argparse.Namespace(endpoint_stdin=True)
            with mock.patch.object(c.sys, "stdin", Input(canary)):
                with self.assertRaisesRegex(Failure, "request_uncertain_use_observe"):
                    c.start(args, api, session)
            saved = session.path.read_bytes()
            self.assertNotIn(canary, saved)
            self.assertNotIn(base64.b64encode(canary), saved)
            restarted = c.Session(Path(directory))
            with mock.patch.object(c.sys, "stdin", Input(b"must-not-be-read")) as stdin:
                receipt = c.start(args, api, restarted)
                self.assertEqual(stdin.buffer.tell(), 0)
            self.assertEqual(receipt["authorization_id"], "start_original")
            grants = [call for call in api.calls if call[1].endswith("/start")]
            self.assertEqual(len(grants), 2)
            self.assertEqual(grants[0][2:], grants[1][2:])
            self.assertEqual(len([call for call in api.calls if call[1].endswith("/workload-secrets")]), 1)
            self.assertEqual(len(api.effects), 2)

    def test_memory_lost_resupply_preserves_expiry_revision_and_key(self):
        with tempfile.TemporaryDirectory() as directory:
            session, api = self.fixture(directory), FakeAPI()
            expires = c.timestamp(time.time() + 300)
            metadata = {"attempt_id": "att_original", "readiness_revision": "a" * 64, "expires_at": expires}
            canary = b"rtmps://receiver.invalid/live?pass=synthetic-resupply"
            body = {**metadata, "values": {"ingest": base64.b64encode(canary).decode()}}
            base, _ = session.target(api)
            session.mutate(api, "supply:att_original", base + "/workload-secrets", body)
            api.ready["secrets"]["status"] = "material_lost"
            with mock.patch.object(c.sys, "stdin", Input(canary)):
                c.start(argparse.Namespace(endpoint_stdin=True), api, session)
            supplies = [call for call in api.calls if call[1].endswith("/workload-secrets")]
            self.assertEqual(len(supplies), 2)
            self.assertEqual(supplies[0][2:], supplies[1][2:])
            grant = next(call[2] for call in api.calls if call[1].endswith("/start"))
            self.assertEqual(grant["execution_deadline"], expires)

    def test_revoked_or_expired_start_does_not_read_secret_or_post(self):
        for kind in ("revoked", "expired"):
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as directory:
                session, api = self.fixture(directory), FakeAPI()
                if kind == "revoked":
                    api.ready["secrets"]["status"] = "revoked"
                else:
                    session.state["plan"]["end_at"] = c.timestamp(time.time() - 1)
                with mock.patch.object(c.sys, "stdin", Input(b"synthetic")) as stdin:
                    with self.assertRaises(Failure):
                        c.start(argparse.Namespace(endpoint_stdin=True), api, session)
                    self.assertEqual(stdin.buffer.tell(), 0)
                self.assertFalse(any(call[0] == "POST" for call in api.calls))

    def test_unavailable_observation_keeps_cursor_and_never_mutates(self):
        with tempfile.TemporaryDirectory() as directory:
            session, api = self.fixture(directory), FakeAPI()
            session.state["log_cursors"] = {"att_original": "original-cursor"}
            api.logs_unavailable = True
            result = c.observe(argparse.Namespace(pages=1), api, session)
            self.assertEqual(result["log_availability"], "api_status_503")
            self.assertEqual(session.state["log_cursors"]["att_original"], "original-cursor")
            self.assertEqual(result["receiver_health"], "not_observed")
            self.assertTrue(all(call[0] == "GET" for call in api.calls))

    def test_completed_log_pagination_stops_and_survives_session_restart(self):
        with tempfile.TemporaryDirectory() as directory:
            session, api = self.fixture(directory), PagedLogAPI(205)
            result = c.observe(argparse.Namespace(pages=100), api, session)
            self.assertEqual(len(result["records"]), 205)
            self.assertEqual(result["records"], api.records)
            self.assertEqual(api.log_calls, 3)
            restarted = c.Session(Path(directory))
            result = c.observe(argparse.Namespace(pages=100), api, restarted)
            self.assertEqual(result["records"], [])
            self.assertEqual(result["log_availability"], "after_completion")
            self.assertEqual(api.log_calls, 3)

    def test_page_budget_resumes_before_completed_log_end(self):
        with tempfile.TemporaryDirectory() as directory:
            session, api = self.fixture(directory), PagedLogAPI(205)
            first = c.observe(argparse.Namespace(pages=1), api, session)
            self.assertEqual(first["records"], api.records[:100])
            restarted = c.Session(Path(directory))
            rest = c.observe(argparse.Namespace(pages=100), api, restarted)
            self.assertEqual(first["records"] + rest["records"], api.records)
            self.assertEqual(api.log_calls, 3)

    def test_live_log_tail_waits_for_new_records_and_terminal_page(self):
        with tempfile.TemporaryDirectory() as directory:
            session, api = self.fixture(directory), PagedLogAPI(2, "live")
            first = c.observe(argparse.Namespace(pages=100), api, session)
            self.assertEqual(first["records"], api.records)
            restarted = c.Session(Path(directory))
            empty = c.observe(argparse.Namespace(pages=100), api, restarted)
            self.assertEqual(empty["records"], [])
            api.records.append({"schema_version": 1, "event": "stopped", "reason": "deadline"})
            api.availability = "after_completion"
            final = c.observe(argparse.Namespace(pages=100), api, restarted)
            self.assertEqual(final["records"], api.records[-1:])
            calls = api.log_calls
            self.assertEqual(c.observe(argparse.Namespace(pages=100), api, c.Session(Path(directory)))["records"], [])
            self.assertEqual(api.log_calls, calls)

    def test_unknown_activity_and_expired_continuation_never_create_attempt(self):
        for kind in ("active", "unknown", "expired"):
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as directory:
                session, api = self.fixture(directory), FakeAPI()
                api.job["state"]["remote_activity"] = kind
                if kind == "expired":
                    session.state["plan"]["end_at"] = c.timestamp(time.time() - 1)
                args = argparse.Namespace(remote_free_bytes=1 << 30, space_observed_at=c.timestamp(time.time()))
                with self.assertRaises(Failure):
                    c.continuation(args, api, session)
                self.assertFalse(any(call[0] == "POST" for call in api.calls))

    def test_continuation_keeps_original_deadline_and_needs_explicit_start(self):
        with tempfile.TemporaryDirectory() as directory:
            session, api = self.fixture(directory), FakeAPI()
            original_end = session.state["plan"]["end_at"]
            session.state["cleanup"] = {"att_original": {"status": "succeeded"}}
            args = argparse.Namespace(remote_free_bytes=1 << 30, space_observed_at=c.timestamp(time.time()))
            with mock.patch.object(c, "prepare_current", return_value={"state": "ready"}):
                c.continuation(args, api, session)
            self.assertEqual(session.state["attempt_id"], "att_continuation")
            self.assertEqual(session.state["plan"]["end_at"], original_end)
            self.assertFalse(any(call[1].endswith("/start") for call in api.calls))

    def test_empty_artifact_requires_verified_delivery_before_cleanup(self):
        with tempfile.TemporaryDirectory() as directory:
            session, api = self.fixture(directory), FakeAPI()
            item = {"artifact_id": "art_empty", "bytes": 0, "sha256": hashlib.sha256(b"").hexdigest(),
                    "path": "control/stderr.log", "role": "log"}
            api.artifacts = [item]
            args = argparse.Namespace(relay_binary="compute-relay", url="http://127.0.0.1:1", workspace="example", token_file=Path("token"))
            def download(argv):
                Path(argv[-1]).write_bytes(b"")
                return {"delivery": "verified-new-file", "workspace_id": "example", "job_id": "job_original",
                        "attempt_id": "att_original", "artifact": item}
            with mock.patch.object(c, "local_command", side_effect=download):
                result = c.finish(args, api, session)
            self.assertEqual(result["effect"], "remote_deleted")
            self.assertIn("art_empty", session.state["downloads"]["att_original"])
            self.assertEqual(len([call for call in api.calls if call[1].endswith("/cleanup")]), 1)

    def test_lost_download_ack_keeps_original_file_and_blocks_cleanup(self):
        with tempfile.TemporaryDirectory() as directory:
            session, api = self.fixture(directory), FakeAPI()
            api.artifacts = [{"artifact_id": "art_uncertain", "bytes": 1, "sha256": hashlib.sha256(b"x").hexdigest()}]
            target = session.directory / "att_original"
            target.mkdir()
            (target / "art_uncertain").write_bytes(b"x")
            with self.assertRaisesRegex(Failure, "download_uncertain_preserve_file"):
                c.finish(argparse.Namespace(), api, session)
            self.assertEqual((target / "art_uncertain").read_bytes(), b"x")
            self.assertFalse(any(call[0] == "POST" for call in api.calls))

    def test_space_and_transport_limits_fail_closed(self):
        args = argparse.Namespace(remote_free_bytes=1024, space_observed_at=c.timestamp(time.time()))
        with self.assertRaisesRegex(Failure, "insufficient_remote_space"):
            c.space_preflight(args, 100, 100)
        args.remote_free_bytes = 1 << 30
        args.space_observed_at = c.timestamp(time.time() - 601)
        with self.assertRaisesRegex(Failure, "remote_space_observation_stale"):
            c.space_preflight(args, 100, 100)
        with self.assertRaisesRegex(Failure, "relay_transport_requires_tls"):
            c.API("http://remote.invalid:7331", "example", "synthetic-token")

    def test_bad_runtime_download_preserves_partial_and_never_publishes(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "runtime.tar.xz"
            lock = {"archive": {"url": "https://runtime.invalid/pinned.tar.xz", "bytes": 4, "sha256": "0" * 64}}
            response = io.BytesIO(b"data")
            response.status = 200
            opener = mock.Mock()
            opener.open.return_value = response
            with mock.patch.object(fetch.urllib.request, "build_opener", return_value=opener):
                with self.assertRaisesRegex(Failure, "runtime_digest_mismatch"):
                    fetch.fetch(output, lock)
            self.assertFalse(output.exists())
            self.assertEqual(output.with_name(output.name + ".partial").read_bytes(), b"data")


if __name__ == "__main__":
    unittest.main()
