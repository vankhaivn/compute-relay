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
                return {"workspace_id": self.workspace, "job_id": "job_original", "attempt_id": "att_original",
                        "next_cursor": "", "artifacts": self.artifacts}
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
            def download(_job, _attempt, _item, output):
                output.write_bytes(b"")
                return {"delivery": "verified-new-file", "workspace_id": "example", "job_id": "job_original",
                        "attempt_id": "att_original", "artifact": item}
            with mock.patch.object(api, "download", side_effect=download, create=True):
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

    def test_failed_artifact_download_never_requests_cleanup(self):
        with tempfile.TemporaryDirectory() as directory:
            session, api = self.fixture(directory), FakeAPI()
            api.artifacts = [{"artifact_id": "art_answer", "bytes": 6, "sha256": hashlib.sha256(b"answer").hexdigest()}]
            with mock.patch.object(api, "download", side_effect=Failure("artifact_delivery_not_verified"), create=True):
                with self.assertRaises(Failure):
                    c.finish(argparse.Namespace(), api, session)
            self.assertFalse(any(call[0] == "POST" for call in api.calls))
            self.assertEqual(session.state["downloads"]["att_original"], {})

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


class ArtifactDownloadTests(unittest.TestCase):
    def transfer(self, directory, body=b"answer", trailer=b"X-Compute-Relay-Verified: true\r\n", *,
                 headers=None, ending=None, item=None, publish=None):
        api = c.API("http://127.0.0.1:7331", "example", "synthetic-token")
        item = item or {"artifact_id": "art_answer", "path": "outputs/answer.txt", "role": "output",
                        "bytes": len(body), "sha256": hashlib.sha256(body).hexdigest()}
        values = {"Content-Type": "application/octet-stream", "X-Artifact-ID": item["artifact_id"],
                  "X-Workspace-ID": "example", "X-Job-ID": "job_original", "X-Attempt-ID": "att_original",
                  "X-Content-Bytes": str(item["bytes"]), "X-Content-SHA256": item["sha256"],
                  "Transfer-Encoding": "chunked", "Trailer": "X-Compute-Relay-Verified"}
        values.update(headers or {})
        raw = b"HTTP/1.1 200 OK\r\n" + b"".join(key.encode() + b": " + value.encode() + b"\r\n"
                                                    for key, value in values.items()) + b"\r\n"
        if ending is None:
            raw += (format(len(body), "x").encode() + b"\r\n" + body + b"\r\n") if body else b""
            raw += b"0\r\n" + trailer + b"\r\n"
        else:
            raw += ending

        class Socket:
            def makefile(self, *_args):
                return io.BytesIO(raw)

            def shutdown(self, *_args):
                pass

        class Connection:
            sock = Socket()

            def connect(self):
                pass

            def request(self, *args, **kwargs):
                self.sent = args, kwargs

            def getresponse(self):
                self.response = self.response_class(self.sock)
                self.response.begin()
                return self.response

            def close(self):
                pass

        connection = Connection()
        output = Path(directory) / item["artifact_id"]
        with mock.patch.object(c.http.client, "HTTPConnection", return_value=connection):
            if publish:
                with mock.patch.object(c.os, "link", side_effect=publish):
                    receipt = api.download("job_original", "att_original", item, output)
            else:
                receipt = api.download("job_original", "att_original", item, output)
        self.assertEqual(connection.sent[0], ("GET", "/v1/workspaces/example/jobs/job_original/artifacts/art_answer/content?attempt_id=att_original"))
        self.assertEqual(connection.sent[1]["headers"]["Accept-Encoding"], "identity")
        return output, receipt

    def test_verified_download_publishes_exact_new_file_including_empty(self):
        for body in (b"answer", b""):
            with self.subTest(body=body), tempfile.TemporaryDirectory() as directory:
                output, receipt = self.transfer(directory, body)
                self.assertEqual(output.read_bytes(), body)
                self.assertEqual(receipt["delivery"], "verified-new-file")
                self.assertEqual(receipt["attempt_id"], "att_original")
                self.assertFalse(output.with_name(output.name + ".partial").exists())
                self.assertEqual(output.stat().st_mode & 0o777, 0o600)

    def test_missing_bad_duplicate_or_truncated_trailer_never_publishes(self):
        for trailer in (b"", b"X-Compute-Relay-Verified: false\r\n",
                        b"X-Compute-Relay-Verified: true\r\nX-Compute-Relay-Verified: true\r\n",
                        b"X-Compute-Relay-Verified: true", b" " * 32769):
            with self.subTest(trailer=trailer[:80]), tempfile.TemporaryDirectory() as directory:
                with self.assertRaises(Failure):
                    self.transfer(directory, trailer=trailer)
                self.assertFalse((Path(directory) / "art_answer").exists())
                self.assertTrue((Path(directory) / "art_answer.partial").exists())

    def test_truncated_body_missing_terminal_chunk_and_hash_mismatch_never_publish(self):
        for ending in (b"6\r\nans", b"6\r\nanswer\r\n", b"7\r\nanswer!\r\n0\r\nX-Compute-Relay-Verified: true\r\n\r\n", b"6\r\nwrong!\r\n0\r\nX-Compute-Relay-Verified: true\r\n\r\n"):
            with self.subTest(ending=ending), tempfile.TemporaryDirectory() as directory:
                with self.assertRaises(Failure):
                    self.transfer(directory, ending=ending)
                self.assertFalse((Path(directory) / "art_answer").exists())

    def test_mismatched_identity_encoding_or_acknowledgement_header_never_publishes(self):
        for headers in ({"X-Attempt-ID": "att_other"}, {"Content-Type": "text/plain"},
                        {"Content-Encoding": "gzip"}, {"Content-Length": "6"},
                        {"Trailer": "Other"}, {"X-Compute-Relay-Verified": "true"},
                        {"X-Content-Bytes": "7"}, {"X-Content-SHA256": "0" * 64}):
            with self.subTest(headers=headers), tempfile.TemporaryDirectory() as directory:
                with self.assertRaises(Failure):
                    self.transfer(directory, headers=headers)
                self.assertFalse((Path(directory) / "art_answer").exists())

    def test_destination_collision_at_publication_preserves_existing_file(self):
        link = c.os.link
        def collide(source, output):
            output.write_bytes(b"original")
            link(source, output)
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaises(Failure):
                self.transfer(directory, publish=collide)
            self.assertEqual((Path(directory) / "art_answer").read_bytes(), b"original")
            self.assertEqual((Path(directory) / "art_answer.partial").read_bytes(), b"answer")

    def test_existing_temporary_file_is_never_overwritten(self):
        with tempfile.TemporaryDirectory() as directory:
            partial = Path(directory) / "art_answer.partial"
            partial.write_bytes(b"original evidence")
            with self.assertRaises(Failure):
                self.transfer(directory)
            self.assertEqual(partial.read_bytes(), b"original evidence")
            self.assertFalse((Path(directory) / "art_answer").exists())


if __name__ == "__main__":
    unittest.main()
