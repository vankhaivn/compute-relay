"""Synthetic material only: finite delivery, log redaction and export exclusion."""

import base64
import copy
import hashlib
import io
import json
import os
from pathlib import Path
import sys
import threading
import time
import unittest
from unittest.mock import patch
from urllib.parse import quote_from_bytes, quote_plus

from test_runner import Fixture
from test_start import after
from relay_runner import contract, files, main, process, secrets

CANARY = b"synthetic-ingest/+?key=with symbols"


class Sink:
    def __init__(self):
        self.raw = bytearray()

    def feed(self, data):
        self.raw.extend(data)

    def close(self):
        pass


class SecretTests(Fixture):
    def prepare(self):
        self.m["execution_control"] = "prepare_then_start"
        self.m["workload_secrets"] = [{"name": "ingest", "exposure": "file"}]
        raw = json.dumps(self.m).encode()
        control = {"version": "compute-relay/start/v1", "job_id": self.m["job_id"],
                   "attempt_id": self.m["attempt_id"], "attempt_nonce": self.m["attempt_nonce"],
                   "manifest_sha256": hashlib.sha256(raw).hexdigest(), "authorization_id": "grant_fixture",
                   "readiness_revision": "a" * 64, "max_remote_wall_seconds": 10,
                   "execution_deadline": after(10), "start_before": after(8),
                   "secrets": {"authorization_id": "secret_fixture", "expires_at": after(20),
                               "slots": self.m["workload_secrets"]}}
        applied = copy.deepcopy(self.m)
        contract.apply_start(applied, raw, json.dumps(control).encode(), time.time())
        root = self.base / "secrets"
        root.mkdir(mode=0o700)
        return applied, control, root

    def deliver(self, control, root, update=None):
        packet = {key: value for key, value in secrets.marker(control).items() if key != "names"}
        packet["values"] = {"ingest": base64.b64encode(CANARY).decode()}
        if update:
            packet.update(update)
        read, write = os.pipe()
        def send():
            try:
                with os.fdopen(write, "wb") as target:
                    target.write(json.dumps(packet).encode())
            except BrokenPipeError:
                pass
        writer = threading.Thread(target=send)
        writer.start()
        try:
            with os.fdopen(read, "rb") as source:
                # Receiver logic tests isolate the kernel mount check. The SSH live
                # fixture separately qualifies the actual private Docker tmpfs.
                with patch.object(secrets, "require_tmpfs", return_value=root):
                    return secrets.receive(control, source, root)
        finally:
            writer.join(timeout=1)
            self.assertFalse(writer.is_alive())

    def test_declaration_and_start_policy_are_strict(self):
        _, control, _ = self.prepare()
        for slots in ([], [{"name": "../ingest", "exposure": "file"}],
                      [{"name": "ingest", "exposure": "env"}],
                      [{"name": "ingest", "exposure": "file"}, {"name": "ingest", "exposure": "file"}]):
            with self.subTest(slots=slots), self.assertRaises(contract.Failure):
                contract.validate({**self.m, "workload_secrets": slots})
        raw = json.dumps(self.m).encode()
        for change in ({"secrets": None}, {"secrets": {**control["secrets"], "slots": []}},
                       {"secrets": {**control["secrets"], "expires_at": after(1)}}):
            with self.subTest(change=change), self.assertRaises(contract.Failure):
                contract.apply_start(copy.deepcopy(self.m), raw, json.dumps({**control, **change}).encode(), time.time())

    def test_secret_expiry_bounds_start_not_a_multi_day_run(self):
        self.m["timeouts"]["remote_wall_seconds"] = 604800
        _, control, _ = self.prepare()
        raw = json.dumps(self.m).encode()
        control.update(manifest_sha256=hashlib.sha256(raw).hexdigest(), max_remote_wall_seconds=604800,
                       execution_deadline=after(604800))
        applied = copy.deepcopy(self.m)
        contract.apply_start(applied, raw, json.dumps(control).encode(), time.time())
        self.assertEqual(applied["timeouts"]["remote_wall_seconds"], 604800)
        with self.assertRaises(contract.Failure):
            contract.validate({**self.m, "timeouts": {**self.m["timeouts"], "remote_wall_seconds": 604801}})

    @unittest.skipUnless(sys.platform == "linux", "Linux receiver deadline")
    def test_receiver_is_bound_one_shot_and_files_are_private(self):
        _, control, root = self.prepare()
        self.assertEqual(self.deliver(control, root), 0)
        self.assertEqual((root / "ingest").read_bytes(), CANARY)
        self.assertEqual((root / "ingest").stat().st_mode & 0o777, 0o400)
        with self.assertRaises(OSError):
            self.deliver(control, root)
        self.assertEqual((root / "ingest").read_bytes(), CANARY)
        with patch.object(secrets, "require_tmpfs", return_value=root):
            values = secrets.wait(control, lambda: None, root)
        self.assertEqual(values["ingest"], CANARY)
        secrets.remove(control, root)
        self.assertFalse((root / "ingest").exists())
        self.assertTrue((root / ".claim").exists())
        for name in (".claim", ".ready"):
            self.assertNotIn(CANARY, (root / name).read_bytes())

    @unittest.skipUnless(sys.platform == "linux", "Linux receiver deadline")
    def test_invalid_packet_leaves_no_material_and_cannot_be_retried(self):
        for update in ({"attempt_id": "another_attempt"}, {"authorization_id": "secret_other"},
                       {"values": {"undeclared": base64.b64encode(CANARY).decode()}},
                       {"values": {"ingest": base64.b64encode(b"x" * 8193).decode()}},
                       {"extra": True}):
            with self.subTest(update=tuple(update)):
                _, control, root = self.prepare()
                with self.assertRaises(contract.Failure):
                    self.deliver(control, root, update)
                self.assertFalse((root / "ingest").exists())
                self.assertTrue((root / ".claim").exists())
                with self.assertRaises(OSError):
                    self.deliver(control, root)
                for child in root.iterdir():
                    child.unlink()
                root.rmdir()

    @unittest.skipUnless(sys.platform == "linux", "Linux receiver deadline")
    def test_stalled_delivery_has_its_own_finite_wait(self):
        _, control, root = self.prepare()
        read, write = os.pipe()
        before = time.monotonic()
        try:
            with os.fdopen(read, "rb") as source, patch.object(secrets, "require_tmpfs", return_value=root), patch.object(secrets, "WAIT_SECONDS", 0.1):
                with self.assertRaises(contract.Failure):
                    secrets.receive(control, source, root)
        finally:
            os.close(write)
        self.assertLess(time.monotonic() - before, 1)
        self.assertFalse((root / ".ready").exists())

    def test_known_values_are_redacted_across_chunks_and_url_forms(self):
        values = (CANARY, b"first\nsecond", b"binary-\xff-\xfe-material")
        guard = secrets.Redactor(values)
        samples = (CANARY, quote_from_bytes(CANARY, safe="").encode(),
                   quote_from_bytes(CANARY, safe="/").encode(), quote_plus(CANARY).encode(),
                   b"first\nsecond", b"binary-\xff-\xfe-material")
        for index, sample in enumerate(samples):
            for cut in range(len(sample) + 1):
                sink = Sink()
                path = self.base / f"log-{index}-{cut}"
                log = process.Log(path, 20000, sink)
                log.redactor = guard
                log.feed(b"safe progress\n" + b"x" * 2048 + sample[:cut])
                log.feed(sample[cut:] + b"\nfinished\n")
                log.close()
                self.assertEqual(path.read_bytes(), sink.raw)
                self.assertFalse(guard.contains(path.read_bytes()))
                self.assertIn(b"finished", path.read_bytes())

    def test_export_scans_chunk_boundaries_and_omits_unsafe_optional_output(self):
        guard = secrets.Redactor((CANARY,))
        root = self.base / "outputs"
        root.mkdir()
        limits = dict(contract.DEFAULT_LIMITS)
        for required in (False, True):
            with self.subTest(required=required):
                path = root / "out.txt"
                path.write_bytes(b"x" * (contract.BUFFER - 5) + CANARY + b"suffix")
                outputs = [{"path": "out.txt", "required": required}]
                if required:
                    with self.assertRaises(contract.Failure):
                        files.artifacts(root, outputs, limits, lambda: None, guard)
                else:
                    self.assertEqual(files.artifacts(root, outputs, limits, lambda: None, guard), [])
                self.assertFalse(path.exists())
        cleaned = guard.safe_json({"environment": {"diagnostic": CANARY.decode()}, "slots": ["ingest"]})
        self.assertNotIn(CANARY, json.dumps(cleaned).encode())

    @unittest.skipUnless(sys.platform == "linux", "Linux remote execution fixture")
    def test_payload_gets_files_only_logs_and_optional_exports_exclude_values(self):
        payload = b'''import os, pathlib, sys, urllib.parse
secret = pathlib.Path(os.environ["CC_SECRET_DIR"], "ingest").read_bytes()
assert secret not in repr(os.environ).encode()
os.write(1, secret[:7]); os.write(1, secret[7:] + b"\\n")
os.write(2, urllib.parse.quote_from_bytes(secret, safe="").encode() + b"\\n")
root = pathlib.Path(os.environ["CC_OUTPUT_DIR"])
(root / "answer.json").write_text('{"answer":42}')
(root / "unsafe.txt").write_bytes(secret)
'''
        self.m = self.make(payload)
        self.m["outputs"].append({"path": "unsafe.txt", "required": False})
        applied, control, root = self.prepare()
        self.deliver(control, root)
        with patch.object(secrets, "SECRET_ROOT", root), patch.object(secrets, "require_tmpfs", return_value=root):
            result = main.execute(applied, self.staging, self.root, "disabled", authorization=control)
        self.assertEqual(result["phase"], "completed", result)
        self.assertEqual([entry["path"] for entry in result["artifacts"]], ["answer.json"])
        self.assertFalse((root / "ingest").exists())
        guard = secrets.Redactor((CANARY,))
        for path in (self.root / "control").iterdir():
            if path.is_file():
                self.assertFalse(guard.contains(path.read_bytes()), path.name)
        self.assertFalse((self.root / "outputs/unsafe.txt").exists())
        provenance = json.loads((self.root / "control/environment.json").read_text())
        self.assertTrue(provenance["workload_secrets"]["files_removed"])

    @unittest.skipUnless(sys.platform == "linux", "Linux remote execution fixture")
    def test_missing_delivery_never_launches_setup_or_payload(self):
        applied, control, root = self.prepare()
        with patch.object(secrets, "SECRET_ROOT", root), patch.object(secrets, "require_tmpfs", return_value=root), patch.object(secrets, "WAIT_SECONDS", 0.1), patch.object(main, "run") as run:
            result = main.execute(applied, self.staging, self.root, "disabled", authorization=control)
        self.assertEqual(result["phase"], "failed")
        run.assert_not_called()
        self.assertFalse((self.root / "outputs/answer.json").exists())

    @unittest.skipUnless(sys.platform == "linux", "Linux mount inspection")
    def test_ordinary_directory_is_not_accepted_as_ephemeral_storage(self):
        with self.assertRaises(contract.Failure):
            secrets.require_tmpfs(self.base)
