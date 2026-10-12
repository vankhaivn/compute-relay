"""One-shot start authority and immutable prepared manifests; synthetic CPU only."""

import copy
from datetime import datetime, timezone
import hashlib
import json
import subprocess
import sys
import time
import unittest

from test_runner import Fixture, SCRIPT
from relay_runner import contract, main


def after(seconds):
    return datetime.fromtimestamp(time.time() + seconds, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class StartTests(Fixture):
    def prepare(self):
        self.m["execution_control"] = "prepare_then_start"
        raw = json.dumps(self.m).encode()
        control = {"version": "compute-relay/start/v1", "job_id": self.m["job_id"],
                   "attempt_id": self.m["attempt_id"], "attempt_nonce": self.m["attempt_nonce"],
                   "manifest_sha256": hashlib.sha256(raw).hexdigest(), "authorization_id": "grant_fixture",
                   "readiness_revision": "a" * 64, "max_remote_wall_seconds": 10,
                   "execution_deadline": after(10), "start_before": after(8)}
        return raw, control

    def test_missing_or_changed_authority_never_creates_work_root(self):
        raw, control = self.prepare()
        with self.assertRaises(contract.Failure):
            main.execute(self.m, self.staging, self.root, "disabled")
        self.assertFalse(self.root.exists())
        for update in ({"attempt_id": "another_attempt"}, {"attempt_nonce": "b" * 64},
                       {"manifest_sha256": "b" * 64}, {"max_remote_wall_seconds": 11},
                       {"execution_deadline": "2099-01-01T00:00:00+00:00"},
                       {"authorize_compute": True}, {"start_before": after(-1)},
                       {"readiness_revision": "malformed"}, {"max_remote_wall_seconds": 7}):
            with self.subTest(update=update), self.assertRaises(contract.Failure):
                contract.apply_start(copy.deepcopy(self.m), raw, json.dumps({**control, **update}).encode(), time.time())
        self.assertFalse(self.root.exists())

    def test_original_absolute_bound_cannot_be_extended(self):
        self.m["timeouts"]["execution_deadline"] = after(5)
        raw, control = self.prepare()
        with self.assertRaises(contract.Failure):
            contract.apply_start(copy.deepcopy(self.m), raw, json.dumps(control).encode(), time.time())
        control["execution_deadline"] = after(4)
        control["start_before"] = after(3)
        m = copy.deepcopy(self.m)
        receipt = contract.apply_start(m, raw, json.dumps(control).encode(), time.time())
        self.assertEqual(receipt["authorization_id"], control["authorization_id"])
        self.assertEqual(m["timeouts"]["execution_deadline"], control["execution_deadline"])
        self.assertEqual(json.dumps(self.m).encode(), raw)

    def test_expired_control_cli_stops_before_execution(self):
        raw, control = self.prepare()
        control["start_before"] = after(-1)
        manifest, authority = self.base / "manifest.json", self.base / "start.json"
        manifest.write_bytes(raw)
        authority.write_text(json.dumps(control))
        result = subprocess.run([sys.executable, str(SCRIPT), "--manifest", str(manifest), "--supervise",
                                 "--staging-root", str(self.staging), "--work-root", str(self.root),
                                 "--network-mode", "disabled", "--start-control", str(authority)],
                                capture_output=True, timeout=3)
        self.assertEqual(result.returncode, 124, result.stderr)
        self.assertFalse(self.root.exists())
        self.assertEqual(manifest.read_bytes(), raw)

    @unittest.skipUnless(sys.platform == "linux", "Linux remote execution fixture")
    def test_controlled_cli_runs_once_with_bound_provenance(self):
        raw, control = self.prepare()
        manifest, authority = self.base / "manifest.json", self.base / "start.json"
        manifest.write_bytes(raw)
        authority.write_text(json.dumps(control))
        command = [sys.executable, str(SCRIPT), "--manifest", str(manifest), "--supervise",
                   "--staging-root", str(self.staging), "--work-root", str(self.root),
                   "--network-mode", "disabled", "--start-control", str(authority)]
        result = subprocess.run(command, capture_output=True, timeout=12)
        self.assertEqual(result.returncode, 0, result.stderr)
        provenance = json.loads((self.root / "control/environment.json").read_text())
        self.assertEqual(provenance["start_authorization"], control)
        self.assertEqual(provenance["execution_deadline"], control["execution_deadline"])
        self.assertEqual(manifest.read_bytes(), raw)
        repeated = subprocess.run(command, capture_output=True, timeout=3)
        self.assertNotEqual(repeated.returncode, 0)
        self.assertEqual(manifest.read_bytes(), raw)
