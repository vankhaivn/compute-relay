"""Synthetic clocks and finite local Linux processes; no provider or network use."""

from datetime import datetime, timezone
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
import unittest
from unittest.mock import patch

from test_runner import Fixture, SCRIPT
from relay_runner import contract, deadline


def utc_after(seconds):
    return datetime.fromtimestamp(time.time() + seconds, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def active(pid):
    try:
        return Path("/proc", str(pid), "stat").read_text().rsplit(")", 1)[1].split()[0] != "Z"
    except FileNotFoundError:
        return False


class BoundTests(unittest.TestCase):
    def test_wall_jumps_and_suspend_cannot_extend_frozen_elapsed_bound(self):
        with patch.object(deadline, "elapsed", return_value=100) as boot, patch.object(deadline.time, "time", return_value=1000) as wall:
            bound = deadline.Bound(100, 1050)
            self.assertEqual(bound.reason, "absolute_deadline")
            self.assertEqual(bound.elapsed_end, 150)
            boot.return_value, wall.return_value = 149, 900
            self.assertFalse(bound.expired())
            # A backward wall jump cannot grant more elapsed time.
            boot.return_value = 150
            self.assertTrue(bound.expired())
            # A forward clock step terminates even before the elapsed ceiling.
            boot.return_value, wall.return_value = 110, 1051
            self.assertTrue(bound.expired())
            # A suspend-like observation gap is caught on the first resumed poll.
            boot.return_value, wall.return_value = 500, 900
            self.assertTrue(bound.expired())

    def test_earlier_relative_budget_wins(self):
        with patch.object(deadline, "elapsed", return_value=100) as boot, patch.object(deadline.time, "time", return_value=1000):
            bound = deadline.Bound(10, 2000)
            self.assertEqual(bound.reason, "relative_budget")
            boot.return_value = 110
            self.assertTrue(bound.expired())


class DeadlineContractTests(Fixture):
    def test_canonical_utc_only_and_old_manifests_unchanged(self):
        self.assertNotIn("execution_deadline", self.m["timeouts"])
        for value in (None, 123, "", "2026-10-12T12:00:00+00:00", "2026-10-12T12:00:00.1Z", "2026-02-30T12:00:00Z", "0001-02-01T00:00:00Z"):
            self.m["timeouts"]["execution_deadline"] = value
            with self.assertRaises(contract.Failure):
                contract.load(json.dumps(self.m).encode())
        self.m["timeouts"]["execution_deadline"] = "2026-10-12T12:00:00Z"
        contract.load(json.dumps(self.m).encode())


@unittest.skipUnless(sys.platform == "linux", "runner execution requires Linux")
class DeadlineExecutionTests(Fixture):
    def test_expired_queued_delivery_never_executes(self):
        self.m["timeouts"]["execution_deadline"] = utc_after(-1)
        result = self.invoke()
        self.assertEqual(result["phase"], "timed_out")
        self.assertEqual(result["error"]["code"], "REMOTE_TIMEOUT")
        self.assertFalse((self.root / "outputs/answer.json").exists())
        provenance = json.loads((self.root / "control/environment.json").read_text())
        self.assertEqual(provenance["processes"], [])
        self.assertEqual(provenance["termination_reason"], "absolute_deadline")

    def test_output_and_term_ignoring_children_do_not_extend_deadline(self):
        self.m = self.make(b'''import json, os, pathlib, signal, subprocess, sys, time
signal.signal(signal.SIGTERM, signal.SIG_IGN)
child = subprocess.Popen([sys.executable, "-c", "import signal,time; signal.signal(signal.SIGTERM, signal.SIG_IGN); time.sleep(30)"])
pathlib.Path(os.environ["CC_OUTPUT_DIR"], "answer.json").write_text(json.dumps({"child": child.pid}))
while True:
    print("bounded-progress", flush=True)
    time.sleep(0.1)
''')
        self.m["timeouts"].update(remote_wall_seconds=20, setup_seconds=6, finalization_grace_seconds=3, execution_deadline=utc_after(4))
        before = time.monotonic()
        result = self.invoke()
        self.assertLess(time.monotonic() - before, 5.5)
        self.assertEqual(result["phase"], "timed_out")
        self.assertTrue(result["timed_out"])
        child = json.loads((self.root / "outputs/answer.json").read_text())["child"]
        self.assertFalse(active(child), "child continued after the absolute bound")
        self.assertEqual(len(result["artifacts"]), 1, "separately bounded finalization was skipped")
        provenance = json.loads((self.root / "control/environment.json").read_text())
        self.assertEqual(provenance["termination_reason"], "absolute_deadline")

    def test_setup_expiry_never_launches_payload(self):
        self.m["execution"] = {"kind": "shell", "command": ["sh", "-c", "touch ../outputs/answer.json"],
                               "dependencies": {"shell_setup": [["sh", "-c", "sleep 20"]]}}
        self.m["timeouts"].update(remote_wall_seconds=20, setup_seconds=10, finalization_grace_seconds=3, execution_deadline=utc_after(3))
        result = self.invoke()
        self.assertEqual(result["phase"], "timed_out")
        self.assertFalse((self.root / "outputs/answer.json").exists())
        provenance = json.loads((self.root / "control/environment.json").read_text())
        self.assertNotIn("payload", [p["phase"] for p in provenance["processes"]])

    def test_independent_watchdog_survives_stopped_runner(self):
        self.m = self.make(b'''import json, os, pathlib, signal, time
signal.signal(signal.SIGTERM, signal.SIG_IGN)
pathlib.Path(os.environ["CC_OUTPUT_DIR"], "answer.json").write_text(json.dumps({"payload": os.getpid(), "runner": os.getppid()}))
time.sleep(30)
''')
        self.m["timeouts"].update(remote_wall_seconds=20, setup_seconds=6, finalization_grace_seconds=2, execution_deadline=utc_after(4))
        manifest = self.base / "manifest.json"
        manifest.write_text(json.dumps(self.m))
        proc = subprocess.Popen([sys.executable, str(SCRIPT), "--manifest", str(manifest), "--supervise",
                                 "--staging-root", str(self.staging), "--work-root", str(self.root), "--network-mode", "disabled"],
                                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)
        pids = {}
        try:
            limit = time.monotonic() + 3
            while time.monotonic() < limit:
                try:
                    pids = json.loads((self.root / "outputs/answer.json").read_text())
                    break
                except (FileNotFoundError, json.JSONDecodeError):
                    time.sleep(0.02)
            self.assertTrue(pids, "fixture payload never started")
            os.kill(pids["runner"], signal.SIGSTOP)
            absolute = deadline.epoch(self.m["timeouts"]["execution_deadline"])
            while time.time() < absolute + 1:
                time.sleep(0.02)
            self.assertFalse(active(pids["payload"]), "watchdog depended on the stopped runner")
            self.assertEqual(proc.wait(timeout=4), 124, "finalization watchdog did not terminate the stopped runner")
        finally:
            for pid in pids.values():
                try:
                    os.kill(pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
            if proc.poll() is None:
                proc.kill()
            proc.wait(timeout=2)
