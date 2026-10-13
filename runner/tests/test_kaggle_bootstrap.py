"""The Kaggle bootstrap executes only the repository's finite CPU fixture."""

import hashlib
import importlib.util
import json
from pathlib import Path
import sys
import unittest
from unittest import mock

from test_runner import Fixture


REPO = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location(
    "kaggle_bootstrap_fixture", REPO / "internal/provider/kaggle/execution_bootstrap.py")
bootstrap = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(bootstrap)
PACKAGE = "_compute_relay_remote_runner"


@unittest.skipUnless(sys.platform == "linux", "Remote execution contract targets Linux only")
class KaggleBootstrapExecutionTests(Fixture):
    def test_current_runner_completes_through_generated_package(self):
        self.assertNotIn(PACKAGE, sys.modules)

        def clear_modules():
            for name in list(sys.modules):
                if name == PACKAGE or name.startswith(PACKAGE + "."):
                    sys.modules.pop(name)

        self.addCleanup(clear_modules)
        marker = self.staging / "relay-stage.bin"
        marker.write_bytes(b"synthetic Kaggle bootstrap marker")
        payload = {
            "marker_sha256": hashlib.sha256(marker.read_bytes()).hexdigest(),
            "manifest": self.m,
            "modules": {path.name: path.read_text()
                        for path in (REPO / "runner/python/relay_runner").glob("*.py")},
        }

        def result_path(value):
            self.assertEqual(value, "/kaggle/working/relay-result")
            return self.root

        with mock.patch.object(bootstrap, "staging_paths", return_value=(self.staging, marker)), \
             mock.patch.object(bootstrap, "Path", side_effect=result_path):
            bootstrap.run_remote(payload)

        result = json.loads((self.root / "control/execution-result.json").read_text())
        self.assertEqual(result["phase"], "completed", result)
        self.assertEqual(result["exit_code"], 0)
        self.assertEqual(result["attempt_id"], self.m["attempt_id"])
        self.assertEqual(result["input_manifest_sha256"], self.m["input_manifest_sha256"])
        answer = (self.root / "outputs/answer.json").read_bytes()
        self.assertEqual(json.loads(answer), {"answer": 42})
        self.assertEqual(result["artifacts"][0]["sha256"], hashlib.sha256(answer).hexdigest())
        self.assertIn("fixture finished", (self.root / "control/stdout.log").read_text())
