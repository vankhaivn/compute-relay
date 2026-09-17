"""Bootstrap wiring with inert fixture modules, never an admitted workload."""
import hashlib
import importlib.util
from pathlib import Path
import signal
import sys
import tempfile
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[3]
SPEC = importlib.util.spec_from_file_location(
    "execution_bootstrap_fixture", ROOT / "internal/provider/kaggle/execution_bootstrap.py")
bootstrap = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(bootstrap)
PACKAGE = "_compute_relay_remote_runner"


class ExecutionBootstrapTests(unittest.TestCase):
    def setUp(self):
        self.assertNotIn(PACKAGE, sys.modules)
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.addCleanup(self.clear_modules)
        self.root = Path(self.directory.name)
        self.staging = self.root / "input" / "fixture-dataset"
        self.staging.mkdir(parents=True)
        self.marker = self.staging / "relay-stage.bin"
        self.marker.write_bytes(b"fixture marker")
        self.payload = {
            "dataset_slug": self.staging.name,
            "marker_sha256": hashlib.sha256(self.marker.read_bytes()).hexdigest(),
            "manifest": {"network": {"remote_internet": "disabled"}, "phase": "completed"},
            # Tiny repository-owned modules isolate bootstrap import/signal wiring.
            # The actual runner module hashes and generated contract are checked by Go.
            "modules": {
                "__init__.py": "VERSION = 'fixture'\n",
                "contract.py": "from . import VERSION\n",
                "files.py": "from .contract import VERSION\n",
                "process.py": "from .files import VERSION\n",
                "main.py": (
                    "from .process import VERSION\n"
                    "import signal\n"
                    "calls = []\n"
                    "def execute(manifest, staging, root, network, stopped):\n"
                    "    calls.append((staging, root, network, stopped()))\n"
                    "    if manifest.get('cancel'):\n"
                    "        signal.getsignal(signal.SIGTERM)(None, None)\n"
                    "        assert stopped()\n"
                    "    if manifest.get('raise'):\n"
                    "        raise RuntimeError('fixture runner failure')\n"
                    "    return {'phase': manifest['phase']}\n"
                ),
            },
        }

    def clear_modules(self):
        for name in list(sys.modules):
            if name == PACKAGE or name.startswith(PACKAGE + "."):
                sys.modules.pop(name)

    def run_bootstrap(self):
        def rooted(value):
            if value == "/kaggle/input":
                return self.root / "input"
            if value == "/kaggle/working/relay-result":
                return self.root / "working" / "relay-result"
            self.fail("bootstrap accessed an unexpected host path")
        with mock.patch.object(bootstrap, "Path", side_effect=rooted), \
             mock.patch.object(bootstrap.sys, "platform", "linux"):
            bootstrap.run_remote(self.payload)

    def test_marker_gates_module_loading_and_runner_entry(self):
        for fault in ("missing", "changed", "oversized"):
            with self.subTest(fault=fault):
                if fault == "missing":
                    self.marker.unlink()
                elif fault == "changed":
                    self.marker.write_bytes(b"different marker")
                else:
                    raw = b"x" * 65537
                    self.marker.write_bytes(raw)
                    self.payload["marker_sha256"] = hashlib.sha256(raw).hexdigest()
                with self.assertRaises(RuntimeError):
                    self.run_bootstrap()
                self.assertNotIn(PACKAGE, sys.modules)
        with mock.patch.object(bootstrap.sys, "platform", "non-linux"):
            with self.assertRaises(RuntimeError):
                bootstrap.run_remote(self.payload)

    def test_provider_mount_symlink_is_resolved_but_marker_cannot_escape(self):
        raw = self.marker.read_bytes()
        self.marker.unlink()
        self.staging.rmdir()
        mounted = self.root / "provider-mount" / self.staging.name
        mounted.mkdir(parents=True)
        mounted_marker = mounted / "relay-stage.bin"
        mounted_marker.write_bytes(raw)
        try:
            self.staging.symlink_to(mounted, target_is_directory=True)
        except OSError as exc:
            self.skipTest("directory symlink unavailable: " + str(exc))
        self.run_bootstrap()
        main = sys.modules[PACKAGE + ".main"]
        self.assertEqual(main.calls[0][0], mounted.resolve())

        self.clear_modules()
        mounted_marker.unlink()
        outside = self.root / "outside-marker.bin"
        outside.write_bytes(raw)
        mounted_marker.symlink_to(outside)
        with self.assertRaisesRegex(RuntimeError, "escaped"):
            self.run_bootstrap()
        self.assertNotIn(PACKAGE, sys.modules)

    def test_relative_imports_arguments_and_duplicate_invocation(self):
        previous = {sig: signal.getsignal(sig) for sig in (signal.SIGTERM, signal.SIGINT)}
        self.run_bootstrap()
        main = sys.modules[PACKAGE + ".main"]
        self.assertEqual(main.VERSION, "fixture")
        self.assertEqual(main.calls, [(self.staging.resolve(), self.root / "working" / "relay-result", "disabled", False)])
        self.assertFalse((self.root / "working").exists())
        for sig, handler in previous.items():
            self.assertEqual(signal.getsignal(sig), handler)
        with self.assertRaisesRegex(RuntimeError, "already installed"):
            self.run_bootstrap()
        self.assertEqual(len(main.calls), 1)

    def test_failure_and_cancellation_restore_signal_handlers(self):
        for fault in ("phase", "exception", "cancellation"):
            with self.subTest(fault=fault):
                self.clear_modules()
                self.payload["manifest"].update(
                    phase="failed" if fault == "phase" else "completed",
                    **{"raise": fault == "exception", "cancel": fault == "cancellation"})
                previous = {sig: signal.getsignal(sig) for sig in (signal.SIGTERM, signal.SIGINT)}
                if fault == "cancellation":
                    self.run_bootstrap()
                else:
                    with self.assertRaises(RuntimeError):
                        self.run_bootstrap()
                self.assertEqual(len(sys.modules[PACKAGE + ".main"].calls), 1)
                for sig, handler in previous.items():
                    self.assertEqual(signal.getsignal(sig), handler)
