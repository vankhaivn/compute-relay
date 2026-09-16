"""A standalone helper self-terminates even without a Go supervisor."""
import subprocess
import sys
import unittest
from test_preflight import ROOT


class PreflightDeadlineTests(unittest.TestCase):
    def test_helper_has_independent_wall_deadline(self):
        # No Go supervisor participates. Run the exact helper's watchdog around
        # a repository-owned blocking callback and require self-termination.
        source = (ROOT / "internal/provider/kaggle/preflight.py").read_text(encoding="utf-8")
        script = ("import time\nns={'__name__':'watchdog_fixture'}\n"
                  + "exec(" + repr(source) + ",ns)\n"
                  + "ns['main']=lambda:time.sleep(60)\nns['bounded_main'](0.1)\n")
        result = subprocess.run([sys.executable, "-I", "-c", script],
                                input=b"", capture_output=True, timeout=5, check=False)
        self.assertEqual(result.returncode, 3)
        self.assertEqual(result.stdout, b"")
        self.assertEqual(result.stderr, b"")

