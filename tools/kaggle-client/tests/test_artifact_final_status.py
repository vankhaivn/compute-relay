"""Additional pinned-SDK end-of-transfer checks; all HTTP is synthetic."""
import importlib.util
import io
import json
import unittest

import test_artifacts as fixtures


@unittest.skipUnless(importlib.util.find_spec("kagglesdk"), "pinned SDK runs in offline client CI")
class ArtifactFinalStatusTests(unittest.TestCase):
    def test_lost_terminal_evidence_after_last_byte_invalidates_transfer(self):
        for status in ("RUNNING", "FUTURE_STATE", None):
            with self.subTest(status=status):
                f = fixtures.ArtifactPinnedSDKTests()
                f.setUp()
                f.pin("outputs/answer.txt")
                original = f.exchange
                def exchange(adapter, request, **kwargs):
                    if request.url.endswith("GetKernelSessionStatus") and f.target_path in f.downloads:
                        f.status = status
                    return original(adapter, request, **kwargs)
                f.exchange = exchange
                sink = io.BytesIO()
                with self.assertRaisesRegex(ValueError, "termination not established"):
                    f.invoke("fetch", sink)
                self.assertEqual(sink.getvalue(), b"yes")
                self.assertEqual(f.downloads, [fixtures.contract.MANIFEST, "outputs/answer.txt"])
                self.assertTrue(all(op in fixtures.bridge.ALLOWED for op, _ in f.calls))

    def test_terminal_failure_keeps_available_manifest_and_logs(self):
        f = fixtures.ArtifactPinnedSDKTests()
        f.setUp()
        f.status = "ERROR"
        f.manifest.update(phase="failed", exit_code=1, artifacts=[])
        f.data[fixtures.contract.MANIFEST] = json.dumps(f.manifest).encode()
        result = f.invoke()
        paths = {entry["path"] for entry in result["files"]}
        self.assertEqual(paths, {fixtures.contract.MANIFEST, "control/stdout.log", "control/environment.json"})
        self.assertNotIn("outputs/answer.txt", f.downloads)
        # Candidate selection is not full manifest validation or success publication;
        # M3 validates failure fields and independently verifies these selected bytes.
