"""Completion must be explicit; a closed producer pipe can hide its source error."""
import importlib.util
import io
import unittest
from test_staging import Backend, fixture, staging


class StagingCompletionTests(unittest.TestCase):
    def test_complete_truncated_missing_and_trailing_acknowledgements(self):
        marker = staging.UPLOAD_COMPLETE
        staging.require_upload_complete(io.BytesIO(marker))
        for raw in (b"", marker[:-1], marker + b"x", b"x" + marker):
            with self.subTest(raw=raw):
                with self.assertRaises(ValueError):
                    staging.require_upload_complete(io.BytesIO(raw))

    def test_fragmented_acknowledgement_and_late_error(self):
        class Fragmented(io.BytesIO):
            def read(self, size=-1):
                return super().read(min(size, 2))
        staging.require_upload_complete(Fragmented(staging.UPLOAD_COMPLETE))

        class LateError(io.BytesIO):
            def read(self, size=-1):
                if self.tell() == len(self.getvalue()):
                    raise OSError("synthetic source failure after marker")
                return super().read(size)
        with self.assertRaises(OSError):
            staging.require_upload_complete(LateError(staging.UPLOAD_COMPLETE))


@unittest.skipUnless(importlib.util.find_spec("kagglesdk"), "requires pinned SDK")
class StagingCompletionSDKTests(unittest.TestCase):
    def test_all_payload_bytes_without_source_ack_cannot_create(self):
        for ending in (b"", staging.UPLOAD_COMPLETE[:-1], staging.UPLOAD_COMPLETE + b"x"):
            with self.subTest(ending=ending):
                request, payloads = fixture()
                backend = Backend(self, request, payloads)
                with self.assertRaises(ValueError):
                    backend.run("create", b"".join(payloads.values()) + ending)
                self.assertEqual(backend.uploaded, payloads)
                self.assertEqual(backend.creates, 0)
                calls = (backend.starts, backend.puts, backend.creates)
                self.assertEqual(backend.run("observe", b"")["status"], "not_found")
                self.assertEqual((backend.starts, backend.puts, backend.creates), calls)

    def test_acknowledged_payload_creates_once(self):
        request, payloads = fixture()
        backend = Backend(self, request, payloads)
        result = backend.run("create", b"".join(payloads.values()) + staging.UPLOAD_COMPLETE)
        self.assertEqual(result["status"], "pending")
        self.assertEqual(backend.creates, 1)
