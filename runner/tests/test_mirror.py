import io
from pathlib import Path
import tempfile
import threading
import time
import unittest
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "python"))

from relay_runner.process import Log, Mirror


class MirrorTests(unittest.TestCase):
    def test_chunk_redaction_is_shared_with_retained_log(self):
        with tempfile.TemporaryDirectory() as tmp:
            stream = io.BytesIO()
            mirror = Mirror(stream)
            log = Log(Path(tmp) / 'stdout.log', 4096, mirror=mirror)
            log.feed(b'progress\ncr1_' + b'A' * 20)
            log.feed(b'A' * 23 + b'\n')
            log.close()
            self.assertEqual(stream.getvalue(), b'progress\n[REDACTED]\n')
            self.assertEqual((Path(tmp) / 'stdout.log').read_bytes(), stream.getvalue())

    def test_blocked_or_broken_output_never_stalls_drain(self):
        release = threading.Event()

        class Blocked:
            def write(self, data):
                release.wait(2)

            def flush(self):
                pass

        mirror = Mirror(Blocked(), limit=100000)
        start = time.monotonic()
        for _ in range(100):
            mirror.feed(b'x' * 8192)
        mirror.close()
        self.assertLess(time.monotonic() - start, 0.2)
        self.assertLessEqual(mirror.queue.qsize(), 8)
        self.assertEqual(mirror.remaining, 0)
        release.set()

        class Broken:
            def write(self, data):
                raise BrokenPipeError()

        mirror = Mirror(Broken())
        mirror.feed(b'hello')
        mirror.close()
        self.assertTrue(mirror.failed)
