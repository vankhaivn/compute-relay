"""Offline fixtures; these never contact an ingest service or run FFmpeg."""

import contextlib
import hashlib
import io
import json
import os
from pathlib import Path
import signal
import sys
import tarfile
import tempfile
import time
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import workload as w


def end_after(seconds):
    from datetime import datetime, timezone
    return datetime.fromtimestamp(int(time.time() + seconds), timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class WorkloadTests(unittest.TestCase):
    def test_progress_requires_media_advance_not_heartbeat(self):
        progress = w.Progress(10)
        progress.feed(b"frame=4\nout_time_", 11)
        progress.feed(b"us=2000000\nprogress=continue\n", 12)
        self.assertEqual((progress.frames, progress.output_us, progress.last_advance), (4, 2000000, 12))
        progress.feed(b"frame=4\nout_time_us=2000000\nprogress=continue\n", 20)
        self.assertEqual(progress.last_advance, 12)
        with self.assertRaisesRegex(w.Failure, "progress_regressed"):
            progress.feed(b"frame=1\nout_time_us=1000000\nprogress=continue\n", 21)

    def test_progress_is_bounded_and_never_echoes_bad_values(self):
        for raw in (b"frame=synthetic-secret\n", b"x" * (w.MAX_PROGRESS_BYTES + 1), b"progress=secret\n"):
            with self.subTest(size=len(raw)), self.assertRaisesRegex(w.Failure, "^invalid_progress$"):
                w.Progress(0).feed(raw, 1)

    def test_clock_jumps_and_suspend_like_boot_gap_do_not_extend_budget(self):
        from datetime import datetime, timezone
        end = datetime.fromtimestamp(1100, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        current = {"wall": 1000, "boot": 20}
        budget = w.Budget(end, 90, boot=lambda: current["boot"], wall=lambda: current["wall"])
        current.update(wall=900, boot=80)
        self.assertEqual(budget.remaining(), 30)
        current["boot"] = 115
        with self.assertRaisesRegex(w.Failure, "deadline_expired"):
            budget.check()
        current.update(wall=1101, boot=21)
        self.assertLess(budget.remaining(), 0)

    def runtime_fixture(self, root, extra=False):
        archive = root / "runtime.tar.xz"
        entries = []
        with tarfile.open(archive, "w:xz") as target:
            for name in ("ffmpeg", "ffprobe", "LICENSE.txt"):
                raw = ("synthetic " + name).encode()
                member = "release/bin/" + name
                header = tarfile.TarInfo(member)
                header.size = len(raw)
                target.addfile(header, io.BytesIO(raw))
                entries.append({"member": member, "bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest()})
            if extra:
                header = tarfile.TarInfo("../../not-extracted")
                header.size = 1
                target.addfile(header, io.BytesIO(b"x"))
        return archive, {"lock_version": 1, "platform": "linux/amd64", "archive": {
            "bytes": archive.stat().st_size, "sha256": hashlib.sha256(archive.read_bytes()).hexdigest()}, "members": entries}

    def test_runtime_extracts_only_exact_members_and_checks_space(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            archive, lock = self.runtime_fixture(root, extra=True)
            budget = w.Budget(end_after(30), 30, boot=time.monotonic)
            with mock.patch.object(w.shutil, "disk_usage", return_value=type("Usage", (), {"free": 1})()):
                with self.assertRaisesRegex(w.Failure, "insufficient_scratch"):
                    w.extract_runtime(archive, root, lock, budget)
            self.assertFalse((root / "finite-rtmps-runtime").exists())
            target = w.extract_runtime(archive, root, lock, budget)
            self.assertEqual({p.name for p in target.iterdir()}, {"ffmpeg", "ffprobe", "LICENSE.txt"})
            self.assertEqual((target / "ffmpeg").stat().st_mode & 0o777, 0o500)
            self.assertFalse((root.parent / "not-extracted").exists())

    def test_runtime_corruption_and_input_size_fail_before_extraction(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            archive, lock = self.runtime_fixture(root)
            lock["archive"]["sha256"] = "0" * 64
            with self.assertRaisesRegex(w.Failure, "input_digest_mismatch"):
                w.extract_runtime(archive, root, lock, w.Budget(end_after(30), 30, boot=time.monotonic))
            with self.assertRaisesRegex(w.Failure, "input_size_limit"):
                w.digest(archive, 1)
            self.assertFalse((root / "finite-rtmps-runtime").exists())

    def test_endpoint_and_command_have_explicit_tls_and_one_copy_process(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "ingest"
            endpoint = "rtmps://receiver.invalid/live?pass=synthetic-only"
            path.write_text(endpoint)
            path.chmod(0o400)
            self.assertEqual(w.read_endpoint(path), endpoint)
            args = w.command(Path("ffmpeg"), Path("inputs"), True, endpoint)
            self.assertEqual(args[args.index("-c") + 1], "copy")
            self.assertEqual(args[args.index("-readrate") + 1], "1")
            self.assertEqual(args[args.index("-readrate_catchup") + 1], "1")
            self.assertEqual(args[args.index("-tls_verify") + 1], "1")
            self.assertFalse(any("reconnect" in arg for arg in args))
            path.chmod(0o600)
            with self.assertRaisesRegex(w.Failure, "invalid_secret_file"):
                w.read_endpoint(path)

    def test_preflight_failure_never_reads_endpoint(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config = {"schema_version": 1, "media": {"bytes": 1, "sha256": "0" * 64, "width": 1920, "height": 1080},
                      "ca": None, "end_at": end_after(120), "startup_seconds": 30, "stall_seconds": 30, "progress_seconds": 10}
            (root / "config.json").write_text(json.dumps(config))
            (root / "manifest.json").write_text(json.dumps({"timeouts": {"execution_deadline": config["end_at"],
                "remote_wall_seconds": 120, "finalization_grace_seconds": 10}}))
            environment = {"CC_INPUT_DIR": str(root), "CC_SCRATCH_DIR": str(root), "CC_OUTPUT_DIR": str(root),
                           "CC_EXECUTION_MANIFEST_PATH": str(root / "manifest.json"), "CC_SECRET_DIR": str(root)}
            budget = w.Budget(end_after(120), 120, boot=time.monotonic)
            with mock.patch.dict(os.environ, environment), mock.patch.object(w.sys, "platform", "linux"), \
                 mock.patch.object(w.time, "CLOCK_BOOTTIME", 7, create=True), mock.patch.object(w, "Budget", return_value=budget), \
                 mock.patch.object(w, "preflight", side_effect=w.Failure("input_unavailable")), \
                 mock.patch.object(w, "read_endpoint") as endpoint, contextlib.redirect_stdout(io.StringIO()):
                old_term, old_int = signal.getsignal(signal.SIGTERM), signal.getsignal(signal.SIGINT)
                try:
                    self.assertEqual(w.main(), 1)
                finally:
                    signal.signal(signal.SIGTERM, old_term)
                    signal.signal(signal.SIGINT, old_int)
            endpoint.assert_not_called()
            summary = json.loads((root / "summary.json").read_text())
            self.assertEqual(summary["reason"], "input_unavailable")
            self.assertFalse(summary["process_terminated"])

    def test_real_preflight_faults_stop_before_secret_or_push(self):
        cases = {"missing": "input_unavailable", "corrupt": "input_digest_mismatch",
                 "oversized": "input_size_limit", "space": "insufficient_scratch", "codec": "incompatible_media"}
        for case, reason in cases.items():
            with self.subTest(case=case), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                archive, lock = self.runtime_fixture(root)
                lock["source_version"] = "synthetic-v1"
                media = b"synthetic-media"
                (root / "media.mp4").write_bytes(media)
                config = {"schema_version": 1, "media": {"bytes": len(media), "sha256": hashlib.sha256(media).hexdigest(),
                          "width": 1920, "height": 1080}, "ca": None, "end_at": end_after(120),
                          "startup_seconds": 30, "stall_seconds": 30, "progress_seconds": 10}
                if case == "missing":
                    archive.unlink()
                elif case == "corrupt":
                    archive.write_bytes(b"x" * archive.stat().st_size)
                elif case == "oversized":
                    config["media"]["bytes"] = w.MAX_MEDIA_BYTES + 1
                (root / "config.json").write_text(json.dumps(config))
                (root / "manifest.json").write_text(json.dumps({"timeouts": {"execution_deadline": config["end_at"],
                    "remote_wall_seconds": 120, "finalization_grace_seconds": 10}}))
                environment = {"CC_INPUT_DIR": str(root), "CC_SCRATCH_DIR": str(root), "CC_OUTPUT_DIR": str(root),
                               "CC_EXECUTION_MANIFEST_PATH": str(root / "manifest.json"), "CC_SECRET_DIR": str(root)}
                read_json = w.read_json
                def read(path, limit=w.MAX_JSON_BYTES):
                    return lock if Path(path).name == "runtime.lock.json" else read_json(path, limit)
                def capture(argv, _budget):
                    if "-version" in argv:
                        return b"ffmpeg synthetic-v1\n"
                    if "-protocols" in argv:
                        return b"rtmps tls\n"
                    return json.dumps({"streams": [
                        {"codec_type": "video", "codec_name": "av1", "pix_fmt": "yuv420p", "width": 1920, "height": 1080},
                        {"codec_type": "audio", "codec_name": "aac", "profile": "LC", "sample_rate": "48000", "channels": 2}],
                        "format": {"duration": "4.0"}}).encode()
                budget = w.Budget(end_after(120), 120, boot=time.monotonic)
                with mock.patch.dict(os.environ, environment), mock.patch.object(w.sys, "platform", "linux"), \
                     mock.patch.object(w.platform, "machine", return_value="x86_64"), \
                     mock.patch.object(w.time, "CLOCK_BOOTTIME", 7, create=True), \
                     mock.patch.object(w, "Budget", return_value=budget), mock.patch.object(w, "read_json", side_effect=read), \
                     mock.patch.object(w.shutil, "disk_usage", return_value=type("Usage", (), {"free": 1 if case == "space" else 1 << 30})()), \
                     mock.patch.object(w, "capture", side_effect=capture) as probe, \
                     mock.patch.object(w, "read_endpoint") as endpoint, mock.patch.object(w, "stream") as push, \
                     contextlib.redirect_stdout(io.StringIO()):
                    old_term, old_int = signal.getsignal(signal.SIGTERM), signal.getsignal(signal.SIGINT)
                    try:
                        self.assertEqual(w.main(), 1)
                    finally:
                        signal.signal(signal.SIGTERM, old_term)
                        signal.signal(signal.SIGINT, old_int)
                endpoint.assert_not_called()
                push.assert_not_called()
                if case != "codec":
                    probe.assert_not_called()
                self.assertEqual(json.loads((root / "summary.json").read_text())["reason"], reason)


@unittest.skipUnless(os.name == "posix", "The workload runs in a Linux process namespace")
class SupervisorTests(unittest.TestCase):
    def run_stream(self, source, startup=0.3, stall=0.3, seconds=20):
        report, captured = {}, io.StringIO()
        config = {"startup_seconds": startup, "stall_seconds": stall, "progress_seconds": 0.1}
        args = [sys.executable, "-c", source, "synthetic-stderr-canary"]
        with contextlib.redirect_stdout(captured):
            reason = w.stream(args, config, w.Budget(end_after(seconds), seconds, boot=time.monotonic), report)
        self.assertTrue(report["process_terminated"])
        self.assertNotIn("synthetic-stderr-canary", captured.getvalue() + json.dumps(report))
        return reason, report

    def test_noisy_stall_is_stopped_without_a_controller(self):
        source = "import sys,time\nsys.stderr.write(sys.argv[1])\nwhile True:\n print('frame=1\\nout_time_us=1000\\nprogress=continue',flush=True)\n time.sleep(.02)"
        reason, report = self.run_stream(source)
        self.assertEqual(reason, "media_stalled")
        self.assertEqual(report["frames"], 1)
        self.assertLess(report["elapsed_ms"], 2000)

    def test_silent_connection_start_is_bounded(self):
        reason, _ = self.run_stream("import time;time.sleep(30)")
        self.assertEqual(reason, "startup_stalled")

    def test_failed_child_is_not_restarted(self):
        reason, report = self.run_stream("raise SystemExit(7)")
        self.assertEqual((reason, report["process_exit_code"]), ("ffmpeg_failed", 7))

    def test_term_ignoring_child_group_is_killed_at_deadline(self):
        source = "import signal,time\nsignal.signal(signal.SIGTERM,signal.SIG_IGN)\nn=0\nwhile True:\n n+=1\n print(f'frame={n}\\nout_time_us={n*100000}\\nprogress=continue',flush=True)\n time.sleep(.02)"
        reason, report = self.run_stream(source, startup=5, stall=5, seconds=4)
        self.assertEqual(reason, "deadline")
        self.assertTrue(report["forced_kill"])
        self.assertEqual(report["process_exit_code"], -signal.SIGKILL)


if __name__ == "__main__":
    unittest.main()
