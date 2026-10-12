#!/usr/bin/env python3
"""Finite, one-process RTMPS stream copy for the Compute Relay Python runner."""

from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import re
import selectors
import shutil
import signal
import stat
import subprocess
import sys
import tarfile
import time
import urllib.parse

MAX_MEDIA_BYTES = 1 << 30
SCRATCH_RESERVE = 64 << 20
MAX_JSON_BYTES = 16384
MAX_PROGRESS_BYTES = 16384
STOP_MARGIN_SECONDS = 2


class Failure(Exception):
    """Messages are fixed codes; never attach external diagnostics or arguments."""


def require(condition, code):
    if not condition:
        raise Failure(code)


def utc():
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def epoch(value):
    require(isinstance(value, str) and re.fullmatch(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ", value), "invalid_deadline")
    try:
        return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc).timestamp()
    except ValueError:
        raise Failure("invalid_deadline") from None


def number(value, low, high, code):
    require(type(value) in (int, float) and math.isfinite(value) and low <= value <= high, code)
    return value


def read_json(path, limit=MAX_JSON_BYTES):
    try:
        with Path(path).open("rb") as source:
            raw = source.read(limit + 1)
        require(len(raw) <= limit, "oversized_configuration")
        value = json.loads(raw)
        require(isinstance(value, dict), "invalid_configuration")
        return value
    except (OSError, UnicodeError, ValueError):
        raise Failure("invalid_configuration") from None


def digest(path, maximum, check=lambda: None, *, allow_empty=False):
    try:
        with Path(path).open("rb") as source:
            info = os.fstat(source.fileno())
            require(stat.S_ISREG(info.st_mode) and not Path(path).is_symlink(), "invalid_input_file")
            require((0 if allow_empty else 1) <= info.st_size <= maximum, "input_size_limit")
            total, result = 0, hashlib.sha256()
            while chunk := source.read(1 << 20):
                check()
                total += len(chunk)
                require(total <= maximum, "input_size_limit")
                result.update(chunk)
            require(total == info.st_size, "input_changed")
            return total, result.hexdigest()
    except OSError:
        raise Failure("input_unavailable") from None


def verify_file(path, expected, maximum, check=lambda: None, *, allow_empty=False):
    require(set(expected) >= {"bytes", "sha256"}, "invalid_configuration")
    require(type(expected["bytes"]) is int and (0 if allow_empty else 1) <= expected["bytes"] <= maximum, "input_size_limit")
    require(isinstance(expected["sha256"], str) and re.fullmatch(r"[a-f0-9]{64}", expected["sha256"]), "invalid_digest")
    size, actual = digest(path, maximum, check, allow_empty=allow_empty)
    require((size, actual) == (expected["bytes"], expected["sha256"]), "input_digest_mismatch")


class Budget:
    def __init__(self, end_at, seconds, boot=None, wall=time.time):
        self.boot = boot or (lambda: time.clock_gettime(time.CLOCK_BOOTTIME))
        self.wall = wall
        self.started = self.boot()
        self.utc_end = epoch(end_at)
        self.boot_end = self.started + min(seconds, self.utc_end - self.wall())
        self.cancelled = False

    def remaining(self):
        return min(self.utc_end - self.wall(), self.boot_end - self.boot())

    def check(self):
        require(not self.cancelled, "terminated")
        require(self.remaining() > STOP_MARGIN_SECONDS, "deadline_expired")


def stop_process(process, grace=1.0):
    """The only child group is this exact invocation; no name-based process search."""
    if process is None:
        return None, False
    forced = False
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    try:
        process.wait(timeout=grace)
    except subprocess.TimeoutExpired:
        forced = True
    # Kill any remaining group members even if the leader already exited.
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    process.wait(timeout=grace)
    return process.returncode, forced


def capture(command, budget, maximum=65536):
    budget.check()
    process = None
    try:
        process = subprocess.Popen(command, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                                   stderr=subprocess.DEVNULL, start_new_session=True)
        output, _ = process.communicate(timeout=min(15, budget.remaining() - STOP_MARGIN_SECONDS))
        require(process.returncode == 0 and len(output) <= maximum, "runtime_probe_failed")
        return output
    except (OSError, subprocess.TimeoutExpired):
        raise Failure("runtime_probe_failed") from None
    finally:
        if process is not None:
            stop_process(process)


def validate_config(config):
    require(set(config) == {"schema_version", "media", "ca", "end_at", "startup_seconds", "stall_seconds", "progress_seconds"}, "invalid_configuration")
    require(config["schema_version"] == 1, "invalid_configuration")
    epoch(config["end_at"])
    media = config["media"]
    require(isinstance(media, dict) and set(media) == {"bytes", "sha256", "width", "height"}, "invalid_configuration")
    for field in ("width", "height"):
        require(type(media[field]) is int and 2 <= media[field] <= 4096 and media[field] % 2 == 0, "incompatible_media")
    require(config["ca"] is None or isinstance(config["ca"], dict) and set(config["ca"]) == {"bytes", "sha256"}, "invalid_configuration")
    for field in ("startup_seconds", "stall_seconds"):
        number(config[field], 5, 120, "invalid_watchdog")
    number(config["progress_seconds"], 5, 60, "invalid_progress_interval")
    return config


def extract_runtime(archive, scratch, lock, budget):
    require(lock.get("lock_version") == 1 and lock.get("platform") == "linux/amd64", "incompatible_runtime")
    verify_file(archive, lock["archive"], 200 << 20, budget.check)
    entries = {item["member"]: item for item in lock["members"]}
    require(len(entries) == 3 and {Path(name).name for name in entries} == {"ffmpeg", "ffprobe", "LICENSE.txt"}, "incompatible_runtime")
    expanded = sum(item["bytes"] for item in entries.values())
    require(expanded <= 300 << 20, "runtime_size_limit")
    require(shutil.disk_usage(scratch).free >= expanded + SCRATCH_RESERVE, "insufficient_scratch")
    target = Path(scratch) / "finite-rtmps-runtime"
    try:
        target.mkdir(mode=0o700)
        with tarfile.open(archive, "r:xz") as source:
            for member in source:
                budget.check()
                if member.name not in entries:
                    continue
                expected = entries.pop(member.name)
                require(member.isfile() and member.size == expected["bytes"], "incompatible_runtime")
                output = target / Path(member.name).name
                count, result = 0, hashlib.sha256()
                with source.extractfile(member) as src, output.open("xb") as dst:
                    while chunk := src.read(1 << 20):
                        budget.check()
                        count += len(chunk)
                        require(count <= expected["bytes"], "runtime_size_limit")
                        result.update(chunk)
                        dst.write(chunk)
                require(count == expected["bytes"] and result.hexdigest() == expected["sha256"], "runtime_digest_mismatch")
                output.chmod(0o400 if output.name == "LICENSE.txt" else 0o500)
        require(not entries, "runtime_member_missing")
        return target
    except (OSError, tarfile.TarError, EOFError):
        raise Failure("runtime_unpack_failed") from None


def preflight(inputs, scratch, lock, config, budget):
    require(sys.platform == "linux" and platform.machine() in {"x86_64", "amd64"}, "unsupported_platform")
    validate_config(config)
    verify_file(inputs / "media.mp4", config["media"], MAX_MEDIA_BYTES, budget.check)
    if config["ca"] is not None:
        verify_file(inputs / "ca.pem", config["ca"], 1 << 20, budget.check)
    runtime = extract_runtime(inputs / "runtime.tar.xz", scratch, lock, budget)
    for binary in ("ffmpeg", "ffprobe"):
        version = capture([str(runtime / binary), "-version"], budget)
        require(lock["source_version"].encode() in version.split(b"\n")[0], "incompatible_runtime")
    protocols = capture([str(runtime / "ffmpeg"), "-hide_banner", "-protocols"], budget).split()
    require(b"rtmps" in protocols and b"tls" in protocols, "rtmps_unavailable")
    raw = capture([str(runtime / "ffprobe"), "-v", "error", "-protocol_whitelist", "file", "-f", "mov",
                   "-show_entries", "stream=codec_type,codec_name,width,height,pix_fmt,profile,sample_rate,channels:format=duration",
                   "-of", "json", str(inputs / "media.mp4")], budget)
    try:
        media = json.loads(raw)
        streams = media["streams"]
        require(len(streams) == 2, "incompatible_media")
        video = next(s for s in streams if s["codec_type"] == "video")
        audio = next(s for s in streams if s["codec_type"] == "audio")
        require(video["codec_name"] == "h264" and video["pix_fmt"] == "yuv420p" and
                video["width"] == config["media"]["width"] and video["height"] == config["media"]["height"], "incompatible_media")
        require(audio["codec_name"] == "aac" and audio["profile"] == "LC" and
                int(audio["sample_rate"]) in (44100, 48000) and 1 <= audio["channels"] <= 2, "incompatible_media")
        duration = number(float(media["format"]["duration"]), 1, 21600, "incompatible_media")
    except (KeyError, ValueError, TypeError, StopIteration):
        raise Failure("incompatible_media") from None
    budget.check()
    return runtime / "ffmpeg", duration


def read_endpoint(path):
    try:
        info = path.lstat()
        require(stat.S_ISREG(info.st_mode) and stat.S_IMODE(info.st_mode) == 0o400 and info.st_size <= 8192, "invalid_secret_file")
        raw = bytearray(path.read_bytes())
        try:
            value = raw.decode("utf-8")
            parsed = urllib.parse.urlsplit(value)
            require(parsed.scheme == "rtmps" and parsed.hostname and parsed.path.startswith("/") and
                    len(parsed.path) > 1 and not parsed.fragment and not any(c.isspace() or ord(c) < 32 for c in value), "invalid_endpoint")
            require(parsed.port is None or 1 <= parsed.port <= 65535, "invalid_endpoint")
            return value
        finally:
            raw[:] = b"\0" * len(raw)
    except (OSError, UnicodeError, ValueError):
        raise Failure("invalid_endpoint") from None


def command(ffmpeg, inputs, ca, endpoint):
    args = [str(ffmpeg), "-hide_banner", "-nostdin", "-loglevel", "error", "-nostats",
            "-stats_period", "1", "-progress", "pipe:1", "-stream_loop", "-1", "-readrate", "1",
            "-readrate_catchup", "1", "-protocol_whitelist", "file", "-i", str(inputs / "media.mp4"),
            "-map", "0:v:0", "-map", "0:a:0", "-c", "copy", "-map_metadata", "-1",
            "-flvflags", "no_duration_filesize", "-f", "flv", "-rw_timeout", "60000000", "-tls_verify", "1"]
    if ca:
        args += ["-ca_file", str(inputs / "ca.pem")]
    return args + [endpoint]


class Progress:
    def __init__(self, started):
        self.output_us = self.frames = 0
        self.last_advance = started
        self.started = started
        self.partial = bytearray()
        self.record = {}
        self.saw_media = False

    def feed(self, data, now):
        self.partial.extend(data)
        require(len(self.partial) <= MAX_PROGRESS_BYTES, "invalid_progress")
        while b"\n" in self.partial:
            raw, _, rest = self.partial.partition(b"\n")
            self.partial = bytearray(rest)
            key, sep, value = bytes(raw).partition(b"=")
            require(sep and len(raw) <= 1024, "invalid_progress")
            if key in (b"frame", b"out_time_us"):
                require(re.fullmatch(rb"-?[0-9]{1,18}", value) is not None, "invalid_progress")
                self.record[key] = max(0, int(value))
            elif key == b"progress":
                require(value in (b"continue", b"end"), "invalid_progress")
                output = self.record.get(b"out_time_us", self.output_us)
                frames = self.record.get(b"frame", self.frames)
                require(output >= self.output_us and frames >= self.frames, "progress_regressed")
                if output > self.output_us and frames > self.frames:
                    self.last_advance, self.saw_media = now, True
                self.output_us, self.frames = output, frames
                self.record.clear()

    def fields(self, now):
        return {"elapsed_ms": round((now - self.started) * 1000), "output_time_us": self.output_us,
                "frames": self.frames, "last_advance_elapsed_ms": round((self.last_advance - self.started) * 1000)}


def emit(value):
    raw = json.dumps(value, sort_keys=True, separators=(",", ":"))
    require(len(raw) <= 1024, "progress_record_limit")
    print(raw, flush=True)


def stream(args, config, budget, report):
    process = None
    progress = Progress(budget.boot())
    next_emit = progress.started
    reason = "ffmpeg_failed"
    try:
        budget.check()
        process = subprocess.Popen(args, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                                   stderr=subprocess.DEVNULL, start_new_session=True, bufsize=0)
        process.args = ["ffmpeg", "<private arguments>"]
        args[-1] = "<private endpoint>"
        with selectors.DefaultSelector() as selector:
            selector.register(process.stdout, selectors.EVENT_READ)
            while True:
                now = budget.boot()
                if budget.cancelled:
                    reason = "terminated"
                    break
                if budget.remaining() <= STOP_MARGIN_SECONDS:
                    reason = "deadline"
                    break
                limit = config["stall_seconds"] if progress.saw_media else config["startup_seconds"]
                if now - progress.last_advance >= limit:
                    reason = "media_stalled" if progress.saw_media else "startup_stalled"
                    break
                if process.poll() is not None:
                    reason = "ffmpeg_failed" if process.returncode else "unexpected_exit"
                    break
                for key, _ in selector.select(timeout=0.1):
                    data = os.read(key.fd, 4096)
                    if data:
                        progress.feed(data, budget.boot())
                    else:
                        selector.unregister(key.fileobj)
                if now >= next_emit:
                    emit({"schema_version": 1, "event": "progress", "observed_at": utc(), **progress.fields(now)})
                    next_emit = now + config["progress_seconds"]
    finally:
        code, forced = stop_process(process)
        if process is not None and process.stdout is not None:
            process.stdout.close()
        report.update(progress.fields(budget.boot()), process_exit_code=code, forced_kill=forced,
                      process_terminated=process is not None and process.poll() is not None)
    return reason


def main():
    report = {"schema_version": 1, "event": "summary", "started_at": utc(), "reason": "preflight_failed",
              "process_terminated": False, "receiver_health": "not_observed"}
    output = Path(os.environ["CC_OUTPUT_DIR"])
    code = 1
    try:
        inputs, scratch = Path(os.environ["CC_INPUT_DIR"]), Path(os.environ["CC_SCRATCH_DIR"])
        config = validate_config(read_json(inputs / "config.json"))
        lock = read_json(Path(__file__).with_name("runtime.lock.json"))
        manifest = read_json(os.environ["CC_EXECUTION_MANIFEST_PATH"], 1 << 20)
        budgets = manifest["timeouts"]
        end_at = min(config["end_at"], budgets["execution_deadline"])
        require(sys.platform == "linux" and hasattr(time, "CLOCK_BOOTTIME"), "unsupported_platform")
        budget = Budget(end_at, budgets["remote_wall_seconds"] - budgets["finalization_grace_seconds"])
        def cancelled(_signum, _frame):
            budget.cancelled = True
        signal.signal(signal.SIGTERM, cancelled)
        signal.signal(signal.SIGINT, cancelled)
        ffmpeg, duration = preflight(inputs, scratch, lock, config, budget)
        report.update(runtime_version=lock["source_version"], runtime_archive_sha256=lock["archive"]["sha256"],
                      media_sha256=config["media"]["sha256"], media_duration_seconds=duration,
                      width=config["media"]["width"], height=config["media"]["height"], effective_end_at=end_at)
        endpoint = read_endpoint(Path(os.environ["CC_SECRET_DIR"]) / "ingest")
        args = command(ffmpeg, inputs, config["ca"] is not None, endpoint)
        endpoint = None
        report["reason"] = stream(args, config, budget, report)
        code = 0 if report["reason"] == "deadline" else 1
    except Failure as exc:
        report["reason"] = str(exc)
    except Exception:
        report["reason"] = "workload_failed"
    report["finished_at"] = utc()
    raw = json.dumps(report, sort_keys=True, separators=(",", ":")) + "\n"
    require(len(raw) <= MAX_JSON_BYTES, "summary_limit")
    with (output / "summary.json").open("x", encoding="utf-8") as target:
        target.write(raw)
    emit({"schema_version": 1, "event": "stopped", "observed_at": report["finished_at"],
          "reason": report["reason"], "process_terminated": report["process_terminated"]})
    return code


if __name__ == "__main__":
    raise SystemExit(main())
