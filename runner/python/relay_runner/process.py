"""Linux process-group supervision with bounded, streaming log capture."""

from dataclasses import dataclass
import os
import re
import selectors
import signal
import subprocess
import time
import sys
import queue
import threading
import codecs

from .contract import BUFFER, Failure

MARKER = b"\n[runner: log truncated]\n"
SECRET = re.compile(rb"cr1_[A-Za-z0-9_-]{43}|(?:gh[pousr]_[A-Za-z0-9]{20,255})|(?:authorization[=: ]+bearer[ ]+|(?:api[_-]?key|token|password|secret)[=:][ ]*)[^\s\"']{1,512}", re.I)


class Mirror:
    """Bounded best-effort output; a blocked notebook cannot stall child drains."""

    def __init__(self, stream, limit=16 << 20):
        self.stream, self.remaining = stream, limit
        self.queue = queue.Queue(maxsize=8)
        self.failed = False
        self.thread = threading.Thread(target=self._drain, daemon=True)
        self.thread.start()

    def _drain(self):
        decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
        text_output = False
        try:
            while True:
                data = self.queue.get()
                if data is None:
                    return
                target = getattr(self.stream, "buffer", self.stream)
                try:
                    target.write(decoder.decode(data) if text_output else data)
                except TypeError:
                    text_output = True
                    target.write(decoder.decode(data))
                target.flush()
        except Exception:
            self.failed = True

    def feed(self, data):
        if self.failed or not data or self.remaining <= 0:
            return
        data = data[:self.remaining]
        self.remaining -= len(data)
        # Queue occupancy bounds memory even if the output writer blocks forever.
        for offset in range(0, len(data), 8192):
            try:
                self.queue.put_nowait(data[offset:offset + 8192])
            except queue.Full:
                return

    def close(self):
        try:
            self.queue.put_nowait(None)
        except queue.Full:
            pass
        self.thread.join(timeout=0.02)


class Log:
    """Best-effort known-pattern redaction; arbitrary workload data is not classified."""

    def __init__(self, path, limit, mirror=None):
        self.file = path.open("xb")
        self.limit, self.written, self.received = limit, 0, 0
        self.truncated = False
        self.pending = b""
        stream = sys.stderr if "stderr" in path.name else sys.stdout
        self.mirror = Mirror(stream) if mirror is None else mirror

    def feed(self, raw):
        self.received += len(raw)
        self.pending += raw
        cut = max(0, len(self.pending) - 1024, self.pending.rfind(b"\n") + 1)
        for match in SECRET.finditer(self.pending):
            if match.start() < cut < match.end():
                cut = match.start()
        self._write(SECRET.sub(b"[REDACTED]", self.pending[:cut]))
        self.pending = self.pending[cut:]

    def _write(self, data):
        self.mirror.feed(data)
        available = max(0, self.limit - len(MARKER) - self.written)
        selected = data[:available]
        self.file.write(selected)
        self.written += len(selected)
        self.truncated |= len(data) > available

    def close(self):
        self._write(SECRET.sub(b"[REDACTED]", self.pending))
        self.pending = b""
        if self.truncated:
            tail = MARKER[:max(0, self.limit - self.written)]
            self.file.write(tail)
            self.written += len(tail)
        self.file.flush()
        os.fsync(self.file.fileno())
        self.file.close()
        self.mirror.close()
        return {"bytes_seen": self.received, "bytes_stored": self.written, "truncated": self.truncated}


@dataclass(frozen=True)
class Outcome:
    exit_code: int | None
    timed_out: bool
    cancelled: bool
    reaped: bool


def kill_group(pid, sig):
    try:
        os.killpg(pid, sig)
    except ProcessLookupError:
        pass


def run(command, cwd, env, deadline, grace, stdout, stderr, cancelled):
    if cancelled() or time.monotonic() >= deadline:
        return Outcome(None, not cancelled(), cancelled(), True)
    # An argument vector and a replacement environment: never shell=True or inherited stdin.
    try:
        proc = subprocess.Popen(command, cwd=cwd, env=env, stdin=subprocess.DEVNULL,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                start_new_session=True, close_fds=True)
    except OSError as exc:
        raise Failure("COMMAND_FAILED", "execution", "Could not launch the declared command") from exc
    timed_out, was_cancelled, stopping, killed = False, False, None, False
    try:
        with selectors.DefaultSelector() as selector:
            for pipe, sink in ((proc.stdout, stdout), (proc.stderr, stderr)):
                os.set_blocking(pipe.fileno(), False)
                selector.register(pipe, selectors.EVENT_READ, sink)
            while True:
                now = time.monotonic()
                code = proc.poll()
                if stopping is None and (code is not None or cancelled() or now >= deadline):
                    was_cancelled = cancelled()
                    timed_out = code is None and now >= deadline and not was_cancelled
                    kill_group(proc.pid, signal.SIGTERM)
                    stopping = now
                if stopping is not None:
                    if not killed and now - stopping >= min(0.2, grace / 2):
                        kill_group(proc.pid, signal.SIGKILL)
                        killed = True
                    if now - stopping >= grace:
                        break
                    if killed and code is not None and not selector.get_map():
                        break
                for key, _ in selector.select(0.02):
                    try:
                        data = os.read(key.fileobj.fileno(), BUFFER)
                    except BlockingIOError:
                        continue
                    if data:
                        key.data.feed(data)
                    else:
                        selector.unregister(key.fileobj)
                        key.fileobj.close()
        kill_group(proc.pid, signal.SIGKILL)
        try:
            code = proc.wait(timeout=max(0.01, min(grace, 0.2)))
        except subprocess.TimeoutExpired:
            code = None
        return Outcome(code, timed_out, was_cancelled, code is not None)
    finally:
        # Also clean up on disk-write errors or an unexpected supervisor exception.
        kill_group(proc.pid, signal.SIGKILL)
        for pipe in (proc.stdout, proc.stderr):
            if not pipe.closed:
                pipe.close()
        try:
            proc.wait(timeout=0.2)
        except subprocess.TimeoutExpired:
            pass  # Do not block forever; no claim that provider hardware was released.
