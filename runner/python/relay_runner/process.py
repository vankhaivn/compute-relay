"""Linux process-group supervision with bounded, streaming log capture."""

from dataclasses import dataclass
import os
import re
import selectors
import signal
import subprocess
import time

from .contract import BUFFER, Failure

MARKER = b"\n[runner: log truncated]\n"
SECRET = re.compile(rb"cr1_[A-Za-z0-9_-]{43}|(?:gh[pousr]_[A-Za-z0-9]{20,255})|(?:authorization[=: ]+bearer[ ]+|(?:api[_-]?key|token|password|secret)[=:][ ]*)[^\s\"']{1,512}", re.I)


class Log:
    """Best-effort known-pattern redaction; arbitrary workload data is not classified."""

    def __init__(self, path, limit):
        self.file = path.open("xb")
        self.limit, self.written, self.received = limit, 0, 0
        self.truncated = False
        self.pending = b""

    def feed(self, raw):
        self.received += len(raw)
        self.pending += raw
        cut = max(0, len(self.pending) - 1024)
        for match in SECRET.finditer(self.pending):
            if match.start() < cut < match.end():
                cut = match.start()
        self._write(SECRET.sub(b"[REDACTED]", self.pending[:cut]))
        self.pending = self.pending[cut:]

    def _write(self, data):
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
