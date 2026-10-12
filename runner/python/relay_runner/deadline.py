"""UTC and suspend-aware elapsed bounds for finite Linux execution."""

from datetime import datetime, timezone
import math
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

POLL_SECONDS = 0.05
TERM_SECONDS = 0.2


def elapsed():
    # Linux BOOTTIME advances across suspend and cannot jump with the wall clock.
    return time.clock_gettime(time.CLOCK_BOOTTIME)


def epoch(value):
    return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc).timestamp()


class Bound:
    def __init__(self, seconds, absolute=0):
        wall, boot = time.time(), elapsed()
        self.absolute = absolute
        self.utc_end = min(wall + seconds, absolute) if absolute else wall + seconds
        self.elapsed_end = boot + max(0, self.utc_end - wall)
        self.reason = "absolute_deadline" if absolute and absolute <= wall + seconds else "relative_budget"

    def expired(self):
        return elapsed() >= self.elapsed_end or bool(self.absolute and time.time() >= self.absolute)

    def until(self, monotonic_end):
        bound = Bound(max(0, monotonic_end - time.monotonic()), self.absolute)
        bound.elapsed_end = min(bound.elapsed_end, self.elapsed_end)
        return bound

    def watch(self, pgid):
        # Independent of runner log drains, filesystem I/O and its main process.
        # Arguments contain only a process-group identity and non-secret time bounds.
        return subprocess.Popen([sys.executable, "-I", str(Path(__file__).resolve()),
                                 str(pgid), str(self.elapsed_end), str(self.absolute), process_identity(pgid)],
                                stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                                stderr=subprocess.DEVNULL, start_new_session=True, close_fds=True)


def process_identity(pid):
    try:
        # Start ticks disambiguate a reused group leader PID. A missing leader may
        # still have living group members, which must also obey the original bound.
        return Path("/proc", str(pid), "stat").read_text().rsplit(")", 1)[1].split()[19]
    except (OSError, IndexError):
        return "missing"


def signal_group(pgid, sig):
    try:
        os.killpg(pgid, sig)
        return True
    except ProcessLookupError:
        return False


def watch_group(pgid, elapsed_end, absolute, identity):
    if pgid <= 1 or pgid == os.getpgrp() or not all(math.isfinite(n) for n in (elapsed_end, absolute)):
        return 64
    while signal_group(pgid, 0):
        current = process_identity(pgid)
        if current != "missing" and current != identity:
            return 0
        if elapsed() >= elapsed_end or absolute and time.time() >= absolute:
            signal_group(pgid, signal.SIGTERM)
            time.sleep(TERM_SECONDS)
            signal_group(pgid, signal.SIGKILL)
            return 0
        time.sleep(POLL_SECONDS)
    return 0


def supervise(manifest, command):
    """Container entry point: independently bound finalization and runner failure.

    When this PID 1 exits Docker tears down the private PID namespace, including
    escaped process groups. Ordinary execution never needs Relay or SSH to stop.
    """
    t = manifest["timeouts"]
    absolute = epoch(t["execution_deadline"])
    end = Bound(t["remote_wall_seconds"] + 2, absolute + t["finalization_grace_seconds"] + 2)
    proc = subprocess.Popen(command, stdin=subprocess.DEVNULL, start_new_session=True, close_fds=True)
    stopped = [False]
    previous = {sig: signal.signal(sig, lambda *_: stopped.__setitem__(0, True))
                for sig in (signal.SIGTERM, signal.SIGINT)}
    try:
        while proc.poll() is None and not end.expired() and not stopped[0]:
            time.sleep(POLL_SECONDS)
        if proc.poll() is not None:
            return proc.returncode
        signal_group(proc.pid, signal.SIGTERM)
        try:
            proc.wait(timeout=TERM_SECONDS)
        except subprocess.TimeoutExpired:
            signal_group(proc.pid, signal.SIGKILL)
        return 124 if end.expired() else 1
    finally:
        signal_group(proc.pid, signal.SIGKILL)
        try:
            proc.wait(timeout=TERM_SECONDS)
        except subprocess.TimeoutExpired:
            pass
        for sig, handler in previous.items():
            signal.signal(sig, handler)


if __name__ == "__main__":
    try:
        raise SystemExit(watch_group(int(sys.argv[1]), float(sys.argv[2]), float(sys.argv[3]), sys.argv[4]))
    except (OSError, ValueError, IndexError):
        raise SystemExit(64)
