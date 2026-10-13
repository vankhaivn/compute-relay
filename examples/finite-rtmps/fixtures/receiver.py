#!/usr/bin/env python3
"""Bounded private MediaMTX fixture, run only in an explicitly owned container."""

import argparse
import hashlib
import http.client
import json
import os
from pathlib import Path
import selectors
import shutil
import signal
import stat
import subprocess
import sys
import time

PRIVATE, DATA = Path("/private"), Path("/data")
EXPECTED_BINARY = "0ffb7f18210d08ee38cb16668c4f3a2c96622e69d401119c0026d078a2b89523"


def require(condition):
    if not condition:
        raise RuntimeError("receiver_fixture_failed")


def write_new(path, data, mode=0o400):
    with path.open("xb") as target:
        os.fchmod(target.fileno(), mode)
        target.write(data)


def private_mount():
    info = PRIVATE.stat()
    require(info.st_uid == os.getuid() and stat.S_IMODE(info.st_mode) == 0o700)
    rows = Path("/proc/self/mountinfo").read_text().splitlines()
    require(any(" /private " in row and " - tmpfs " in row and
                all(flag in row for flag in ("nosuid", "nodev", "noexec")) for row in rows))


def deliver():
    private_mount()
    write_new(PRIVATE / ".claim", b"claimed\n")
    raw = bytearray()
    end = time.monotonic() + 20
    with selectors.DefaultSelector() as selector:
        selector.register(sys.stdin.buffer, selectors.EVENT_READ)
        while time.monotonic() < end:
            if not selector.select(min(1, max(0, end - time.monotonic()))):
                continue
            chunk = os.read(sys.stdin.fileno(), 4096)
            if not chunk:
                break
            raw.extend(chunk)
            require(len(raw) <= 16384)
        else:
            raise RuntimeError("receiver_delivery_timeout")
    try:
        values = json.loads(raw)
        require(set(values) == {"key_pem", "cert_pem", "password"})
        require(all(isinstance(v, str) for v in values.values()))
        require(32 <= len(values["password"]) <= 128 and values["password"].isalnum())
        write_new(PRIVATE / "server.key", values["key_pem"].encode())
        write_new(PRIVATE / "server.crt", values["cert_pem"].encode())
        config = {
            "logLevel": "warn", "logDestinations": ["stdout"],
            "authMethod": "internal", "authInternalUsers": [
                {"user": "relay-publisher", "pass": values["password"], "ips": [],
                 "permissions": [{"action": "publish", "path": "finite-test"}]},
                {"user": "any", "pass": "", "ips": ["127.0.0.1", "::1"],
                 "permissions": [{"action": "api"}, {"action": "metrics"}]}],
            "api": True, "apiAddress": "127.0.0.1:9997",
            "metrics": True, "metricsAddress": "127.0.0.1:9998",
            "rtsp": False, "rtmp": True, "rtmpEncryption": "strict", "rtmpsAddress": ":1936",
            "rtmpServerKey": str(PRIVATE / "server.key"), "rtmpServerCert": str(PRIVATE / "server.crt"),
            "hls": False, "webrtc": False, "srt": False, "moq": False,
            "paths": {"finite-test": {"record": True, "recordPath": "/data/recordings/%path/%Y-%m-%d_%H-%M-%S-%f",
                "recordFormat": "fmp4", "recordPartDuration": "1s", "recordSegmentDuration": "15s",
                "recordDeleteAfter": "60s"}}}
        write_new(PRIVATE / "mediamtx.json", json.dumps(config).encode())
        write_new(PRIVATE / ".ready", b"ready\n")
    finally:
        raw[:] = b"\0" * len(raw)
    return {"delivered": True}


def observe():
    connection = http.client.HTTPConnection("127.0.0.1", 9997, timeout=2)
    try:
        connection.request("GET", "/v3/paths/get/finite-test")
        response = connection.getresponse()
        raw = response.read(65537)
        require(len(raw) <= 65536)
        if response.status == 404:
            return {"api_available": True, "ready": False}
        require(response.status == 200)
        value = json.loads(raw)
        allowed = {"name", "ready", "readyTime", "tracks", "bytesReceived", "inboundBytes", "outboundBytes"}
        return {"api_available": True, **{k: v for k, v in value.items() if k in allowed}}
    except (OSError, ValueError, http.client.HTTPException):
        return {"api_available": False}
    finally:
        connection.close()


def records():
    root = DATA / "recordings" / "finite-test"
    return [{"name": path.name, "bytes": path.stat().st_size, "mtime": path.stat().st_mtime}
            for path in sorted(root.glob("*.mp4")) if path.is_file() and not path.is_symlink()]


def sample():
    files = records()
    # The newest segment can still be open. Preserve one completed segment only.
    require(len(files) >= 2)
    candidate = DATA / "recordings" / "finite-test" / files[-2]["name"]
    target = DATA / "sample.mp4"
    temporary = DATA / "sample.pending"
    require(candidate.stat().st_size < 16 << 20)
    with candidate.open("rb") as source, temporary.open("wb") as output:
        shutil.copyfileobj(source, output)
    temporary.replace(target)
    return {"bytes": target.stat().st_size, "sha256": hashlib.sha256(target.read_bytes()).hexdigest()}


def supervise(end_at):
    private_mount()
    require(os.getpid() == 1 and 0 < end_at - time.time() <= 32400)
    started = time.clock_gettime(time.CLOCK_BOOTTIME)
    boot_end = started + end_at - time.time()
    stopping = [False]
    def stop(_signum, _frame):
        stopping[0] = True
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    def remaining():
        return min(end_at - time.time(), boot_end - time.clock_gettime(time.CLOCK_BOOTTIME))
    delivery_end = time.monotonic() + 30
    while not (PRIVATE / ".ready").exists():
        require(not stopping[0] and remaining() > 0 and time.monotonic() < delivery_end)
        time.sleep(0.1)
    binary = Path("/fixture/mediamtx")
    require(hashlib.sha256(binary.read_bytes()).hexdigest() == EXPECTED_BINARY)
    process = subprocess.Popen([str(binary), str(PRIVATE / "mediamtx.json")],
                               stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                               stderr=subprocess.DEVNULL, start_new_session=True)
    write_new(DATA / "state.json", json.dumps({"pid": process.pid, "end_at": end_at, "started_at": time.time()}).encode())
    reason = "deadline"
    try:
        while remaining() > 0 and not stopping[0]:
            if process.poll() is not None:
                reason = "receiver_exited"
                break
            if shutil.disk_usage(DATA).used > 112 << 20:
                reason = "recording_limit"
                break
            time.sleep(0.2)
        if stopping[0]:
            reason = "terminated"
    finally:
        for action in (signal.SIGCONT, signal.SIGTERM):
            try:
                os.killpg(process.pid, action)
            except ProcessLookupError:
                pass
        try:
            process.wait(timeout=2)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait(timeout=2)
    return {"reason": reason, "exit_code": process.returncode, "finished_at": time.time()}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("supervise", "deliver", "observe", "records", "sample", "stall", "disconnect"))
    parser.add_argument("--end-at", type=int)
    args = parser.parse_args()
    try:
        if args.action == "supervise":
            result = supervise(args.end_at)
        elif args.action in {"stall", "disconnect"}:
            value = json.loads((DATA / "state.json").read_bytes())
            require(type(value["pid"]) is int and value["pid"] > 1)
            os.kill(value["pid"], signal.SIGSTOP if args.action == "stall" else signal.SIGTERM)
            result = {"requested": args.action}
        else:
            result = {"deliver": deliver, "observe": observe, "records": records, "sample": sample}[args.action]()
        print(json.dumps(result, sort_keys=True))
        return 0
    except Exception:
        print("receiver_fixture_failed", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
