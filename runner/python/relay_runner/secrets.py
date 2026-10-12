"""One-shot workload material in a private tmpfs; never provider credentials."""

import base64
import json
import os
from pathlib import Path
import re
import selectors
import stat
import time
from urllib.parse import quote_from_bytes, quote_plus

from .contract import Failure, fields, require, strict_json
from .deadline import Bound, epoch

SECRET_ROOT = Path("/relay/secrets")
MAX_PACKET = 48 << 10
MAX_SLOT = 8 << 10
MAX_TOTAL = 32 << 10
TMPFS_BYTES = 128 << 10
WAIT_SECONDS = 30


class SecretFound(Exception):
    """A known value must not enter an exported output."""


class Redactor:
    def __init__(self, values=()):
        variants = set()
        for value in values:
            value = bytes(value)
            require(0 < len(value) <= MAX_SLOT)
            variants.update((value, quote_from_bytes(value, safe="").encode(),
                             quote_from_bytes(value, safe="/").encode(), quote_plus(value).encode()))
            try:
                text = value.decode("utf-8")
                variants.update(json.dumps(text, ensure_ascii=ascii)[1:-1].encode()
                                for ascii in (False, True))
            except UnicodeError:
                pass
        self.needles = tuple(sorted(variants, key=lambda x: (-len(x), x)))
        self.pattern = re.compile(b"|".join(re.escape(x) for x in self.needles)) if self.needles else None
        self.hold = max((len(x) - 1 for x in self.needles), default=0)
        self.multiline = any(b"\n" in x for x in self.needles)
        self.marker = b"" if self.contains(b"[REDACTED]") else b"[REDACTED]"

    def contains(self, data):
        return self.pattern is not None and self.pattern.search(data) is not None

    def redact(self, data):
        if self.pattern is None:
            return data
        for _ in range(3):
            data = self.pattern.sub(self.marker, data)
            if not self.contains(data):
                return data
        return b""  # Do not publish values formed across replacement boundaries.

    def scanner(self):
        tail = b""

        def scan(data):
            nonlocal tail
            joined = tail + data
            if self.contains(joined):
                raise SecretFound()
            tail = joined[-self.hold:] if self.hold else b""
        return scan

    def safe_json(self, value):
        """Scrub strings before encoding; refuse any remaining serialized match."""
        def visit(item):
            if isinstance(item, str):
                return self.redact(item.encode()).decode("utf-8", errors="replace")
            if isinstance(item, list):
                return [visit(child) for child in item]
            if isinstance(item, dict):
                return {visit(key): visit(child) for key, child in item.items()}
            return item
        sanitized = visit(value)
        if self.contains(json.dumps(sanitized, sort_keys=True, separators=(",", ":")).encode()):
            raise Failure("ARTIFACT_COLLECTION_FAILED", "results", "Control output contains protected material")
        return sanitized

    def clear(self):
        self.needles, self.pattern = (), None


def require_tmpfs(root):
    root = Path(root).absolute()
    info = root.lstat()
    require(stat.S_ISDIR(info.st_mode) and stat.S_IMODE(info.st_mode) == 0o700 and
            info.st_uid == os.geteuid() and info.st_gid == os.getegid())
    with open("/proc/self/mountinfo", "rb") as source:
        raw = source.read((1 << 20) + 1)
    require(len(raw) <= 1 << 20)
    matches = []
    for line in raw.decode("utf-8").splitlines():
        before, after = line.split(" - ", 1)
        tokens, device = before.split(), after.split()
        mount = Path(tokens[4])
        if mount == root or mount in root.parents:
            matches.append((len(str(mount)), device[0], tokens[5].split(",")))
    require(bool(matches))
    _, kind, options = max(matches)
    require(kind == "tmpfs" and all(x in options for x in ("rw", "nosuid", "nodev", "noexec")))
    usage = os.statvfs(root)
    require(usage.f_blocks * usage.f_frsize <= TMPFS_BYTES)
    return root


def marker(control):
    return {"version": "compute-relay/secret-delivery/v1",
            **{key: control[key] for key in ("job_id", "attempt_id", "attempt_nonce", "manifest_sha256")},
            "authorization_id": control["secrets"]["authorization_id"],
            "expires_at": control["secrets"]["expires_at"],
            "names": sorted(slot["name"] for slot in control["secrets"]["slots"])}


def write_new(root, name, value):
    directory = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        fd = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o400, dir_fd=directory)
        with os.fdopen(fd, "wb") as output:
            output.write(value)
            output.flush()
    finally:
        os.close(directory)


def read_packet(stream, bound):
    packet = bytearray()
    fd = stream.fileno()
    try:
        with selectors.DefaultSelector() as selector:
            selector.register(fd, selectors.EVENT_READ)
            while not bound.expired():
                if not selector.select(0.05):
                    continue
                chunk = os.read(fd, min(8192, MAX_PACKET + 1 - len(packet)))
                if not chunk:
                    return packet
                packet.extend(chunk)
                require(len(packet) <= MAX_PACKET)
        raise Failure("REMOTE_TIMEOUT", "execution", "Workload secret delivery expired")
    except Exception:
        packet[:] = b"\x00" * len(packet)
        raise


def receive(control, stream, root=None):
    """Fixed bootstrap invoked once through Docker exec stdin after start intent."""
    require("secrets" in control)
    root = require_tmpfs(SECRET_ROOT if root is None else root)
    expected = marker(control)
    # This claim is not removed after failure. Duplicate delivery cannot overwrite
    # files or unblock another payload in this container lifetime.
    write_new(root, ".claim", b"claimed\n")
    raw, values = bytearray(), {}
    written = []
    try:
        bound = Bound(WAIT_SECONDS, min(epoch(control["execution_deadline"]), epoch(expected["expires_at"])))
        raw = read_packet(stream, bound)
        packet = strict_json(bytes(raw), MAX_PACKET)
        fields(packet, ("version", "job_id", "attempt_id", "attempt_nonce", "manifest_sha256", "authorization_id", "expires_at", "values"))
        require({key: packet[key] for key in packet if key != "values"} ==
                {key: expected[key] for key in expected if key != "names"})
        require(type(packet["values"]) is dict and sorted(packet["values"]) == expected["names"])
        for name, encoded in packet["values"].items():
            require(type(encoded) is str and len(encoded) <= 10924)
            value = bytearray(base64.b64decode(encoded, validate=True))
            values[name] = value
            require(0 < len(value) <= MAX_SLOT and base64.b64encode(value).decode() == encoded)
        require(sum(map(len, values.values())) <= MAX_TOTAL and not bound.expired())
        for name, value in values.items():
            require(not bound.expired())
            write_new(root, name, value)
            written.append(name)
        write_new(root, ".ready", json.dumps(expected, sort_keys=True).encode())
        return 0
    except (Failure, OSError, ValueError, TypeError, KeyError):
        for name in written:
            try:
                (root / name).unlink()
            except OSError:
                pass
        try:
            write_new(root, ".failed", b"unavailable\n")
        except OSError:
            pass
        raise Failure("COMMAND_FAILED", "execution", "Workload secret delivery unavailable") from None
    finally:
        raw[:] = b"\x00" * len(raw)
        for value in values.values():
            value[:] = b"\x00" * len(value)


def wait(control, check, root=None):
    root = require_tmpfs(SECRET_ROOT if root is None else root)
    expected = marker(control)
    bound = Bound(WAIT_SECONDS, min(epoch(control["execution_deadline"]), epoch(expected["expires_at"])))
    while not (root / ".ready").exists():
        check()
        if bound.expired() or (root / ".failed").exists():
            raise Failure("COMMAND_FAILED", "execution", "Required workload secrets unavailable")
        time.sleep(0.05)
    from .files import open_file
    with open_file(root, ".ready") as source:
        require(strict_json(source.read(4097), 4096) == expected)
    values = {}
    try:
        for name in expected["names"]:
            check()
            require(not bound.expired())
            with open_file(root, name) as source:
                info = os.fstat(source.fileno())
                require(stat.S_IMODE(info.st_mode) == 0o400 and info.st_uid == os.geteuid())
                values[name] = bytearray(source.read(MAX_SLOT + 1))
                require(0 < len(values[name]) <= MAX_SLOT)
        require(sum(map(len, values.values())) <= MAX_TOTAL)
        return values
    except Exception:
        for value in values.values():
            value[:] = b"\x00" * len(value)
        raise


def remove(control, root=None):
    """Remove slot files before output publication; keep the one-shot marker."""
    if control is None or "secrets" not in control:
        return
    root = Path(SECRET_ROOT if root is None else root)
    for slot in control["secrets"]["slots"]:
        try:
            (root / slot["name"]).unlink()
        except FileNotFoundError:
            pass
