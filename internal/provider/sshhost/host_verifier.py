"""Fixed, stream-only verifier for a stopped Relay container archive.

The controller supplies an allowlist of names and sizes. This program receives a
Docker archive on stdin and emits only SHA-256 digests and sizes on stdout.
"""

import base64
import hashlib
import json
import posixpath
import signal
import sys
import tarfile


MAX_WIRE_BYTES = 5 << 30
MAX_ENTRIES = 256
ALLOWED_DIRS = {"relay", "relay/runner", "relay/runner/relay_runner", "relay/staging"}
MARKER = "relay/staged.json"
ZERO_BLOCKS = b"\0" * 1024


class InvalidArchive(Exception):
    pass


class BoundedInput:
    def __init__(self):
        self.total = 0
        self.tail = b""

    def read(self, count):
        if count < 0:
            raise InvalidArchive("unbounded read")
        chunk = sys.stdin.buffer.read(min(count, MAX_WIRE_BYTES - self.total + 1))
        self.total += len(chunk)
        if self.total > MAX_WIRE_BYTES:
            raise InvalidArchive("archive too large")
        self.tail = (self.tail + chunk)[-1024:]
        return chunk


def name_of(member):
    name = member.name.rstrip("/")
    if (
        not name
        or len(name) > 256
        or name.startswith("/")
        or "\\" in name
        or any(part in ("", ".", "..") for part in name.split("/"))
        or posixpath.normpath(name) != name
    ):
        raise InvalidArchive("unsafe path")
    return name


def expected_files():
    try:
        value = json.loads(base64.b64decode(sys.argv[2], validate=True))
        names = [entry["name"] for entry in value]
        sizes = [entry["bytes"] for entry in value]
        if (
            not 1 <= len(names) <= MAX_ENTRIES - len(ALLOWED_DIRS) - 1
            or names != sorted(set(names))
            or any(not isinstance(name, str) or not name.startswith("relay/") for name in names)
            or any(type(size) is not int or size < 0 or size > MAX_WIRE_BYTES for size in sizes)
            or sum(sizes) > MAX_WIRE_BYTES - (1 << 20)
        ):
            raise ValueError
        return dict(zip(names, sizes))
    except (ValueError, KeyError, TypeError, IndexError) as exc:
        raise InvalidArchive("invalid allowlist") from exc


def hash_file(stream, size):
    digest = hashlib.sha256()
    remaining = size
    while remaining:
        chunk = stream.read(min(remaining, 1 << 20))
        if not chunk:
            raise InvalidArchive("truncated file")
        digest.update(chunk)
        remaining -= len(chunk)
    return {"bytes": size, "sha256": digest.hexdigest()}


def scan():
    expected = expected_files()
    reader = BoundedInput()
    seen = set()
    files = {}
    marker = None
    with tarfile.open(fileobj=reader, mode="r|", bufsize=512) as archive:
        for member in archive:
            name = name_of(member)
            if name in seen or len(seen) >= MAX_ENTRIES:
                raise InvalidArchive("duplicate or excessive entry")
            seen.add(name)
            if member.isdir():
                if name not in ALLOWED_DIRS:
                    raise InvalidArchive("unexpected directory")
                continue
            if member.type not in (tarfile.REGTYPE, tarfile.AREGTYPE):
                raise InvalidArchive("non-regular entry")
            maximum = 4096 if name == MARKER else expected.get(name)
            if maximum is None or member.size < 0 or member.size > maximum:
                raise InvalidArchive("unexpected file or size")
            if name != MARKER and member.size != maximum:
                raise InvalidArchive("size mismatch")
            stream = archive.extractfile(member)
            if stream is None:
                raise InvalidArchive("unreadable file")
            value = hash_file(stream, member.size)
            if name == MARKER:
                marker = value
            else:
                files[name] = value
    if files.keys() != expected.keys():
        raise InvalidArchive("missing file")
    # With a single-block stream buffer, tarfile stops at the first zero end
    # block. Require the second full end block, then reject any trailing data.
    second_end_block = reader.read(512)
    if second_end_block != b"\0" * 512:
        raise InvalidArchive("missing second tar terminator")
    while True:
        chunk = reader.read(8192)
        if not chunk:
            break
        if any(chunk):
            raise InvalidArchive("trailing bytes")
    if reader.tail != ZERO_BLOCKS:
        raise InvalidArchive("missing tar terminator")
    return {"files": [files[name] for name in expected], "marker": marker}


def main():
    signal.alarm(3600)
    try:
        result = scan()
    except Exception:
        print("verification failed", file=sys.stderr)
        return 2
    print(json.dumps(result, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
