"""Bounded file snapshots and strict M2-07 bundle extraction in a fresh remote root."""

import hashlib
import json
import os
from pathlib import Path
import stat
import tarfile
import zlib

from .contract import BUFFER, DIGEST, Failure, disjoint, fields, integer, relative, require, strict_json


def open_file(root, name):
    relative(name)
    fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        parts = name.split("/")
        for part in parts[:-1]:
            child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
            os.close(fd)
            fd = child
        leaf = os.open(parts[-1], os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=fd)
        info = os.fstat(leaf)
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
            os.close(leaf)
            raise Failure("INVALID_INPUT_PATH", "validation", "Unsafe file type or link")
        return os.fdopen(leaf, "rb")
    finally:
        os.close(fd)


def stable(info):
    return (info.st_dev, info.st_ino, info.st_mode, info.st_size, info.st_mtime_ns, info.st_ctime_ns)


def hash_file(source, limit, check, destination=None):
    before = os.fstat(source.fileno())
    if before.st_size > limit:
        raise Failure("INPUT_TOO_LARGE", "input_preparation", "File exceeds byte limit")
    digest, size = hashlib.sha256(), 0
    while True:
        check()
        data = source.read(min(BUFFER, limit - size + 1))
        if not data:
            break
        size += len(data)
        if size > limit:
            raise Failure("INPUT_TOO_LARGE", "input_preparation", "File exceeds byte limit")
        digest.update(data)
        if destination is not None:
            destination.write(data)
    if stable(before) != stable(os.fstat(source.fileno())) or size != before.st_size:
        raise Failure("INPUT_CHANGED", "input_preparation", "File changed during snapshot")
    return size, digest.hexdigest()


def copy_verified(staging, entry, destination, check):
    destination.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    with open_file(staging, entry["path"]) as src, destination.open("xb") as dst:
        size, digest = hash_file(src, entry["bytes"], check, dst)
        dst.flush()
        os.fsync(dst.fileno())
    if size != entry["bytes"] or digest != entry["sha256"]:
        raise Failure("INPUT_DIGEST_MISMATCH", "input_preparation", "Staged input identity mismatch")


class GzipStream:
    """One gzip member; bounded decompression and no hidden concatenated/trailing data."""

    def __init__(self, source, limit, check):
        self.source, self.left, self.check = source, limit, check
        self.z = zlib.decompressobj(31)
        self.pending = b""

    def read(self, count):
        self.check()
        result = bytearray()
        while len(result) < count and not self.z.eof:
            self.check()
            if not self.pending:
                self.pending = self.source.read(BUFFER)
                require(bool(self.pending))
            chunk = self.z.decompress(self.pending, min(count - len(result), BUFFER, self.left + 1))
            self.pending = self.z.unconsumed_tail
            self.left -= len(chunk)
            require(self.left >= 0)
            result.extend(chunk)
        return bytes(result)

    def exact(self, count):
        raw = self.read(count)
        require(len(raw) == count)
        return raw

    def finish(self):
        require(not self.read(1))
        require(self.z.eof and not self.z.unused_data and not self.source.read(1))


def header(stream):
    raw = stream.exact(512)
    if raw == bytes(512):
        require(stream.exact(512) == bytes(512))
        return None
    require(raw[156:157] == b"0" and raw[257:265] == b"ustar\x0000")
    item = tarfile.TarInfo.frombuf(raw, "ascii", "strict")
    require(item.isreg() and not item.linkname and not item.uname and not item.gname)
    require(item.uid == item.gid == 0 and item.mode in (0o644, 0o755) and item.size >= 0)
    return item


def padding(stream, size):
    count = (-size) % 512
    require(stream.exact(count) == bytes(count))


def extract_bundle(bundle, code, limits, check):
    # Never call extract/extractall: headers are checked BEFORE interpreting members.
    with bundle.open("rb") as src:
        stream = GzipStream(src, limits["expanded_bytes"] + (4 << 20) + 10001 * 1024 + 1024, check)
        item = header(stream)
        require(item is not None and item.name == ".compute-relay/bundle.json" and 0 < item.size <= 4 << 20)
        manifest = strict_json(stream.exact(item.size), limit=4 << 20)
        padding(stream, item.size)
        fields(manifest, ("bundle_version", "files"))
        require(manifest["bundle_version"] == "compute-relay/bundle/v1")
        entries = manifest["files"]
        require(type(entries) is list and 1 <= len(entries) <= 10000)
        for entry in entries:
            fields(entry, ("path", "bytes", "sha256", "executable"))
            relative(entry["path"])
            integer(entry["bytes"], 0, limits["expanded_bytes"])
            require(type(entry["sha256"]) is str and DIGEST.fullmatch(entry["sha256"]))
            require(type(entry["executable"]) is bool)
        disjoint([x["path"] for x in entries])
        require([x["path"] for x in entries] == sorted(x["path"] for x in entries))
        require(sum(x["bytes"] for x in entries) <= limits["expanded_bytes"])
        for entry in entries:
            check()
            item = header(stream)
            mode = 0o755 if entry["executable"] else 0o644
            require(item is not None and item.name == "code/" + entry["path"] and item.size == entry["bytes"] and item.mode == mode)
            path = code / entry["path"]
            path.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
            digest, left = hashlib.sha256(), item.size
            with path.open("xb") as dst:
                while left:
                    data = stream.exact(min(left, BUFFER))
                    dst.write(data)
                    digest.update(data)
                    left -= len(data)
            require(digest.hexdigest() == entry["sha256"])
            path.chmod(mode)
            padding(stream, item.size)
        require(header(stream) is None)
        stream.finish()
    return manifest


def atomic_json(path, value):
    raw = (json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n").encode()
    temporary = path.with_suffix(".tmp")
    with temporary.open("xb") as out:
        out.write(raw)
        out.flush()
        os.fsync(out.fileno())
    os.replace(temporary, path)
    fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def artifacts(root, outputs, limits, check):
    require(root.is_dir() and not root.is_symlink())
    result, total, visited = [], 0, 0
    for output in outputs:
        check()
        name = output["path"]
        path = root / name
        # Reject intermediate links before walking directories, including empty ones.
        cursor = root
        missing = False
        for part in name.split("/"):
            cursor = cursor / part
            try:
                info = cursor.lstat()
            except FileNotFoundError:
                missing = True
                break
            require(not stat.S_ISLNK(info.st_mode))
        if missing:
            if output["required"]:
                raise Failure("ARTIFACT_MISSING", "results", "Required output missing")
            continue
        is_directory = output.get("kind", "file") == "directory"
        require(stat.S_ISDIR(info.st_mode) == is_directory)
        names = [name]
        if is_directory:
            names = []
            pending = [path]
            while pending:
                check()
                directory = pending.pop()
                with os.scandir(directory) as children:
                    for child in children:
                        check()
                        visited += 1
                        require(visited <= limits["artifact_files"] * 2)
                        rel = Path(child.path).relative_to(root).as_posix()
                        relative(rel)
                        if child.is_symlink():
                            raise Failure("ARTIFACT_COLLECTION_FAILED", "results", "Unsafe output link")
                        if child.is_dir(follow_symlinks=False):
                            pending.append(Path(child.path))
                        elif child.is_file(follow_symlinks=False):
                            names.append(rel)
                        else:
                            raise Failure("ARTIFACT_COLLECTION_FAILED", "results", "Unsafe output type")
        own_bytes = 0
        for name in sorted(names):
            check()
            require(len(result) < limits["artifact_files"])
            with open_file(root, name) as src:
                size, digest = hash_file(src, min(limits["artifact_bytes"] - total, output.get("max_bytes", limits["artifact_bytes"]) - own_bytes), check)
            result.append({"path": name, "bytes": size, "sha256": digest})
            total += size
            own_bytes += size
    disjoint([x["path"] for x in result])
    return sorted(result, key=lambda x: x["path"])
