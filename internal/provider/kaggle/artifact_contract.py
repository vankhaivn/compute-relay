"""Pure bounded output selection. No SDK, filesystem access or workload execution."""
import hashlib
import json
import re
from urllib.parse import urlsplit

PREFIX = "relay-result/"
MANIFEST = "control/execution-result.json"
CONTROL_LIMITS = {MANIFEST: 1 << 20, "control/stdout.log": 20 << 20,
                  "control/stderr.log": 20 << 20, "control/environment.json": 1 << 20}
MAX_FILES = 10004
MAX_BYTES = 4 << 30
MAX_LIST_FILES = 20000
MAX_PAGES = 256
DIGEST = re.compile(r"[0-9a-f]{64}\Z")


def strict_json(raw):
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError("duplicate JSON field")
            result[key] = value
        return result
    def invalid(_):
        raise ValueError("invalid JSON number")
    result = json.loads(raw.decode("utf-8"), object_pairs_hook=pairs, parse_constant=invalid)
    if type(result) is not dict:
        raise ValueError("expected JSON object")
    return result


def safe_path(path):
    if type(path) is not str or not 1 <= len(path) <= 512:
        return False
    if any(ord(c) < 32 or ord(c) > 126 for c in path) or any(c in path for c in '\\:%<>"|?*'):
        return False
    for part in path.split("/"):
        stem = part.split(".", 1)[0].upper()
        if (not part or len(part) > 255 or part in (".", "..") or part[-1] in (".", " ")
                or stem in {"CON", "PRN", "AUX", "NUL"}
                or re.fullmatch(r"(?:COM|LPT)[0-9]", stem)):
            return False
    return True


def collision_free(paths):
    seen = set()
    for path in sorted(paths, key=str.lower):
        if not safe_path(path):
            raise ValueError("unsafe output path")
        key = path.lower()
        parts = key.split("/")
        if key in seen or any("/".join(parts[:i]) in seen for i in range(1, len(parts))):
            raise ValueError("conflicting output path")
        seen.add(key)


def signed_url(url):
    # A deliberately closed data-plane host. No account token is sent here.
    if type(url) is not str or not 1 <= len(url) <= 16384 or any(ord(c) < 33 or ord(c) > 126 for c in url) or "\\" in url:
        raise ValueError("invalid storage URL")
    parsed = urlsplit(url)
    if (parsed.scheme != "https" or parsed.netloc not in ("storage.googleapis.com", "storage.googleapis.com:443")
            or not parsed.path.startswith("/") or parsed.fragment):
        raise ValueError("unapproved storage destination")
    return url


def entry(path, size, digest):
    if not safe_path(path) or type(size) is not int or not 0 <= size <= MAX_BYTES or type(digest) is not str or not DIGEST.fullmatch(digest):
        raise ValueError("invalid artifact identity")
    return {"path": path, "bytes": size, "sha256": digest}


def declarations_valid(outputs):
    if type(outputs) is not list or not 1 <= len(outputs) <= 64:
        raise ValueError("invalid output declarations")
    for item in outputs:
        if (type(item) is not dict or set(item) != {"path", "kind", "required", "max_bytes"}
                or not safe_path(item["path"]) or item["kind"] not in ("file", "directory")
                or type(item["required"]) is not bool or type(item["max_bytes"]) is not int
                or not 0 <= item["max_bytes"] <= MAX_BYTES):
            raise ValueError("invalid output declaration")
    collision_free([item["path"] for item in outputs])


def select_manifest(raw, identity, outputs, listed, max_bytes=MAX_BYTES, max_files=MAX_FILES):
    """Candidate metadata only; M3 performs full schema/phase and byte verification."""
    if len(raw) > CONTROL_LIMITS[MANIFEST]:
        raise ValueError("result manifest too large")
    m = strict_json(raw)
    keys = {"job_id", "attempt_id", "attempt_nonce", "bundle_sha256", "input_manifest_sha256"}
    if set(identity) != keys or any(type(identity[k]) is not str or not identity[k] or m.get(k) != identity[k] for k in keys):
        raise ValueError("result belongs to another attempt")
    declarations_valid(outputs)
    files = m.get("artifacts")
    if type(files) is not list or len(files) > max_files - 1:
        raise ValueError("invalid result catalog")
    selected = [entry(MANIFEST, len(raw), hashlib.sha256(raw).hexdigest())]
    total = len(raw)
    counts, sizes = [0] * len(outputs), [0] * len(outputs)
    for f in files:
        if type(f) is not dict or set(f) - {"path", "bytes", "sha256", "media_type"} or not {"path", "bytes", "sha256"} <= set(f):
            raise ValueError("invalid manifest artifact")
        candidate = entry("outputs/" + f["path"], f["bytes"], f["sha256"])
        match = next((i for i, d in enumerate(outputs) if
                      d["kind"] == "file" and f["path"] == d["path"] or
                      d["kind"] == "directory" and f["path"].startswith(d["path"] + "/")), None)
        if match is None or PREFIX + candidate["path"] not in listed:
            raise ValueError("undeclared or missing result")
        counts[match] += 1
        sizes[match] += f["bytes"]
        if outputs[match]["max_bytes"] and sizes[match] > outputs[match]["max_bytes"]:
            raise ValueError("output exceeds frozen bound")
        total += f["bytes"]
        selected.append(candidate)
    collision_free([f["path"] for f in selected])
    if total > max_bytes or len(selected) > max_files or PREFIX + MANIFEST not in listed:
        raise ValueError("incomplete or excessive result")
    if m.get("phase") == "completed" and any(d["required"] and not counts[i] for i, d in enumerate(outputs)):
        raise ValueError("required output absent")
    return sorted(selected, key=lambda f: f["path"])
