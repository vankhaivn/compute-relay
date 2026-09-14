"""Strict, provider-neutral resolved attempt contract; no I/O or code execution."""

import hashlib
import json
import re

VERSION = "compute-relay/runner/v1"
RUNNER_VERSION = "0.1.0"
BUFFER = 64 * 1024
MAX_MANIFEST = 1 << 20
ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}\Z")
DIGEST = re.compile(r"[a-f0-9]{64}\Z")
DEFAULT_LIMITS = {
    "bundle_bytes": 100 << 20,
    "expanded_bytes": 500 << 20,
    "input_bytes": 4 << 30,
    "artifact_bytes": 4 << 30,
    "artifact_files": 10000,
    "stdout_bytes": 20 << 20,
    "stderr_bytes": 20 << 20,
}


class Failure(Exception):
    """Fixed diagnostic, never a raw exception, command, environment, or input path."""

    def __init__(self, code="INVALID_JOB_SPEC", stage="validation", message="Invalid runner manifest"):
        super().__init__(message)
        self.code, self.stage, self.message = code, stage, message

    def wire(self):
        return {"code": self.code, "stage": self.stage, "message": self.message}


def require(ok):
    if not ok:
        raise Failure()


def fields(value, required, optional=()):
    require(type(value) is dict and set(required) <= value.keys())
    require(not (value.keys() - set(required) - set(optional)))


def integer(value, low, high):
    require(type(value) is int and low <= value <= high)


def text(value, maximum=4096):
    require(type(value) is str and 0 < len(value.encode("utf-8")) <= maximum)
    require("\x00" not in value)


def relative(value, dot=False):
    text(value, 240)
    if value == "." and dot:
        return
    require(all(32 <= ord(c) < 127 and c not in '\\:*?"<>|' for c in value))
    parts = value.split("/")
    require(len(parts) <= 32)
    for part in parts:
        require(part not in ("", ".", "..") and len(part) <= 100 and part.rstrip(" .") == part)
        base = part.split(".")[0].upper()
        require(base not in {"CON", "PRN", "AUX", "NUL", "CONIN$", "CONOUT$"})
        require(not re.fullmatch(r"(?:COM|LPT)[0-9]", base))
        require(part.lower() not in {".compute-relay", ".git", ".ssh", ".env"})


def disjoint(paths):
    """Reject file/parent and case collisions, including differently cased parents."""
    names, leaves = {}, set()
    for name in paths:
        relative(name)
        parts = name.split("/")
        for n in range(1, len(parts) + 1):
            prefix = "/".join(parts[:n])
            key = prefix.lower()
            require(key not in leaves and names.get(key, prefix) == prefix)
            if n == len(parts):
                require(key not in names)
                leaves.add(key)
            names[key] = prefix


def strict_json(raw, limit=MAX_MANIFEST):
    require(len(raw) <= limit)

    def unique(pairs):
        result = {}
        for key, value in pairs:
            require(key not in result)
            result[key] = value
        return result

    try:
        return json.loads(raw, object_pairs_hook=unique, parse_constant=lambda _: require(False))
    except (ValueError, TypeError, RecursionError, UnicodeError) as exc:
        raise Failure() from exc


def input_digest(inputs):
    """Hash UTF-8 compact sorted-key JSON, sorted by target; exclude staging paths."""
    frozen = [{k: x[k] for k in ("name", "target", "bytes", "sha256")} for x in inputs]
    frozen.sort(key=lambda x: x["target"])
    raw = json.dumps(frozen, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()
    return hashlib.sha256(raw).hexdigest()


def argv(value, maximum=64):
    require(type(value) is list and 1 <= len(value) <= maximum)
    for arg in value:
        text(arg)


def validate(m):
    fields(m, ("manifest_version", "job_id", "attempt_id", "attempt_nonce", "bundle", "inputs",
               "input_manifest_sha256", "execution", "resources", "network", "timeouts", "outputs"), ("limits",))
    require(m["manifest_version"] == VERSION)
    for key in ("job_id", "attempt_id"):
        require(type(m[key]) is str and ID.fullmatch(m[key]))
    nonce = m["attempt_nonce"]
    require(type(nonce) is str and re.fullmatch(r"[A-Za-z0-9._:-]{16,256}", nonce))
    require(type(m["input_manifest_sha256"]) is str and DIGEST.fullmatch(m["input_manifest_sha256"]))
    fields(m["bundle"], ("path", "bytes", "sha256"))
    require(type(m["inputs"]) is list and len(m["inputs"]) <= 64)
    limits = dict(DEFAULT_LIMITS)
    supplied = m.get("limits", {})
    fields(supplied, (), DEFAULT_LIMITS)
    for key, value in supplied.items():
        integer(value, 1, DEFAULT_LIMITS[key])
        limits[key] = value
    names = set()
    for entry in [m["bundle"]] + m["inputs"]:
        if entry is not m["bundle"]:
            fields(entry, ("name", "path", "target", "bytes", "sha256"))
            require(type(entry["name"]) is str and re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9._-]{0,63}", entry["name"]))
            require(entry["name"] not in names)
            names.add(entry["name"])
        relative(entry["path"])
        integer(entry["bytes"], 0, limits["bundle_bytes"] if entry is m["bundle"] else 2 << 30)
        require(type(entry["sha256"]) is str and DIGEST.fullmatch(entry["sha256"]))
    require(sum(x["bytes"] for x in m["inputs"]) <= limits["input_bytes"])
    disjoint([x["target"] for x in m["inputs"]])
    e = m["execution"]
    fields(e, ("kind", "command"), ("working_directory", "environment", "dependencies"))
    require(e["kind"] in ("python", "shell"))
    argv(e["command"])
    require(e["command"][0] in (("python", "python3") if e["kind"] == "python" else ("bash", "sh")))
    relative(e.get("working_directory", "."), dot=True)
    env = e.get("environment", {})
    require(type(env) is dict and len(env) <= 64)
    for key, value in env.items():
        require(re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,127}", key))
        upper = key.upper()
        require(not upper.startswith(("CC_", "PYTHON", "PIP_", "LD_", "CUDA_", "NVIDIA_", "AWS_", "KAGGLE_")))
        require(upper not in ("PATH", "HOME", "TMPDIR", "VIRTUAL_ENV", "BASH_ENV", "ENV", "SHELLOPTS", "BASHOPTS"))
        require(not any(word in upper for word in ("TOKEN", "SECRET", "PASSWORD", "CREDENTIAL", "API_KEY", "PROXY")))
        require(type(value) is str and len(value.encode("utf-8")) <= 4096 and "\x00" not in value)
    deps = e.get("dependencies", {})
    fields(deps, (), ("python_requirements", "shell_setup"))
    if "python_requirements" in deps:
        relative(deps["python_requirements"])
    if "shell_setup" in deps:
        require(e["kind"] == "shell" and type(deps["shell_setup"]) is list and len(deps["shell_setup"]) <= 32)
        for cmd in deps["shell_setup"]:
            argv(cmd, 32)
    fields(m["resources"], ("accelerator",), ("minimum_gpu_count", "minimum_gpu_memory_bytes"))
    resources = m["resources"]
    require(resources["accelerator"] in ("cpu", "gpu"))
    if resources["accelerator"] == "gpu":
        integer(resources.get("minimum_gpu_count"), 1, 8)
        if "minimum_gpu_memory_bytes" in resources:
            integer(resources["minimum_gpu_memory_bytes"], 1, 274877906944)
    else:
        require(len(resources) == 1)
    fields(m["network"], ("remote_internet",))
    require(m["network"]["remote_internet"] in ("disabled", "required"))
    fields(m["timeouts"], ("remote_wall_seconds", "setup_seconds", "finalization_grace_seconds"))
    t = m["timeouts"]
    for key, maximum in (("remote_wall_seconds", 86400), ("setup_seconds", 21600), ("finalization_grace_seconds", 3600)):
        integer(t[key], 1, maximum)
    require(t["setup_seconds"] + t["finalization_grace_seconds"] < t["remote_wall_seconds"])
    outputs = m["outputs"]
    require(type(outputs) is list and 1 <= len(outputs) <= 64)
    for out in outputs:
        fields(out, ("path", "required"), ("kind", "max_bytes"))
        relative(out["path"])
        require(type(out["required"]) is bool and out.get("kind", "file") in ("file", "directory"))
        if "max_bytes" in out:
            integer(out["max_bytes"], 1, limits["artifact_bytes"])
    disjoint([x["path"] for x in outputs])
    return limits


def load(raw):
    manifest = strict_json(raw)
    return manifest, validate(manifest)
