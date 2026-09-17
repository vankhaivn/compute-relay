# Fixed remote-only bootstrap. The control plane only constructs/hashes this source.
import base64
import hashlib
import json
from pathlib import Path
import signal
import sys
import types


def staging_paths(payload):
    requested = Path("/kaggle/input") / payload["dataset_slug"]
    try:
        staging = requested.resolve(strict=True)
    except (OSError, RuntimeError):
        raise RuntimeError("Compute Relay staging mount is unavailable") from None
    if not staging.is_dir():
        raise RuntimeError("Compute Relay staging mount is not a directory")
    marker = staging / "relay-stage.bin"
    try:
        marker = marker.resolve(strict=True)
    except (OSError, RuntimeError):
        raise RuntimeError("Compute Relay staging marker is unavailable") from None
    try:
        marker.relative_to(staging)
    except ValueError:
        raise RuntimeError("Compute Relay staging marker escaped its dataset mount") from None
    if not marker.is_file():
        raise RuntimeError("Compute Relay staging marker is not a regular file")
    return staging, marker


def run_remote(payload):
    if sys.platform != "linux":
        raise RuntimeError("Compute Relay runner requires Linux")
    staging, marker = staging_paths(payload)
    with marker.open("rb") as stream:
        raw = stream.read(65537)
    if len(raw) > 65536 or hashlib.sha256(raw).hexdigest() != payload["marker_sha256"]:
        raise RuntimeError("Compute Relay staging identity changed")
    # The exact source bytes came from the build's checked runner asset lock.
    # Only these fixed modules are loaded; workload files cannot provide imports.
    package_name = "_compute_relay_remote_runner"
    if package_name in sys.modules:
        raise RuntimeError("Compute Relay runner was already installed in this process")
    package = types.ModuleType(package_name)
    package.__path__ = []
    package.__package__ = package_name
    sys.modules[package_name] = package
    for name in ("__init__", "contract", "files", "process", "main"):
        if name == "__init__":
            module = package
        else:
            module = types.ModuleType(package_name + "." + name)
            module.__package__ = package_name
            sys.modules[module.__name__] = module
        exec(compile(payload["modules"][name + ".py"], "compute-relay/" + name + ".py", "exec"), module.__dict__)
    stopped = [False]
    previous = {}
    try:
        for sig in (signal.SIGTERM, signal.SIGINT):
            previous[sig] = signal.signal(sig, lambda *_: stopped.__setitem__(0, True))
        manifest = payload["manifest"]
        # A new result directory is mandatory; a rerun cannot overwrite old output.
        result = sys.modules[package_name + ".main"].execute(
            manifest, staging, Path("/kaggle/working/relay-result"),
            manifest["network"]["remote_internet"], lambda: stopped[0])
        if result["phase"] != "completed":
            raise RuntimeError("Compute Relay payload did not complete; inspect its result manifest")
    finally:
        for sig, handler in previous.items():
            signal.signal(sig, handler)
