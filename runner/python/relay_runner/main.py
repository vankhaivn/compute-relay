"""One finite remote attempt. Never imported or invoked by the Go admission path."""

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import shutil
import signal
import sys
import tarfile
import zlib
import time

from .contract import MAX_MANIFEST, RUNNER_VERSION, Failure, input_digest, load, require
from .files import artifacts, atomic_json, copy_verified, extract_bundle, open_file
from .process import Log, run

# Runs in an isolated interpreter, not the workload's module search path. Its stdout is
# ordinary bounded diagnostics; the structured observation has a separate bounded file.
PROBE = r'''
import importlib.metadata, json, platform, sys
result = {"python": platform.python_version(), "platform": platform.system(), "packages": []}
for dist in importlib.metadata.distributions():
    if len(result["packages"]) >= 512:
        result["packages_truncated"] = True
        break
    result["packages"].append({"name": str(dist.metadata.get("Name", "unknown"))[:128], "version": str(dist.version)[:128]})
result["packages"].sort(key=lambda p: (p["name"].lower(), p["version"]))
if sys.argv[2] == "gpu":
    import torch
    count = min(8, torch.cuda.device_count())
    result["gpu"] = {"count": count, "memory_bytes": [], "names": [], "cuda": str(torch.version.cuda), "framework": "torch/" + str(torch.__version__)}
    for device in range(count):
        p = torch.cuda.get_device_properties(device)
        result["gpu"]["memory_bytes"].append(int(p.total_memory))
        result["gpu"]["names"].append(str(p.name)[:256])
        x = torch.tensor([2.0, 3.0], device="cuda:" + str(device))
        assert (x * x).sum().item() == 13.0
        torch.cuda.synchronize(device)
        del x
with open(sys.argv[1], "x", encoding="utf-8") as f:
    json.dump(result, f, sort_keys=True)
'''


def utc():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def safe_environment(root, execution):
    env = {"PATH": "/usr/local/bin:/usr/bin:/bin", "LANG": "C.UTF-8",
           "HOME": str(root / "scratch/home"), "TMPDIR": str(root / "scratch"),
           "PYTHONUNBUFFERED": "1", "PYTHONNOUSERSITE": "1"}
    # Explicit provider environment allowlist, not a dump of the runner's environment.
    for key in ("CUDA_VISIBLE_DEVICES", "NVIDIA_VISIBLE_DEVICES", "LD_LIBRARY_PATH"):
        value = os.environ.get(key)
        if value is not None and len(value) <= 4096 and "\x00" not in value:
            env[key] = value
    env.update(execution.get("environment", {}))
    return env


def executable(command, python):
    if command[0] in ("python", "python3"):
        return [str(python)] + command[1:]
    # shell_setup can use explicit remote tools, but never implicit shell evaluation.
    if "/" in command[0] or "\\" in command[0]:
        raise Failure("COMMAND_FAILED", "execution", "Executable must be selected by name")
    tool = shutil.which(command[0], path="/usr/local/bin:/usr/bin:/bin")
    if tool is None:
        raise Failure("COMMAND_FAILED", "execution", "Declared executable unavailable")
    return [tool] + command[1:]


def pinned_requirements(code, name):
    # No editable/VCS/URL/index options, source builds, resolver-driven upgrades or
    # framework replacement. Complete transitive pins are the workload author's duty.
    with open_file(code, name) as src:
        raw = src.read(65537)
    require(len(raw) <= 65536)
    lines = []
    for line in raw.decode("ascii").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        match = re.fullmatch(r"([A-Za-z0-9][A-Za-z0-9._-]{0,127})==([A-Za-z0-9][A-Za-z0-9.!+_-]{0,127})", line)
        require(match is not None)
        name = match[1].lower().replace("_", "-").replace(".", "-")
        require(name not in {"torch", "torchvision", "torchaudio", "tensorflow", "tensorflow-gpu", "jax", "jaxlib", "triton", "pip", "setuptools"})
        require(not name.startswith(("nvidia-", "cuda-", "cupy")))
        lines.append(line)
    require(0 < len(lines) <= 256)
    return "\n".join(lines) + "\n"


def execute(manifest, staging, root, network_mode, cancelled=lambda: False):
    """Explicit remote/test entry; work root must be new. No retry or existing-root resume."""
    manifest, limits = load(json.dumps(manifest).encode())
    if sys.platform != "linux":
        raise Failure("UNSUPPORTED_CAPABILITY", "validation", "Runner execution requires Linux")
    staging, root = Path(staging).absolute(), Path(root).absolute()
    require(staging.is_dir() and not staging.is_symlink())
    require(staging != root and not staging.is_relative_to(root) and not root.is_relative_to(staging))
    # Parent is adapter-owned. Do not reuse an attempt directory or overwrite old results.
    root.mkdir(mode=0o700)
    for name in ("code", "inputs", "outputs", "scratch", "control", "scratch/home"):
        (root / name).mkdir(mode=0o700)
    started, start = utc(), time.monotonic()
    budgets = manifest["timeouts"]
    wall_end = start + budgets["remote_wall_seconds"]
    work_end = wall_end - budgets["finalization_grace_seconds"]
    setup_end = min(start + budgets["setup_seconds"], work_end)
    cleanup_grace = min(1.0, budgets["finalization_grace_seconds"] / 4)
    phase = "preparation"
    result = {
        "manifest_version": "1", "runner_version": RUNNER_VERSION,
        **{k: manifest[k] for k in ("job_id", "attempt_id", "attempt_nonce", "input_manifest_sha256")},
        "bundle_sha256": manifest["bundle"]["sha256"], "started_at": started,
        "finished_at": started, "phase": "failed", "exit_code": None, "timed_out": False,
        "resource_check": {"gpu_required": manifest["resources"]["accelerator"] == "gpu", "gpu_verified": False},
        "artifacts": [], "error": None,
    }
    provenance = {"runner_version": RUNNER_VERSION, "deadline_origin": "runner_start_monotonic",
                  "network_requested": manifest["network"]["remote_internet"],
                  "network_adapter_declared": network_mode,
                  "network_enforcement": "provider_required_not_runner_enforced",
                  "processes": [], "logs": {}, "phase_seconds": {}}
    logs = {name: Log(root / ("control/" + name + ".log"), limits[name + "_bytes"]) for name in ("stdout", "stderr")}

    def check(deadline=setup_end):
        if cancelled():
            raise Failure("COMMAND_FAILED", "execution", "Runner termination requested")
        if time.monotonic() >= deadline:
            raise Failure("REMOTE_TIMEOUT", "execution", "Runner time budget exceeded")

    def command(args, cwd, env, deadline, label):
        check(deadline)
        before = time.monotonic()
        outcome = run(args, cwd, env, deadline, cleanup_grace, logs["stdout"], logs["stderr"], cancelled)
        provenance["processes"].append({"phase": label, "seconds": time.monotonic() - before,
                                        "exit_code": outcome.exit_code, "timed_out": outcome.timed_out,
                                        "cancelled": outcome.cancelled, "leader_reaped": outcome.reaped})
        if label == "payload":
            result["exit_code"] = outcome.exit_code
        if outcome.cancelled:
            raise Failure("COMMAND_FAILED", "execution", "Runner termination requested")
        if outcome.timed_out:
            raise Failure("REMOTE_TIMEOUT", "execution", "Runner time budget exceeded")
        if not outcome.reaped:
            raise Failure("PROVIDER_EXECUTION_LOST", "execution", "Process termination not established")
        if outcome.exit_code != 0:
            code = "DEPENDENCY_SETUP_FAILED" if label == "setup" else "COMMAND_FAILED"
            raise Failure(code, "execution", "Declared command failed")

    def probe(python, env, deadline, suffix):
        path = root / ("control/environment-" + suffix + ".json")
        command([str(python), "-I", "-c", PROBE, str(path), manifest["resources"]["accelerator"]], root / "control", env, deadline, "resource_check")
        with open_file(root, "control/" + path.name) as source:
            raw = source.read(MAX_MANIFEST + 1)
        require(len(raw) <= MAX_MANIFEST)
        observation = json.loads(raw)
        provenance["environment"] = observation
        if result["resource_check"]["gpu_required"]:
            gpu = observation.get("gpu", {})
            count = manifest["resources"]["minimum_gpu_count"]
            memory = manifest["resources"].get("minimum_gpu_memory_bytes", 0)
            require(type(gpu.get("count")) is int and gpu["count"] >= count)
            require(len(gpu.get("memory_bytes", [])) >= count)
            require(sum(size >= memory for size in gpu["memory_bytes"]) >= count)
            result["resource_check"].update(gpu_verified=True, device_name=", ".join(gpu["names"])[:256],
                                             cuda_version=gpu["cuda"][:128], framework=gpu["framework"][:128])

    try:
        check()
        if network_mode != manifest["network"]["remote_internet"]:
            raise Failure("UNSUPPORTED_CAPABILITY", "validation", "Adapter network mode does not satisfy the job")
        if input_digest(manifest["inputs"]) != manifest["input_manifest_sha256"]:
            raise Failure("INPUT_DIGEST_MISMATCH", "input_preparation", "Frozen input manifest identity mismatch")
        bundle = root / "control/bundle.tar.gz"
        copy_verified(staging, manifest["bundle"], bundle, check)
        extract_bundle(bundle, root / "code", limits, check)
        bundle.unlink()
        for entry in manifest["inputs"]:
            copy_verified(staging, entry, root / "inputs" / entry["target"], check)
        env = safe_environment(root, manifest["execution"])
        env.update({"CC_JOB_ID": manifest["job_id"], "CC_ATTEMPT_ID": manifest["attempt_id"],
                    **{"CC_" + key + "_DIR": str(root / name) for key, name in (("CODE", "code"), ("INPUT", "inputs"), ("OUTPUT", "outputs"), ("SCRATCH", "scratch"))},
                    "CC_EXECUTION_MANIFEST_PATH": str(root / "control/execution-manifest.json")})
        atomic_json(root / "control/execution-manifest.json", manifest)
        cwd = root / "code" / manifest["execution"].get("working_directory", ".")
        require(cwd.is_dir())  # Only validated, link-free extraction has populated code.
        provenance["phase_seconds"][phase] = time.monotonic() - start
        phase = "resource_check"
        python = Path(sys.executable).absolute()
        probe(python, env, setup_end, "before")
        phase = "setup"
        deps = manifest["execution"].get("dependencies", {})
        if "python_requirements" in deps:
            if network_mode != "required":
                raise Failure("UNSUPPORTED_CAPABILITY", "validation", "Python requirements require explicit remote internet")
            req = root / "control/requirements.txt"
            req.write_text(pinned_requirements(root / "code", deps["python_requirements"]), encoding="ascii")
            venv = root / "scratch/venv"
            command([str(python), "-I", "-m", "venv", "--system-site-packages", str(venv)], root / "control", env, setup_end, "setup")
            python = venv / "bin/python"
            command([str(python), "-I", "-m", "pip", "--isolated", "--disable-pip-version-check", "--no-input", "install", "--no-deps", "--only-binary=:all:", "--retries", "0", "--timeout", "10", "--requirement", str(req)], root / "control", {**env, "PIP_CONFIG_FILE": os.devnull}, setup_end, "setup")
            env["PATH"] = str(venv / "bin") + ":" + env["PATH"]
            env["VIRTUAL_ENV"] = str(venv)
        for setup in deps.get("shell_setup", []):
            command(executable(setup, python), cwd, env, setup_end, "setup")
        if deps:
            phase = "resource_check"
            probe(python, env, setup_end, "after")
        check(setup_end)
        phase = "payload"
        command(executable(manifest["execution"]["command"], python), cwd, env, work_end, "payload")
        result["phase"] = "completed"
    except Failure as exc:
        result["error"] = exc.wire()
        result["timed_out"] = exc.code == "REMOTE_TIMEOUT"
        result["phase"] = ("cancelled" if cancelled() else "timed_out" if result["timed_out"] else
                           "setup_failed" if phase == "setup" else "resource_check_failed" if phase == "resource_check" else "failed")
        if phase == "resource_check" and not result["timed_out"] and not cancelled():
            result["resource_check"]["gpu_verified"] = False
            result["error"] = Failure("RESOURCE_REQUIREMENT_UNSATISFIED", "validation", "Required environment or GPU check failed").wire()
    except (OSError, ValueError, TypeError, KeyError, UnicodeError, tarfile.TarError, zlib.error):
        # Invalid archives, files or dependency metadata cannot produce false success.
        result["phase"] = "setup_failed" if phase == "setup" else "failed"
        result["error"] = Failure("DEPENDENCY_SETUP_FAILED" if phase == "setup" else "INPUT_FETCH_FAILED",
                                   "execution" if phase == "setup" else "input_preparation", "Runner preparation unavailable").wire()
    if result["error"] is not None:
        result["error"]["details"] = {"runner_phase": phase}
    try:
        result["artifacts"] = artifacts(root / "outputs", manifest["outputs"], limits,
                                          lambda: check(wall_end - cleanup_grace))
    except (Failure, OSError, ValueError) as exc:
        # Never overwrite an original setup/payload failure/exit code with collection noise.
        if result["error"] is None:
            result["phase"] = "failed"
            if isinstance(exc, Failure) and exc.code in ("ARTIFACT_MISSING", "REMOTE_TIMEOUT"):
                result["error"] = exc.wire()
                result["timed_out"] = exc.code == "REMOTE_TIMEOUT"
                if result["timed_out"]:
                    result["phase"] = "timed_out"
            else:
                result["error"] = Failure("ARTIFACT_COLLECTION_FAILED", "results", "Required outputs unsafe or over limits").wire()
    finally:
        for name, log in logs.items():
            provenance["logs"][name] = log.close()
    result["finished_at"] = utc()
    provenance["duration_seconds"] = time.monotonic() - start
    atomic_json(root / "control/environment.json", provenance)
    atomic_json(root / "control/execution-result.json", result)
    return result



def main(argv=None):
    parser = argparse.ArgumentParser(description="Finite remote runner; never a local compute provider")
    parser.add_argument("--manifest", required=True)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--validate-only", action="store_true")
    mode.add_argument("--execute", action="store_true")
    parser.add_argument("--staging-root")
    parser.add_argument("--work-root")
    parser.add_argument("--network-mode", choices=("disabled", "required"))
    args = parser.parse_args(argv)
    stopped = [False]
    previous = {}
    try:
        with open(args.manifest, "rb") as source:
            manifest, limits = load(source.read(MAX_MANIFEST + 1))
        if args.validate_only:
            print(json.dumps({"status": "valid", "runner_version": RUNNER_VERSION, "executed": False}))
            return 0
        require(args.staging_root and args.work_root and args.network_mode)
        for sig in (signal.SIGTERM, signal.SIGINT):
            previous[sig] = signal.signal(sig, lambda *_: stopped.__setitem__(0, True))
        result = execute(manifest, args.staging_root, args.work_root, args.network_mode, lambda: stopped[0])
        print(json.dumps({"phase": result["phase"], "exit_code": result["exit_code"], "runner_version": RUNNER_VERSION}))
        return 0 if result["phase"] == "completed" else 1
    except (Failure, OSError, ValueError, TypeError, KeyError, RecursionError):
        print("Runner rejected input or could not finalize; no result is implied", file=sys.stderr)
        return 64
    finally:
        for sig, handler in previous.items():
            signal.signal(sig, handler)
