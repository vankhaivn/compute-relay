"""Bounded, no-shell subprocess execution for provider tooling."""

from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
import shutil
import signal
import subprocess
import threading
import time
from typing import Mapping


_DEFAULT_TIMEOUT_SECONDS = 15.0
_DEFAULT_MAX_OUTPUT_BYTES = 256 * 1024
_READ_CHUNK_BYTES = 16 * 1024
_POLL_SECONDS = 0.01

# Credentials are never inherited by an inventory command. Authentication probes are a
# separate, explicitly authorized task and will use a dedicated credential resolver.
_SENSITIVE_ENVIRONMENT = frozenset(
    {
        "KAGGLE_API_TOKEN",
        "KAGGLE_KEY",
        "KAGGLE_USERNAME",
    }
)

# Keep only variables needed for executable discovery and native process startup. Avoid
# forwarding arbitrary application/provider environment into the client process.
_INHERITED_ENVIRONMENT = frozenset(
    {
        "COMSPEC",
        "HOME",
        "LANG",
        "LC_ALL",
        "PATH",
        "PATHEXT",
        "SYSTEMDRIVE",
        "SYSTEMROOT",
        "TEMP",
        "TMP",
        "TMPDIR",
        "USERPROFILE",
        "WINDIR",
    }
)


class ProcessError(RuntimeError):
    """Base class for provider-tool process failures."""


class ProcessTimedOut(ProcessError):
    """Raised when a process exceeds its finite deadline."""


class ProcessOutputLimitExceeded(ProcessError):
    """Raised when stdout or stderr exceeds its configured bound."""


class ExecutableNotFound(ProcessError):
    """Raised when a configured executable cannot be resolved."""


@dataclass(frozen=True)
class CommandSpec:
    """One allowlisted provider-tool command invocation."""

    executable: str
    arguments: tuple[str, ...]
    working_directory: Path
    environment: Mapping[str, str] | None = None
    timeout_seconds: float = _DEFAULT_TIMEOUT_SECONDS
    max_output_bytes: int = _DEFAULT_MAX_OUTPUT_BYTES

    def validate(self) -> None:
        if not self.executable:
            raise ValueError("executable must not be empty")
        if self.timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        if self.max_output_bytes <= 0:
            raise ValueError("max_output_bytes must be positive")
        if not self.working_directory.is_dir():
            raise ValueError("working_directory must be an existing directory")
        if any("\x00" in argument for argument in self.arguments):
            raise ValueError("arguments must not contain NUL bytes")


@dataclass(frozen=True)
class CommandResult:
    """Bounded output and timing from one completed command."""

    argv: tuple[str, ...]
    exit_code: int
    stdout: bytes
    stderr: bytes
    duration_seconds: float


class _BoundedBytes:
    def __init__(self, limit: int) -> None:
        self._limit = limit
        self._data = bytearray()
        self._lock = threading.Lock()
        self.exceeded = threading.Event()

    def append(self, chunk: bytes) -> None:
        with self._lock:
            remaining = self._limit - len(self._data)
            if remaining > 0:
                self._data.extend(chunk[:remaining])
            if len(chunk) > remaining:
                self.exceeded.set()

    def value(self) -> bytes:
        with self._lock:
            return bytes(self._data)


def sanitized_environment(overrides: Mapping[str, str] | None = None) -> dict[str, str]:
    """Build a minimal environment and reject credential injection."""

    environment = {
        key: value
        for key, value in os.environ.items()
        if key.upper() in _INHERITED_ENVIRONMENT and key.upper() not in _SENSITIVE_ENVIRONMENT
    }
    environment.update(
        {
            "NO_COLOR": "1",
            "PAGER": "cat",
            "PYTHONIOENCODING": "utf-8",
            "PYTHONNOUSERSITE": "1",
            "PYTHONUTF8": "1",
        }
    )

    if overrides:
        for key, value in overrides.items():
            normalized = key.upper()
            if normalized in _SENSITIVE_ENVIRONMENT:
                raise ValueError(f"credential environment variable {key!r} is not allowed")
            if "\x00" in key or "\x00" in value:
                raise ValueError("environment names and values must not contain NUL bytes")
            environment[key] = value
    return environment


def resolve_executable(executable: str) -> str:
    """Resolve an executable without invoking a shell."""

    candidate = Path(executable).expanduser()
    if candidate.parent != Path(".") or candidate.is_absolute():
        resolved = candidate.resolve(strict=False)
        if not resolved.is_file():
            raise ExecutableNotFound(f"executable does not exist: {resolved}")
        return str(resolved)

    resolved_name = shutil.which(executable)
    if resolved_name is None:
        raise ExecutableNotFound(f"executable not found on PATH: {executable}")
    return str(Path(resolved_name).resolve())


def run_command(spec: CommandSpec) -> CommandResult:
    """Run one command with bounded output, deadline, and process-tree cleanup."""

    spec.validate()
    executable = resolve_executable(spec.executable)
    argv = (executable, *spec.arguments)
    stdout_buffer = _BoundedBytes(spec.max_output_bytes)
    stderr_buffer = _BoundedBytes(spec.max_output_bytes)

    popen_kwargs: dict[str, object] = {
        "args": argv,
        "cwd": spec.working_directory,
        "env": sanitized_environment(spec.environment),
        "stdin": subprocess.DEVNULL,
        "stdout": subprocess.PIPE,
        "stderr": subprocess.PIPE,
        "shell": False,
        "close_fds": True,
    }
    if os.name == "nt":
        popen_kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
    else:
        popen_kwargs["start_new_session"] = True

    started = time.monotonic()
    process = subprocess.Popen(**popen_kwargs)  # type: ignore[arg-type]
    assert process.stdout is not None
    assert process.stderr is not None

    stdout_thread = threading.Thread(
        target=_read_stream,
        args=(process.stdout, stdout_buffer),
        name="kaggle-probe-stdout",
        daemon=True,
    )
    stderr_thread = threading.Thread(
        target=_read_stream,
        args=(process.stderr, stderr_buffer),
        name="kaggle-probe-stderr",
        daemon=True,
    )
    stdout_thread.start()
    stderr_thread.start()

    deadline = started + spec.timeout_seconds
    failure: ProcessError | None = None
    try:
        while process.poll() is None:
            if stdout_buffer.exceeded.is_set() or stderr_buffer.exceeded.is_set():
                failure = ProcessOutputLimitExceeded(
                    f"command output exceeded {spec.max_output_bytes} bytes per stream"
                )
                _kill_process_tree(process)
                break
            if time.monotonic() >= deadline:
                failure = ProcessTimedOut(
                    f"command exceeded {spec.timeout_seconds:.3f} second deadline"
                )
                _kill_process_tree(process)
                break
            time.sleep(_POLL_SECONDS)
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        _kill_process_tree(process)
        process.wait(timeout=5)
        if failure is None:
            failure = ProcessTimedOut("process did not terminate within the cleanup deadline")
    finally:
        stdout_thread.join(timeout=5)
        stderr_thread.join(timeout=5)
        process.stdout.close()
        process.stderr.close()

    duration = time.monotonic() - started
    if failure is not None:
        raise failure

    return CommandResult(
        argv=argv,
        exit_code=process.returncode,
        stdout=stdout_buffer.value(),
        stderr=stderr_buffer.value(),
        duration_seconds=duration,
    )


def _read_stream(stream: object, destination: _BoundedBytes) -> None:
    read = getattr(stream, "read")
    try:
        while True:
            chunk = read(_READ_CHUNK_BYTES)
            if not chunk:
                return
            destination.append(chunk)
            if destination.exceeded.is_set():
                return
    except (OSError, ValueError):
        # A concurrent process-tree kill can close the pipe. The main thread reports the
        # timeout/output-limit failure that caused it.
        return


def _kill_process_tree(process: subprocess.Popen[bytes]) -> None:
    if process.poll() is not None:
        return

    if os.name == "nt":
        try:
            subprocess.run(
                ["taskkill", "/PID", str(process.pid), "/T", "/F"],
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=5,
                check=False,
                shell=False,
            )
        except (OSError, subprocess.TimeoutExpired):
            process.kill()
        return

    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        return
    except OSError:
        process.kill()
