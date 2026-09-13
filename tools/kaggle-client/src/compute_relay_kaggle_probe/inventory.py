"""Credential-free inventory of the pinned official Kaggle client surface."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import datetime, timezone
import hashlib
from importlib import metadata
from pathlib import Path
import platform
import sys
import tempfile
from typing import Any

from .process import CommandResult, CommandSpec, ProcessError, run_command


SCHEMA_VERSION = "compute-relay/kaggle-client-inventory/v1alpha1"
EXPECTED_PACKAGE_VERSIONS: Mapping[str, str] = {
    "kaggle": "2.2.4",
    "kagglesdk": "0.1.35",
}
PACKAGE_INVENTORY = (
    "kaggle",
    "kagglesdk",
    "protobuf",
    "requests",
    "urllib3",
    "jupytext",
)
COMMAND_INVENTORY: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("version", ("--version",)),
    ("root_help", ("--help",)),
    ("kernels_help", ("kernels", "--help")),
    ("datasets_help", ("datasets", "--help")),
    ("quota_help", ("quota", "--help")),
)


class InventoryError(RuntimeError):
    """Raised when a no-credential inventory cannot establish its exact baseline."""


PackageVersion = Callable[[str], str]
CommandRunner = Callable[[CommandSpec], CommandResult]
Clock = Callable[[], datetime]


def collect_inventory(
    kaggle_executable: str,
    *,
    command_runner: CommandRunner = run_command,
    package_version: PackageVersion = metadata.version,
    clock: Clock = lambda: datetime.now(timezone.utc),
    timeout_seconds: float = 15.0,
    max_output_bytes: int = 256 * 1024,
) -> dict[str, Any]:
    """Collect versions and help surfaces without authenticating or calling Kaggle APIs."""

    package_versions = _package_versions(package_version)
    mismatches = {
        package: {"expected": expected, "observed": package_versions.get(package)}
        for package, expected in EXPECTED_PACKAGE_VERSIONS.items()
        if package_versions.get(package) != expected
    }
    if mismatches:
        raise InventoryError(f"pinned package version mismatch: {mismatches}")

    with tempfile.TemporaryDirectory(prefix="compute-relay-kaggle-inventory-") as temporary:
        temporary_path = Path(temporary)
        config_directory = temporary_path / "config"
        working_directory = temporary_path / "work"
        config_directory.mkdir(mode=0o700)
        working_directory.mkdir(mode=0o700)

        commands = []
        for name, arguments in COMMAND_INVENTORY:
            try:
                result = command_runner(
                    CommandSpec(
                        executable=kaggle_executable,
                        arguments=arguments,
                        working_directory=working_directory,
                        environment={"KAGGLE_CONFIG_DIR": str(config_directory)},
                        timeout_seconds=timeout_seconds,
                        max_output_bytes=max_output_bytes,
                    )
                )
            except ProcessError as exc:
                raise InventoryError(f"{name} inventory command failed safely: {exc}") from exc

            command = _command_record(name, arguments, result)
            commands.append(command)
            if result.exit_code != 0:
                raise InventoryError(
                    f"{name} inventory command exited {result.exit_code}; "
                    "no credential or remote call was attempted"
                )

    checked_at = clock().astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    return {
        "schema_version": SCHEMA_VERSION,
        "checked_at": checked_at,
        "credential_access": "not_attempted",
        "provider_network_calls": "not_attempted",
        "python": {
            "version": platform.python_version(),
            "implementation": platform.python_implementation(),
            "executable_name": Path(sys.executable).name,
        },
        "platform": {
            "system": platform.system(),
            "release": platform.release(),
            "machine": platform.machine(),
        },
        "packages": package_versions,
        "expected_packages": dict(EXPECTED_PACKAGE_VERSIONS),
        "commands": commands,
    }


def _package_versions(package_version: PackageVersion) -> dict[str, str | None]:
    versions: dict[str, str | None] = {}
    for package in PACKAGE_INVENTORY:
        try:
            versions[package] = package_version(package)
        except metadata.PackageNotFoundError:
            versions[package] = None
    return versions


def _command_record(
    name: str,
    arguments: tuple[str, ...],
    result: CommandResult,
) -> dict[str, Any]:
    stdout_text = result.stdout.decode("utf-8", errors="replace")
    stderr_text = result.stderr.decode("utf-8", errors="replace")
    return {
        "name": name,
        "arguments": list(arguments),
        "exit_code": result.exit_code,
        "duration_ms": round(result.duration_seconds * 1000),
        "stdout": stdout_text,
        "stdout_bytes": len(result.stdout),
        "stdout_sha256": hashlib.sha256(result.stdout).hexdigest(),
        "stderr": stderr_text,
        "stderr_bytes": len(result.stderr),
        "stderr_sha256": hashlib.sha256(result.stderr).hexdigest(),
    }
