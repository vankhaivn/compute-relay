"""Validation for generated M1-01 inventory evidence."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from .inventory import COMMAND_INVENTORY, EXPECTED_PACKAGE_VERSIONS, SCHEMA_VERSION


DEPENDENCY_SCHEMA_VERSION = "compute-relay/python-dependency-inventory/v1alpha1"


class ValidationError(RuntimeError):
    """Raised when generated evidence violates the pinned offline contract."""


def validate_reports(
    inventory: Mapping[str, Any],
    dependencies: Mapping[str, Any],
    *,
    credential_canaries: Sequence[str] = (),
) -> None:
    """Validate local-only inventory and dependency reports."""

    if inventory.get("schema_version") != SCHEMA_VERSION:
        raise ValidationError("unexpected inventory schema")
    if inventory.get("credential_access") != "not_attempted":
        raise ValidationError("inventory must not access credentials")
    if inventory.get("provider_network_calls") != "not_attempted":
        raise ValidationError("inventory must not call provider APIs")

    python_version = str(inventory.get("python", {}).get("version", ""))
    if not python_version.startswith("3.11."):
        raise ValidationError(f"inventory used unexpected Python version: {python_version!r}")

    packages = inventory.get("packages")
    if not isinstance(packages, Mapping):
        raise ValidationError("inventory packages must be an object")
    for package, expected in EXPECTED_PACKAGE_VERSIONS.items():
        if packages.get(package) != expected:
            raise ValidationError(
                f"inventory package {package!r} = {packages.get(package)!r}, want {expected!r}"
            )

    commands = inventory.get("commands")
    if not isinstance(commands, list):
        raise ValidationError("inventory commands must be an array")
    expected_commands = [name for name, _arguments in COMMAND_INVENTORY]
    observed_commands = [command.get("name") for command in commands if isinstance(command, Mapping)]
    if observed_commands != expected_commands:
        raise ValidationError(
            f"inventory command allowlist = {observed_commands!r}, want {expected_commands!r}"
        )
    if any(command.get("exit_code") != 0 for command in commands if isinstance(command, Mapping)):
        raise ValidationError("every inventory command must exit successfully")

    if dependencies.get("schema_version") != DEPENDENCY_SCHEMA_VERSION:
        raise ValidationError("unexpected dependency inventory schema")
    dependency_entries = dependencies.get("packages")
    if not isinstance(dependency_entries, list):
        raise ValidationError("dependency packages must be an array")
    dependency_versions = {
        str(entry.get("name", "")).lower(): entry.get("version")
        for entry in dependency_entries
        if isinstance(entry, Mapping)
    }
    for package, expected in EXPECTED_PACKAGE_VERSIONS.items():
        if dependency_versions.get(package) != expected:
            raise ValidationError(
                f"dependency package {package!r} = {dependency_versions.get(package)!r}, "
                f"want {expected!r}"
            )

    serialized = f"{inventory!r}\n{dependencies!r}"
    for canary in credential_canaries:
        if canary and canary in serialized:
            raise ValidationError("credential canary leaked into generated evidence")
