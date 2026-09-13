from __future__ import annotations

from datetime import datetime, timezone
from importlib import metadata
import sys
import unittest

from compute_relay_kaggle_probe.inventory import (
    COMMAND_INVENTORY,
    EXPECTED_PACKAGE_VERSIONS,
    InventoryError,
    collect_inventory,
)
from compute_relay_kaggle_probe.process import CommandResult, CommandSpec


class InventoryTests(unittest.TestCase):
    def test_collect_inventory_is_credential_free_and_bounded(self) -> None:
        seen: list[CommandSpec] = []

        def runner(spec: CommandSpec) -> CommandResult:
            seen.append(spec)
            self.assertIn("KAGGLE_CONFIG_DIR", spec.environment or {})
            return CommandResult(
                argv=(sys.executable, *spec.arguments),
                exit_code=0,
                stdout=("ok " + " ".join(spec.arguments)).encode(),
                stderr=b"",
                duration_seconds=0.012,
            )

        versions = {
            **EXPECTED_PACKAGE_VERSIONS,
            "protobuf": "7.34.1",
            "requests": "2.33.1",
            "urllib3": "2.6.3",
            "jupytext": "1.19.1",
        }

        def package_version(name: str) -> str:
            try:
                return versions[name]
            except KeyError as exc:
                raise metadata.PackageNotFoundError(name) from exc

        report = collect_inventory(
            sys.executable,
            command_runner=runner,
            package_version=package_version,
            clock=lambda: datetime(2026, 9, 13, 12, 0, tzinfo=timezone.utc),
        )

        self.assertEqual(report["credential_access"], "not_attempted")
        self.assertEqual(report["provider_network_calls"], "not_attempted")
        self.assertEqual(report["checked_at"], "2026-09-13T12:00:00Z")
        self.assertEqual(len(seen), len(COMMAND_INVENTORY))
        self.assertEqual([item["name"] for item in report["commands"]], [name for name, _ in COMMAND_INVENTORY])
        self.assertNotIn("secret", str(report).lower())

    def test_package_version_mismatch_fails_before_commands(self) -> None:
        called = False

        def runner(_spec: CommandSpec) -> CommandResult:
            nonlocal called
            called = True
            raise AssertionError("runner must not be called after a pin mismatch")

        def package_version(name: str) -> str:
            if name == "kaggle":
                return "9.9.9"
            if name == "kagglesdk":
                return "0.1.35"
            raise metadata.PackageNotFoundError(name)

        with self.assertRaisesRegex(InventoryError, "version mismatch"):
            collect_inventory(
                sys.executable,
                command_runner=runner,
                package_version=package_version,
            )
        self.assertFalse(called)

    def test_nonzero_help_command_is_not_accepted(self) -> None:
        def runner(spec: CommandSpec) -> CommandResult:
            return CommandResult(
                argv=(sys.executable, *spec.arguments),
                exit_code=7 if spec.arguments == ("kernels", "--help") else 0,
                stdout=b"",
                stderr=b"failure",
                duration_seconds=0.001,
            )

        def package_version(name: str) -> str:
            if name in EXPECTED_PACKAGE_VERSIONS:
                return EXPECTED_PACKAGE_VERSIONS[name]
            raise metadata.PackageNotFoundError(name)

        with self.assertRaisesRegex(InventoryError, "kernels_help inventory command exited 7"):
            collect_inventory(
                sys.executable,
                command_runner=runner,
                package_version=package_version,
            )


if __name__ == "__main__":
    unittest.main()
