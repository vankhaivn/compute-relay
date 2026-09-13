from __future__ import annotations

import unittest

from compute_relay_kaggle_probe.inventory import COMMAND_INVENTORY, EXPECTED_PACKAGE_VERSIONS, SCHEMA_VERSION
from compute_relay_kaggle_probe.validation import DEPENDENCY_SCHEMA_VERSION, ValidationError, validate_reports


def valid_inventory() -> dict:
    return {
        "schema_version": SCHEMA_VERSION,
        "credential_access": "not_attempted",
        "provider_network_calls": "not_attempted",
        "python": {"version": "3.11.16"},
        "packages": dict(EXPECTED_PACKAGE_VERSIONS),
        "commands": [
            {"name": name, "exit_code": 0}
            for name, _arguments in COMMAND_INVENTORY
        ],
    }


def valid_dependencies() -> dict:
    return {
        "schema_version": DEPENDENCY_SCHEMA_VERSION,
        "packages": [
            {"name": name, "version": version}
            for name, version in EXPECTED_PACKAGE_VERSIONS.items()
        ],
    }


class ValidationTests(unittest.TestCase):
    def test_accepts_expected_reports(self) -> None:
        validate_reports(valid_inventory(), valid_dependencies(), credential_canaries=("secret-canary",))

    def test_rejects_provider_call_claim(self) -> None:
        inventory = valid_inventory()
        inventory["provider_network_calls"] = "attempted"
        with self.assertRaisesRegex(ValidationError, "must not call provider APIs"):
            validate_reports(inventory, valid_dependencies())

    def test_rejects_credential_canary(self) -> None:
        inventory = valid_inventory()
        inventory["commands"][0]["stdout"] = "secret-canary"
        with self.assertRaisesRegex(ValidationError, "credential canary"):
            validate_reports(
                inventory,
                valid_dependencies(),
                credential_canaries=("secret-canary",),
            )


if __name__ == "__main__":
    unittest.main()
