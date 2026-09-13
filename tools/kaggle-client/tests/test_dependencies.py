from __future__ import annotations

import email.message
import unittest

from compute_relay_kaggle_probe.dependencies import collect_dependency_inventory


class FakeDistribution:
    def __init__(self, name: str, version: str, headers: list[tuple[str, str]]) -> None:
        message = email.message.Message()
        message["Name"] = name
        for key, value in headers:
            message[key] = value
        self.metadata = message
        self.version = version


class DependencyInventoryTests(unittest.TestCase):
    def test_inventory_is_sorted_and_preserves_license_declarations(self) -> None:
        report = collect_dependency_inventory(
            [
                FakeDistribution(
                    "Zulu",
                    "2.0",
                    [("License-Expression", "Apache-2.0"), ("Project-URL", "Source, https://example.test/z")],
                ),
                FakeDistribution(
                    "alpha",
                    "1.0",
                    [("License", "  BSD   License  "), ("Classifier", "License :: OSI Approved")],
                ),
                FakeDistribution("compute-relay-kaggle-probe", "0.1.0", []),
            ]
        )

        self.assertEqual([item["name"] for item in report["packages"]], ["alpha", "Zulu"])
        self.assertEqual(report["packages"][0]["license"], "BSD License")
        self.assertEqual(report["packages"][1]["license_expression"], "Apache-2.0")
        self.assertEqual(
            report["packages"][1]["project_urls"],
            ["Source, https://example.test/z"],
        )


if __name__ == "__main__":
    unittest.main()
