from __future__ import annotations

from io import StringIO
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from compute_relay_kaggle_probe import cli


_REPORT = {
    "schema_version": "compute-relay/kaggle-client-inventory/v1alpha1",
    "credential_access": "not_attempted",
    "provider_network_calls": "not_attempted",
}


class CLITests(unittest.TestCase):
    @mock.patch.object(cli, "collect_inventory", return_value=_REPORT)
    def test_inventory_writes_stdout(self, collect: mock.Mock) -> None:
        stdout = StringIO()
        stderr = StringIO()
        code = cli.main(
            ["inventory", "--kaggle-executable", "fake-kaggle"],
            stdout=stdout,
            stderr=stderr,
        )
        self.assertEqual(code, 0)
        self.assertEqual(json.loads(stdout.getvalue()), _REPORT)
        self.assertEqual(stderr.getvalue(), "")
        collect.assert_called_once()

    @mock.patch.object(cli, "collect_inventory", return_value=_REPORT)
    def test_inventory_publishes_file_atomically(self, _collect: mock.Mock) -> None:
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory, "evidence", "inventory.json")
            stdout = StringIO()
            stderr = StringIO()
            code = cli.main(
                ["inventory", "--kaggle-executable", "fake-kaggle", "--output", str(output)],
                stdout=stdout,
                stderr=stderr,
            )
            self.assertEqual(code, 0)
            self.assertEqual(json.loads(output.read_text(encoding="utf-8")), _REPORT)
            self.assertFalse(output.with_name(f".{output.name}.tmp").exists())
            self.assertIn("wrote report", stderr.getvalue())


if __name__ == "__main__":
    unittest.main()
