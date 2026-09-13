from __future__ import annotations

import json
import os
from pathlib import Path
import sys
import tempfile
import unittest

from compute_relay_kaggle_probe.process import (
    CommandSpec,
    ProcessOutputLimitExceeded,
    ProcessTimedOut,
    run_command,
    sanitized_environment,
)


class ProcessTests(unittest.TestCase):
    def run_python(
        self,
        source: str,
        *arguments: str,
        timeout_seconds: float = 5.0,
        max_output_bytes: int = 64 * 1024,
        environment: dict[str, str] | None = None,
    ):
        with tempfile.TemporaryDirectory() as directory:
            script = Path(directory, "child.py")
            script.write_text(source, encoding="utf-8")
            return run_command(
                CommandSpec(
                    executable=sys.executable,
                    arguments=(str(script), *arguments),
                    working_directory=Path(directory),
                    environment=environment,
                    timeout_seconds=timeout_seconds,
                    max_output_bytes=max_output_bytes,
                )
            )

    def test_arguments_are_not_shell_evaluated(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            sentinel = Path(directory, "should-not-exist")
            argument = f"; touch {sentinel}"
            result = self.run_python(
                "import json, sys; print(json.dumps(sys.argv[1:]))",
                argument,
            )
            self.assertEqual(json.loads(result.stdout), [argument])
            self.assertFalse(sentinel.exists())

    def test_sensitive_environment_is_not_inherited(self) -> None:
        old = {name: os.environ.get(name) for name in ("KAGGLE_API_TOKEN", "KAGGLE_KEY", "KAGGLE_USERNAME")}
        self.addCleanup(self._restore_environment, old)
        os.environ["KAGGLE_API_TOKEN"] = "secret-token-canary"
        os.environ["KAGGLE_KEY"] = "secret-key-canary"
        os.environ["KAGGLE_USERNAME"] = "secret-user-canary"

        result = self.run_python(
            "import json, os; print(json.dumps({k: os.environ.get(k) for k in "
            "['KAGGLE_API_TOKEN', 'KAGGLE_KEY', 'KAGGLE_USERNAME']}))"
        )
        self.assertEqual(
            json.loads(result.stdout),
            {"KAGGLE_API_TOKEN": None, "KAGGLE_KEY": None, "KAGGLE_USERNAME": None},
        )

    def test_sensitive_environment_cannot_be_injected(self) -> None:
        with self.assertRaisesRegex(ValueError, "credential environment variable"):
            sanitized_environment({"KAGGLE_API_TOKEN": "not-allowed"})

    def test_timeout_terminates_process(self) -> None:
        with self.assertRaises(ProcessTimedOut):
            self.run_python(
                "import time; time.sleep(30)",
                timeout_seconds=0.15,
            )

    def test_output_limit_terminates_process(self) -> None:
        with self.assertRaises(ProcessOutputLimitExceeded):
            self.run_python(
                "import sys; sys.stdout.write('x' * 1048576); sys.stdout.flush()",
                max_output_bytes=1024,
            )

    @staticmethod
    def _restore_environment(values: dict[str, str | None]) -> None:
        for name, value in values.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value


if __name__ == "__main__":
    unittest.main()
