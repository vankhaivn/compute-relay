"""Validate generated reports in cross-platform CI."""

from __future__ import annotations

import json
from pathlib import Path
import platform
import sys

from compute_relay_kaggle_probe.validation import ValidationError, validate_reports


CANARIES = (
    "ci-secret-token-canary",
    "ci-secret-user-canary",
    "ci-secret-key-canary",
)


def load(path: str) -> dict:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def main(argv: list[str]) -> int:
    if len(argv) != 4:
        print(
            "usage: ci_validate.py INVENTORY DEPENDENCIES LINUX_CANONICAL",
            file=sys.stderr,
        )
        return 2

    inventory_path, dependencies_path, canonical_path = argv[1:]
    try:
        inventory = load(inventory_path)
        dependencies = load(dependencies_path)
        validate_reports(inventory, dependencies, credential_canaries=CANARIES)
        if platform.system() == "Linux" and dependencies != load(canonical_path):
            raise ValidationError("dependency-inventory.json is stale")
    except (OSError, json.JSONDecodeError, ValidationError) as exc:
        print(f"Kaggle client evidence validation failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
