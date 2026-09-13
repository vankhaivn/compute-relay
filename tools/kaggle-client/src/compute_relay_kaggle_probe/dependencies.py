"""Deterministic dependency and declared-license inventory."""

from __future__ import annotations

from collections.abc import Iterable
import hashlib
from importlib import metadata
from typing import Any


SCHEMA_VERSION = "compute-relay/python-dependency-inventory/v1alpha1"
_PROJECT_NAME = "compute-relay-kaggle-probe"
_MAX_INLINE_LICENSE_CHARACTERS = 512


def collect_dependency_inventory(
    distributions: Iterable[metadata.Distribution] | None = None,
) -> dict[str, Any]:
    """Record installed runtime packages and their wheel metadata.

    The fields are declarations supplied by package publishers. They support dependency
    review but do not replace examination of each package's complete license files.
    """

    installed = metadata.distributions() if distributions is None else distributions
    packages: list[dict[str, Any]] = []
    for distribution in installed:
        name = distribution.metadata.get("Name")
        if not name or name.lower() == _PROJECT_NAME:
            continue
        classifiers = sorted(
            value
            for value in distribution.metadata.get_all("Classifier", [])
            if value.startswith("License ::")
        )
        project_urls = sorted(distribution.metadata.get_all("Project-URL", []))
        license_fields = _license_fields(distribution.metadata.get("License"))
        packages.append(
            {
                "name": name,
                "version": distribution.version,
                "license_expression": distribution.metadata.get("License-Expression"),
                **license_fields,
                "license_classifiers": classifiers,
                "project_urls": project_urls,
            }
        )

    packages.sort(key=lambda package: package["name"].lower())
    return {
        "schema_version": SCHEMA_VERSION,
        "metadata_scope": "installed runtime distributions",
        "metadata_notice": (
            "License fields are package-publisher declarations; review distributed license "
            "files before release."
        ),
        "packages": packages,
    }


def _license_fields(value: str | None) -> dict[str, Any]:
    if value is None:
        return {
            "license": None,
            "license_text_bytes": 0,
            "license_text_sha256": None,
            "license_truncated": False,
        }

    raw = value.encode("utf-8")
    normalized = " ".join(value.split())
    truncated = len(normalized) > _MAX_INLINE_LICENSE_CHARACTERS
    return {
        "license": normalized[:_MAX_INLINE_LICENSE_CHARACTERS] or None,
        "license_text_bytes": len(raw),
        "license_text_sha256": hashlib.sha256(raw).hexdigest(),
        "license_truncated": truncated,
    }
