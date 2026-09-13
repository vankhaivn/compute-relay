"""Command-line entry point for the credential-free Kaggle client probe."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import shutil
import sys
from typing import Sequence, TextIO

from .dependencies import collect_dependency_inventory
from .inventory import InventoryError, collect_inventory


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="compute-relay-kaggle-probe",
        description=(
            "Inspect the pinned official Kaggle client without loading credentials or "
            "calling provider APIs."
        ),
    )
    subcommands = parser.add_subparsers(dest="command", required=True)

    inventory = subcommands.add_parser(
        "inventory",
        help="record package versions and local help/version command surfaces",
    )
    inventory.add_argument(
        "--kaggle-executable",
        default="kaggle",
        help="explicit executable path or PATH name (default: kaggle)",
    )
    inventory.add_argument(
        "--output",
        default="-",
        help="JSON output path; '-' writes to stdout (default: -)",
    )
    inventory.add_argument(
        "--timeout-seconds",
        type=float,
        default=15.0,
        help="deadline for each local inventory command (default: 15)",
    )
    inventory.add_argument(
        "--max-output-bytes",
        type=int,
        default=256 * 1024,
        help="maximum bytes captured per stdout/stderr stream (default: 262144)",
    )
    dependencies = subcommands.add_parser(
        "dependencies",
        help="record exact installed runtime versions and declared license metadata",
    )
    dependencies.add_argument(
        "--output",
        default="-",
        help="JSON output path; '-' writes to stdout (default: -)",
    )
    return parser


def main(
    argv: Sequence[str] | None = None,
    *,
    stdout: TextIO = sys.stdout,
    stderr: TextIO = sys.stderr,
) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "dependencies":
        return _write_report(collect_dependency_inventory(), args.output, stdout, stderr)

    if args.command != "inventory":
        parser.error(f"unsupported command: {args.command}")

    if args.timeout_seconds <= 0:
        parser.error("--timeout-seconds must be positive")
    if args.max_output_bytes <= 0:
        parser.error("--max-output-bytes must be positive")

    executable = _resolve_requested_executable(args.kaggle_executable)
    try:
        report = collect_inventory(
            executable,
            timeout_seconds=args.timeout_seconds,
            max_output_bytes=args.max_output_bytes,
        )
    except InventoryError as exc:
        print(f"kaggle client inventory failed: {exc}", file=stderr)
        return 1

    return _write_report(report, args.output, stdout, stderr)


def _write_report(report: object, output_path: str, stdout: TextIO, stderr: TextIO) -> int:
    serialized = json.dumps(report, indent=2, sort_keys=True, ensure_ascii=False) + "\n"
    if output_path == "-":
        stdout.write(serialized)
        return 0

    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_name(f".{output.name}.tmp")
    temporary.write_text(serialized, encoding="utf-8", newline="\n")
    temporary.replace(output)
    print(f"wrote report to {output}", file=stderr)
    return 0


def _resolve_requested_executable(requested: str) -> str:
    candidate = Path(requested).expanduser()
    if candidate.parent != Path(".") or candidate.is_absolute():
        return str(candidate)
    resolved = shutil.which(requested)
    return resolved or requested
