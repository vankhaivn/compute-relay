"""Credential-free source, contract-decoder and CPU fixture checks."""

import argparse
import ast
import json
from pathlib import Path
import subprocess
import sys
import tabnanny
import tokenize

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "python"))
from relay_runner.contract import Failure, load


def candidates():
    return sorted(p for p in ROOT.rglob("*") if p.suffix in (".py", ".json"))


def main():
    argparse.ArgumentParser(description=__doc__).parse_args()
    for path in candidates():
        raw = path.read_bytes()
        text = raw.decode("utf-8")
        if b"\r" in raw or not raw.endswith(b"\n") or any(line.rstrip() != line for line in text.splitlines()):
            raise RuntimeError("Noncanonical source whitespace: " + str(path.relative_to(ROOT)))
        if path.suffix == ".py":
            ast.parse(text, filename=str(path), feature_version=(3, 11))
            with tokenize.open(path) as source:
                tabnanny.process_tokens(tokenize.generate_tokens(source.readline))
        else:
            json.loads(raw)
    for path in sorted((ROOT / "examples").glob("request.*.json")):
        valid = ".invalid." not in path.name
        try:
            load(path.read_bytes())
        except Failure:
            if valid:
                raise
        else:
            if not valid:
                raise RuntimeError("Negative fixture accepted")
    # Only repository-owned synthetic fixtures execute. No live/provider credentials.
    return subprocess.call([sys.executable, "-m", "unittest", "discover", "-s", str(ROOT / "tests"), "-v"])


if __name__ == "__main__":
    raise SystemExit(main())
