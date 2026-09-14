"""Credential-free source, lock, contract-decoder and CPU fixture checks."""

import argparse
import ast
import hashlib
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
    return sorted(p for p in ROOT.rglob("*") if p.suffix in (".py", ".json") and p.name != "assets.lock.json")


def snapshot():
    files = []
    for path in candidates():
        raw = path.read_bytes()
        files.append({"path": "runner/" + path.relative_to(ROOT).as_posix(), "bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest()})
    return {"lock_version": 1, "files": files}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lock", action="store_true", help="update reviewed source/contract identities only")
    args = parser.parse_args()
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
    lock = ROOT / "assets.lock.json"
    expected = snapshot()
    if args.lock:
        lock.write_text(json.dumps(expected, indent=2) + "\n", encoding="utf-8")
        return 0
    if json.loads(lock.read_text()) != expected:
        raise RuntimeError("Runner assets changed; review and run python runner/check.py --lock")
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
