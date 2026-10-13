"""Check the example's stdlib source and synthetic tests; no downloads or providers."""

import ast
import json
from pathlib import Path
import subprocess
import sys
import tabnanny
import tokenize

ROOT = Path(__file__).resolve().parent

for path in sorted(ROOT.rglob("*")):
    if path.suffix not in {".py", ".json"}:
        continue
    raw = path.read_bytes()
    text = raw.decode("utf-8")
    if b"\r" in raw or not raw.endswith(b"\n") or any(line.rstrip() != line for line in text.splitlines()):
        raise RuntimeError("Noncanonical example source: " + path.name)
    if path.suffix == ".py":
        ast.parse(text, filename=str(path), feature_version=(3, 11))
        with tokenize.open(path) as source:
            tabnanny.process_tokens(tokenize.generate_tokens(source.readline))
    else:
        json.loads(raw)

raise SystemExit(subprocess.call([sys.executable, "-m", "unittest", "discover", "-s", str(ROOT / "tests"), "-v"]))
