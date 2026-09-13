#!/usr/bin/env sh
set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
repo_root=$(CDPATH= cd -- "$script_dir/.." && pwd)
project_dir="$repo_root/tools/kaggle-client"

task=${1:-}
[ -n "$task" ] || {
  printf '%s\n' 'usage: scripts/kaggle-client.sh <sync|lock-check|test|inventory|dependencies|check> [args...]' >&2
  exit 2
}
shift

cd "$project_dir"

case "$task" in
  sync)
    exec uv sync --locked --no-dev "$@"
    ;;
  lock-check)
    exec uv lock --check "$@"
    ;;
  test)
    uv run --locked python -m compileall -q src tests ci_validate.py
    exec uv run --locked python -m unittest discover -s tests -v "$@"
    ;;
  inventory)
    exec uv run --locked compute-relay-kaggle-probe inventory "$@"
    ;;
  dependencies)
    exec uv run --locked compute-relay-kaggle-probe dependencies "$@"
    ;;
  check)
    uv lock --check
    uv run --locked python -m compileall -q src tests ci_validate.py
    uv run --locked python -m unittest discover -s tests -v
    tmp_dir=$(mktemp -d)
    trap 'rm -rf "$tmp_dir"' EXIT HUP INT TERM
    uv run --locked compute-relay-kaggle-probe inventory --output "$tmp_dir/inventory.json"
    uv run --locked compute-relay-kaggle-probe dependencies --output "$tmp_dir/dependencies.json"
    if [ "$(uname -s)" = "Linux" ]; then
      uv run --locked python -c 'from pathlib import Path; import sys; expected=Path("dependency-inventory.json").read_bytes(); actual=Path(sys.argv[1]).read_bytes(); raise SystemExit(0 if expected == actual else "dependency-inventory.json is stale")' "$tmp_dir/dependencies.json"
    fi
    ;;
  *)
    printf 'unknown Kaggle client task: %s\n' "$task" >&2
    exit 2
    ;;
esac
