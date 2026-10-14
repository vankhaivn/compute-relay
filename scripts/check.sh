#!/usr/bin/env sh
set -eu

cd "$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"

printf '%s\n' '==> gofmt'
unformatted=$(find . \( -name .git -o -name .local -o -name .venv -o -name vendor \) -prune -o -type f -name '*.go' -print0 | xargs -0 gofmt -l)
if [ -n "$unformatted" ]; then
    printf 'Go files need gofmt:\n%s\n' "$unformatted" >&2
    exit 1
fi

printf '%s\n' '==> go mod tidy -diff'
go mod tidy -diff
printf '%s\n' '==> go vet ./...'
go vet ./...

python=$(uv python find --offline --no-python-downloads 3.11.16)
"$python" -c 'import platform; assert platform.python_version() == "3.11.16", platform.python_version()'
export PYTHONDONTWRITEBYTECODE=1
unset CR_RUNNER_RESULTS_DIR
if [ "$(uname -s)" = Linux ]; then
    runner_results=$(mktemp -d "${TMPDIR:-/tmp}/relay-runner-results.XXXXXX")
    trap 'rm -rf "$runner_results"' 0
    trap 'exit 1' HUP INT TERM
    CR_RUNNER_RESULTS_DIR=$runner_results
    export CR_RUNNER_RESULTS_DIR
fi
printf '%s\n' '==> runner checks (Python 3.11.16)'
"$python" runner/check.py
printf '%s\n' '==> finite RTMPS example checks (Python 3.11.16)'
"$python" examples/finite-rtmps/check.py

printf '%s\n' '==> go test ./... (includes fault matrix and generated runner contracts)'
go test ./... -count=1
