$ErrorActionPreference = "Stop"

$RepoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $RepoRoot

go run ./cmd/devtool @args
if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
}
