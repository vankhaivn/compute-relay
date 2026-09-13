$ErrorActionPreference = "Stop"

$RepoRoot = git rev-parse --show-toplevel
if ($LASTEXITCODE -ne 0) {
    throw "Run this script inside the Compute Relay Git repository."
}

Set-Location $RepoRoot
git config core.hooksPath .githooks
git config commit.template .gitmessage

Write-Host "Configured core.hooksPath=.githooks"
Write-Host "Configured commit.template=.gitmessage"
