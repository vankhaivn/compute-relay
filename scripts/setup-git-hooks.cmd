@echo off
setlocal

for /f "delims=" %%I in ('git rev-parse --show-toplevel 2^>nul') do set "REPO_ROOT=%%I"
if not defined REPO_ROOT (
  echo Run this script inside the Compute Relay Git repository. 1>&2
  exit /b 1
)

cd /d "%REPO_ROOT%"
git config core.hooksPath .githooks || exit /b 1
git config commit.template .gitmessage || exit /b 1

echo Configured core.hooksPath=.githooks
echo Configured commit.template=.gitmessage
