@echo off
setlocal

cd /d "%~dp0\.." || exit /b 1
go run ./cmd/devtool %*
exit /b %ERRORLEVEL%
