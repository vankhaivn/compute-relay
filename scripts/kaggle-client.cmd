@echo off
setlocal

powershell.exe -NoLogo -NoProfile -ExecutionPolicy Bypass -File "%~dp0kaggle-client.ps1" %*
exit /b %ERRORLEVEL%
