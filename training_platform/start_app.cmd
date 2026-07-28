@echo off
setlocal

REM Always run from this file's folder so the PowerShell launcher can find the app.
cd /d "%~dp0"
powershell -ExecutionPolicy Bypass -File "%~dp0start_app.ps1"
