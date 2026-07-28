$ErrorActionPreference = "Stop"

# Resolve paths from this script location so the launcher works even when it is
# called from a different terminal folder.
$platformRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$appFile = Join-Path $platformRoot "streamlit_app\app.py"
$venvRoot = Join-Path $platformRoot ".venv"
$venvPython = Join-Path $venvRoot "Scripts\python.exe"
$runtimeRoot = Join-Path $env:USERPROFILE ".cache\codex-runtimes\codex-primary-runtime\dependencies"
$python = Join-Path $runtimeRoot "python\python.exe"
$requirements = Join-Path $platformRoot "requirements.txt"

if (-not (Test-Path -LiteralPath $python)) {
    throw "Could not find bundled Python at: $python"
}

if (-not (Test-Path -LiteralPath $appFile)) {
    throw "Could not find Streamlit app at: $appFile"
}

# Keep the app self-contained. The first run creates a local virtual
# environment inside training_platform instead of requiring global Python setup.
if (-not (Test-Path -LiteralPath $venvPython)) {
    Write-Host "Creating local Python environment..."
    & $python -m venv $venvRoot
}

# Install dependencies only when Streamlit is missing. This keeps normal starts
# quick while still letting a fresh clone bootstrap itself.
& $venvPython -c "import openai, pandas, streamlit" *> $null
if ($LASTEXITCODE -ne 0) {
    Write-Host "Installing app dependencies..."
    & $venvPython -m pip install -r $requirements
}

Write-Host "Starting Training Platform..."
Write-Host "Open: http://localhost:8501/"
Write-Host ""

Set-Location -LiteralPath $platformRoot
& $venvPython -m streamlit run $appFile
