Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$repoRoot = (git rev-parse --show-toplevel).Trim()
Set-Location $repoRoot

./scripts/install-hooks.ps1

if (-not (Test-Path -LiteralPath ".env")) {
  Copy-Item -LiteralPath ".env.example" -Destination ".env"
  Write-Host "Created .env from .env.example"
}

$pythonCmd = $null
if (Get-Command py -ErrorAction SilentlyContinue) {
  $pythonCmd = "py"
} elseif (Get-Command python -ErrorAction SilentlyContinue) {
  $pythonCmd = "python"
} else {
  throw "Python not found"
}

if (-not (Test-Path -LiteralPath ".venv")) {
  if ($pythonCmd -eq "py") {
    & py -3 -m venv .venv
  } else {
    & python -m venv .venv
  }
}

$venvPython = Join-Path ".venv" "Scripts/python.exe"
if (-not (Test-Path -LiteralPath $venvPython)) {
  throw "Virtualenv python not found at $venvPython"
}

& $venvPython -m pip install --upgrade pip
& $venvPython -m pip install -r requirements.txt
./scripts/check_env.ps1 -EnvPath ".env" -ExamplePath ".env.example" -RequiredPath ".env.required"

Write-Host "Bootstrap complete. Activate with: .\.venv\Scripts\Activate.ps1"
