Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$repoRoot = (git rev-parse --show-toplevel).Trim()
Set-Location $repoRoot

git config core.hooksPath .githooks

Write-Host "Hooks installed."
Write-Host ("core.hooksPath=" + (git config core.hooksPath))
Write-Host "If needed, run from Git Bash once: chmod +x .githooks/pre-commit scripts/check_env.sh"
