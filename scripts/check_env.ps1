param(
  [string]$EnvPath = ".env",
  [string]$ExamplePath = ".env.example",
  [string]$RequiredPath = ".env.required"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Get-EnvKeys {
  param([string]$Path)

  if (-not (Test-Path -LiteralPath $Path)) {
    throw "File not found: $Path"
  }

  $keys = New-Object System.Collections.Generic.HashSet[string]

  foreach ($raw in Get-Content -LiteralPath $Path) {
    $line = $raw.Trim()
    if ($line.Length -eq 0) { continue }
    if ($line.StartsWith("#")) { continue }

    if ($line.StartsWith("export ")) {
      $line = $line.Substring(7).Trim()
    }

    $eqIndex = $line.IndexOf("=")
    if ($eqIndex -le 0) { continue }

    $key = $line.Substring(0, $eqIndex).Trim()
    if ($key -match '^[A-Za-z_][A-Za-z0-9_]*$') {
      [void]$keys.Add($key)
    }
  }

  return $keys
}

function Get-KeyList {
  param([string]$Path)

  if (-not (Test-Path -LiteralPath $Path)) {
    throw "File not found: $Path"
  }

  $keys = New-Object System.Collections.Generic.HashSet[string]

  foreach ($raw in Get-Content -LiteralPath $Path) {
    $line = $raw.Trim()
    if ($line.Length -eq 0) { continue }
    if ($line.StartsWith("#")) { continue }

    # allow lines like KEY or KEY=value
    if ($line.StartsWith("export ")) {
      $line = $line.Substring(7).Trim()
    }

    $candidate = $line
    $eqIndex = $line.IndexOf("=")
    if ($eqIndex -gt 0) {
      $candidate = $line.Substring(0, $eqIndex).Trim()
    }

    if ($candidate -match '^[A-Za-z_][A-Za-z0-9_]*$') {
      [void]$keys.Add($candidate)
    }
  }

  return $keys
}

try {
  if (-not (Test-Path -LiteralPath $ExamplePath)) {
    throw "File not found: $ExamplePath"
  }

  if (-not (Test-Path -LiteralPath $EnvPath)) {
    throw "File not found: $EnvPath"
  }

  $exampleKeys = Get-EnvKeys -Path $ExamplePath
  $actual = Get-EnvKeys -Path $EnvPath

  $requiredSource = $ExamplePath
  if (Test-Path -LiteralPath $RequiredPath) {
    $required = Get-KeyList -Path $RequiredPath
    $requiredSource = $RequiredPath
  } else {
    $required = $exampleKeys
  }

  $missing = @($required | Where-Object { -not $actual.Contains($_) } | Sort-Object)
  $extra = @($actual | Where-Object { -not $exampleKeys.Contains($_) } | Sort-Object)

  Write-Host "ENV check" -ForegroundColor Cyan
  Write-Host "- required source: $requiredSource"
  Write-Host "- required keys:   $($required.Count)"
  Write-Host "- actual keys:     $($actual.Count)"

  if ($missing.Count -gt 0) {
    Write-Host "`nMissing required keys in ${EnvPath}:" -ForegroundColor Red
    $missing | ForEach-Object { Write-Host "  - $_" }
  }

  if ($extra.Count -gt 0) {
    Write-Host "`nExtra keys in ${EnvPath} (not in ${ExamplePath}):" -ForegroundColor Yellow
    $extra | ForEach-Object { Write-Host "  - $_" }
  }

  if ($missing.Count -eq 0) {
    Write-Host "`nOK: no missing required keys." -ForegroundColor Green
    exit 0
  }

  exit 1
} catch {
  Write-Host "ERROR: $($_.Exception.Message)" -ForegroundColor Red
  exit 2
}
