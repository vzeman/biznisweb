param(
    [string]$CandidatesFile,
    [string]$ArticlesFile
)

$ErrorActionPreference = "Stop"
$root = Resolve-Path (Join-Path $PSScriptRoot "..\..\..")
$guard = Join-Path $root "content\VEVO_CONTENT\tools\vevo_duplicate_guard.py"
$publicContentGuard = Join-Path $root "content\VEVO_CONTENT\tools\vevo_public_content_guard.py"
$projectAudit = Join-Path $root "content\VEVO_CONTENT\tools\vevo_project_audit.py"
$depthGuard = Join-Path $root "content\VEVO_CONTENT\tools\vevo_article_depth_guard.py"
$htmlSafetyGuard = Join-Path $root "content\VEVO_CONTENT\tools\vevo_html_safety_guard.py"
$testsDir = Join-Path $root "content\VEVO_CONTENT\tests"

function Invoke-PythonChecked {
    param(
        [Parameter(Mandatory = $true)]
        [string[]]$PythonArgs
    )

    & python -X utf8 @PythonArgs
    if ($LASTEXITCODE -ne 0) {
        throw "Python check failed with exit code $LASTEXITCODE`: python -X utf8 $($PythonArgs -join ' ')"
    }
}

if (-not (Test-Path $guard)) {
    throw "VEVO duplicate guard not found: $guard"
}

if (-not (Test-Path $publicContentGuard)) {
    throw "VEVO public content guard not found: $publicContentGuard"
}

if (-not (Test-Path $projectAudit)) {
    throw "VEVO project audit not found: $projectAudit"
}

if (-not (Test-Path $depthGuard)) {
    throw "VEVO article depth guard not found: $depthGuard"
}

if (-not (Test-Path $htmlSafetyGuard)) {
    throw "VEVO HTML safety guard not found: $htmlSafetyGuard"
}

if (-not (Test-Path $testsDir)) {
    throw "VEVO guard tests not found: $testsDir"
}

Invoke-PythonChecked @("-m", "unittest", "discover", "-s", $testsDir, "-p", "test_*.py")

$publicContentReport = Join-Path ([System.IO.Path]::GetTempPath()) "vevo-public-content-audit.json"
Invoke-PythonChecked @($publicContentGuard, "--report", $publicContentReport)

$projectAuditReport = Join-Path ([System.IO.Path]::GetTempPath()) "vevo-project-audit.json"
Invoke-PythonChecked @($projectAudit, "--report", $projectAuditReport)

if ($CandidatesFile) {
    Invoke-PythonChecked @($guard, "--file", $CandidatesFile)
}

if ($ArticlesFile) {
    if (-not (Test-Path $ArticlesFile)) {
        throw "VEVO article JSON not found: $ArticlesFile"
    }
    $batchPublicContentReport = Join-Path ([System.IO.Path]::GetTempPath()) "vevo-batch-public-content-audit.json"
    $batchHtmlSafetyReport = Join-Path ([System.IO.Path]::GetTempPath()) "vevo-batch-html-safety-audit.json"
    Invoke-PythonChecked @($publicContentGuard, "--report", $batchPublicContentReport, $ArticlesFile)
    Invoke-PythonChecked @($depthGuard, $ArticlesFile)
    Invoke-PythonChecked @($htmlSafetyGuard, "--report", $batchHtmlSafetyReport, $ArticlesFile)
}

Write-Host "VEVO_CONTENT check OK."
