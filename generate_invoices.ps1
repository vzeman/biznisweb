# PowerShell script to generate invoices for BizniWeb orders
# This script is intended to be run daily via Windows Task Scheduler

# Set script directory
$scriptPath = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $scriptPath

Write-Host "$(Get-Date): Starting invoice generation..." -ForegroundColor Green

# Function to activate virtual environment
function Activate-VirtualEnvironment {
    $venvPaths = @(
        ".\venv\Scripts\Activate.ps1",
        ".\venv\Scripts\activate.ps1",
        ".\venv\bin\Activate.ps1"
    )
    
    foreach ($path in $venvPaths) {
        if (Test-Path $path) {
            Write-Host "Activating virtual environment from: $path" -ForegroundColor Yellow
            & $path
            return $true
        }
    }
    
    Write-Host "Warning: Virtual environment not found" -ForegroundColor Yellow
    return $false
}

# Activate virtual environment
$venvActivated = Activate-VirtualEnvironment

# Find Python executable
$pythonCmd = $null
if (Get-Command python -ErrorAction SilentlyContinue) {
    $pythonCmd = "python"
} elseif (Get-Command python3 -ErrorAction SilentlyContinue) {
    $pythonCmd = "python3"
} else {
    Write-Host "Error: Python not found!" -ForegroundColor Red
    exit 1
}

Write-Host "Using Python command: $pythonCmd" -ForegroundColor Cyan

# Run invoice generation with all passed arguments
try {
    if ($args.Count -gt 0) {
        & $pythonCmd generate_invoices.py $args
    } else {
        & $pythonCmd generate_invoices.py
    }
    
    if ($LASTEXITCODE -eq 0) {
        Write-Host "$(Get-Date): Invoice generation completed successfully" -ForegroundColor Green
    } else {
        Write-Host "$(Get-Date): Invoice generation failed with error code $LASTEXITCODE" -ForegroundColor Red
        exit $LASTEXITCODE
    }
} catch {
    Write-Host "Error running invoice generation: $_" -ForegroundColor Red
    exit 1
}

# Note: PowerShell doesn't need explicit deactivation of virtual environment
exit 0