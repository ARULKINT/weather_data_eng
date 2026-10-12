<#
.SYNOPSIS
    Turnkey execution script for the Weather Data Engineering Pipeline.
.DESCRIPTION
    Validates Python virtual environment, dependencies, environment variables,
    PostgreSQL JDBC driver, and triggers main.py.
#>

param (
    [switch]$SkipExtract = $false,
    [switch]$RunTests = $false
)

$ErrorActionPreference = "Stop"

function Write-Step {
    param ([string]$Message)
    Write-Host "`n[+] $Message" -ForegroundColor Cyan
}

function Write-Success {
    param ([string]$Message)
    Write-Host "[OK] $Message" -ForegroundColor Green
}

function Write-WarningMsg {
    param ([string]$Message)
    Write-Host "[WARN] $Message" -ForegroundColor Yellow
}

function Write-ErrorMsg {
    param ([string]$Message)
    Write-Host "[ERROR] $Message" -ForegroundColor Red
}

Write-Host "============================================================" -ForegroundColor Blue
Write-Host "     WEATHER DATA ENGINEERING PIPELINE RUNNER" -ForegroundColor Blue
Write-Host "============================================================" -ForegroundColor Blue

$RepoRoot = Split-Path -Parent $MyInvocation.MyCommand.Definition

# 1. Check Python Virtual Environment
Write-Step "Checking Python Virtual Environment..."
$PythonExe = Join-Path $RepoRoot "venv\Scripts\python.exe"
if (-not (Test-Path $PythonExe)) {
    Write-WarningMsg "Virtual environment not found at .\venv, attempting global python..."
    $PythonExe = "python"
} else {
    Write-Success "Found virtual environment: $PythonExe"
}

# 2. Check Environment Variables (.env)
Write-Step "Verifying .env configuration..."
$EnvFile = Join-Path $RepoRoot ".env"
if (-not (Test-Path $EnvFile)) {
    Write-WarningMsg ".env file not found. Copying from .env.example..."
    $EnvExample = Join-Path $RepoRoot ".env.example"
    if (Test-Path $EnvExample) {
        Copy-Item $EnvExample $EnvFile
        Write-WarningMsg ".env created from template. Please update with your actual API key and DB credentials!"
    } else {
        Write-ErrorMsg "Neither .env nor .env.example found."
    }
} else {
    Write-Success ".env file detected."
}

# 3. Check PostgreSQL JDBC Driver
Write-Step "Verifying PostgreSQL JDBC Driver..."
$JarFile = Join-Path $RepoRoot "drivers\postgresql-42.7.13.jar"
if (-not (Test-Path $JarFile)) {
    Write-ErrorMsg "PostgreSQL JDBC driver not found at $JarFile!"
    exit 1
} else {
    Write-Success "Found JDBC Driver: $JarFile"
}

# 4. Optional: Run Test Suite
if ($RunTests) {
    Write-Step "Running Unit Tests..."
    & $PythonExe -m unittest tests/test_validator.py tests/test_settings.py
    if ($LASTEXITCODE -ne 0) {
        Write-ErrorMsg "Unit tests failed. Aborting pipeline run."
        exit $LASTEXITCODE
    }
    Write-Success "All tests passed!"
}

# 5. Execute Pipeline
Write-Step "Launching Weather ETL and Data Warehouse Pipeline..."
$StartTime = Get-Date

& $PythonExe -m scripts.main

if ($LASTEXITCODE -eq 0) {
    $Duration = (Get-Date) - $StartTime
    Write-Success "Pipeline completed successfully!"
} else {
    Write-ErrorMsg "Pipeline encountered errors during execution."
    exit $LASTEXITCODE
}
