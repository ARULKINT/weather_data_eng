@echo off
REM ============================================================
REM Weather Data Engineering Pipeline - Windows Batch Launcher
REM ============================================================

echo ============================================================
echo      WEATHER DATA ENGINEERING PIPELINE RUNNER (BATCH)
echo ============================================================

set SCRIPT_DIR=%~dp0
cd /d "%SCRIPT_DIR%"

if exist "venv\Scripts\python.exe" (
    set "PYTHON_CMD=venv\Scripts\python.exe"
) else (
    set "PYTHON_CMD=python"
)

if not exist ".env" (
    if exist ".env.example" (
        echo [!] .env missing, copying from .env.example...
        copy .env.example .env >nul
    )
)

if not exist "drivers\postgresql-42.7.13.jar" (
    echo [ERROR] PostgreSQL JDBC Driver missing in drivers\ folder!
    pause
    exit /b 1
)

echo [+] Executing Pipeline via %PYTHON_CMD%...
%PYTHON_CMD% -m scripts.main

if %ERRORLEVEL% equ 0 (
    echo [SUCCESS] Pipeline execution completed successfully.
) else (
    echo [ERROR] Pipeline failed with error code %ERRORLEVEL%.
)

pause
