@echo off
REM Windows batch script to generate invoices for BizniWeb orders
REM This script is intended to be run daily via Windows Task Scheduler

REM Set script directory
set SCRIPT_DIR=%~dp0
cd /d "%SCRIPT_DIR%"

echo %date% %time%: Starting invoice generation...

REM Check if virtual environment exists
if exist "venv\Scripts\activate.bat" (
    echo Activating virtual environment...
    call venv\Scripts\activate.bat
) else if exist "venv\Scripts\activate" (
    echo Activating virtual environment...
    call venv\Scripts\activate
) else (
    echo Warning: Virtual environment not found, proceeding without activation
)

REM Check which Python command to use
where python >nul 2>nul
if %errorlevel% equ 0 (
    set PYTHON_CMD=python
) else (
    where python3 >nul 2>nul
    if %errorlevel% equ 0 (
        set PYTHON_CMD=python3
    ) else (
        echo Error: Python not found in PATH!
        exit /b 1
    )
)

echo Using Python command: %PYTHON_CMD%

REM Run invoice generation
%PYTHON_CMD% generate_invoices.py %*

REM Check exit status
if %errorlevel% equ 0 (
    echo %date% %time%: Invoice generation completed successfully
) else (
    echo %date% %time%: Invoice generation failed with error code %errorlevel%
    exit /b %errorlevel%
)

REM Deactivate virtual environment if it was activated
if defined VIRTUAL_ENV (
    call deactivate
)

exit /b 0