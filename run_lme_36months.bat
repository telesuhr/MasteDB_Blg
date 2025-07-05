@echo off
echo ========================================
echo LME Copper 36 Months Extension Process
echo ========================================

REM Check Python path
where python >nul 2>&1
if %errorlevel% neq 0 (
    echo ERROR: Python is not installed or not in PATH
    pause
    exit /b 1
)

REM Create logs directory
if not exist logs mkdir logs

echo.
echo [1/2] Add 13-36 months tenor types to M_TenorType master table
echo Please execute SQL script: sql\add_tenor_types_36months.sql
echo.
pause

echo.
echo [2/2] Fetching 36 months data from Bloomberg API
echo.
echo Running: python update_lme_36months.py
echo.

python update_lme_36months.py 2>&1

echo.
echo Python script exit code: %errorlevel%

if %errorlevel% neq 0 (
    echo.
    echo ERROR: Data fetch process failed (Exit code: %errorlevel%)
    echo Check the error message above for details
    pause
    exit /b 1
)

echo.
echo ========================================
echo LME Copper 36 months extension completed
echo ========================================
pause