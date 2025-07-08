@echo off
echo ========================================
echo DEBUG: LME Copper 36 Months Extension
echo ========================================

echo.
echo Current directory: %CD%
echo.

echo Checking Python installation...
python --version

echo.
echo Checking if update_lme_36months.py exists...
if exist update_lme_36months.py (
    echo Found: update_lme_36months.py
) else (
    echo ERROR: update_lme_36months.py not found!
)

echo.
echo Checking project structure...
dir /b

echo.
echo Checking src directory...
if exist src (
    echo Found: src directory
    dir /b src
) else (
    echo ERROR: src directory not found!
)

echo.
echo Checking config directory...
if exist config (
    echo Found: config directory
    dir /b config
) else (
    echo ERROR: config directory not found!
)

echo.
echo Testing Python import...
python -c "import sys; print('Python path:'); [print(p) for p in sys.path[:5]]"

echo.
echo Testing basic imports...
python -c "import os, sys; print('Basic imports OK')"

echo.
echo Testing project imports...
python -c "import sys; sys.path.insert(0, '.'); from config.bloomberg_config import BLOOMBERG_TICKERS; print('Config import OK')"

echo.
echo Running update_lme_36months.py with full error output...
python -u update_lme_36months.py

echo.
echo Exit code: %errorlevel%
echo.
pause