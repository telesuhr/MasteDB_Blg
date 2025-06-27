@echo off
REM 初回データロード実行スクリプト (Windows)

echo Starting Bloomberg data initial load...
echo This may take several hours to complete.

REM 仮想環境の有効化（存在する場合）
if exist "venv\Scripts\activate.bat" (
    call venv\Scripts\activate.bat
    echo Virtual environment activated
)

REM 依存パッケージのチェック
python -c "import blpapi; import pyodbc; import pandas; print('Dependencies check passed')"
if %errorlevel% neq 0 (
    echo Error: Required packages not installed. Please run: pip install -r requirements.txt
    exit /b 1
)

REM Bloomberg Terminal接続チェック
echo Checking Bloomberg Terminal connection...
python -c "import blpapi; session = blpapi.Session(); result = session.start(); session.stop(); exit(0 if result else 1)"
if %errorlevel% neq 0 (
    echo Error: Cannot connect to Bloomberg Terminal. Please ensure Bloomberg Terminal is running.
    exit /b 1
)

REM SQL Server接続チェック
echo Checking SQL Server connection...
python -c "import sys, os; sys.path.insert(0, '.'); import pyodbc; from config.database_config import get_connection_string; conn = pyodbc.connect(get_connection_string()); conn.close(); print('SQL Server connection successful')"
if %errorlevel% neq 0 (
    echo Error: Cannot connect to SQL Server. Please check connection settings.
    exit /b 1
)

REM メイン処理の実行
echo Starting initial data load...
python run.py --mode initial

if %errorlevel% equ 0 (
    echo Initial data load completed successfully!
) else (
    echo Initial data load failed. Please check logs for details.
    exit /b 1
)