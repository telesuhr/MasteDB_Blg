@echo off
REM 日次更新実行スクリプト (Windows)

echo Starting Bloomberg data daily update...

REM 仮想環境の有効化（存在する場合）
if exist "venv\Scripts\activate.bat" (
    call venv\Scripts\activate.bat
)

REM Bloomberg Terminal接続チェック
python -c "import blpapi; session = blpapi.Session(); result = session.start(); session.stop(); exit(0 if result else 1)"
if %errorlevel% neq 0 (
    echo Error: Cannot connect to Bloomberg Terminal
    exit /b 1
)

REM メイン処理の実行
python run.py --mode daily

if %errorlevel% equ 0 (
    echo Daily update completed successfully!
) else (
    echo Daily update failed. Please check logs for details.
    exit /b 1
)