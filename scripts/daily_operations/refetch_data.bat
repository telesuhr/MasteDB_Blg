@echo off
REM 修正後のデータ再取得

echo ===================================================
echo データ再取得開始（修正版プロセッサー使用）
echo ===================================================
echo.

REM Python環境の確認
python --version
if %errorlevel% neq 0 (
    echo Python not found. Please install Python.
    pause
    exit /b 1
)

echo.
echo 1. まず6/8単日のデータを取得します...
echo.

python src/fetch_historical_with_mapping.py 2025-06-08 2025-06-08

if %errorlevel% neq 0 (
    echo.
    echo エラーが発生しました。
    echo 依存関係の問題がある場合は、requirements.txtをインストールしてください:
    echo pip install -r requirements_no_blpapi.txt
    pause
    exit /b 1
)

echo.
echo ===================================================
echo データ取得が完了しました
echo ===================================================
pause