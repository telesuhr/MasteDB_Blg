@echo off
REM ================================================
REM 欠損データ補完スクリプト
REM 指定日数分の過去データを取得
REM 使用例: catch_up_missing_data.bat 7
REM ================================================

set DAYS_BACK=%1
if "%DAYS_BACK%"=="" set DAYS_BACK=7

echo Catching up missing data for last %DAYS_BACK% days...

REM 仮想環境の有効化
if exist "venv\Scripts\activate.bat" (
    call venv\Scripts\activate.bat
)

REM Pythonスクリプトで日付計算と実行
python -c "from datetime import datetime, timedelta; end_date = datetime.now().strftime('%%Y%%m%%d'); start_date = (datetime.now() - timedelta(days=%DAYS_BACK%)).strftime('%%Y%%m%%d'); import subprocess; subprocess.run(['python', 'src/main.py', '--mode', 'daily', '--start-date', start_date, '--end-date', end_date])"

if %errorlevel% equ 0 (
    echo Data catch-up completed successfully!
) else (
    echo Data catch-up failed. Please check logs.
    exit /b 1
)