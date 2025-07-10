@echo off
REM ===================================================
REM 期間を指定してデータを取得
REM ===================================================

echo ===================================================
echo Fetch Data with Date Range
echo ===================================================
echo.

REM 仮想環境のアクティベート（存在する場合）
if exist "..\..\venv\Scripts\activate.bat" (
    call ..\..\venv\Scripts\activate.bat
)

REM プロジェクトルートに移動
cd /d "%~dp0\..\.."

if "%1"=="" goto :interactive
if "%2"=="" goto :interactive

REM コマンドライン引数から日付を取得
set start_date=%1
set end_date=%2
goto :execute

:interactive
REM 対話的に日付を入力
set /p start_date=開始日を入力 (YYYY-MM-DD): 
set /p end_date=終了日を入力 (YYYY-MM-DD): 

:execute
echo.
echo 取得期間: %start_date% から %end_date%
echo.

REM 全カテゴリのデータを取得
python src/run_with_dates.py %start_date% %end_date%

if %errorlevel% neq 0 (
    echo.
    echo ERROR: データ取得に失敗しました
    pause
    exit /b 1
)

echo.
echo ===================================================
echo データ取得が完了しました
echo ===================================================
pause