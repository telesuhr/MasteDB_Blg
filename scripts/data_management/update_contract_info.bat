@echo off
REM ===================================================
REM 実契約情報をBloomberg APIから更新
REM ===================================================

echo ===================================================
echo Update Actual Contract Information from Bloomberg API
echo ===================================================
echo.

REM 仮想環境のアクティベート（存在する場合）
if exist "..\..\venv\Scripts\activate.bat" (
    call ..\..\venv\Scripts\activate.bat
)

REM プロジェクトルートに移動
cd /d "%~dp0\..\.."

if "%1"=="" (
    echo 全ての不完全な契約情報を更新します
    echo.
    python src/fetch_actual_contract_info.py
) else (
    echo 指定された契約を更新します: %*
    echo.
    python src/fetch_actual_contract_info.py %*
)

if %errorlevel% neq 0 (
    echo.
    echo ERROR: 契約情報の更新に失敗しました
    pause
    exit /b 1
)

echo.
echo ===================================================
echo 契約情報の更新が完了しました
echo ===================================================
pause