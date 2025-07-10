@echo off
REM ===================================================
REM 特定の契約情報をBloomberg APIから更新
REM ===================================================

echo ===================================================
echo Update Specific Contract Information from Bloomberg
echo ===================================================
echo.

REM 仮想環境のアクティベート（存在する場合）
if exist "..\..\venv\Scripts\activate.bat" (
    call ..\..\venv\Scripts\activate.bat
)

REM プロジェクトルートに移動
cd /d "%~dp0\..\.."

echo 2025年の契約を更新します: LPF25 LPG25 LPH25 LPJ25
echo.

python src/fetch_actual_contract_info.py LPF25 LPG25 LPH25 LPJ25

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