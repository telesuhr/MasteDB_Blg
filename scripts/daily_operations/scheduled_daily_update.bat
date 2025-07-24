@echo off
REM ================================================
REM Bloomberg日次更新 - スケジュール実行用
REM 実行時刻: 平日18:00（各市場クローズ後）
REM ================================================

REM ログファイル名を日付付きで設定
set LOG_DATE=%date:~0,4%%date:~5,2%%date:~8,2%
set LOG_FILE=logs\scheduled_update_%LOG_DATE%.log

echo ================================================ >> %LOG_FILE%
echo Bloomberg Daily Update Started at %date% %time% >> %LOG_FILE%
echo ================================================ >> %LOG_FILE%

REM プロジェクトディレクトリに移動
cd /d C:\Users\09848\Git\MasteDB_Blg

REM 仮想環境の有効化
if exist "venv\Scripts\activate.bat" (
    call venv\Scripts\activate.bat
)

REM Bloomberg Terminal接続チェック
python -c "import blpapi; session = blpapi.Session(); result = session.start(); session.stop(); exit(0 if result else 1)" 2>>%LOG_FILE%
if %errorlevel% neq 0 (
    echo ERROR: Bloomberg Terminal not running >> %LOG_FILE%
    REM メール通知やTeams通知を追加可能
    exit /b 1
)

REM 市場タイミングを考慮した日次更新実行
echo Starting enhanced daily update... >> %LOG_FILE%
python scripts\daily_operations\run_daily.py >> %LOG_FILE% 2>&1

if %errorlevel% equ 0 (
    echo Daily update completed successfully >> %LOG_FILE%
    
    REM 成功時のサマリーレポート生成
    python -c "from src.utils import generate_daily_summary; generate_daily_summary()" >> %LOG_FILE% 2>&1
    
) else (
    echo ERROR: Daily update failed >> %LOG_FILE%
    REM エラー通知を追加可能
)

echo ================================================ >> %LOG_FILE%
echo Update finished at %time% >> %LOG_FILE%
echo ================================================ >> %LOG_FILE%