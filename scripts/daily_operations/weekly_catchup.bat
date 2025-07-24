@echo off
REM ================================================
REM Bloomberg週次データ補完バッチ
REM 毎週土曜日に実行し、過去1週間の欠損データを取得
REM ================================================

REM ログファイル設定
set LOG_DATE=%date:~0,4%%date:~5,2%%date:~8,2%
set LOG_FILE=logs\weekly_catchup_%LOG_DATE%.log

echo ================================================ >> %LOG_FILE%
echo Weekly Data Catch-up Started at %date% %time% >> %LOG_FILE%
echo ================================================ >> %LOG_FILE%

REM プロジェクトディレクトリに移動
cd /d C:\Users\09848\Git\MasteDB_Blg

REM 仮想環境の有効化
if exist "venv\Scripts\activate.bat" (
    call venv\Scripts\activate.bat
    echo Virtual environment activated >> %LOG_FILE%
)

REM Bloomberg Terminal接続チェック（オプション）
python -c "import blpapi; session = blpapi.Session(); result = session.start(); session.stop(); exit(0 if result else 1)" 2>>%LOG_FILE%
if %errorlevel% neq 0 (
    echo WARNING: Bloomberg Terminal not running. Will check for missing data only. >> %LOG_FILE%
)

REM 欠損データのチェックと自動補完
echo Checking for missing data in the last 7 days... >> %LOG_FILE%
python scripts\data_management\check_missing_dates.py --days 7 --auto-fill >> %LOG_FILE% 2>&1

if %errorlevel% equ 0 (
    echo Weekly catch-up completed successfully >> %LOG_FILE%
    
    REM 補完結果のサマリーを生成
    echo. >> %LOG_FILE%
    echo Data Summary: >> %LOG_FILE%
    python -c "import sys; sys.path.insert(0, '.'); from src.database import DatabaseManager; db = DatabaseManager(); db.connect(); import pandas as pd; with db.get_connection() as conn: result = pd.read_sql('SELECT COUNT(*) as Records, MAX(LastUpdated) as LastUpdate FROM T_CommodityPrice WHERE LastUpdated >= DATEADD(hour, -24, GETDATE())', conn); print(f'Records updated in last 24h: {result.Records[0]}'); print(f'Last update: {result.LastUpdate[0]}'); db.disconnect()" >> %LOG_FILE% 2>&1
    
) else (
    echo ERROR: Weekly catch-up failed >> %LOG_FILE%
)

echo ================================================ >> %LOG_FILE%
echo Catch-up finished at %time% >> %LOG_FILE%
echo ================================================ >> %LOG_FILE%