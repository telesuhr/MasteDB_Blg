@echo off
REM ===================================================
REM 全カテゴリのデータを取得
REM ===================================================

echo ===================================================
echo Fetch All Data Categories from Bloomberg
echo ===================================================
echo.

REM 仮想環境のアクティベート（存在する場合）
if exist "..\..\venv\Scripts\activate.bat" (
    call ..\..\venv\Scripts\activate.bat
)

REM プロジェクトルートに移動
cd /d "%~dp0\..\.."

REM 日付の設定
set /p start_date=開始日を入力 (YYYY-MM-DD): 
set /p end_date=終了日を入力 (YYYY-MM-DD): 

echo.
echo 期間: %start_date% から %end_date%
echo.

REM Pythonスクリプトを作成して実行
echo import sys > temp_fetch_all.py
echo from datetime import datetime >> temp_fetch_all.py
echo from bloomberg_api import BloombergDataFetcher >> temp_fetch_all.py
echo from database import DatabaseManager >> temp_fetch_all.py
echo from main import BloombergSQLIngestor >> temp_fetch_all.py
echo from config.bloomberg_config import BLOOMBERG_TICKERS >> temp_fetch_all.py
echo import logging >> temp_fetch_all.py
echo. >> temp_fetch_all.py
echo logging.basicConfig(level=logging.INFO, format='%%(asctime)s - %%(name)s - %%(levelname)s - %%(message)s') >> temp_fetch_all.py
echo logger = logging.getLogger(__name__) >> temp_fetch_all.py
echo. >> temp_fetch_all.py
echo # Bloomberg APIとDB接続 >> temp_fetch_all.py
echo bloomberg_fetcher = BloombergDataFetcher() >> temp_fetch_all.py
echo db_manager = DatabaseManager() >> temp_fetch_all.py
echo. >> temp_fetch_all.py
echo if not bloomberg_fetcher.connect(): >> temp_fetch_all.py
echo     logger.error("Bloomberg API接続に失敗しました") >> temp_fetch_all.py
echo     sys.exit(1) >> temp_fetch_all.py
echo. >> temp_fetch_all.py
echo try: >> temp_fetch_all.py
echo     ingestor = BloombergSQLIngestor() >> temp_fetch_all.py
echo     ingestor.bloomberg = bloomberg_fetcher >> temp_fetch_all.py
echo     ingestor.db_manager = db_manager >> temp_fetch_all.py
echo     ingestor.db_manager.load_master_data() >> temp_fetch_all.py
echo. >> temp_fetch_all.py
echo     from data_processor import DataProcessor >> temp_fetch_all.py
echo     ingestor.processor = DataProcessor(ingestor.db_manager) >> temp_fetch_all.py
echo. >> temp_fetch_all.py
echo     # 全カテゴリを処理 >> temp_fetch_all.py
echo     start_date = "%start_date%".replace('-', '') >> temp_fetch_all.py
echo     end_date = "%end_date%".replace('-', '') >> temp_fetch_all.py
echo. >> temp_fetch_all.py
echo     for category_name, ticker_info in BLOOMBERG_TICKERS.items(): >> temp_fetch_all.py
echo         logger.info(f"処理中: {category_name}") >> temp_fetch_all.py
echo         try: >> temp_fetch_all.py
echo             record_count = ingestor.process_category(category_name, ticker_info, start_date, end_date) >> temp_fetch_all.py
echo             logger.info(f"{category_name}: {record_count}件のレコードを保存") >> temp_fetch_all.py
echo         except Exception as e: >> temp_fetch_all.py
echo             logger.error(f"{category_name}の処理でエラー: {e}") >> temp_fetch_all.py
echo. >> temp_fetch_all.py
echo finally: >> temp_fetch_all.py
echo     bloomberg_fetcher.disconnect() >> temp_fetch_all.py

python temp_fetch_all.py

del temp_fetch_all.py

if %errorlevel% neq 0 (
    echo.
    echo ERROR: データ取得に失敗しました
    pause
    exit /b 1
)

echo.
echo ===================================================
echo 全カテゴリのデータ取得が完了しました
echo ===================================================
pause