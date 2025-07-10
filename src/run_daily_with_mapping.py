"""
日次データ更新スクリプト（マッピング更新付き）
価格データ取得前に、その日のGeneric-Actual契約マッピングを更新する
"""
import sys
from datetime import datetime, timedelta
from bloomberg_api import BloombergDataFetcher
from database import DatabaseManager
from main import BloombergSQLIngestor
from historical_mapping_updater import HistoricalMappingUpdater
from fetch_actual_contract_info import ActualContractInfoUpdater
import logging

# ロギング設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    """日次更新処理のメイン関数"""
    # 今日の日付
    today = datetime.now().date()
    today_str = today.strftime('%Y-%m-%d')
    
    # Bloomberg APIとDB接続
    bloomberg_fetcher = BloombergDataFetcher()
    db_manager = DatabaseManager()
    
    if not bloomberg_fetcher.connect():
        logger.error("Bloomberg API接続に失敗しました")
        return 1
        
    try:
        # 1. まず実契約の情報を更新（Bloomberg APIから正確なデータを取得）
        logger.info("=== 実契約情報をBloomberg APIから更新 ===")
        contract_updater = ActualContractInfoUpdater(bloomberg_fetcher, db_manager)
        contract_updater.update_contract_info()
        
        # 2. 過去3日分のマッピングを更新（週末対応）
        logger.info("=== Generic-Actual契約マッピングを更新 ===")
        mapping_updater = HistoricalMappingUpdater(bloomberg_fetcher, db_manager)
        three_days_ago = (today - timedelta(days=3)).strftime('%Y-%m-%d')
        mapping_updater.update_historical_mappings(three_days_ago, today_str)
        
        # 3. その後、通常の日次データ取得
        logger.info("=== 日次データ取得を開始 ===")
        ingestor = BloombergSQLIngestor(bloomberg_fetcher, db_manager)
        ingestor.run_daily_update()
        
        logger.info("日次更新処理が完了しました")
        return 0
        
    except Exception as e:
        logger.error(f"エラーが発生しました: {e}")
        return 1
    finally:
        bloomberg_fetcher.disconnect()

if __name__ == "__main__":
    sys.exit(main())