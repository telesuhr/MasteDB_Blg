"""
修正されたプロセッサーでのテスト実行
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), 'src'))

from datetime import datetime
import logging

# ロギング設定
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_fixed_processor():
    """修正されたプロセッサーでデータを取得"""
    try:
        # 必要なモジュールをインポート
        from src.bloomberg_api import BloombergDataFetcher
        from src.database import DatabaseManager
        from src.main import BloombergSQLIngestor
        from src.historical_mapping_updater import HistoricalMappingUpdater
        from config.bloomberg_config import BLOOMBERG_TICKERS
        
        # 接続
        bloomberg_fetcher = BloombergDataFetcher()
        db_manager = DatabaseManager()
        
        if not bloomberg_fetcher.connect():
            logger.error("Bloomberg API接続に失敗")
            return
            
        db_manager.connect()
        
        # 1. マッピングデータ取得
        logger.info("=== マッピングデータ取得 ===")
        mapping_updater = HistoricalMappingUpdater(bloomberg_fetcher, db_manager)
        mapping_updater.update_historical_mappings('2025-06-08', '2025-06-08')
        
        # 2. LME銅価格データのみテスト
        logger.info("=== LME銅価格データ取得 ===")
        ingestor = BloombergSQLIngestor()
        ingestor.bloomberg = bloomberg_fetcher
        ingestor.db_manager = db_manager
        ingestor.db_manager.load_master_data()
        
        from src.data_processor import DataProcessor
        ingestor.processor = DataProcessor(ingestor.db_manager)
        
        # LME_COPPER_PRICESのみ処理
        if 'LME_COPPER_PRICES' in BLOOMBERG_TICKERS:
            ticker_info = BLOOMBERG_TICKERS['LME_COPPER_PRICES']
            
            # LME Cash価格のみテスト
            test_securities = ['LMCADY Index', 'CAD TT00 Comdty', 'LP1 Comdty']
            test_ticker_info = ticker_info.copy()
            test_ticker_info['securities'] = test_securities
            
            logger.info(f"テスト対象: {test_securities}")
            
            record_count = ingestor.process_category(
                'LME_COPPER_PRICES_TEST', 
                test_ticker_info, 
                '20250608', 
                '20250608'
            )
            
            logger.info(f"処理結果: {record_count}件")
            
            # 結果を確認
            with db_manager.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT DataType, COUNT(*) as cnt
                    FROM T_CommodityPrice
                    WHERE TradeDate = '2025-06-08'
                    GROUP BY DataType
                """)
                
                logger.info("データタイプ別件数:")
                for row in cursor.fetchall():
                    logger.info(f"  {row[0]}: {row[1]}件")
                    
    except Exception as e:
        logger.error(f"エラー: {e}")
        import traceback
        logger.error(traceback.format_exc())
    finally:
        try:
            bloomberg_fetcher.disconnect()
            db_manager.disconnect()
        except:
            pass

if __name__ == "__main__":
    test_fixed_processor()