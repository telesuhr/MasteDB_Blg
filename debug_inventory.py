#!/usr/bin/env python3
"""
在庫データ更新のデバッグ版（詳細ログ出力）
"""
import sys
import os
import logging
from datetime import datetime, timedelta

# プロジェクトルートとsrcディレクトリをPythonパスに追加
project_root = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.join(project_root, 'src')
sys.path.insert(0, project_root)
sys.path.insert(0, src_dir)

# ログレベルをDEBUGに設定
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s | %(levelname)s | %(name)s | %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('debug_inventory.log')
    ]
)

from src.main import BloombergSQLIngestor
from config.bloomberg_config import INITIAL_LOAD_PERIODS, BLOOMBERG_TICKERS
from config.logging_config import logger

def debug_inventory_update():
    """LME在庫データのみをデバッグモードで更新"""
    ingestor = BloombergSQLIngestor()
    
    try:
        # 初期化
        ingestor.initialize()
        
        # 短期間のデータのみ取得（デバッグ用に1週間分）
        end_date = datetime.now().strftime('%Y%m%d')
        start_date = (datetime.now() - timedelta(days=7)).strftime('%Y%m%d')
        
        logger.info(f"Debugging inventory data from {start_date} to {end_date}")
        
        # LME在庫のみ処理
        ticker_info = BLOOMBERG_TICKERS['LME_INVENTORY']
        
        # MEST地域を除外
        for data_type, tickers in ticker_info['securities'].items():
            ticker_info['securities'][data_type] = [
                t for t in tickers if '%MEST' not in t
            ]
        if '%MEST Index' in ticker_info['region_mapping']:
            del ticker_info['region_mapping']['%MEST Index']
            
        logger.debug("Filtered ticker_info: %s", ticker_info)
        
        result = ingestor.process_category('LME_INVENTORY', ticker_info, start_date, end_date)
        logger.info(f"Updated {result} LME_INVENTORY records")
        
        # 成功したらクリーンアップ
        ingestor.cleanup()
        
        print("\n=== デバッグ完了 ===")
        print("詳細ログは debug_inventory.log を確認してください")
        
    except Exception as e:
        logger.error(f"Error updating inventory data: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        ingestor.cleanup()
        raise

if __name__ == "__main__":
    debug_inventory_update()