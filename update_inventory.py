#!/usr/bin/env python3
"""
LME在庫データのみを更新するスクリプト
"""
import sys
import os
from datetime import datetime, timedelta

# プロジェクトルートとsrcディレクトリをPythonパスに追加
project_root = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.join(project_root, 'src')
sys.path.insert(0, project_root)
sys.path.insert(0, src_dir)

from src.main import BloombergSQLIngestor
from config.bloomberg_config import INITIAL_LOAD_PERIODS
from config.logging_config import logger

def update_inventory_only():
    """全在庫データ（LME、SHFE、CMX）を更新"""
    ingestor = BloombergSQLIngestor()
    
    try:
        # 初期化
        ingestor.initialize()
        
        # 開始日・終了日の設定（過去5年分）
        end_date = datetime.now().strftime('%Y%m%d')
        start_date = (datetime.now() - timedelta(days=365 * INITIAL_LOAD_PERIODS['inventory'])).strftime('%Y%m%d')
        
        logger.info(f"Updating inventory data from {start_date} to {end_date}")
        
        total_records = 0
        
        # LME在庫データ処理
        result = ingestor.process_category('LME_INVENTORY', start_date, end_date)
        logger.info(f"Updated {result} LME inventory records")
        total_records += result
        
        # SHFE在庫データ処理
        result = ingestor.process_category('SHFE_INVENTORY', start_date, end_date)
        logger.info(f"Updated {result} SHFE inventory records")
        total_records += result
        
        # CMX在庫データ処理
        result = ingestor.process_category('CMX_INVENTORY', start_date, end_date)
        logger.info(f"Updated {result} CMX inventory records")
        total_records += result
        
        logger.info(f"Successfully updated {total_records} total inventory records")
        
        # 成功したらクリーンアップ
        ingestor.cleanup()
        
    except Exception as e:
        logger.error(f"Error updating inventory data: {e}")
        ingestor.cleanup()
        raise

if __name__ == "__main__":
    update_inventory_only()