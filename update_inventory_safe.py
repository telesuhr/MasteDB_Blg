#!/usr/bin/env python3
"""
LME在庫データのみを更新するスクリプト（エラーハンドリング強化版）
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
from config.bloomberg_config import INITIAL_LOAD_PERIODS, BLOOMBERG_TICKERS
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
        
        # 在庫カテゴリのみ処理
        inventory_categories = ['LME_INVENTORY', 'SHFE_INVENTORY', 'CMX_INVENTORY']
        
        for category in inventory_categories:
            try:
                ticker_info = BLOOMBERG_TICKERS[category]
                
                # MEST地域を除外（Bloombergで認識されないため）
                if category == 'LME_INVENTORY':
                    # MESTを含むティッカーを除外
                    for data_type, tickers in ticker_info['securities'].items():
                        ticker_info['securities'][data_type] = [
                            t for t in tickers if '@MEST' not in t
                        ]
                    # region_mappingからもMESTを削除
                    if '@MEST Index' in ticker_info['region_mapping']:
                        del ticker_info['region_mapping']['@MEST Index']
                
                result = ingestor.process_category(category, ticker_info, start_date, end_date)
                logger.info(f"Updated {result} {category} records")
                total_records += result
            except Exception as e:
                logger.error(f"Failed to process {category}: {e}")
                logger.error(f"Error type: {type(e).__name__}")
                import traceback
                logger.error(f"Traceback: {traceback.format_exc()}")
        
        logger.info(f"Successfully updated {total_records} total inventory records")
        
        # 成功したらクリーンアップ
        ingestor.cleanup()
        
    except Exception as e:
        logger.error(f"Error updating inventory data: {e}")
        ingestor.cleanup()
        raise

if __name__ == "__main__":
    update_inventory_only()