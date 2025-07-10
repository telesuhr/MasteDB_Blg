"""
自動マッピングシステムのシンプルなテスト
T_CommodityPriceの実際のスキーマを確認
"""
import sys
import os

# プロジェクトルートをPythonパスに追加
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), 'src'))

from database import DatabaseManager
from loguru import logger


def check_table_schema():
    """T_CommodityPriceテーブルのスキーマを確認"""
    db_manager = DatabaseManager()
    
    try:
        db_manager.connect()
        
        with db_manager.get_connection() as conn:
            cursor = conn.cursor()
            
            # カラム情報を取得
            cursor.execute("""
                SELECT 
                    COLUMN_NAME,
                    DATA_TYPE,
                    IS_NULLABLE
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = 'T_CommodityPrice'
                ORDER BY ORDINAL_POSITION
            """)
            
            logger.info("T_CommodityPriceのカラム:")
            for row in cursor:
                logger.info(f"  {row[0]:<30} {row[1]:<15} {row[2]}")
                
            # サンプルデータ確認
            cursor.execute("""
                SELECT TOP 5 *
                FROM T_CommodityPrice
                WHERE TradeDate >= '2025-04-01'
                ORDER BY TradeDate DESC
            """)
            
            # カラム名を取得
            columns = [desc[0] for desc in cursor.description]
            logger.info(f"\nカラムリスト: {columns}")
            
    finally:
        db_manager.disconnect()


if __name__ == "__main__":
    check_table_schema()