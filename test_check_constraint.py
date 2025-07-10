"""
CHECK制約の確認スクリプト
"""
import sys
import os

# プロジェクトルートをPythonパスに追加
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), 'src'))

from database import DatabaseManager
from loguru import logger


def check_constraint():
    """CHECK制約を確認"""
    db_manager = DatabaseManager()
    
    try:
        db_manager.connect()
        
        with db_manager.get_connection() as conn:
            cursor = conn.cursor()
            
            # CHECK制約の定義を確認
            cursor.execute("""
                SELECT 
                    cc.name AS ConstraintName,
                    cc.definition
                FROM sys.check_constraints cc
                WHERE cc.parent_object_id = OBJECT_ID('T_CommodityPrice')
                AND cc.name LIKE '%DataType%'
            """)
            
            logger.info("CHECK制約:")
            for row in cursor:
                logger.info(f"  {row[0]}: {row[1]}")
                
            # 現在のデータパターンを確認
            cursor.execute("""
                SELECT 
                    DataType,
                    COUNT(*) as Count,
                    SUM(CASE WHEN GenericID IS NOT NULL THEN 1 ELSE 0 END) as HasGenericID,
                    SUM(CASE WHEN ActualContractID IS NOT NULL THEN 1 ELSE 0 END) as HasActualID
                FROM T_CommodityPrice
                WHERE TradeDate >= '2025-01-01'
                GROUP BY DataType
            """)
            
            logger.info("\nデータパターン:")
            for row in cursor:
                logger.info(f"  {row[0]}: Count={row[1]}, GenericID={row[2]}, ActualID={row[3]}")
                
    finally:
        db_manager.disconnect()


if __name__ == "__main__":
    check_constraint()