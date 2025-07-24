"""
M_Metalテーブルに各取引所の銅が登録されていることを確認するスクリプト
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.database import DatabaseManager
from config.logging_config import logger

def ensure_metal_master():
    """M_Metalテーブルに必要なデータを確認・作成"""
    db_manager = DatabaseManager()
    db_manager.connect()
    
    try:
        with db_manager.get_connection() as conn:
            cursor = conn.cursor()
            
            # 必要な金属マスターデータ
            metals = [
                ('COPPER', 'Copper', 'USD', 'LME', 'LME Copper'),
                ('COPPER', 'Copper', 'CNY', 'SHFE', 'SHFE Copper'),
                ('COPPER', 'Copper', 'USD', 'COMEX', 'COMEX Copper'),
            ]
            
            for metal_code, metal_name, currency, exchange, description in metals:
                # 既存チェック
                cursor.execute("""
                    SELECT MetalID FROM M_Metal 
                    WHERE MetalCode = ? AND ExchangeCode = ?
                """, (metal_code, exchange))
                
                if not cursor.fetchone():
                    # 新規作成
                    cursor.execute("""
                        INSERT INTO M_Metal 
                        (MetalCode, MetalName, CurrencyCode, ExchangeCode, Description)
                        VALUES (?, ?, ?, ?, ?)
                    """, (metal_code, metal_name, currency, exchange, description))
                    logger.info(f"Created metal master: {metal_code} - {exchange}")
                else:
                    logger.info(f"Metal already exists: {metal_code} - {exchange}")
            
            conn.commit()
            
            # 確認
            cursor.execute("""
                SELECT MetalID, MetalCode, ExchangeCode, CurrencyCode
                FROM M_Metal
                WHERE MetalCode = 'COPPER'
                ORDER BY ExchangeCode
            """)
            
            print("\nCurrent Metal Master Data:")
            print("ID | Code   | Exchange | Currency")
            print("-" * 35)
            for row in cursor:
                print(f"{row[0]:2} | {row[1]:6} | {row[2]:8} | {row[3]}")
                
    finally:
        db_manager.disconnect()

if __name__ == "__main__":
    ensure_metal_master()