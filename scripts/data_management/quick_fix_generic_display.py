"""
SHFE/COMEXのGeneric先物表示を修正する簡易スクリプト
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.database import DatabaseManager
from config.logging_config import logger

def fix_generic_display():
    """Generic先物のDataTypeとGenericIDを修正"""
    db_manager = DatabaseManager()
    db_manager.connect()
    
    try:
        with db_manager.get_connection() as conn:
            cursor = conn.cursor()
            
            # 1. データの現状を確認
            logger.info("Checking current data status...")
            cursor.execute("""
                SELECT 
                    MetalID,
                    DataType,
                    GenericID,
                    COUNT(*) as RecordCount
                FROM T_CommodityPrice
                WHERE TradeDate >= '2025-07-20'
                AND MetalID IN (
                    SELECT MetalID FROM M_Metal WHERE ExchangeCode IN ('SHFE', 'COMEX')
                )
                GROUP BY MetalID, DataType, GenericID
                ORDER BY MetalID, DataType
            """)
            
            print("\nCurrent data distribution:")
            for row in cursor:
                print(f"MetalID: {row[0]}, DataType: {row[1]}, GenericID: {row[2]}, Count: {row[3]}")
            
            # 2. SHFE/COMEXのGeneric先物データを修正
            logger.info("\nFixing SHFE/COMEX generic futures data...")
            
            # まず、価格パターンからGeneric番号を推定してGenericIDを設定
            update_queries = []
            
            # SHFEの場合（価格が70000-80000範囲）
            for i in range(1, 13):
                update_queries.append(f"""
                    UPDATE T_CommodityPrice
                    SET GenericID = (SELECT GenericID FROM M_GenericFutures WHERE GenericTicker = 'CU{i} Comdty'),
                        DataType = 'Generic'
                    WHERE MetalID = (SELECT MetalID FROM M_Metal WHERE MetalCode = 'COPPER' AND ExchangeCode = 'SHFE')
                    AND DataType IS NULL
                    AND GenericID IS NULL
                    -- 価格パターンとGeneric番号の相関を使用（仮定）
                    AND TradeDate >= '2025-07-20'
                """)
            
            # COMEXの場合（価格が500-700範囲）
            for i in range(1, 27):
                update_queries.append(f"""
                    UPDATE T_CommodityPrice
                    SET GenericID = (SELECT GenericID FROM M_GenericFutures WHERE GenericTicker = 'HG{i} Comdty'),
                        DataType = 'Generic'
                    WHERE MetalID = (SELECT MetalID FROM M_Metal WHERE MetalCode = 'COPPER' AND ExchangeCode = 'COMEX')
                    AND DataType IS NULL
                    AND GenericID IS NULL
                    -- 価格パターンとGeneric番号の相関を使用（仮定）
                    AND TradeDate >= '2025-07-20'
                """)
            
            # 3. 実際の修正は手動マッピングが必要
            logger.info("\nManual mapping needed. Creating mapping reference...")
            
            # ビューを作成して手動マッピングを容易にする
            cursor.execute("""
                IF EXISTS (SELECT * FROM sys.views WHERE name = 'V_UnmappedGenericPrices')
                    DROP VIEW V_UnmappedGenericPrices
            """)
            
            cursor.execute("""
                CREATE VIEW V_UnmappedGenericPrices AS
                SELECT 
                    cp.PriceID,
                    cp.TradeDate,
                    m.MetalCode,
                    m.ExchangeCode,
                    cp.LastPrice,
                    cp.Volume,
                    cp.OpenInterest,
                    cp.DataType,
                    cp.GenericID,
                    CASE 
                        WHEN m.ExchangeCode = 'SHFE' THEN 
                            CASE 
                                WHEN cp.OpenInterest > 100000 THEN 'Likely CU1 or CU2'
                                WHEN cp.OpenInterest > 50000 THEN 'Likely CU3 or CU4'
                                WHEN cp.OpenInterest > 20000 THEN 'Likely CU5-CU7'
                                ELSE 'Likely CU8-CU12'
                            END
                        WHEN m.ExchangeCode = 'COMEX' THEN
                            CASE 
                                WHEN cp.OpenInterest > 50000 THEN 'Likely HG1-HG3'
                                WHEN cp.OpenInterest > 10000 THEN 'Likely HG4-HG6'
                                WHEN cp.OpenInterest > 1000 THEN 'Likely HG7-HG12'
                                ELSE 'Likely HG13-HG26'
                            END
                        ELSE ''
                    END as ProbableGeneric
                FROM T_CommodityPrice cp
                JOIN M_Metal m ON cp.MetalID = m.MetalID
                WHERE cp.DataType IS NULL
                AND m.ExchangeCode IN ('SHFE', 'COMEX')
                AND cp.TradeDate >= '2025-07-20'
            """)
            
            conn.commit()
            logger.info("Created V_UnmappedGenericPrices view for manual mapping assistance")
            
            # 4. サンプルデータを表示
            cursor.execute("""
                SELECT TOP 20 
                    ExchangeCode,
                    LastPrice,
                    OpenInterest,
                    ProbableGeneric
                FROM V_UnmappedGenericPrices
                ORDER BY ExchangeCode, OpenInterest DESC
            """)
            
            print("\nSample unmapped data with probable generic mapping:")
            print("Exchange | Price    | OpenInt | Probable Generic")
            print("-" * 50)
            for row in cursor:
                print(f"{row[0]:8} | {row[1]:8.2f} | {row[2]:7} | {row[3]}")
            
    finally:
        db_manager.disconnect()

if __name__ == "__main__":
    fix_generic_display()