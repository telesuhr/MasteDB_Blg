"""
マッピング状況をチェックするスクリプト
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.database import DatabaseManager
from config.logging_config import logger
import pandas as pd

def check_mapping_status():
    """マッピング状況を確認"""
    db_manager = DatabaseManager()
    db_manager.connect()
    
    try:
        with db_manager.get_connection() as conn:
            # 1. M_GenericFuturesの状況確認
            logger.info("=== Generic Futures Master Status ===")
            query1 = """
            SELECT ExchangeCode, COUNT(*) as GenericCount
            FROM M_GenericFutures
            WHERE IsActive = 1
            GROUP BY ExchangeCode
            ORDER BY ExchangeCode
            """
            result1 = pd.read_sql(query1, conn)
            print("\nGeneric Futures by Exchange:")
            print(result1)
            
            # 2. 最新のマッピング状況
            logger.info("\n=== Latest Mapping Status ===")
            query2 = """
            SELECT 
                gf.ExchangeCode,
                COUNT(DISTINCT gf.GenericID) as TotalGenerics,
                COUNT(DISTINCT gm.GenericID) as MappedGenerics,
                MAX(gm.MappingDate) as LatestMappingDate
            FROM M_GenericFutures gf
            LEFT JOIN T_GenericContractMapping gm ON gf.GenericID = gm.GenericID
                AND gm.MappingDate = (SELECT MAX(MappingDate) FROM T_GenericContractMapping)
            WHERE gf.IsActive = 1
            GROUP BY gf.ExchangeCode
            ORDER BY gf.ExchangeCode
            """
            result2 = pd.read_sql(query2, conn)
            print("\nMapping Coverage by Exchange:")
            print(result2)
            
            # 3. 未マッピングのGeneric先物
            logger.info("\n=== Unmapped Generic Futures ===")
            query3 = """
            SELECT 
                gf.GenericTicker,
                gf.ExchangeCode,
                gf.GenericNumber
            FROM M_GenericFutures gf
            WHERE gf.IsActive = 1
            AND NOT EXISTS (
                SELECT 1 
                FROM T_GenericContractMapping gm 
                WHERE gf.GenericID = gm.GenericID
                AND gm.MappingDate >= DATEADD(day, -7, GETDATE())
            )
            ORDER BY gf.ExchangeCode, gf.GenericNumber
            """
            result3 = pd.read_sql(query3, conn)
            print("\nUnmapped Generic Futures (last 7 days):")
            print(result3)
            
            # 4. ActualContract の存在確認
            logger.info("\n=== Actual Contracts Status ===")
            query4 = """
            SELECT 
                LEFT(ContractTicker, 2) as Exchange,
                COUNT(*) as ContractCount,
                MIN(LastTradeableDate) as EarliestExpiry,
                MAX(LastTradeableDate) as LatestExpiry
            FROM M_ActualContract
            WHERE IsActive = 1
            GROUP BY LEFT(ContractTicker, 2)
            ORDER BY Exchange
            """
            result4 = pd.read_sql(query4, conn)
            print("\nActual Contracts by Exchange:")
            print(result4)
            
    finally:
        db_manager.disconnect()

if __name__ == "__main__":
    check_mapping_status()