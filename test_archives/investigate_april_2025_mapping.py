"""
Investigate April 2025 LP1 mapping issue
Where LP1 Comdty is mapped to LPK25 (May 2025) with LastTradeableDate of July 2025
"""
import sys
import os
from datetime import datetime, date

# Add project root and src to path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
src_dir = os.path.join(project_root, 'src')
sys.path.insert(0, project_root)
sys.path.insert(0, src_dir)

from database import DatabaseManager
import pandas as pd

def investigate_april_2025_mapping():
    """Investigate the incorrect April 2025 mapping"""
    db_manager = DatabaseManager()
    
    try:
        db_manager.connect()
        
        with db_manager.get_connection() as conn:
            # 1. Check current mapping for April 2025
            print("\n=== April 2025 LP1 Mapping ===")
            query = """
                SELECT 
                    m.TradeDate,
                    g.GenericTicker,
                    a.ContractTicker,
                    a.ContractMonth,
                    a.ContractYear,
                    a.ContractMonthCode,
                    a.LastTradeableDate,
                    m.DaysToExpiry,
                    m.CreatedAt
                FROM T_GenericContractMapping m
                JOIN M_GenericFutures g ON m.GenericID = g.GenericID
                JOIN M_ActualContract a ON m.ActualContractID = a.ActualContractID
                WHERE g.GenericTicker = 'LP1 Comdty'
                    AND m.TradeDate = '2025-04-15'
            """
            df = pd.read_sql(query, conn)
            print(df)
            
            # 2. Check all LP1 mappings for April 2025
            print("\n=== All LP1 Mappings in April 2025 ===")
            query = """
                SELECT 
                    m.TradeDate,
                    g.GenericTicker,
                    a.ContractTicker,
                    a.ContractMonth,
                    a.ContractYear,
                    a.LastTradeableDate,
                    m.DaysToExpiry
                FROM T_GenericContractMapping m
                JOIN M_GenericFutures g ON m.GenericID = g.GenericID
                JOIN M_ActualContract a ON m.ActualContractID = a.ActualContractID
                WHERE g.GenericTicker = 'LP1 Comdty'
                    AND m.TradeDate >= '2025-04-01'
                    AND m.TradeDate <= '2025-04-30'
                ORDER BY m.TradeDate
            """
            df = pd.read_sql(query, conn)
            print(df.to_string())
            
            # 3. Check what contracts exist for May 2025 (K is May)
            print("\n=== May 2025 (K) Contracts ===")
            query = """
                SELECT 
                    ContractTicker,
                    MetalID,
                    ExchangeCode,
                    ContractMonth,
                    ContractYear,
                    ContractMonthCode,
                    LastTradeableDate,
                    CreatedAt
                FROM M_ActualContract
                WHERE ContractMonthCode = 'K' 
                    AND ContractYear = 2025
                ORDER BY MetalID, ExchangeCode
            """
            df = pd.read_sql(query, conn)
            print(df.to_string())
            
            # 4. Check LME contract month codes and typical rollover pattern
            print("\n=== LME Copper Contract Schedule ===")
            query = """
                SELECT 
                    ContractTicker,
                    ContractMonth,
                    ContractYear,
                    ContractMonthCode,
                    LastTradeableDate,
                    DATEDIFF(day, '2025-04-15', LastTradeableDate) as DaysFromApril15
                FROM M_ActualContract
                WHERE MetalID = 1  -- Copper
                    AND ExchangeCode = 'LME'
                    AND ContractYear = 2025
                    AND ContractMonth >= 4  -- April and later
                ORDER BY ContractMonth
            """
            df = pd.read_sql(query, conn)
            print(df.to_string())
            
            # 5. Check rollover days setting for LP1
            print("\n=== LP1 Rollover Settings ===")
            query = """
                SELECT 
                    GenericTicker,
                    RolloverDays,
                    LastTradeableDate,
                    FutureDeliveryDateLast,
                    LastRefreshDate
                FROM M_GenericFutures
                WHERE GenericTicker = 'LP1 Comdty'
            """
            df = pd.read_sql(query, conn)
            print(df.to_string())
            
            # 6. Check when the incorrect mapping was created
            print("\n=== Mapping Creation History ===")
            query = """
                SELECT TOP 10
                    m.TradeDate,
                    a.ContractTicker,
                    m.CreatedAt,
                    m.DaysToExpiry
                FROM T_GenericContractMapping m
                JOIN M_GenericFutures g ON m.GenericID = g.GenericID
                JOIN M_ActualContract a ON m.ActualContractID = a.ActualContractID
                WHERE g.GenericTicker = 'LP1 Comdty'
                    AND a.ContractTicker = 'LPK25 Comdty'
                ORDER BY m.TradeDate
            """
            df = pd.read_sql(query, conn)
            print(df.to_string())
            
    except Exception as e:
        print(f"Error: {e}")
        
    finally:
        db_manager.disconnect()


if __name__ == "__main__":
    investigate_april_2025_mapping()