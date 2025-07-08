"""
満期日情報の状況を確認
"""
import pyodbc

# データベース接続
DATABASE_CONFIG = {
    'driver': 'ODBC Driver 17 for SQL Server',
    'server': 'jcz.database.windows.net',
    'database': 'JCL',
    'username': 'TKJCZ01',
    'password': 'P@ssw0rdmbkazuresql'
}

def get_connection():
    conn_str = f"""
    DRIVER={{{DATABASE_CONFIG['driver']}}};
    SERVER={DATABASE_CONFIG['server']};
    DATABASE={DATABASE_CONFIG['database']};
    UID={DATABASE_CONFIG['username']};
    PWD={DATABASE_CONFIG['password']};
    """
    return pyodbc.connect(conn_str)

def verify_maturity_dates():
    with get_connection() as conn:
        cursor = conn.cursor()
        
        print("=== M_GenericFuturesテーブルの満期日情報 ===")
        cursor.execute("""
            SELECT 
                ExchangeCode,
                COUNT(*) as Total,
                COUNT(LastTradeableDate) as HasLastTradeable,
                COUNT(FutureDeliveryDateLast) as HasDelivery
            FROM M_GenericFutures
            WHERE IsActive = 1
            GROUP BY ExchangeCode
            ORDER BY ExchangeCode
        """)
        
        print("取引所 | 総数 | 最終取引日 | 受渡日")
        print("-" * 40)
        for row in cursor.fetchall():
            print(f"{row[0]:6} | {row[1]:4} | {row[2]:10} | {row[3]:6}")
            
        print("\n=== T_CommodityPriceテーブルのMaturityDate ===")
        cursor.execute("""
            SELECT 
                gf.ExchangeCode,
                COUNT(*) as Total,
                COUNT(cp.MaturityDate) as HasMaturityDate
            FROM T_CommodityPrice cp
            JOIN M_GenericFutures gf ON cp.GenericID = gf.GenericID
            WHERE cp.DataType = 'Generic'
                AND cp.TradeDate = '2025-06-06'
            GROUP BY gf.ExchangeCode
            ORDER BY gf.ExchangeCode
        """)
        
        print("\n取引所 | 総数 | MaturityDate")
        print("-" * 30)
        for row in cursor.fetchall():
            print(f"{row[0]:6} | {row[1]:4} | {row[2]:13}")
            
        print("\n=== サンプルデータ（CMX） ===")
        cursor.execute("""
            SELECT TOP 5
                gf.GenericTicker,
                gf.LastTradeableDate as MasterLastTradeable,
                gf.FutureDeliveryDateLast as MasterDelivery,
                cp.MaturityDate as PriceMaturity
            FROM T_CommodityPrice cp
            JOIN M_GenericFutures gf ON cp.GenericID = gf.GenericID
            WHERE cp.DataType = 'Generic'
                AND cp.TradeDate = '2025-06-06'
                AND gf.ExchangeCode = 'CMX'
            ORDER BY gf.GenericNumber
        """)
        
        print("\nTicker    | Master最終取引日 | Master受渡日 | Price満期日")
        print("-" * 60)
        for row in cursor.fetchall():
            print(f"{row[0]:10} | {row[1] or 'NULL':16} | {row[2] or 'NULL':12} | {row[3] or 'NULL'}")

if __name__ == "__main__":
    verify_maturity_dates()