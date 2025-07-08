"""
データ取得の問題を詳細にチェック
"""
import pyodbc
from datetime import datetime

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

def check_data():
    with get_connection() as conn:
        cursor = conn.cursor()
        
        # 1. 2025-06-08のデータ件数
        cursor.execute("""
            SELECT COUNT(*) FROM T_CommodityPrice 
            WHERE TradeDate = '2025-06-08'
        """)
        total_count = cursor.fetchone()[0]
        print(f"2025-06-08の総レコード数: {total_count}")
        
        # 2. データタイプ別の内訳
        cursor.execute("""
            SELECT DataType, COUNT(*) as cnt
            FROM T_CommodityPrice
            WHERE TradeDate = '2025-06-08'
            GROUP BY DataType
            ORDER BY DataType
        """)
        print("\nデータタイプ別内訳:")
        for row in cursor.fetchall():
            print(f"  {row[0]}: {row[1]}件")
        
        # 3. GenericIDの内訳（ティッカー付き）
        cursor.execute("""
            SELECT 
                cp.GenericID,
                gf.GenericTicker,
                gf.ExchangeCode,
                COUNT(*) as cnt
            FROM T_CommodityPrice cp
            LEFT JOIN M_GenericFutures gf ON cp.GenericID = gf.GenericID
            WHERE cp.TradeDate = '2025-06-08' AND cp.DataType = 'Generic'
            GROUP BY cp.GenericID, gf.GenericTicker, gf.ExchangeCode
            ORDER BY gf.ExchangeCode, gf.GenericTicker
        """)
        print("\nGeneric先物の内訳:")
        current_exchange = None
        for row in cursor.fetchall():
            if current_exchange != row[2]:
                current_exchange = row[2]
                print(f"\n[{current_exchange}]")
            print(f"  {row[1]}: {row[3]}件 (GenericID: {row[0]})")
        
        # 4. CashとTomNextの確認
        cursor.execute("""
            SELECT DataType, MetalID, COUNT(*) as cnt
            FROM T_CommodityPrice
            WHERE TradeDate = '2025-06-08' 
                AND DataType IN ('Cash', 'TomNext')
            GROUP BY DataType, MetalID
        """)
        print("\nCash/TomNext価格:")
        for row in cursor.fetchall():
            print(f"  {row[0]}: {row[2]}件")
        
        # 5. M_GenericFuturesの総数を確認
        cursor.execute("""
            SELECT ExchangeCode, COUNT(*) as cnt
            FROM M_GenericFutures
            WHERE IsActive = 1
            GROUP BY ExchangeCode
        """)
        print("\nM_GenericFuturesマスタ:")
        for row in cursor.fetchall():
            print(f"  {row[0]}: {row[1]}件")
        
        # 6. ActualContractIDの状況
        cursor.execute("""
            SELECT 
                DataType,
                CASE WHEN ActualContractID IS NULL THEN 'NULL' ELSE 'NOT NULL' END as Status,
                COUNT(*) as cnt
            FROM T_CommodityPrice
            WHERE TradeDate = '2025-06-08'
            GROUP BY DataType, CASE WHEN ActualContractID IS NULL THEN 'NULL' ELSE 'NOT NULL' END
            ORDER BY DataType
        """)
        print("\nActualContractIDの状況:")
        for row in cursor.fetchall():
            print(f"  {row[0]} - {row[1]}: {row[2]}件")
        
        # 7. 直近のマッピングデータ確認
        cursor.execute("""
            SELECT TOP 10
                m.TradeDate,
                gf.GenericTicker,
                ac.ContractTicker
            FROM T_GenericContractMapping m
            JOIN M_GenericFutures gf ON m.GenericID = gf.GenericID
            JOIN M_ActualContract ac ON m.ActualContractID = ac.ActualContractID
            WHERE m.TradeDate = '2025-06-08'
            ORDER BY gf.GenericTicker
        """)
        print("\n2025-06-08のマッピング（最初の10件）:")
        for row in cursor.fetchall():
            print(f"  {row[1]} -> {row[2]}")

if __name__ == "__main__":
    check_data()