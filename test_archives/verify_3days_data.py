"""
3営業日分のデータ取得結果を確認
"""
import pyodbc
import pandas as pd
from datetime import datetime, timedelta

# データベース接続文字列
CONNECTION_STRING = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=jcz.database.windows.net;"
    "DATABASE=JCL;"
    "UID=TKJCZ01;"
    "PWD=P@ssw0rdmbkazuresql;"
    "Encrypt=yes;"
    "TrustServerCertificate=no;"
    "Connection Timeout=30;"
)

def verify_data():
    """データ検証"""
    print("=== 3営業日分データ検証 ===")
    
    conn = pyodbc.connect(CONNECTION_STRING)
    
    # 1. 日付別サマリー
    print("\n1. 日付別データサマリー:")
    query = """
        SELECT 
            p.TradeDate,
            COUNT(DISTINCT m.Name) as 金属数,
            COUNT(DISTINCT t.Name) as テナー数,
            COUNT(*) as レコード数,
            MIN(p.SettlementPrice) as 最小価格,
            MAX(p.SettlementPrice) as 最大価格
        FROM T_CommodityPrice p
        JOIN M_Metal m ON p.MetalID = m.MetalID
        JOIN M_TenorType t ON p.TenorTypeID = t.TenorTypeID
        WHERE p.TradeDate >= DATEADD(day, -10, GETDATE())
        GROUP BY p.TradeDate
        ORDER BY p.TradeDate DESC
    """
    df = pd.read_sql(query, conn)
    print(df.to_string(index=False))
    
    # 2. 取引所別サマリー
    print("\n2. 取引所別サマリー (直近3営業日):")
    query = """
        SELECT 
            g.ExchangeCode,
            COUNT(DISTINCT p.TradeDate) as 営業日数,
            COUNT(DISTINCT g.GenericNumber) as 銘柄数,
            COUNT(*) as レコード数,
            AVG(p.SettlementPrice) as 平均価格,
            MIN(p.SettlementPrice) as 最小価格,
            MAX(p.SettlementPrice) as 最大価格
        FROM T_CommodityPrice p
        JOIN M_GenericFutures g ON p.GenericID = g.GenericID
        WHERE p.TradeDate >= DATEADD(day, -10, GETDATE())
        GROUP BY g.ExchangeCode
        ORDER BY g.ExchangeCode
    """
    df = pd.read_sql(query, conn)
    df['ExchangeCode'] = df['ExchangeCode'].apply(lambda x: 'COMEX' if x == 'CMX' else x)
    print(df.to_string(index=False))
    
    # 3. 各取引所の最新データサンプル
    print("\n3. 各取引所の最新データサンプル:")
    
    for exchange in ['LME', 'SHFE', 'CMX']:
        display_name = 'COMEX' if exchange == 'CMX' else exchange
        print(f"\n{display_name}:")
        query = f"""
            SELECT TOP 5
                p.TradeDate,
                g.GenericTicker,
                p.SettlementPrice,
                p.OpenPrice,
                p.HighPrice,
                p.LowPrice,
                p.Volume,
                p.OpenInterest
            FROM T_CommodityPrice p
            JOIN M_GenericFutures g ON p.GenericID = g.GenericID
            WHERE g.ExchangeCode = '{exchange}'
                AND p.TradeDate = (
                    SELECT MAX(TradeDate) 
                    FROM T_CommodityPrice p2
                    JOIN M_GenericFutures g2 ON p2.GenericID = g2.GenericID
                    WHERE g2.ExchangeCode = '{exchange}'
                )
            ORDER BY g.GenericNumber
        """
        df = pd.read_sql(query, conn)
        print(df.to_string(index=False))
    
    # 4. データ品質チェック
    print("\n4. データ品質チェック:")
    query = """
        SELECT 
            g.ExchangeCode,
            SUM(CASE WHEN p.SettlementPrice IS NULL THEN 1 ELSE 0 END) as NULL価格数,
            SUM(CASE WHEN p.Volume IS NULL THEN 1 ELSE 0 END) as NULL出来高数,
            SUM(CASE WHEN p.OpenInterest IS NULL THEN 1 ELSE 0 END) as NULL建玉数,
            COUNT(*) as 総レコード数
        FROM T_CommodityPrice p
        JOIN M_GenericFutures g ON p.GenericID = g.GenericID
        WHERE p.TradeDate >= DATEADD(day, -10, GETDATE())
        GROUP BY g.ExchangeCode
        ORDER BY g.ExchangeCode
    """
    df = pd.read_sql(query, conn)
    df['ExchangeCode'] = df['ExchangeCode'].apply(lambda x: 'COMEX' if x == 'CMX' else x)
    print(df.to_string(index=False))
    
    conn.close()
    print("\n=== 検証完了 ===")

if __name__ == "__main__":
    verify_data()