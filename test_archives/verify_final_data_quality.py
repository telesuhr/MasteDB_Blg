"""
最終データ品質確認
"""
import pyodbc
import pandas as pd

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

def verify_final_data():
    """最終データ確認"""
    print("=== 最終データ品質確認 ===")
    
    conn = pyodbc.connect(CONNECTION_STRING)
    
    # 1. 詳細なサンプルデータ（各取引所5件ずつ）
    print("\n1. 各取引所の詳細サンプルデータ:")
    
    for exchange in ['CMX', 'LME', 'SHFE']:
        display_name = 'COMEX' if exchange == 'CMX' else exchange
        print(f"\n{display_name}:")
        
        query = f"""
            SELECT TOP 5
                p.PriceID,
                p.TradeDate,
                g.GenericTicker,
                g.GenericNumber,
                p.SettlementPrice,
                p.OpenPrice,
                p.HighPrice,
                p.LowPrice,
                p.LastPrice,
                p.Volume,
                p.OpenInterest,
                p.LastUpdated
            FROM T_CommodityPrice_V2 p
            JOIN M_GenericFutures g ON p.GenericID = g.GenericID
            WHERE p.DataType = 'Generic' 
                AND g.ExchangeCode = '{exchange}'
                AND p.TradeDate = (
                    SELECT MAX(TradeDate) 
                    FROM T_CommodityPrice_V2 p2
                    JOIN M_GenericFutures g2 ON p2.GenericID = g2.GenericID
                    WHERE g2.ExchangeCode = '{exchange}'
                )
            ORDER BY g.GenericNumber
        """
        
        df = pd.read_sql(query, conn)
        print(df.to_string(index=False))
    
    # 2. OHLC完全性チェック
    print("\n2. OHLCデータ完全性チェック（Generic番号別）:")
    query = """
        WITH OHLCCheck AS (
            SELECT 
                g.ExchangeCode,
                g.GenericNumber,
                COUNT(*) as レコード数,
                SUM(CASE 
                    WHEN p.OpenPrice IS NOT NULL 
                         AND p.HighPrice IS NOT NULL 
                         AND p.LowPrice IS NOT NULL 
                    THEN 1 ELSE 0 
                END) as OHLC完全数
            FROM T_CommodityPrice_V2 p
            JOIN M_GenericFutures g ON p.GenericID = g.GenericID
            WHERE p.DataType = 'Generic'
            GROUP BY g.ExchangeCode, g.GenericNumber
        )
        SELECT 
            ExchangeCode,
            MIN(GenericNumber) as 開始番号,
            MAX(CASE WHEN OHLC完全数 > 0 THEN GenericNumber END) as OHLC最終番号,
            MAX(GenericNumber) as 最終番号,
            SUM(OHLC完全数) as OHLC総数,
            SUM(レコード数) as 総レコード数
        FROM OHLCCheck
        GROUP BY ExchangeCode
        ORDER BY ExchangeCode
    """
    df = pd.read_sql(query, conn)
    df['ExchangeCode'] = df['ExchangeCode'].apply(lambda x: 'COMEX' if x == 'CMX' else x)
    print(df.to_string(index=False))
    
    # 3. LastUpdatedの時刻確認
    print("\n3. LastUpdated時刻確認（最新10件）:")
    query = """
        SELECT TOP 10
            g.ExchangeCode,
            g.GenericTicker,
            p.TradeDate,
            p.LastUpdated,
            DATEPART(HOUR, p.LastUpdated) as 時,
            DATEPART(MINUTE, p.LastUpdated) as 分
        FROM T_CommodityPrice_V2 p
        JOIN M_GenericFutures g ON p.GenericID = g.GenericID
        WHERE p.DataType = 'Generic'
        ORDER BY p.LastUpdated DESC
    """
    df = pd.read_sql(query, conn)
    df['ExchangeCode'] = df['ExchangeCode'].apply(lambda x: 'COMEX' if x == 'CMX' else x)
    print(df.to_string(index=False))
    
    # 4. 問題のあるレコード確認（HG5以降のOHLCデータ）
    print("\n4. CMX/COMEXのOHLCデータ有無（Generic 1-12）:")
    query = """
        SELECT 
            g.GenericTicker,
            g.GenericNumber,
            p.TradeDate,
            CASE WHEN p.OpenPrice IS NOT NULL THEN '○' ELSE '×' END as [Open],
            CASE WHEN p.HighPrice IS NOT NULL THEN '○' ELSE '×' END as [High],
            CASE WHEN p.LowPrice IS NOT NULL THEN '○' ELSE '×' END as [Low],
            CASE WHEN p.Volume IS NOT NULL THEN '○' ELSE '×' END as [Volume],
            CASE WHEN p.OpenInterest IS NOT NULL THEN '○' ELSE '×' END as [OI]
        FROM T_CommodityPrice_V2 p
        JOIN M_GenericFutures g ON p.GenericID = g.GenericID
        WHERE p.DataType = 'Generic' 
            AND g.ExchangeCode = 'CMX'
            AND g.GenericNumber <= 12
            AND p.TradeDate = (SELECT MAX(TradeDate) FROM T_CommodityPrice_V2)
        ORDER BY g.GenericNumber
    """
    df = pd.read_sql(query, conn)
    print(df.to_string(index=False))
    
    conn.close()
    print("\n=== 確認完了 ===")

if __name__ == "__main__":
    verify_final_data()