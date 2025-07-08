"""
T_CommodityPrice_V2のデータ品質分析
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

def analyze_data_quality():
    """データ品質分析"""
    print("=== T_CommodityPrice_V2 データ品質分析 ===")
    
    conn = pyodbc.connect(CONNECTION_STRING)
    
    # 1. 取引所別のNULL値分析
    print("\n1. 取引所別NULL値分析:")
    query = """
        SELECT 
            g.ExchangeCode,
            COUNT(*) as 総レコード数,
            -- 価格フィールドのNULL数
            SUM(CASE WHEN p.SettlementPrice IS NULL THEN 1 ELSE 0 END) as NULL_Settlement,
            SUM(CASE WHEN p.OpenPrice IS NULL THEN 1 ELSE 0 END) as NULL_Open,
            SUM(CASE WHEN p.HighPrice IS NULL THEN 1 ELSE 0 END) as NULL_High,
            SUM(CASE WHEN p.LowPrice IS NULL THEN 1 ELSE 0 END) as NULL_Low,
            SUM(CASE WHEN p.LastPrice IS NULL THEN 1 ELSE 0 END) as NULL_Last,
            SUM(CASE WHEN p.Volume IS NULL THEN 1 ELSE 0 END) as NULL_Volume,
            SUM(CASE WHEN p.OpenInterest IS NULL THEN 1 ELSE 0 END) as NULL_OI,
            -- NULL率
            CAST(SUM(CASE WHEN p.OpenPrice IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS DECIMAL(5,2)) as NULL_Open率,
            CAST(SUM(CASE WHEN p.Volume IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS DECIMAL(5,2)) as NULL_Volume率,
            CAST(SUM(CASE WHEN p.OpenInterest IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS DECIMAL(5,2)) as NULL_OI率
        FROM T_CommodityPrice_V2 p
        JOIN M_GenericFutures g ON p.GenericID = g.GenericID
        WHERE p.DataType = 'Generic'
        GROUP BY g.ExchangeCode
        ORDER BY g.ExchangeCode
    """
    df = pd.read_sql(query, conn)
    print(df.to_string(index=False))
    
    # 2. 銘柄別のデータ完全性チェック（各取引所サンプル）
    print("\n2. 銘柄別データ完全性（各取引所の最初の5銘柄）:")
    query = """
        WITH RankedData AS (
            SELECT 
                g.ExchangeCode,
                g.GenericTicker,
                g.GenericNumber,
                p.TradeDate,
                CASE 
                    WHEN p.OpenPrice IS NOT NULL AND p.HighPrice IS NOT NULL 
                         AND p.LowPrice IS NOT NULL AND p.Volume IS NOT NULL THEN '完全'
                    WHEN p.SettlementPrice IS NOT NULL AND p.LastPrice IS NOT NULL THEN '部分的'
                    ELSE '不完全'
                END as データ品質,
                ROW_NUMBER() OVER (PARTITION BY g.ExchangeCode ORDER BY g.GenericNumber) as rn
            FROM T_CommodityPrice_V2 p
            JOIN M_GenericFutures g ON p.GenericID = g.GenericID
            WHERE p.DataType = 'Generic'
        )
        SELECT 
            ExchangeCode,
            GenericTicker,
            COUNT(*) as レコード数,
            SUM(CASE WHEN データ品質 = '完全' THEN 1 ELSE 0 END) as 完全データ数,
            SUM(CASE WHEN データ品質 = '部分的' THEN 1 ELSE 0 END) as 部分データ数,
            SUM(CASE WHEN データ品質 = '不完全' THEN 1 ELSE 0 END) as 不完全データ数
        FROM RankedData
        WHERE rn <= 5
        GROUP BY ExchangeCode, GenericTicker, GenericNumber
        ORDER BY ExchangeCode, GenericNumber
    """
    df = pd.read_sql(query, conn)
    print(df.to_string(index=False))
    
    # 3. 日付別のデータ品質
    print("\n3. 日付別データ品質:")
    query = """
        SELECT 
            p.TradeDate,
            g.ExchangeCode,
            COUNT(*) as レコード数,
            AVG(CASE WHEN p.OpenPrice IS NOT NULL THEN 1.0 ELSE 0.0 END) * 100 as OHLC有効率,
            AVG(CASE WHEN p.Volume IS NOT NULL THEN 1.0 ELSE 0.0 END) * 100 as Volume有効率,
            AVG(CASE WHEN p.OpenInterest IS NOT NULL THEN 1.0 ELSE 0.0 END) * 100 as OI有効率
        FROM T_CommodityPrice_V2 p
        JOIN M_GenericFutures g ON p.GenericID = g.GenericID
        WHERE p.DataType = 'Generic'
        GROUP BY p.TradeDate, g.ExchangeCode
        ORDER BY p.TradeDate DESC, g.ExchangeCode
    """
    df = pd.read_sql(query, conn)
    print(df.to_string(index=False))
    
    # 4. 価格の異常値チェック
    print("\n4. 価格異常値チェック（SettlementとLastの乖離）:")
    query = """
        SELECT TOP 20
            g.ExchangeCode,
            g.GenericTicker,
            p.TradeDate,
            p.SettlementPrice,
            p.LastPrice,
            ABS(p.SettlementPrice - p.LastPrice) as 価格差,
            CASE 
                WHEN p.SettlementPrice <> 0 
                THEN ABS((p.SettlementPrice - p.LastPrice) / p.SettlementPrice * 100)
                ELSE 0 
            END as 乖離率
        FROM T_CommodityPrice_V2 p
        JOIN M_GenericFutures g ON p.GenericID = g.GenericID
        WHERE p.DataType = 'Generic'
            AND p.SettlementPrice IS NOT NULL
            AND p.LastPrice IS NOT NULL
            AND p.SettlementPrice <> p.LastPrice
        ORDER BY ABS(p.SettlementPrice - p.LastPrice) DESC
    """
    df = pd.read_sql(query, conn)
    if not df.empty:
        print(df.to_string(index=False))
    else:
        print("価格差異なし（SettlementPrice = LastPrice）")
    
    # 5. ティッカー別の取得フィールド傾向
    print("\n5. 取引所別の主要フィールド取得状況:")
    query = """
        SELECT 
            g.ExchangeCode,
            MIN(g.GenericNumber) as 開始番号,
            MAX(g.GenericNumber) as 終了番号,
            COUNT(DISTINCT g.GenericNumber) as 銘柄数,
            -- 各フィールドが取得できている銘柄数
            COUNT(DISTINCT CASE WHEN p.OpenPrice IS NOT NULL THEN g.GenericNumber END) as OHLC取得銘柄数,
            COUNT(DISTINCT CASE WHEN p.Volume IS NOT NULL THEN g.GenericNumber END) as Volume取得銘柄数,
            COUNT(DISTINCT CASE WHEN p.OpenInterest IS NOT NULL THEN g.GenericNumber END) as OI取得銘柄数
        FROM T_CommodityPrice_V2 p
        JOIN M_GenericFutures g ON p.GenericID = g.GenericID
        WHERE p.DataType = 'Generic'
        GROUP BY g.ExchangeCode
        ORDER BY g.ExchangeCode
    """
    df = pd.read_sql(query, conn)
    print(df.to_string(index=False))
    
    conn.close()
    print("\n=== 分析完了 ===")

if __name__ == "__main__":
    analyze_data_quality()