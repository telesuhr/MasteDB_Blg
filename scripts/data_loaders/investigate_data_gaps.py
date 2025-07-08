"""
データ歯抜け問題の詳細調査
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

def investigate_gaps():
    """データ歯抜け調査"""
    print("=== データ歯抜け問題調査 ===")
    
    conn = pyodbc.connect(CONNECTION_STRING)
    
    # 1. GenericIDとティッカーの対応確認
    print("\n1. 問題のGenericIDとティッカー対応:")
    query = """
        SELECT 
            GenericID,
            GenericTicker,
            ExchangeCode,
            GenericNumber,
            MetalID
        FROM M_GenericFutures
        WHERE GenericID IN (197, 198, 199, 200, 201, 202, 205)
        ORDER BY GenericID
    """
    df = pd.read_sql(query, conn)
    print(df.to_string(index=False))
    
    # 2. CMX全体のGenericIDマッピング確認
    print("\n2. CMX取引所の全GenericIDマッピング:")
    query = """
        SELECT 
            GenericID,
            GenericTicker,
            GenericNumber
        FROM M_GenericFutures
        WHERE ExchangeCode = 'CMX'
        ORDER BY GenericNumber
    """
    df = pd.read_sql(query, conn)
    print(df.to_string(index=False))
    
    # 3. 歯抜けデータの詳細分析
    print("\n3. データ有無パターン分析（CMX）:")
    query = """
        SELECT 
            g.GenericNumber,
            g.GenericTicker,
            p.TradeDate,
            CASE 
                WHEN p.SettlementPrice IS NOT NULL THEN 'あり'
                ELSE 'なし'
            END as Settlement,
            CASE 
                WHEN p.OpenPrice IS NOT NULL THEN 'あり'
                ELSE 'なし'
            END as OHLC,
            CASE 
                WHEN p.Volume IS NOT NULL THEN 'あり'
                ELSE 'なし'
            END as Volume,
            p.SettlementPrice,
            p.OpenPrice,
            p.Volume
        FROM T_CommodityPrice_V2 p
        JOIN M_GenericFutures g ON p.GenericID = g.GenericID
        WHERE g.ExchangeCode = 'CMX'
            AND p.TradeDate >= '2025-07-07'
        ORDER BY p.TradeDate DESC, g.GenericNumber
    """
    df = pd.read_sql(query, conn)
    print("\n最初の20件:")
    print(df.head(20).to_string(index=False))
    
    # 4. Bloomberg APIで実際に取得されたデータの確認
    print("\n4. 各限月の最新価格データ状況:")
    query = """
        WITH LatestData AS (
            SELECT 
                g.GenericNumber,
                g.GenericTicker,
                MAX(CASE WHEN p.SettlementPrice IS NOT NULL THEN p.TradeDate END) as 最終Settlement日,
                MAX(CASE WHEN p.OpenPrice IS NOT NULL THEN p.TradeDate END) as 最終OHLC日,
                MAX(CASE WHEN p.Volume IS NOT NULL THEN p.TradeDate END) as 最終Volume日
            FROM M_GenericFutures g
            LEFT JOIN T_CommodityPrice_V2 p ON g.GenericID = p.GenericID
            WHERE g.ExchangeCode = 'CMX'
            GROUP BY g.GenericNumber, g.GenericTicker
        )
        SELECT 
            GenericNumber,
            GenericTicker,
            最終Settlement日,
            最終OHLC日,
            最終Volume日,
            CASE 
                WHEN 最終OHLC日 IS NOT NULL THEN '○'
                ELSE '×'
            END as OHLC取得可否
        FROM LatestData
        ORDER BY GenericNumber
    """
    df = pd.read_sql(query, conn)
    print(df.to_string(index=False))
    
    conn.close()
    print("\n=== 調査完了 ===")

if __name__ == "__main__":
    investigate_gaps()