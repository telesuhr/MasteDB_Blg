"""
欠落しているレコードの確認
Bloombergから取得したがDBに保存されていないレコードを調査
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

def check_missing_records():
    """欠落レコード確認"""
    print("=== 欠落レコード確認 ===")
    
    conn = pyodbc.connect(CONNECTION_STRING)
    
    # 1. 各取引所・日付で期待されるレコード数と実際のレコード数
    print("\n1. 期待レコード数 vs 実際レコード数:")
    query = """
        WITH ExpectedRecords AS (
            -- 各取引所の全銘柄数
            SELECT 
                ExchangeCode,
                COUNT(*) as 銘柄数
            FROM M_GenericFutures
            GROUP BY ExchangeCode
        ),
        ActualRecords AS (
            -- 実際のレコード数（日付別）
            SELECT 
                g.ExchangeCode,
                p.TradeDate,
                COUNT(DISTINCT g.GenericID) as 実績銘柄数
            FROM T_CommodityPrice_V2 p
            JOIN M_GenericFutures g ON p.GenericID = g.GenericID
            WHERE p.DataType = 'Generic'
                AND p.TradeDate >= '2025-07-07'
            GROUP BY g.ExchangeCode, p.TradeDate
        )
        SELECT 
            e.ExchangeCode,
            a.TradeDate,
            e.銘柄数 as 全銘柄数,
            ISNULL(a.実績銘柄数, 0) as 実績銘柄数,
            e.銘柄数 - ISNULL(a.実績銘柄数, 0) as 欠落数
        FROM ExpectedRecords e
        CROSS JOIN (SELECT DISTINCT TradeDate FROM T_CommodityPrice_V2 WHERE TradeDate >= '2025-07-07') d
        LEFT JOIN ActualRecords a ON e.ExchangeCode = a.ExchangeCode AND d.TradeDate = a.TradeDate
        ORDER BY e.ExchangeCode, d.TradeDate DESC
    """
    df = pd.read_sql(query, conn)
    df['ExchangeCode'] = df['ExchangeCode'].apply(lambda x: 'COMEX' if x == 'CMX' else x)
    print(df.to_string(index=False))
    
    # 2. CMXで欠落している具体的な銘柄（2025-07-08）
    print("\n2. CMX/COMEXで2025-07-08に欠落している銘柄:")
    query = """
        SELECT 
            g.GenericNumber,
            g.GenericTicker,
            CASE WHEN p.GenericID IS NULL THEN '欠落' ELSE '存在' END as 状態
        FROM M_GenericFutures g
        LEFT JOIN T_CommodityPrice_V2 p 
            ON g.GenericID = p.GenericID 
            AND p.TradeDate = '2025-07-08'
            AND p.DataType = 'Generic'
        WHERE g.ExchangeCode = 'CMX'
            AND g.GenericNumber <= 27
        ORDER BY g.GenericNumber
    """
    df = pd.read_sql(query, conn)
    print(df.to_string(index=False))
    
    # 3. Volume onlyレコードの可能性を確認
    print("\n3. 取引がある日と価格がある日の比較（CMX）:")
    query = """
        SELECT 
            g.GenericNumber,
            g.GenericTicker,
            MAX(CASE WHEN p.Volume IS NOT NULL THEN p.TradeDate END) as 最終取引日,
            MAX(CASE WHEN p.SettlementPrice IS NOT NULL THEN p.TradeDate END) as 最終価格日,
            CASE 
                WHEN MAX(CASE WHEN p.Volume IS NOT NULL THEN p.TradeDate END) > 
                     MAX(CASE WHEN p.SettlementPrice IS NOT NULL THEN p.TradeDate END)
                THEN '○'
                ELSE '×'
            END as Volume_Only可能性
        FROM M_GenericFutures g
        LEFT JOIN T_CommodityPrice_V2 p ON g.GenericID = p.GenericID
        WHERE g.ExchangeCode = 'CMX'
            AND g.GenericNumber <= 15
        GROUP BY g.GenericNumber, g.GenericTicker
        ORDER BY g.GenericNumber
    """
    df = pd.read_sql(query, conn)
    print(df.to_string(index=False))
    
    conn.close()
    print("\n=== 確認完了 ===")

if __name__ == "__main__":
    check_missing_records()