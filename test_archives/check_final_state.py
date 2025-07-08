"""
最終データ状態確認
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

conn = pyodbc.connect(CONNECTION_STRING)

print("=== 最終データ状態 ===")

# 取引所別サマリー
query1 = """
SELECT 
    g.ExchangeCode,
    COUNT(DISTINCT p.TradeDate) as 営業日数,
    COUNT(DISTINCT p.GenericID) as 銘柄数,
    COUNT(*) as レコード数,
    SUM(CASE WHEN p.SettlementPrice IS NULL AND p.LastPrice IS NULL THEN 1 ELSE 0 END) as VolumeOnly数
FROM T_CommodityPrice_V2 p
JOIN M_GenericFutures g ON p.GenericID = g.GenericID
WHERE p.DataType = 'Generic'
GROUP BY g.ExchangeCode
ORDER BY g.ExchangeCode
"""

df = pd.read_sql(query1, conn)
print("\n取引所別サマリー:")
print(df.to_string(index=False))

# Volume-onlyレコードの詳細
query2 = """
SELECT TOP 5
    g.GenericTicker,
    p.TradeDate,
    p.Volume,
    p.OpenInterest
FROM T_CommodityPrice_V2 p
JOIN M_GenericFutures g ON p.GenericID = g.GenericID
WHERE p.SettlementPrice IS NULL 
    AND p.LastPrice IS NULL
    AND p.DataType = 'Generic'
    AND p.Volume IS NOT NULL
ORDER BY p.TradeDate DESC, g.GenericNumber
"""

df2 = pd.read_sql(query2, conn)
print("\nVolume-onlyレコードの例:")
print(df2.to_string(index=False))

conn.close()