"""
LMEの満期計算問題を調査
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
cursor = conn.cursor()

print("=== M_GenericFuturesテーブル構造確認 ===")

# カラム一覧
cursor.execute("""
SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'M_GenericFutures'
ORDER BY ORDINAL_POSITION
""")

print("\nM_GenericFutures columns:")
for row in cursor.fetchall():
    col_name = row[0]
    data_type = row[1]
    max_len = f"({row[2]})" if row[2] else ""
    print(f"  {col_name}: {data_type}{max_len}")

# 満期関連カラムの存在確認
print("\n=== 満期関連カラムの確認 ===")
cursor.execute("""
SELECT COUNT(*) 
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'M_GenericFutures'
    AND COLUMN_NAME IN ('MaturityRule', 'RolloverDays', 'LastTradingDayRule')
""")
count = cursor.fetchone()[0]
print(f"満期関連カラム数: {count}")

if count > 0:
    # LMEデータ確認
    print("\n=== LMEデータの確認 ===")
    cursor.execute("""
    SELECT 
        GenericTicker,
        ExchangeCode,
        GenericNumber,
        MaturityRule,
        RolloverDays,
        LastTradingDayRule
    FROM M_GenericFutures
    WHERE ExchangeCode = 'LME'
    ORDER BY GenericNumber
    """)
    
    df = pd.DataFrame.from_records(
        cursor.fetchall(),
        columns=['GenericTicker', 'ExchangeCode', 'GenericNumber', 
                 'MaturityRule', 'RolloverDays', 'LastTradingDayRule']
    )
    print(df.to_string(index=False))
    
    # 満期計算のテスト
    print("\n=== 満期計算テスト（LME） ===")
    cursor.execute("""
    SELECT TOP 10
        GenericTicker,
        GenericNumber,
        MaturityRule,
        dbo.CalculateMaturityDate('2025-07-08', MaturityRule) as CalculatedMaturity
    FROM M_GenericFutures
    WHERE ExchangeCode = 'LME'
    ORDER BY GenericNumber
    """)
    
    print("Ticker         Number  Rule              CalculatedMaturity")
    print("-" * 60)
    for row in cursor.fetchall():
        print(f"{row[0]:<15} {row[1]:<6} {row[2] or 'NULL':<20} {row[3] or 'NULL'}")

# LMEの実際のデータ確認
print("\n=== LMEの価格データ確認 ===")
cursor.execute("""
SELECT 
    g.GenericTicker,
    g.GenericNumber,
    COUNT(p.TradeDate) as DataCount,
    MIN(p.TradeDate) as MinDate,
    MAX(p.TradeDate) as MaxDate
FROM M_GenericFutures g
LEFT JOIN T_CommodityPrice_V2 p ON g.GenericID = p.GenericID
WHERE g.ExchangeCode = 'LME'
GROUP BY g.GenericTicker, g.GenericNumber
ORDER BY g.GenericNumber
""")

df = pd.DataFrame.from_records(
    cursor.fetchall(),
    columns=['GenericTicker', 'GenericNumber', 'DataCount', 'MinDate', 'MaxDate']
)
print(df.to_string(index=False))

conn.close()