"""
HG5の状態確認
"""
import pyodbc

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

# CMX HG5の状況確認
cursor.execute("""
SELECT 
    g.GenericNumber,
    g.GenericTicker,
    p.TradeDate,
    p.SettlementPrice,
    p.LastPrice,
    p.OpenPrice,
    p.Volume,
    p.OpenInterest
FROM M_GenericFutures g
LEFT JOIN T_CommodityPrice_V2 p ON g.GenericID = p.GenericID AND p.TradeDate = '2025-07-08'
WHERE g.GenericTicker = 'HG5 Comdty'
""")

print("HG5 Comdty on 2025-07-08:")
row = cursor.fetchone()
if row:
    print(f"  GenericNumber: {row[0]}")
    print(f"  Ticker: {row[1]}") 
    print(f"  TradeDate: {row[2]}")
    print(f"  SettlementPrice: {row[3]}")
    print(f"  LastPrice: {row[4]}")
    print(f"  OpenPrice: {row[5]}")
    print(f"  Volume: {row[6]}")
    print(f"  OpenInterest: {row[7]}")
    
    if row[2] is None:
        print("  → データベースに存在しない（未保存）")
else:
    print("  No data found")

conn.close()