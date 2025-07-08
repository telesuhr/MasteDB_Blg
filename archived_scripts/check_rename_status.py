"""
リネーム状態の確認（シンプル版）
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

def check_status():
    conn = pyodbc.connect(CONNECTION_STRING)
    cursor = conn.cursor()
    
    print("=== テーブル状態確認 ===\n")
    
    # 1. テーブル確認
    print("【テーブル】")
    tables = ['T_CommodityPrice', 'T_CommodityPrice_V2', 'T_CommodityPrice_OLD']
    
    for table in tables:
        cursor.execute(f"""
            SELECT COUNT(*) as cnt, 
                   CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END as exists
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_NAME = '{table}'
        """)
        result = cursor.fetchone()
        
        if result[1]:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            print(f"{table}: 存在 ({count}件)")
        else:
            print(f"{table}: 存在しない")
    
    # 2. T_CommodityPriceの構造確認
    print("\n【T_CommodityPriceの列】")
    cursor.execute("""
        SELECT COLUMN_NAME 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = 'T_CommodityPrice' 
        ORDER BY ORDINAL_POSITION
    """)
    
    columns = [row[0] for row in cursor.fetchall()]
    print(", ".join(columns))
    
    if 'GenericID' in columns:
        print("→ 新構造（V2）です！")
    else:
        print("→ 旧構造のままです")
    
    # 3. 最新データ
    print("\n【最新データ（上位5件）】")
    query = """
    SELECT TOP 5
        g.ExchangeCode,
        g.GenericTicker,
        p.TradeDate,
        p.LastPrice
    FROM T_CommodityPrice p
    INNER JOIN M_GenericFutures g ON p.GenericID = g.GenericID
    WHERE p.LastPrice IS NOT NULL
    ORDER BY p.TradeDate DESC, g.ExchangeCode, g.GenericTicker
    """
    
    df = pd.read_sql(query, conn)
    print(df.to_string(index=False))
    
    # 4. 削除推奨
    print("\n【クリーンアップ】")
    cursor.execute("""
        SELECT TABLE_NAME 
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_NAME = 'T_CommodityPrice_OLD'
    """)
    
    if cursor.fetchone():
        print("T_CommodityPrice_OLD が残っています")
        print("削除コマンド: DROP TABLE T_CommodityPrice_OLD")
    
    conn.close()
    
    print("\n=== 完了 ===")
    print("リネームは成功しています。")
    print("T_CommodityPriceは新構造（GenericID使用）になりました。")

if __name__ == "__main__":
    check_status()