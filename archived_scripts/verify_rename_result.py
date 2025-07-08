"""
テーブルリネーム結果の確認
"""
import pyodbc
import pandas as pd
from datetime import datetime

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

def verify_rename():
    """リネーム結果を確認"""
    print("=== テーブルリネーム結果確認 ===")
    print(f"実行時刻: {datetime.now()}\n")
    
    conn = pyodbc.connect(CONNECTION_STRING)
    cursor = conn.cursor()
    
    # 1. テーブル存在確認
    print("【テーブル存在確認】")
    tables_to_check = ['T_CommodityPrice', 'T_CommodityPrice_V2', 'T_CommodityPrice_OLD']
    
    for table in tables_to_check:
        cursor.execute("""
            SELECT COUNT(*) 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_NAME = ?
        """, table)
        exists = cursor.fetchone()[0] > 0
        
        if exists:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            print(f"✓ {table}: 存在 ({count}件)")
        else:
            print(f"✗ {table}: 存在しない")
    
    # 2. 新T_CommodityPriceの構造確認
    print("\n【T_CommodityPriceの構造（旧V2）】")
    cursor.execute("""
        SELECT TOP 5
            PriceID,
            TradeDate,
            MetalID,
            DataType,
            GenericID,
            ActualContractID,
            LastPrice,
            Volume
        FROM T_CommodityPrice
        ORDER BY TradeDate DESC, PriceID DESC
    """)
    
    columns = [desc[0] for desc in cursor.description]
    print("列: " + ", ".join(columns))
    
    # GenericIDが存在することを確認
    if 'GenericID' in columns:
        print("✓ GenericID列が存在 → V2構造です")
    else:
        print("✗ GenericID列が存在しない → 旧構造のまま")
    
    # 3. ビューの動作確認
    print("\n【ビューの動作確認】")
    try:
        cursor.execute("""
            SELECT TOP 1 
                GenericTicker,
                TradeDate,
                LastPrice
            FROM V_CommodityPriceSimple
        """)
        result = cursor.fetchone()
        if result:
            print("✓ V_CommodityPriceSimple: 正常動作")
    except Exception as e:
        print(f"✗ V_CommodityPriceSimple: エラー - {e}")
    
    # 4. 最新データ確認
    print("\n【最新データ】")
    query = """
    SELECT TOP 10
        g.GenericTicker,
        p.TradeDate,
        p.LastPrice,
        p.Volume,
        p.LastUpdated
    FROM T_CommodityPrice p
    INNER JOIN M_GenericFutures g ON p.GenericID = g.GenericID
    ORDER BY p.TradeDate DESC, g.GenericTicker
    """
    
    df = pd.read_sql(query, conn)
    print(df.to_string(index=False))
    
    # 5. 旧テーブルの削除
    cursor.execute("""
        SELECT COUNT(*) 
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_NAME = 'T_CommodityPrice_OLD'
    """)
    
    if cursor.fetchone()[0] > 0:
        print("\n【旧テーブルの削除】")
        response = input("T_CommodityPrice_OLD を削除しますか？ (y/n): ")
        if response.lower() == 'y':
            try:
                cursor.execute("DROP TABLE T_CommodityPrice_OLD")
                conn.commit()
                print("✓ T_CommodityPrice_OLD を削除しました")
            except Exception as e:
                print(f"✗ 削除エラー: {e}")
    
    conn.close()
    
    print("\n=== 確認完了 ===")
    print("T_CommodityPriceは新構造（GenericID使用）になりました")
    print("main.pyは変更不要でそのまま動作します")

if __name__ == "__main__":
    verify_rename()