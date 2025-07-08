"""
T_CommodityPrice_V2テーブルの状況確認
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

def check_table_status():
    """テーブル状況確認"""
    print("=== T_CommodityPrice_V2テーブル状況確認 ===")
    
    conn = pyodbc.connect(CONNECTION_STRING)
    cursor = conn.cursor()
    
    # 1. テーブル存在確認
    print("\n1. テーブル存在確認:")
    cursor.execute("""
        SELECT COUNT(*) 
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_NAME = 'T_CommodityPrice_V2'
    """)
    exists = cursor.fetchone()[0]
    print(f"T_CommodityPrice_V2テーブル: {'存在する' if exists else '存在しない'}")
    
    if exists:
        # 2. カラム情報
        print("\n2. カラム情報:")
        cursor.execute("""
            SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = 'T_CommodityPrice_V2'
            ORDER BY ORDINAL_POSITION
        """)
        for col in cursor.fetchall():
            print(f"  {col[0]}: {col[1]} ({col[2]})")
        
        # 3. 制約情報
        print("\n3. CHECK制約:")
        cursor.execute("""
            SELECT 
                cc.CONSTRAINT_NAME,
                cc.CHECK_CLAUSE
            FROM INFORMATION_SCHEMA.CHECK_CONSTRAINTS cc
            JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                ON cc.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
            WHERE tc.TABLE_NAME = 'T_CommodityPrice_V2'
        """)
        for constraint in cursor.fetchall():
            print(f"  {constraint[0]}: {constraint[1]}")
        
        # 4. データ件数
        cursor.execute("SELECT COUNT(*) FROM T_CommodityPrice_V2")
        count = cursor.fetchone()[0]
        print(f"\n4. データ件数: {count}")
        
        # 5. M_GenericFuturesの状況
        print("\n5. M_GenericFuturesテーブル:")
        cursor.execute("""
            SELECT 
                ExchangeCode,
                COUNT(*) as 銘柄数,
                MIN(GenericNumber) as 最小番号,
                MAX(GenericNumber) as 最大番号
            FROM M_GenericFutures
            GROUP BY ExchangeCode
            ORDER BY ExchangeCode
        """)
        for row in cursor.fetchall():
            exchange = 'COMEX' if row[0] == 'CMX' else row[0]
            print(f"  {exchange}: {row[1]}銘柄 (Generic {row[2]}-{row[3]})")
        
        # 6. 最新のエラーログ確認
        print("\n6. 最新のデータ確認:")
        cursor.execute("""
            SELECT TOP 5
                TradeDate,
                DataType,
                GenericID,
                ActualContractID,
                SettlementPrice
            FROM T_CommodityPrice_V2
            ORDER BY CreatedAt DESC
        """)
        recent_data = cursor.fetchall()
        if recent_data:
            print("  最新データあり:")
            for row in recent_data:
                print(f"    {row[0]}, Type={row[1]}, GenericID={row[2]}, ActualID={row[3]}, Price={row[4]}")
        else:
            print("  データなし")
    
    conn.close()
    print("\n=== 確認完了 ===")

if __name__ == "__main__":
    check_table_status()