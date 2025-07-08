"""
テーブル内容確認
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

def check_tables():
    """テーブル内容確認"""
    print("=== テーブル内容確認 ===")
    
    try:
        conn = pyodbc.connect(CONNECTION_STRING)
        cursor = conn.cursor()
        
        # M_GenericFuturesのレコード数
        cursor.execute("SELECT COUNT(*) FROM M_GenericFutures")
        count = cursor.fetchone()[0]
        print(f"\nM_GenericFuturesレコード数: {count}")
        
        if count == 0:
            print("テーブルが空です。データを再投入する必要があります。")
        else:
            # カラム情報確認
            cursor.execute("""
                SELECT COLUMN_NAME, DATA_TYPE 
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_NAME = 'M_GenericFutures'
                ORDER BY ORDINAL_POSITION
            """)
            print("\nM_GenericFuturesのカラム:")
            for row in cursor.fetchall():
                print(f"  {row[0]}: {row[1]}")
            
            # 最初の5件表示
            cursor.execute("""
                SELECT TOP 5 
                    GenericID, 
                    GenericTicker, 
                    ExchangeCode,
                    GenericNumber
                FROM M_GenericFutures 
                ORDER BY ExchangeCode, GenericNumber
            """)
            print("\n最初の5件:")
            for row in cursor.fetchall():
                print(f"  GenericID={row[0]}, {row[1]}, {row[2]}, GenericNumber={row[3]}")
        
        conn.close()
        
    except Exception as e:
        print(f"エラー: {e}")

if __name__ == "__main__":
    check_tables()