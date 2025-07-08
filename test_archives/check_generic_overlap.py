"""
GenericID重複の詳細確認
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

def check_overlap():
    """GenericID重複確認"""
    print("=== GenericID重複状況確認 ===")
    
    try:
        conn = pyodbc.connect(CONNECTION_STRING)
        
        # GenericIDが重複している範囲を確認
        query = """
            SELECT 
                GenericID,
                GenericTicker,
                ExchangeCode,
                GenericNumber,
                CreatedDate
            FROM M_GenericFutures
            WHERE GenericID BETWEEN 37 AND 48
            ORDER BY GenericID
        """
        
        df = pd.read_sql(query, conn)
        print(f"\nGenericID 37-48の範囲（重複範囲）:")
        print(df.to_string(index=False))
        
        # 作成日時で並べて確認
        print("\n\n作成日時順に確認:")
        cursor = conn.cursor()
        cursor.execute("""
            SELECT 
                ExchangeCode,
                MIN(CreatedDate) as 最初の作成日時,
                MAX(CreatedDate) as 最後の作成日時,
                COUNT(*) as 銘柄数
            FROM M_GenericFutures
            GROUP BY ExchangeCode
            ORDER BY MIN(CreatedDate)
        """)
        
        for row in cursor.fetchall():
            display_name = "COMEX" if row[0] == "CMX" else row[0]
            print(f"{display_name}: {row[1]} - {row[2]} ({row[3]}銘柄)")
        
        # 重複を解消する必要があるデータを確認
        print("\n\n取引所別のGenericNumber分布:")
        cursor.execute("""
            SELECT 
                ExchangeCode,
                MIN(GenericNumber) as 最小番号,
                MAX(GenericNumber) as 最大番号,
                COUNT(DISTINCT GenericNumber) as ユニーク番号数,
                COUNT(*) as レコード数
            FROM M_GenericFutures
            GROUP BY ExchangeCode
        """)
        
        for row in cursor.fetchall():
            display_name = "COMEX" if row[0] == "CMX" else row[0]
            print(f"{display_name}: 番号 {row[1]}-{row[2]}, {row[3]}種類, {row[4]}レコード")
        
        conn.close()
        
    except Exception as e:
        print(f"エラー: {e}")

if __name__ == "__main__":
    check_overlap()