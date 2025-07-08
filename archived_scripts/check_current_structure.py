"""
現在のデータ構造確認
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

def check_structure():
    """現在の構造確認"""
    print("=== 現在のデータ構造確認 ===")
    
    try:
        conn = pyodbc.connect(CONNECTION_STRING)
        
        # M_GenericFuturesの内容確認
        query = """
            SELECT 
                GenericID,
                GenericTicker,
                ExchangeCode,
                GenericNumber
            FROM M_GenericFutures
            WHERE GenericNumber IN (1, 2, 3)
            ORDER BY ExchangeCode, GenericNumber
        """
        
        df = pd.read_sql(query, conn)
        print("\n各取引所の最初の3銘柄:")
        print(df.to_string(index=False))
        
        print("\n\nユーザーの期待:")
        print("LP1: GenericID=1")
        print("LP2: GenericID=2")
        print("CU1: GenericID=1")
        print("CU2: GenericID=2")
        print("HG1: GenericID=1")
        print("HG2: GenericID=2")
        
        print("\n現在の実装:")
        print("LP1: GenericID=1, GenericNumber=1")
        print("LP2: GenericID=2, GenericNumber=2")
        print("CU1: GenericID=37, GenericNumber=1")
        print("CU2: GenericID=38, GenericNumber=2")
        print("HG1: GenericID=49, GenericNumber=1")
        print("HG2: GenericID=50, GenericNumber=2")
        
        print("\n\n問題点:")
        print("GenericIDは主キー（PRIMARY KEY）のため、取引所間で重複できません。")
        print("現在の設計では、GenericNumberがN番限月を表しています。")
        
        conn.close()
        
    except Exception as e:
        print(f"エラー: {e}")

if __name__ == "__main__":
    check_structure()