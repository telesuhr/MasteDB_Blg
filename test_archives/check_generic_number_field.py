"""
GenericNumberフィールドの確認
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

def check_generic_number():
    """GenericNumberフィールド確認"""
    print("=== M_GenericFuturesのGenericNumberフィールド確認 ===")
    
    try:
        conn = pyodbc.connect(CONNECTION_STRING)
        
        # 各取引所のGenericNumberを確認
        query = """
            SELECT 
                GenericID as 主キー,
                GenericTicker as ティッカー,
                ExchangeCode as 取引所,
                GenericNumber as 第N限月
            FROM M_GenericFutures
            WHERE GenericNumber <= 5 OR GenericNumber >= 57
            ORDER BY ExchangeCode, GenericNumber
        """
        
        df = pd.read_sql(query, conn)
        print("\n各取引所の最初と最後の銘柄:")
        print(df.to_string(index=False))
        
        print("\n\n重要な理解:")
        print("- GenericID: データベースの主キー（1-108の連番）")
        print("- GenericNumber: 第N限月を表す番号")
        print("  - LME: 1-60 (LP1-LP60)")
        print("  - SHFE: 1-12 (CU1-CU12)")
        print("  - CMX: 1-36 (HG1-HG36)")
        
        # 統計情報
        cursor = conn.cursor()
        cursor.execute("""
            SELECT 
                ExchangeCode,
                MIN(GenericNumber) as 最小,
                MAX(GenericNumber) as 最大,
                COUNT(*) as 銘柄数
            FROM M_GenericFutures
            GROUP BY ExchangeCode
            ORDER BY ExchangeCode
        """)
        
        print("\n\n各取引所のGenericNumber範囲:")
        for row in cursor.fetchall():
            display_name = "COMEX" if row[0] == "CMX" else row[0]
            print(f"{display_name}: {row[1]}-{row[2]} ({row[3]}銘柄)")
        
        conn.close()
        
    except Exception as e:
        print(f"エラー: {e}")

if __name__ == "__main__":
    check_generic_number()