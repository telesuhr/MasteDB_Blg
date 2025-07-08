"""
GenericIDの確認
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

def check_generic_ids():
    """GenericID確認"""
    print("=== GenericID確認 ===")
    
    try:
        conn = pyodbc.connect(CONNECTION_STRING)
        cursor = conn.cursor()
        
        # 全体のGenericID範囲
        cursor.execute("SELECT MIN(GenericID), MAX(GenericID), COUNT(*) FROM M_GenericFutures")
        result = cursor.fetchone()
        print(f"\nGenericID範囲: {result[0]} - {result[1]} (合計{result[2]}銘柄)")
        
        # 取引所別のGenericID範囲
        print("\n取引所別GenericID範囲:")
        cursor.execute("""
            SELECT 
                ExchangeCode,
                MIN(GenericID) as 最小ID,
                MAX(GenericID) as 最大ID,
                COUNT(*) as 銘柄数
            FROM M_GenericFutures
            GROUP BY ExchangeCode
            ORDER BY MIN(GenericID)
        """)
        
        for row in cursor.fetchall():
            display_name = "COMEX" if row[0] == "CMX" else row[0]
            print(f"  {display_name}: GenericID {row[1]}-{row[2]} ({row[3]}銘柄)")
        
        # 各取引所の最初と最後の銘柄
        print("\n各取引所の銘柄例:")
        for exchange in ['LME', 'CMX', 'SHFE']:
            cursor.execute("""
                SELECT TOP 3 GenericID, GenericTicker, GenericNumber
                FROM M_GenericFutures
                WHERE ExchangeCode = ?
                ORDER BY GenericNumber
            """, (exchange,))
            
            display_name = "COMEX" if exchange == "CMX" else exchange
            print(f"\n{display_name} (最初の3銘柄):")
            for row in cursor.fetchall():
                print(f"  GenericID={row[0]}: {row[1]} (#{row[2]})")
            
            cursor.execute("""
                SELECT TOP 3 GenericID, GenericTicker, GenericNumber
                FROM M_GenericFutures
                WHERE ExchangeCode = ?
                ORDER BY GenericNumber DESC
            """, (exchange,))
            
            print(f"{display_name} (最後の3銘柄):")
            for row in cursor.fetchall():
                print(f"  GenericID={row[0]}: {row[1]} (#{row[2]})")
        
        # GenericIDの連続性チェック
        print("\n\nGenericIDの連続性チェック:")
        cursor.execute("""
            SELECT GenericID, GenericTicker, ExchangeCode
            FROM M_GenericFutures
            ORDER BY GenericID
        """)
        
        all_records = cursor.fetchall()
        prev_id = None
        gaps = []
        
        for row in all_records:
            if prev_id is not None and row[0] != prev_id + 1:
                gaps.append((prev_id, row[0]))
            prev_id = row[0]
        
        if gaps:
            print(f"  ギャップあり: {len(gaps)}箇所")
            for gap in gaps[:5]:  # 最初の5つ
                print(f"    ID {gap[0]} の次が ID {gap[1]}")
        else:
            print("  ギャップなし（連続している）")
        
        conn.close()
        
    except Exception as e:
        print(f"エラー: {e}")

if __name__ == "__main__":
    check_generic_ids()