"""
M_GenericFuturesテーブルのデータ復元
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

def restore_generic_futures():
    """M_GenericFuturesデータ復元"""
    print("=== M_GenericFuturesデータ復元 ===")
    
    try:
        conn = pyodbc.connect(CONNECTION_STRING)
        cursor = conn.cursor()
        
        # MetalID取得
        cursor.execute("""
            SELECT MetalID, MetalCode
            FROM M_Metal
            WHERE MetalCode IN ('COPPER', 'CU_SHFE', 'CU_CMX')
        """)
        metal_ids = {}
        for row in cursor.fetchall():
            if row[1] == 'COPPER':
                metal_ids['LME'] = row[0]
            elif row[1] == 'CU_SHFE':
                metal_ids['SHFE'] = row[0]
            elif row[1] == 'CU_CMX':
                metal_ids['CMX'] = row[0]
        
        print(f"MetalID確認: LME={metal_ids.get('LME')}, SHFE={metal_ids.get('SHFE')}, CMX={metal_ids.get('CMX')}")
        
        created_count = 0
        
        # LME: LP1-LP60 (GenericNumber=1-60)
        print("\nLME作成中...")
        for i in range(1, 61):
            ticker = f'LP{i} Comdty'
            description = f'LME Copper Generic {i}{"st" if i == 1 else "nd" if i == 2 else "rd" if i == 3 else "th"} Future'
            
            cursor.execute("""
                INSERT INTO M_GenericFutures (
                    GenericTicker, MetalID, ExchangeCode, 
                    GenericNumber, Description, IsActive
                ) VALUES (?, ?, ?, ?, ?, ?)
            """, (ticker, metal_ids['LME'], 'LME', i, description, 1))
            created_count += 1
        print(f"  LME: {60}銘柄作成")
        
        # SHFE: CU1-CU12 (GenericNumber=1-12)
        print("SHFE作成中...")
        for i in range(1, 13):
            ticker = f'CU{i} Comdty'
            description = f'SHFE Copper Generic {i}{"st" if i == 1 else "nd" if i == 2 else "rd" if i == 3 else "th"} Future'
            
            cursor.execute("""
                INSERT INTO M_GenericFutures (
                    GenericTicker, MetalID, ExchangeCode, 
                    GenericNumber, Description, IsActive
                ) VALUES (?, ?, ?, ?, ?, ?)
            """, (ticker, metal_ids['SHFE'], 'SHFE', i, description, 1))
            created_count += 1
        print(f"  SHFE: {12}銘柄作成")
        
        # CMX: HG1-HG36 (GenericNumber=1-36)
        print("COMEX作成中...")
        for i in range(1, 37):
            ticker = f'HG{i} Comdty'
            description = f'COMEX Copper Generic {i}{"st" if i == 1 else "nd" if i == 2 else "rd" if i == 3 else "th"} Future'
            
            cursor.execute("""
                INSERT INTO M_GenericFutures (
                    GenericTicker, MetalID, ExchangeCode, 
                    GenericNumber, Description, IsActive
                ) VALUES (?, ?, ?, ?, ?, ?)
            """, (ticker, metal_ids['CMX'], 'CMX', i, description, 1))
            created_count += 1
        print(f"  COMEX: {36}銘柄作成")
        
        conn.commit()
        print(f"\n合計{created_count}銘柄作成完了")
        
        # 結果確認
        print("\n=== 作成結果確認 ===")
        
        # GenericIDとGenericNumberの確認
        cursor.execute("""
            SELECT 
                ExchangeCode,
                MIN(GenericID) as 最小ID,
                MAX(GenericID) as 最大ID,
                MIN(GenericNumber) as 最小Number,
                MAX(GenericNumber) as 最大Number,
                COUNT(*) as 銘柄数
            FROM M_GenericFutures
            GROUP BY ExchangeCode
            ORDER BY MIN(GenericID)
        """)
        
        print("\n各取引所の範囲:")
        for row in cursor.fetchall():
            display_name = "COMEX" if row[0] == "CMX" else row[0]
            print(f"  {display_name}:")
            print(f"    GenericID: {row[1]}-{row[2]}")
            print(f"    GenericNumber: {row[3]}-{row[4]}")
            print(f"    銘柄数: {row[5]}")
        
        # サンプル表示
        print("\n各取引所の最初の3銘柄:")
        for exchange in ['LME', 'SHFE', 'CMX']:
            cursor.execute("""
                SELECT TOP 3 GenericID, GenericTicker, GenericNumber
                FROM M_GenericFutures
                WHERE ExchangeCode = ?
                ORDER BY GenericNumber
            """, (exchange,))
            
            display_name = "COMEX" if exchange == "CMX" else exchange
            print(f"\n{display_name}:")
            for row in cursor.fetchall():
                print(f"  {row[1]}: GenericID={row[0]}, GenericNumber={row[2]}")
        
        conn.close()
        
        print("\n" + "=" * 50)
        print("重要な理解:")
        print("- GenericID: 自動採番される主キー（1から連番）")
        print("- GenericNumber: 第N限月を表す番号（各取引所で1から）")
        print("=" * 50)
        
        return True
        
    except Exception as e:
        print(f"エラー: {e}")
        return False

if __name__ == "__main__":
    restore_generic_futures()