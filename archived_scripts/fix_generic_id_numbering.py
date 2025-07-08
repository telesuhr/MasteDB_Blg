"""
GenericIDを各取引所で独立した番号体系に修正
LME: LP1-LP60 → GenericID 1-60
SHFE: CU1-CU12 → GenericID 1-12
CMX: HG1-HG36 → GenericID 1-36
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

def fix_generic_id_numbering():
    """GenericID番号体系修正"""
    print("=== GenericID番号体系修正 ===")
    print("目標:")
    print("  LME: LP1-LP60 → GenericID 1-60")
    print("  SHFE: CU1-CU12 → GenericID 1-12")
    print("  CMX: HG1-HG36 → GenericID 1-36")
    
    try:
        conn = pyodbc.connect(CONNECTION_STRING)
        cursor = conn.cursor()
        
        # 現在の状況確認
        print("\n現在の状況:")
        cursor.execute("""
            SELECT 
                ExchangeCode,
                MIN(GenericID) as 最小ID,
                MAX(GenericID) as 最大ID,
                COUNT(*) as 銘柄数
            FROM M_GenericFutures
            GROUP BY ExchangeCode
            ORDER BY ExchangeCode
        """)
        
        for row in cursor.fetchall():
            display_name = "COMEX" if row[0] == "CMX" else row[0]
            print(f"  {display_name}: GenericID {row[1]}-{row[2]} ({row[3]}銘柄)")
        
        # GenericIDが主キーで外部キー制約もあるため、
        # 新しい構造で再作成する必要がある
        
        print("\n警告: この操作により既存のデータがすべて削除されます。")
        
        # 1. 既存データのクリーンアップ
        print("\n1. 既存データクリーンアップ...")
        
        # 外部キー制約のため、逆順で削除
        cursor.execute("DELETE FROM T_CommodityPrice_V2")
        price_deleted = cursor.rowcount
        
        cursor.execute("DELETE FROM T_GenericContractMapping")
        mapping_deleted = cursor.rowcount
        
        cursor.execute("DELETE FROM M_ActualContract")
        contract_deleted = cursor.rowcount
        
        cursor.execute("DELETE FROM M_GenericFutures")
        generic_deleted = cursor.rowcount
        
        print(f"  価格データ: {price_deleted}件削除")
        print(f"  マッピング: {mapping_deleted}件削除")
        print(f"  実契約: {contract_deleted}件削除")
        print(f"  ジェネリック先物: {generic_deleted}件削除")
        
        # 2. M_GenericFuturesの再構築
        print("\n2. M_GenericFutures再構築...")
        
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
        
        created_count = 0
        
        # LME: LP1-LP60 (GenericID 1-60)
        print("  LME作成中...")
        for i in range(1, 61):
            ticker = f'LP{i} Comdty'
            description = f'LME Copper Generic {i}{"st" if i == 1 else "nd" if i == 2 else "rd" if i == 3 else "th"} Future'
            
            cursor.execute("""
                INSERT INTO M_GenericFutures (
                    GenericID, GenericTicker, MetalID, ExchangeCode, 
                    GenericNumber, Description, IsActive
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (i, ticker, metal_ids['LME'], 'LME', i, description, 1))
            created_count += 1
        
        # SHFE: CU1-CU12 (GenericID 1-12)
        print("  SHFE作成中...")
        for i in range(1, 13):
            ticker = f'CU{i} Comdty'
            description = f'SHFE Copper Generic {i}{"st" if i == 1 else "nd" if i == 2 else "rd" if i == 3 else "th"} Future'
            
            cursor.execute("""
                INSERT INTO M_GenericFutures (
                    GenericID, GenericTicker, MetalID, ExchangeCode, 
                    GenericNumber, Description, IsActive
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (i, ticker, metal_ids['SHFE'], 'SHFE', i, description, 1))
            created_count += 1
        
        # CMX: HG1-HG36 (GenericID 1-36)
        print("  COMEX作成中...")
        for i in range(1, 37):
            ticker = f'HG{i} Comdty'
            description = f'COMEX Copper Generic {i}{"st" if i == 1 else "nd" if i == 2 else "rd" if i == 3 else "th"} Future'
            
            cursor.execute("""
                INSERT INTO M_GenericFutures (
                    GenericID, GenericTicker, MetalID, ExchangeCode, 
                    GenericNumber, Description, IsActive
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (i, ticker, metal_ids['CMX'], 'CMX', i, description, 1))
            created_count += 1
        
        conn.commit()
        print(f"\n  合計{created_count}銘柄作成完了")
        
        # 3. 結果確認
        print("\n3. 修正後の状況:")
        cursor.execute("""
            SELECT 
                ExchangeCode,
                MIN(GenericID) as 最小ID,
                MAX(GenericID) as 最大ID,
                COUNT(*) as 銘柄数,
                COUNT(DISTINCT GenericID) as ユニークID数
            FROM M_GenericFutures
            GROUP BY ExchangeCode
            ORDER BY ExchangeCode
        """)
        
        for row in cursor.fetchall():
            display_name = "COMEX" if row[0] == "CMX" else row[0]
            print(f"  {display_name}: GenericID {row[1]}-{row[2]} ({row[3]}銘柄, {row[4]}ユニークID)")
        
        # 4. 重複確認
        print("\n4. GenericID重複確認:")
        cursor.execute("""
            SELECT GenericID, COUNT(*) as 出現回数
            FROM M_GenericFutures
            GROUP BY GenericID
            HAVING COUNT(*) > 1
            ORDER BY GenericID
        """)
        
        duplicates = cursor.fetchall()
        if duplicates:
            print(f"  重複あり: {len(duplicates)}個のGenericID")
            for row in duplicates[:5]:
                print(f"    GenericID {row[0]}: {row[1]}回出現")
        else:
            print("  重複なし - 修正失敗（GenericIDは取引所間で独立していない）")
        
        # 実際の構造確認
        print("\n5. 実際のテーブル構造確認:")
        cursor.execute("""
            SELECT TOP 5 GenericID, GenericTicker, ExchangeCode
            FROM M_GenericFutures
            WHERE ExchangeCode = 'LME'
            ORDER BY GenericNumber
        """)
        print("  LME:")
        for row in cursor.fetchall():
            print(f"    {row[1]}: GenericID={row[0]}")
        
        cursor.execute("""
            SELECT TOP 5 GenericID, GenericTicker, ExchangeCode
            FROM M_GenericFutures
            WHERE ExchangeCode = 'SHFE'
            ORDER BY GenericNumber
        """)
        print("  SHFE:")
        for row in cursor.fetchall():
            print(f"    {row[1]}: GenericID={row[0]}")
        
        cursor.execute("""
            SELECT TOP 5 GenericID, GenericTicker, ExchangeCode
            FROM M_GenericFutures
            WHERE ExchangeCode = 'CMX'
            ORDER BY GenericNumber
        """)
        print("  COMEX:")
        for row in cursor.fetchall():
            print(f"    {row[1]}: GenericID={row[0]}")
        
        conn.close()
        
        print("\n" + "=" * 50)
        print("注意: GenericIDは主キーのため、取引所間で重複できません。")
        print("現在の設計では、GenericIDはシステム全体でユニークである必要があります。")
        print("=" * 50)
        
        return True
        
    except Exception as e:
        print(f"エラー: {e}")
        return False

if __name__ == "__main__":
    fix_generic_id_numbering()