"""
不足銘柄の確認とM_GenericFuturesテーブルへの追加
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

def check_and_add_missing_tickers():
    """不足銘柄の確認と追加"""
    print("=== 不足銘柄の確認と追加 ===")
    
    try:
        conn = pyodbc.connect(CONNECTION_STRING)
        cursor = conn.cursor()
        
        # 期待する銘柄数
        expected_tickers = {
            'LME': [f'LP{i} Comdty' for i in range(1, 61)],     # LP1-LP60 (60銘柄)
            'SHFE': [f'CU{i} Comdty' for i in range(1, 13)],   # CU1-CU12 (12銘柄)
            'CMX': [f'HG{i} Comdty' for i in range(1, 37)]     # HG1-HG36 (36銘柄)
        }
        
        print("期待銘柄数:")
        for exchange, tickers in expected_tickers.items():
            display_name = "COMEX" if exchange == "CMX" else exchange
            print(f"  {display_name}: {len(tickers)}銘柄")
        
        # 各取引所の現在の銘柄数確認
        print("\n現在のM_GenericFutures状況:")
        for exchange in expected_tickers.keys():
            cursor.execute("""
                SELECT COUNT(*), MIN(GenericNumber), MAX(GenericNumber)
                FROM M_GenericFutures 
                WHERE ExchangeCode = ?
            """, (exchange,))
            result = cursor.fetchone()
            
            actual_count = result[0]
            min_num = result[1] if result[1] else 0
            max_num = result[2] if result[2] else 0
            expected_count = len(expected_tickers[exchange])
            
            display_name = "COMEX" if exchange == "CMX" else exchange
            status = "OK" if actual_count == expected_count else "NG"
            print(f"  {display_name}: {actual_count}/{expected_count}銘柄 {status} (番号: {min_num}-{max_num})")
        
        # MetalIDの確認
        print("\nMetalID確認:")
        cursor.execute("""
            SELECT MetalID, MetalCode, MetalName
            FROM M_Metal
            WHERE MetalCode IN ('COPPER', 'CU_SHFE', 'CU_CMX')
            ORDER BY MetalID
        """)
        metal_results = cursor.fetchall()
        
        metal_ids = {}
        for row in metal_results:
            print(f"  MetalID {row[0]}: {row[1]} ({row[2]})")
            if row[1] == 'COPPER':
                metal_ids['LME'] = row[0]
            elif row[1] == 'CU_SHFE':
                metal_ids['SHFE'] = row[0]
            elif row[1] == 'CU_CMX':
                metal_ids['CMX'] = row[0]
        
        # 不足銘柄の追加
        print("\n不足銘柄の追加:")
        total_added = 0
        
        for exchange, expected_list in expected_tickers.items():
            if exchange not in metal_ids:
                print(f"  {exchange}: MetalIDが見つかりません")
                continue
                
            metal_id = metal_ids[exchange]
            display_name = "COMEX" if exchange == "CMX" else exchange
            
            # 既存銘柄の取得
            cursor.execute("""
                SELECT GenericTicker
                FROM M_GenericFutures 
                WHERE ExchangeCode = ?
            """, (exchange,))
            existing_tickers = set(row[0] for row in cursor.fetchall())
            
            # 不足銘柄の特定
            missing_tickers = []
            for ticker in expected_list:
                if ticker not in existing_tickers:
                    missing_tickers.append(ticker)
            
            if missing_tickers:
                print(f"  {display_name}: {len(missing_tickers)}銘柄を追加中...")
                
                for ticker in missing_tickers:
                    # GenericNumberを取得（LP1 -> 1, CU2 -> 2, HG3 -> 3）
                    if exchange == 'LME':
                        generic_number = int(ticker[2:-7])  # LP1 Comdty -> 1
                    elif exchange == 'SHFE':
                        generic_number = int(ticker[2:-7])  # CU1 Comdty -> 1
                    elif exchange == 'CMX':
                        generic_number = int(ticker[2:-7])  # HG1 Comdty -> 1
                    
                    description = f"{display_name} Copper Generic {generic_number}{'st' if generic_number == 1 else 'nd' if generic_number == 2 else 'rd' if generic_number == 3 else 'th'} Future"
                    
                    cursor.execute("""
                        INSERT INTO M_GenericFutures (
                            GenericTicker, MetalID, ExchangeCode, GenericNumber, 
                            Description, IsActive
                        ) VALUES (?, ?, ?, ?, ?, ?)
                    """, (ticker, metal_id, exchange, generic_number, description, 1))
                    
                    total_added += 1
                
                print(f"    {len(missing_tickers)}銘柄追加完了")
            else:
                print(f"  {display_name}: 追加不要（全銘柄存在）")
        
        conn.commit()
        
        # 最終確認
        print(f"\n=== 追加結果 ===")
        print(f"合計追加銘柄数: {total_added}")
        
        print("\n追加後のM_GenericFutures状況:")
        for exchange in expected_tickers.keys():
            cursor.execute("""
                SELECT COUNT(*)
                FROM M_GenericFutures 
                WHERE ExchangeCode = ?
            """, (exchange,))
            actual_count = cursor.fetchone()[0]
            expected_count = len(expected_tickers[exchange])
            
            display_name = "COMEX" if exchange == "CMX" else exchange
            status = "OK" if actual_count == expected_count else "NG"
            print(f"  {display_name}: {actual_count}/{expected_count}銘柄 {status}")
        
        conn.close()
        return total_added
        
    except Exception as e:
        print(f"エラー: {e}")
        return 0

if __name__ == "__main__":
    added_count = check_and_add_missing_tickers()
    if added_count > 0:
        print(f"\n{added_count}銘柄を追加しました")
    else:
        print("\n全銘柄が既に存在しています")