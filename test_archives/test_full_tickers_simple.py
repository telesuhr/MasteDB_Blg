"""
全銘柄対応テスト（シンプル版）
エラー詳細表示付き
"""
import pyodbc
import pandas as pd
from datetime import datetime, date, timedelta

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

def test_full_tickers():
    """全銘柄テスト"""
    print("=== 全銘柄対応テスト ===")
    
    try:
        conn = pyodbc.connect(CONNECTION_STRING)
        
        # 各取引所の銘柄定義（最初の3銘柄でテスト）
        exchange_tickers = {
            'LME': ['LP1 Comdty', 'LP2 Comdty', 'LP3 Comdty'],
            'SHFE': ['CU1 Comdty', 'CU2 Comdty', 'CU3 Comdty'],
            'CMX': ['HG1 Comdty', 'HG2 Comdty', 'HG3 Comdty']
        }
        
        # 全銘柄存在確認
        print("\n全銘柄存在確認:")
        for exchange, tickers in exchange_tickers.items():
            display_name = "COMEX" if exchange == "CMX" else exchange
            
            cursor = conn.cursor()
            placeholders = ','.join(['?' for _ in tickers])
            cursor.execute(f"""
                SELECT COUNT(*) 
                FROM M_GenericFutures 
                WHERE ExchangeCode = ? AND GenericTicker IN ({placeholders})
            """, [exchange] + tickers)
            
            count = cursor.fetchone()[0]
            status = "OK" if count == len(tickers) else "NG"
            print(f"  {display_name}: {count}/{len(tickers)} {status}")
        
        # 価格データ作成テスト
        target_date = datetime.now().date()
        print(f"\n{target_date} 価格データ作成テスト:")
        
        success_count = 0
        for exchange, tickers in exchange_tickers.items():
            display_name = "COMEX" if exchange == "CMX" else exchange
            
            try:
                # マッピング情報取得
                cursor = conn.cursor()
                placeholders = ','.join(['?' for _ in tickers])
                cursor.execute(f"""
                    SELECT g.GenericID, g.GenericTicker, g.MetalID, m.ActualContractID
                    FROM M_GenericFutures g
                    LEFT JOIN T_GenericContractMapping m ON m.GenericID = g.GenericID 
                        AND m.TradeDate = ?
                    WHERE g.ExchangeCode = ? AND g.GenericTicker IN ({placeholders})
                    ORDER BY g.GenericNumber
                """, [target_date, exchange] + tickers)
                
                mapping_results = cursor.fetchall()
                print(f"  {display_name}: {len(mapping_results)}件のマッピング情報取得")
                
                # マッピングが存在しない場合の処理
                if len(mapping_results) > 0:
                    has_mapping = sum(1 for row in mapping_results if row[3] is not None)
                    print(f"    マッピング済み: {has_mapping}/{len(mapping_results)}")
                    
                    if has_mapping > 0:
                        success_count += 1
                        print(f"    {display_name}: 処理可能")
                    else:
                        print(f"    {display_name}: マッピング未作成のため価格データ作成不可")
                else:
                    print(f"    {display_name}: ジェネリック先物情報なし")
                
            except Exception as e:
                print(f"    {display_name} エラー: {e}")
        
        # 価格データサンプル確認
        print(f"\n最新価格データサンプル:")
        cursor.execute("""
            SELECT TOP 5
                g.ExchangeCode,
                g.GenericTicker,
                p.TradeDate,
                p.SettlementPrice
            FROM T_CommodityPrice_V2 p
            JOIN M_GenericFutures g ON p.GenericID = g.GenericID
            WHERE p.TradeDate >= DATEADD(day, -3, GETDATE())
            ORDER BY p.TradeDate DESC, g.ExchangeCode, g.GenericNumber
        """)
        
        price_samples = cursor.fetchall()
        for row in price_samples:
            exchange_display = "COMEX" if row[0] == "CMX" else row[0]
            print(f"  {exchange_display} {row[1]}: {row[2]} -> {row[3]}")
        
        conn.close()
        
        print(f"\n=== テスト結果 ===")
        print(f"処理可能取引所: {success_count}/3")
        
        return success_count == 3
        
    except Exception as e:
        print(f"テストエラー: {e}")
        return False

if __name__ == "__main__":
    success = test_full_tickers()
    if success:
        print("OK 全銘柄テスト成功")
    else:
        print("NG 全銘柄テスト失敗")