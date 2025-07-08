"""
満期関連情報の追加とビューの作成
"""
import pyodbc
from datetime import datetime

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

def execute_sql_file():
    """SQLファイルを実行"""
    print("=== 満期関連情報の追加開始 ===")
    
    conn = pyodbc.connect(CONNECTION_STRING)
    cursor = conn.cursor()
    
    try:
        with open('sql/alter_generic_futures_add_maturity.sql', 'r', encoding='utf-8') as f:
            sql_content = f.read()
            
        # GOステートメントで分割
        sql_statements = sql_content.split('\nGO\n')
        
        for i, stmt in enumerate(sql_statements):
            if stmt.strip() and not stmt.strip().startswith('-- 使用例'):
                print(f"\nステートメント {i+1} を実行中...")
                try:
                    cursor.execute(stmt)
                    if 'ALTER TABLE' in stmt:
                        print("   テーブル変更完了")
                    elif 'UPDATE' in stmt:
                        rows = cursor.rowcount
                        print(f"   {rows}行更新完了")
                    elif 'CREATE' in stmt and 'FUNCTION' in stmt:
                        print("   関数作成完了")
                    elif 'CREATE' in stmt and 'VIEW' in stmt:
                        print("   ビュー作成完了")
                    conn.commit()
                except Exception as e:
                    print(f"   エラー: {e}")
                    # エラーが発生してもできるだけ続行
                    conn.rollback()
                    
    except Exception as e:
        print(f"ファイル読み込みエラー: {e}")
        
    conn.close()
    print("\n=== 処理完了 ===")

def test_maturity_view():
    """満期ビューのテスト"""
    print("\n=== 満期ビューテスト ===")
    
    conn = pyodbc.connect(CONNECTION_STRING)
    
    try:
        # 基本的な満期情報の確認
        cursor = conn.cursor()
        cursor.execute("""
            SELECT TOP 20
                ExchangeDisplayName,
                GenericNumber,
                GenericTicker,
                TradeDate,
                MaturityMonth,
                LastTradingDay,
                TradingDaysRemaining,
                RolloverDate,
                ShouldRollover,
                Volume
            FROM V_CommodityPriceWithMaturity
            WHERE TradeDate = (SELECT MAX(TradeDate) FROM V_CommodityPriceWithMaturity)
                AND MetalCode LIKE 'CU%'
            ORDER BY ExchangeDisplayName, GenericNumber
        """)
        
        print("\n【満期情報付き価格データ】")
        print(f"{'Exchange':<10} {'Generic':<15} {'TradeDate':<12} {'Maturity':<12} {'LastTrade':<12} {'DaysLeft':<10} {'Rollover':<12} {'ShouldRoll':<10} {'Volume':<10}")
        print("-" * 120)
        
        for row in cursor.fetchall():
            exchange = row[0][:10]
            ticker = row[2][:15]
            trade_date = str(row[3])
            maturity = str(row[4]) if row[4] else 'N/A'
            last_trade = str(row[5]) if row[5] else 'N/A'
            days_left = str(row[6]) if row[6] is not None else 'N/A'
            rollover = str(row[7]) if row[7] else 'N/A'
            should_roll = row[8]
            volume = str(row[9]) if row[9] is not None else '0'
            
            print(f"{exchange:<10} {ticker:<15} {trade_date:<12} {maturity:<12} {last_trade:<12} {days_left:<10} {rollover:<12} {should_roll:<10} {volume:<10}")
            
        # ロールオーバーが必要な契約の確認
        cursor.execute("""
            SELECT 
                ExchangeDisplayName,
                GenericTicker,
                LastTradingDay,
                TradingDaysRemaining,
                Volume
            FROM V_CommodityPriceWithMaturity
            WHERE TradeDate = (SELECT MAX(TradeDate) FROM V_CommodityPriceWithMaturity)
                AND ShouldRollover = 'Yes'
                AND Volume > 0
            ORDER BY TradingDaysRemaining
        """)
        
        rows = cursor.fetchall()
        if rows:
            print("\n【ロールオーバーが必要な契約】")
            print(f"{'Exchange':<10} {'Ticker':<15} {'LastTrade':<12} {'DaysLeft':<10} {'Volume':<10}")
            print("-" * 60)
            
            for row in rows:
                print(f"{row[0]:<10} {row[1]:<15} {str(row[2]):<12} {row[3]:<10} {row[4]:<10}")
        else:
            print("\n現在ロールオーバーが必要な契約はありません。")
            
    except Exception as e:
        print(f"テストエラー: {e}")
        
    conn.close()

def check_current_structure():
    """現在のテーブル構造を確認"""
    print("\n=== 現在のテーブル構造確認 ===")
    
    conn = pyodbc.connect(CONNECTION_STRING)
    cursor = conn.cursor()
    
    # M_GenericFuturesの構造確認
    cursor.execute("""
        SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = 'M_GenericFutures'
        ORDER BY ORDINAL_POSITION
    """)
    
    print("\nM_GenericFutures columns:")
    for row in cursor.fetchall():
        col_name = row[0]
        data_type = row[1]
        max_len = f"({row[2]})" if row[2] else ""
        print(f"  {col_name}: {data_type}{max_len}")
        
    conn.close()

def main():
    """メイン処理"""
    print(f"実行時刻: {datetime.now()}")
    
    # 現在の構造確認
    check_current_structure()
    
    # SQL実行
    execute_sql_file()
    
    # テスト実行
    test_maturity_view()
    
    print("\n=== 完了 ===")

if __name__ == "__main__":
    main()