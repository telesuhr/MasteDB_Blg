"""
削除予定の不要なテーブルとビューをリストアップ
"""
import pyodbc
import pandas as pd
from datetime import datetime, timedelta

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

def list_unused_objects():
    """不要なオブジェクトをリストアップ"""
    print("=== 削除予定の不要なテーブル・ビュー ===")
    print(f"確認日時: {datetime.now()}\n")
    
    conn = pyodbc.connect(CONNECTION_STRING)
    cursor = conn.cursor()
    
    # 1. 未使用テーブルの確認
    print("【削除予定テーブル】")
    print("-" * 60)
    
    # M_ActualContract - 作成したが未使用
    cursor.execute("SELECT COUNT(*) FROM M_ActualContract")
    count = cursor.fetchone()[0]
    print(f"1. M_ActualContract")
    print(f"   - レコード数: {count}")
    print(f"   - 理由: 設計変更により不要（ActualContractは使用していない）")
    
    # T_GenericContractMapping - 作成したが未使用
    cursor.execute("SELECT COUNT(*) FROM T_GenericContractMapping")
    count = cursor.fetchone()[0]
    print(f"\n2. T_GenericContractMapping")
    print(f"   - レコード数: {count}")
    print(f"   - 理由: 設計変更により不要（マッピング不要）")
    
    # 更新が停止しているテーブル
    print(f"\n3. T_MacroEconomicIndicator")
    cursor.execute("SELECT COUNT(*), MAX(TradeDate) FROM T_MacroEconomicIndicator")
    result = cursor.fetchone()
    print(f"   - レコード数: {result[0]}")
    print(f"   - 最終更新: {result[1]}")
    print(f"   - 理由: 2025年5月以降更新なし、使用されていない")
    
    print(f"\n4. T_BandingReport")
    cursor.execute("SELECT COUNT(*), MAX(TradeDate) FROM T_BandingReport")
    result = cursor.fetchone()
    print(f"   - レコード数: {result[0]}")
    print(f"   - 最終更新: {result[1]}")
    print(f"   - 理由: 7月以降更新なし、使用されていない")
    
    # 2. 存在しないビューの確認
    print("\n\n【削除予定ビュー（存在しないが参照されている）】")
    print("-" * 60)
    
    missing_views = [
        'V_CommodityPriceWithAttributes',
        'V_GenericFuturesWithMaturity',
        'V_TradingDaysCalculationDetail'
    ]
    
    for view in missing_views:
        cursor.execute("""
            SELECT COUNT(*) 
            FROM sys.views 
            WHERE name = ?
        """, view)
        exists = cursor.fetchone()[0] > 0
        print(f"{view}: {'存在する' if exists else '存在しない（参照削除必要）'}")
    
    # 3. 外部キー制約の確認
    print("\n\n【外部キー制約の確認】")
    print("-" * 60)
    
    tables_to_check = ['M_ActualContract', 'T_GenericContractMapping']
    
    for table in tables_to_check:
        cursor.execute("""
            SELECT 
                fk.name as FK_Name,
                OBJECT_NAME(fk.parent_object_id) as ParentTable,
                OBJECT_NAME(fk.referenced_object_id) as ReferencedTable
            FROM sys.foreign_keys fk
            WHERE OBJECT_NAME(fk.referenced_object_id) = ?
        """, table)
        
        constraints = cursor.fetchall()
        if constraints:
            print(f"\n{table} の外部キー制約:")
            for fk in constraints:
                print(f"  - {fk[0]}: {fk[1]} → {fk[2]}")
        else:
            print(f"\n{table}: 外部キー制約なし（削除可能）")
    
    # 4. 削除コマンドの生成
    print("\n\n【削除用SQLコマンド】")
    print("-" * 60)
    print("-- 外部キー制約がある場合は先に削除")
    print("-- ALTER TABLE T_CommodityPrice DROP CONSTRAINT FK_T_CommodityPrice_ActualContract;")
    print()
    print("-- テーブル削除")
    print("DROP TABLE IF EXISTS M_ActualContract;")
    print("DROP TABLE IF EXISTS T_GenericContractMapping;")
    print("DROP TABLE IF EXISTS T_MacroEconomicIndicator;")
    print("DROP TABLE IF EXISTS T_BandingReport;")
    
    # 5. 現在アクティブなテーブルの確認（削除しないもの）
    print("\n\n【維持すべきテーブル（アクティブ）】")
    print("-" * 60)
    
    active_tables = [
        ('T_CommodityPrice', 'メイン価格テーブル（V2構造）'),
        ('T_LMEInventory', 'LME在庫データ'),
        ('T_OtherExchangeInventory', 'SHFE/CMX在庫'),
        ('T_MarketIndicator', '市場指標'),
        ('T_CompanyStockPrice', '企業株価'),
        ('T_COTR', 'COTRデータ（週次）'),
        ('M_TradingCalendar', '営業日カレンダー（2000-2039）')
    ]
    
    for table, description in active_tables:
        cursor.execute(f"SELECT COUNT(*), MAX(LastUpdated) FROM {table}")
        result = cursor.fetchone()
        print(f"{table}: {description}")
        if result[0]:
            print(f"  レコード数: {result[0]}, 最終更新: {result[1]}")
        print()
    
    conn.close()
    
    print("\n=== まとめ ===")
    print("削除予定:")
    print("- テーブル: 4個（M_ActualContract, T_GenericContractMapping, T_MacroEconomicIndicator, T_BandingReport）")
    print("- ビュー: なし（存在しないビューへの参照のみ）")
    print("\n※外部キー制約がある場合は、制約を先に削除する必要があります")

if __name__ == "__main__":
    list_unused_objects()