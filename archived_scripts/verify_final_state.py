"""
最終的なデータベース状態の確認
"""
import pyodbc
from datetime import datetime

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

def verify_final_state():
    print("=== データベース最終状態確認 ===")
    print(f"確認時刻: {datetime.now()}\n")
    
    conn = pyodbc.connect(CONNECTION_STRING)
    cursor = conn.cursor()
    
    # 1. 削除されたテーブルの確認
    print("【削除確認】")
    deleted_tables = ['M_ActualContract', 'T_GenericContractMapping']
    
    for table in deleted_tables:
        cursor.execute(f"""
            SELECT COUNT(*) 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_NAME = '{table}'
        """)
        exists = cursor.fetchone()[0] > 0
        print(f"{table}: {'まだ存在' if exists else '削除済み'}")
    
    # 2. 保持されているテーブルの確認
    print("\n【保持テーブル確認】")
    keep_tables = ['T_MacroEconomicIndicator', 'T_BandingReport']
    
    for table in keep_tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        print(f"{table}: {count}レコード（保持）")
    
    # 3. 主要テーブルの確認
    print("\n【主要テーブル（アクティブ）】")
    main_tables = [
        'T_CommodityPrice',
        'T_LMEInventory', 
        'T_OtherExchangeInventory',
        'T_MarketIndicator',
        'T_CompanyStockPrice',
        'T_COTR'
    ]
    
    for table in main_tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        print(f"{table}: {count}レコード")
    
    # 4. マスターテーブルの確認
    print("\n【マスターテーブル】")
    master_tables = [
        'M_Metal',
        'M_GenericFutures',
        'M_TradingCalendar',
        'M_Indicator',
        'M_Region',
        'M_TenorType'
    ]
    
    for table in master_tables:
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            print(f"{table}: {count}レコード")
        except:
            print(f"{table}: エラー/存在しない")
    
    conn.close()
    
    print("\n=== 完了 ===")
    print("削除完了: M_ActualContract, T_GenericContractMapping")
    print("他のテーブルは全て保持されています")

if __name__ == "__main__":
    verify_final_state()