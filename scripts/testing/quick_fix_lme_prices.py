"""
LME価格データの問題を迅速に修正
依存関係の問題を回避して直接実行
"""
import os
import sys
import pyodbc
import pandas as pd
from datetime import datetime
import re

# データベース設定を直接定義
DATABASE_CONFIG = {
    'driver': 'ODBC Driver 17 for SQL Server',
    'server': 'jcz.database.windows.net',
    'database': 'JCL',
    'username': 'TKJCZ01',
    'password': 'P@ssw0rdmbkazuresql'
}

def get_connection():
    """データベース接続を取得"""
    conn_str = f"""
    DRIVER={{{DATABASE_CONFIG['driver']}}};
    SERVER={DATABASE_CONFIG['server']};
    DATABASE={DATABASE_CONFIG['database']};
    UID={DATABASE_CONFIG['username']};
    PWD={DATABASE_CONFIG['password']};
    """
    return pyodbc.connect(conn_str)

def check_and_update_constraints():
    """CHECK制約を確認して必要に応じて更新"""
    with get_connection() as conn:
        cursor = conn.cursor()
        
        # 現在のCHECK制約を確認
        cursor.execute("""
            SELECT cc.name, cc.definition
            FROM sys.check_constraints cc
            JOIN sys.tables t ON cc.parent_object_id = t.object_id
            WHERE t.name = 'T_CommodityPrice'
        """)
        
        constraints = cursor.fetchall()
        print("現在のCHECK制約:")
        for name, definition in constraints:
            print(f"  {name}: {definition}")
            
            # Cash、TomNextが含まれていない制約があれば更新が必要
            if 'DataType' in definition and 'Cash' not in definition:
                print(f"\n制約 {name} の更新が必要です")
                
                # 古い制約を削除
                try:
                    cursor.execute(f"ALTER TABLE T_CommodityPrice DROP CONSTRAINT {name}")
                    conn.commit()
                    print(f"制約 {name} を削除しました")
                    
                    # 新しい制約を追加
                    cursor.execute("""
                        ALTER TABLE T_CommodityPrice ADD CONSTRAINT CHK_T_CommodityPrice_DataType
                        CHECK (
                            (DataType = 'Generic' AND GenericID IS NOT NULL AND ActualContractID IS NULL) OR
                            (DataType = 'Actual' AND ActualContractID IS NOT NULL AND GenericID IS NULL) OR
                            (DataType = 'Cash' AND GenericID IS NULL AND ActualContractID IS NULL) OR
                            (DataType = 'TomNext' AND GenericID IS NULL AND ActualContractID IS NULL)
                        )
                    """)
                    conn.commit()
                    print("新しい制約を追加しました")
                except Exception as e:
                    print(f"制約の更新中にエラー: {e}")
                    conn.rollback()

def verify_data_types():
    """データタイプ別のレコード数を確認"""
    with get_connection() as conn:
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT DataType, COUNT(*) as cnt
            FROM T_CommodityPrice
            GROUP BY DataType
            ORDER BY DataType
        """)
        
        print("\nデータタイプ別レコード数:")
        for data_type, count in cursor.fetchall():
            print(f"  {data_type}: {count}件")

def main():
    """メイン処理"""
    print("=== LME価格データ問題の修正 ===")
    
    # 1. CHECK制約の確認と更新
    check_and_update_constraints()
    
    # 2. 現在のデータ状況を確認
    verify_data_types()
    
    print("\n修正が完了しました。")
    print("再度 fetch_historical_with_mapping.py を実行してデータを取得してください。")

if __name__ == "__main__":
    main()