"""
削除予定の不要なテーブルとビューをリストアップ（修正版）
"""
import pyodbc
import pandas as pd
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

def list_unused_objects():
    """不要なオブジェクトをリストアップ"""
    print("=== 削除予定の不要なテーブル・ビュー ===")
    print(f"確認日時: {datetime.now()}\n")
    
    conn = pyodbc.connect(CONNECTION_STRING)
    cursor = conn.cursor()
    
    # 1. 未使用テーブルの確認
    print("【削除予定テーブル】")
    print("-" * 70)
    
    # 各テーブルの状況を確認
    unused_tables = []
    
    # M_ActualContract
    try:
        cursor.execute("SELECT COUNT(*) FROM M_ActualContract")
        count = cursor.fetchone()[0]
        unused_tables.append({
            'name': 'M_ActualContract',
            'count': count,
            'reason': '設計変更により不要（ActualContractは使用していない）'
        })
    except:
        pass
    
    # T_GenericContractMapping
    try:
        cursor.execute("SELECT COUNT(*) FROM T_GenericContractMapping")
        count = cursor.fetchone()[0]
        unused_tables.append({
            'name': 'T_GenericContractMapping',
            'count': count,
            'reason': '設計変更により不要（マッピング不要）'
        })
    except:
        pass
    
    # T_MacroEconomicIndicator（列名を確認）
    try:
        cursor.execute("""
            SELECT COUNT(*) as cnt,
                   MAX(LastUpdated) as last_update
            FROM T_MacroEconomicIndicator
        """)
        result = cursor.fetchone()
        unused_tables.append({
            'name': 'T_MacroEconomicIndicator',
            'count': result[0],
            'last_update': result[1],
            'reason': '更新が停止している'
        })
    except:
        pass
    
    # T_BandingReport（列名を確認）
    try:
        cursor.execute("""
            SELECT COUNT(*) as cnt,
                   MAX(LastUpdated) as last_update
            FROM T_BandingReport
        """)
        result = cursor.fetchone()
        unused_tables.append({
            'name': 'T_BandingReport',
            'count': result[0],
            'last_update': result[1],
            'reason': '更新が停止している'
        })
    except:
        pass
    
    # テーブル情報を表示
    for i, table in enumerate(unused_tables, 1):
        print(f"\n{i}. {table['name']}")
        print(f"   レコード数: {table.get('count', 'N/A')}")
        if 'last_update' in table:
            print(f"   最終更新: {table.get('last_update', 'N/A')}")
        print(f"   理由: {table['reason']}")
    
    # 2. 外部キー制約の確認
    print("\n\n【外部キー制約の確認】")
    print("-" * 70)
    
    for table in ['M_ActualContract', 'T_GenericContractMapping']:
        cursor.execute("""
            SELECT 
                fk.name as FK_Name,
                OBJECT_NAME(fk.parent_object_id) as ParentTable
            FROM sys.foreign_keys fk
            WHERE OBJECT_NAME(fk.referenced_object_id) = ?
        """, table)
        
        constraints = cursor.fetchall()
        if constraints:
            print(f"\n{table} を参照している外部キー:")
            for fk in constraints:
                print(f"  - {fk[0]} ({fk[1]}テーブルから)")
        else:
            print(f"\n{table}: 外部キー制約なし → 削除可能")
    
    # 3. 存在しないビュー
    print("\n\n【存在しないビュー（エラーの原因）】")
    print("-" * 70)
    
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
        if not exists:
            print(f"- {view} （コード内の参照を削除する必要あり）")
    
    # 4. 削除用SQL
    print("\n\n【削除用SQLコマンド】")
    print("-" * 70)
    print("```sql")
    print("-- 外部キー制約の削除（必要な場合）")
    print("-- ALTER TABLE <親テーブル> DROP CONSTRAINT <制約名>;")
    print()
    print("-- 不要テーブルの削除")
    for table in unused_tables:
        print(f"DROP TABLE IF EXISTS {table['name']};")
    print("```")
    
    # 5. 最近更新されているテーブル（削除しない）
    print("\n\n【アクティブなテーブル（削除しない）】")
    print("-" * 70)
    
    cursor.execute("""
        SELECT 
            t.name as TableName,
            p.rows as RowCount,
            MAX(s.last_user_update) as LastUpdate
        FROM sys.tables t
        INNER JOIN sys.partitions p ON t.object_id = p.object_id
        LEFT JOIN sys.dm_db_index_usage_stats s ON t.object_id = s.object_id
        WHERE t.name LIKE 'T_%' 
            AND p.index_id <= 1
            AND t.name NOT IN ('T_GenericContractMapping', 'T_MacroEconomicIndicator', 'T_BandingReport')
        GROUP BY t.name, p.rows
        HAVING MAX(s.last_user_update) >= DATEADD(day, -7, GETDATE())
            OR t.name IN ('T_CommodityPrice', 'T_LMEInventory', 'T_MarketIndicator')
        ORDER BY t.name
    """)
    
    for row in cursor.fetchall():
        print(f"{row[0]}: {row[1]}レコード (最終更新: {row[2]})")
    
    conn.close()
    
    print("\n\n=== まとめ ===")
    print(f"削除予定テーブル: {len(unused_tables)}個")
    print("削除予定ビュー: 0個（存在しないビューへの参照のみ）")
    print("\n注意: 外部キー制約がある場合は、先に制約を削除してください")

if __name__ == "__main__":
    list_unused_objects()