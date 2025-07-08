"""
T_CommodityPrice_V2をT_CommodityPriceにリネーム
旧T_CommodityPriceのデータは削除
"""
import pyodbc
from datetime import datetime
from config.logging_config import logger

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

def rename_tables():
    """テーブルのリネーム処理"""
    logger.info("=== テーブルリネーム処理開始 ===")
    logger.info(f"実行時刻: {datetime.now()}")
    
    conn = pyodbc.connect(CONNECTION_STRING)
    cursor = conn.cursor()
    
    try:
        # 1. 現在の状況確認
        logger.info("\n【現在の状況確認】")
        
        # 旧テーブルのレコード数
        cursor.execute("SELECT COUNT(*) FROM T_CommodityPrice")
        old_count = cursor.fetchone()[0]
        logger.info(f"T_CommodityPrice (旧): {old_count}件")
        
        # 新テーブルのレコード数
        cursor.execute("SELECT COUNT(*) FROM T_CommodityPrice_V2")
        new_count = cursor.fetchone()[0]
        logger.info(f"T_CommodityPrice_V2 (新): {new_count}件")
        
        # 2. 旧T_CommodityPriceのデータを削除
        logger.info("\n【旧テーブルのデータ削除】")
        cursor.execute("DELETE FROM T_CommodityPrice")
        deleted_count = cursor.rowcount
        conn.commit()
        logger.info(f"✓ T_CommodityPrice から {deleted_count}件削除")
        
        # 3. テーブルリネーム
        logger.info("\n【テーブルリネーム】")
        
        # 旧テーブルを一時的な名前に変更
        cursor.execute("EXEC sp_rename 'T_CommodityPrice', 'T_CommodityPrice_OLD'")
        conn.commit()
        logger.info("✓ T_CommodityPrice → T_CommodityPrice_OLD")
        
        # 新テーブルを正式名称に変更
        cursor.execute("EXEC sp_rename 'T_CommodityPrice_V2', 'T_CommodityPrice'")
        conn.commit()
        logger.info("✓ T_CommodityPrice_V2 → T_CommodityPrice")
        
        # 4. 関連するインデックスや制約の確認
        logger.info("\n【インデックス・制約の確認】")
        
        # プライマリキー制約の確認
        cursor.execute("""
            SELECT name 
            FROM sys.key_constraints 
            WHERE parent_object_id = OBJECT_ID('T_CommodityPrice')
            AND type = 'PK'
        """)
        pk = cursor.fetchone()
        if pk:
            logger.info(f"プライマリキー: {pk[0]}")
        
        # インデックスの確認
        cursor.execute("""
            SELECT name, type_desc
            FROM sys.indexes 
            WHERE object_id = OBJECT_ID('T_CommodityPrice')
            AND is_primary_key = 0
        """)
        for idx in cursor.fetchall():
            logger.info(f"インデックス: {idx[0]} ({idx[1]})")
        
        # 5. ビューの更新確認
        logger.info("\n【関連ビューの確認】")
        cursor.execute("""
            SELECT DISTINCT 
                v.name as ViewName
            FROM sys.views v
            INNER JOIN sys.sql_expression_dependencies d 
                ON v.object_id = d.referencing_id
            WHERE d.referenced_entity_name = 'T_CommodityPrice'
            ORDER BY v.name
        """)
        
        views = cursor.fetchall()
        if views:
            logger.info("T_CommodityPriceを参照しているビュー:")
            for view in views:
                logger.info(f"  - {view[0]}")
        else:
            logger.info("T_CommodityPriceを参照しているビューはありません")
        
        # 6. 最終確認
        logger.info("\n【最終確認】")
        
        # 新しいT_CommodityPriceの構造確認
        cursor.execute("""
            SELECT 
                c.name as ColumnName,
                t.name as DataType,
                c.max_length,
                c.is_nullable
            FROM sys.columns c
            INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
            WHERE c.object_id = OBJECT_ID('T_CommodityPrice')
            ORDER BY c.column_id
        """)
        
        logger.info("\n新T_CommodityPriceの構造:")
        logger.info("列名 | データ型 | NULL許可")
        logger.info("-" * 40)
        for col in cursor.fetchall():
            null_str = "YES" if col[3] else "NO"
            logger.info(f"{col[0]} | {col[1]} | {null_str}")
        
        # データ件数確認
        cursor.execute("SELECT COUNT(*) FROM T_CommodityPrice")
        final_count = cursor.fetchone()[0]
        logger.info(f"\n最終レコード数: {final_count}件")
        
        logger.info("\n✓ テーブルリネーム完了")
        logger.info("  T_CommodityPrice_V2 → T_CommodityPrice")
        logger.info("  旧T_CommodityPriceは空の状態でT_CommodityPrice_OLDとして保持")
        
    except Exception as e:
        logger.error(f"エラー: {e}")
        conn.rollback()
        raise
        
    finally:
        conn.close()

def cleanup_old_table():
    """旧テーブルの削除（オプション）"""
    logger.info("\n=== 旧テーブル削除（オプション） ===")
    
    response = input("T_CommodityPrice_OLDを削除しますか？ (y/n): ")
    if response.lower() == 'y':
        conn = pyodbc.connect(CONNECTION_STRING)
        cursor = conn.cursor()
        
        try:
            cursor.execute("DROP TABLE IF EXISTS T_CommodityPrice_OLD")
            conn.commit()
            logger.info("✓ T_CommodityPrice_OLD を削除しました")
        except Exception as e:
            logger.error(f"削除エラー: {e}")
            conn.rollback()
        finally:
            conn.close()
    else:
        logger.info("T_CommodityPrice_OLD を保持します")

def main():
    """メイン処理"""
    try:
        # テーブルリネーム
        rename_tables()
        
        # 旧テーブル削除（オプション）
        cleanup_old_table()
        
        logger.info("\n【完了】")
        logger.info("main.pyやその他のプログラムは変更不要です")
        logger.info("T_CommodityPriceは新しい構造（GenericID使用）になりました")
        
    except Exception as e:
        logger.error(f"処理失敗: {e}")
        logger.info("\n手動でのリカバリが必要な場合:")
        logger.info("1. T_CommodityPrice_OLD → T_CommodityPrice にリネーム")
        logger.info("2. T_CommodityPrice → T_CommodityPrice_V2 にリネーム")

if __name__ == "__main__":
    main()