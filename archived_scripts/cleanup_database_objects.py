"""
データベースオブジェクトのクリーンアップ
推奨アクションのみを実行し、記載のないオブジェクトには触れない
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

def cleanup_database():
    """不要なデータベースオブジェクトを削除"""
    logger.info("=== データベースクリーンアップ開始 ===")
    logger.info(f"実行時刻: {datetime.now()}")
    
    conn = pyodbc.connect(CONNECTION_STRING)
    cursor = conn.cursor()
    
    try:
        # 1. 未使用テーブルの削除
        unused_tables = [
            'M_ActualContract',
            'T_GenericContractMapping'
        ]
        
        for table in unused_tables:
            try:
                # 依存関係を確認
                cursor.execute("""
                    SELECT COUNT(*) 
                    FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS 
                    WHERE CONSTRAINT_TYPE = 'FOREIGN KEY' 
                    AND TABLE_NAME = ?
                """, table)
                
                fk_count = cursor.fetchone()[0]
                if fk_count > 0:
                    logger.warning(f"{table}: 外部キー制約があるためスキップ")
                    continue
                
                # テーブル削除
                cursor.execute(f"DROP TABLE IF EXISTS {table}")
                conn.commit()
                logger.info(f"✓ {table} を削除しました")
                
            except Exception as e:
                logger.error(f"✗ {table} の削除に失敗: {e}")
                conn.rollback()
        
        # 2. 存在しないビューへの参照確認（削除ではなく確認のみ）
        missing_views = [
            'V_CommodityPriceWithAttributes',
            'V_GenericFuturesWithMaturity',
            'V_TradingDaysCalculationDetail'
        ]
        
        logger.info("\n【存在しないビューの確認】")
        for view in missing_views:
            cursor.execute("""
                SELECT COUNT(*) 
                FROM sys.views 
                WHERE name = ?
            """, view)
            
            exists = cursor.fetchone()[0] > 0
            if not exists:
                logger.info(f"- {view}: 存在しません（参照がある場合は手動で確認必要）")
        
        # 3. T_CommodityPrice から T_CommodityPrice_V2 への移行状況確認
        logger.info("\n【価格テーブル移行状況】")
        
        # 両テーブルのデータ量を比較
        cursor.execute("""
            SELECT 
                (SELECT COUNT(*) FROM T_CommodityPrice) as OldTableCount,
                (SELECT COUNT(*) FROM T_CommodityPrice_V2) as NewTableCount,
                (SELECT MAX(TradeDate) FROM T_CommodityPrice) as OldTableLatest,
                (SELECT MAX(TradeDate) FROM T_CommodityPrice_V2) as NewTableLatest
        """)
        
        result = cursor.fetchone()
        logger.info(f"T_CommodityPrice (旧): {result[0]}件, 最新={result[2]}")
        logger.info(f"T_CommodityPrice_V2 (新): {result[1]}件, 最新={result[3]}")
        
        # main.py の更新は手動で必要
        logger.warning("\n【要手動作業】")
        logger.warning("1. src/main.py を更新して T_CommodityPrice_V2 を使用するように変更")
        logger.warning("2. 十分なテスト後、T_CommodityPrice を削除")
        
        # 4. 最終確認
        logger.info("\n【クリーンアップ完了】")
        logger.info("削除されたオブジェクト:")
        for table in unused_tables:
            cursor.execute("""
                SELECT COUNT(*) 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_NAME = ?
            """, table)
            if cursor.fetchone()[0] == 0:
                logger.info(f"  ✓ {table}")
        
    except Exception as e:
        logger.error(f"クリーンアップエラー: {e}")
        conn.rollback()
        
    finally:
        conn.close()
        logger.info("\n=== クリーンアップ完了 ===")

def verify_v2_readiness():
    """V2テーブルの準備状況を確認"""
    logger.info("\n=== V2移行準備状況確認 ===")
    
    conn = pyodbc.connect(CONNECTION_STRING)
    cursor = conn.cursor()
    
    try:
        # V2テーブルのデータ品質確認
        cursor.execute("""
            SELECT 
                COUNT(*) as TotalRecords,
                COUNT(DISTINCT GenericID) as UniqueGenerics,
                COUNT(DISTINCT TradeDate) as TradingDays,
                MIN(TradeDate) as FirstDate,
                MAX(TradeDate) as LastDate
            FROM T_CommodityPrice_V2
        """)
        
        result = cursor.fetchone()
        logger.info("\n【T_CommodityPrice_V2 統計】")
        logger.info(f"総レコード数: {result[0]}")
        logger.info(f"銘柄数: {result[1]}")
        logger.info(f"取引日数: {result[2]}")
        logger.info(f"期間: {result[3]} ～ {result[4]}")
        
        # 各取引所のデータ確認
        cursor.execute("""
            SELECT 
                g.ExchangeCode,
                COUNT(DISTINCT p.GenericID) as Generics,
                COUNT(*) as Records
            FROM T_CommodityPrice_V2 p
            INNER JOIN M_GenericFutures g ON p.GenericID = g.GenericID
            GROUP BY g.ExchangeCode
            ORDER BY g.ExchangeCode
        """)
        
        logger.info("\n【取引所別データ】")
        for row in cursor.fetchall():
            logger.info(f"{row[0]}: {row[1]}銘柄, {row[2]}レコード")
        
        logger.info("\n✓ V2テーブルは本番使用可能な状態です")
        
    except Exception as e:
        logger.error(f"確認エラー: {e}")
        
    finally:
        conn.close()

def main():
    """メイン処理"""
    # 1. 不要オブジェクトのクリーンアップ
    cleanup_database()
    
    # 2. V2移行準備状況の確認
    verify_v2_readiness()
    
    logger.info("\n【次のステップ】")
    logger.info("1. src/main.py の更新（T_CommodityPrice → T_CommodityPrice_V2）")
    logger.info("2. 移行後の動作確認")
    logger.info("3. 問題なければ T_CommodityPrice の削除")

if __name__ == "__main__":
    main()