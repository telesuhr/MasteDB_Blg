"""
M_ActualContractとT_GenericContractMappingのみを削除
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

def delete_unused_tables():
    """M_ActualContractとT_GenericContractMappingのみを削除"""
    logger.info("=== 不要テーブルの削除 ===")
    logger.info(f"実行時刻: {datetime.now()}")
    logger.info("削除対象: M_ActualContract, T_GenericContractMapping のみ")
    
    conn = pyodbc.connect(CONNECTION_STRING)
    cursor = conn.cursor()
    
    try:
        # 1. 削除前の確認
        logger.info("\n【削除前の確認】")
        
        # 外部キー制約の確認
        cursor.execute("""
            SELECT 
                fk.name as FK_Name,
                OBJECT_NAME(fk.parent_object_id) as ParentTable,
                OBJECT_NAME(fk.referenced_object_id) as ReferencedTable
            FROM sys.foreign_keys fk
            WHERE OBJECT_NAME(fk.referenced_object_id) IN ('M_ActualContract', 'T_GenericContractMapping')
        """)
        
        constraints = cursor.fetchall()
        if constraints:
            logger.info("外部キー制約:")
            for fk in constraints:
                logger.info(f"  - {fk[0]}: {fk[1]} → {fk[2]}")
        
        # 2. 外部キー制約の削除
        logger.info("\n【外部キー制約の削除】")
        
        # T_GenericContractMapping → M_ActualContract の制約を削除
        try:
            cursor.execute("ALTER TABLE T_GenericContractMapping DROP CONSTRAINT FK_T_GenericContractMapping_Actual")
            conn.commit()
            logger.info("✓ FK_T_GenericContractMapping_Actual を削除")
        except Exception as e:
            logger.warning(f"FK_T_GenericContractMapping_Actual: {e}")
        
        # T_CommodityPrice → M_ActualContract の制約を削除
        try:
            cursor.execute("ALTER TABLE T_CommodityPrice DROP CONSTRAINT FK_T_CommodityPrice_V2_ActualContractID")
            conn.commit()
            logger.info("✓ FK_T_CommodityPrice_V2_ActualContractID を削除")
        except Exception as e:
            logger.warning(f"FK_T_CommodityPrice_V2_ActualContractID: {e}")
        
        # 3. テーブルの削除
        logger.info("\n【テーブルの削除】")
        
        # T_GenericContractMapping を先に削除（M_ActualContractに依存）
        try:
            cursor.execute("DROP TABLE IF EXISTS T_GenericContractMapping")
            conn.commit()
            logger.info("✓ T_GenericContractMapping を削除")
        except Exception as e:
            logger.error(f"T_GenericContractMapping 削除エラー: {e}")
        
        # M_ActualContract を削除
        try:
            cursor.execute("DROP TABLE IF EXISTS M_ActualContract")
            conn.commit()
            logger.info("✓ M_ActualContract を削除")
        except Exception as e:
            logger.error(f"M_ActualContract 削除エラー: {e}")
        
        # 4. 削除結果の確認
        logger.info("\n【削除結果の確認】")
        
        cursor.execute("""
            SELECT TABLE_NAME 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_NAME IN ('M_ActualContract', 'T_GenericContractMapping')
        """)
        
        remaining = cursor.fetchall()
        if remaining:
            logger.warning("削除されなかったテーブル:")
            for table in remaining:
                logger.warning(f"  - {table[0]}")
        else:
            logger.info("✓ 両テーブルが正常に削除されました")
        
        # 5. 保持するテーブルの確認
        logger.info("\n【保持するテーブル（削除しない）】")
        keep_tables = [
            'T_MacroEconomicIndicator',
            'T_BandingReport'
        ]
        
        for table in keep_tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            logger.info(f"{table}: {count}レコード → 保持")
        
        logger.info("\n=== 削除完了 ===")
        logger.info("削除: M_ActualContract, T_GenericContractMapping")
        logger.info("保持: T_MacroEconomicIndicator, T_BandingReport")
        
    except Exception as e:
        logger.error(f"処理エラー: {e}")
        conn.rollback()
        
    finally:
        conn.close()

def main():
    """メイン処理"""
    delete_unused_tables()

if __name__ == "__main__":
    main()