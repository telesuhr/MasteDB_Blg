"""
LME特殊データを手動でロード
"""
import sys
import os
from datetime import datetime, timedelta

# プロジェクトルートとsrcディレクトリをPythonパスに追加
project_root = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.join(project_root, 'src')
sys.path.insert(0, project_root)
sys.path.insert(0, src_dir)

from final_optimized_loader import OptimizedMultiExchangeLoader
from config.logging_config import logger

def main():
    """LMEデータを再ロード"""
    logger.info("=== LME全データ再ロード開始 ===")
    logger.info(f"実行時刻: {datetime.now()}")
    
    try:
        # ローダーインスタンス作成
        loader = OptimizedMultiExchangeLoader()
        
        # LMEデータをロード（7日分）
        loader.load_exchange_data('LME', days_back=7)
        
        logger.info("\n=== ロード完了 ===")
        
        # 結果確認
        import pyodbc
        conn_str = (
            "DRIVER={ODBC Driver 17 for SQL Server};"
            "SERVER=jcz.database.windows.net;"
            "DATABASE=JCL;"
            "UID=TKJCZ01;"
            "PWD=P@ssw0rdmbkazuresql;"
            "Encrypt=yes;"
            "TrustServerCertificate=no;"
            "Connection Timeout=30;"
        )
        
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        
        # 特殊ティッカーのデータ確認
        cursor.execute("""
            SELECT 
                g.GenericTicker,
                COUNT(DISTINCT p.TradeDate) as Days,
                MAX(p.TradeDate) as Latest,
                MAX(p.LastPrice) as Price
            FROM T_CommodityPrice_V2 p
            INNER JOIN M_GenericFutures g ON p.GenericID = g.GenericID
            WHERE g.GenericTicker IN ('LMCADY Index', 'CAD TT00 Comdty', 'LMCADS03 Comdty', 'LMCADS 0003 Comdty')
            GROUP BY g.GenericTicker
            ORDER BY g.GenericTicker
        """)
        
        results = cursor.fetchall()
        if results:
            logger.info("\n【LME特殊ティッカーのロード結果】")
            for row in results:
                logger.info(f"{row[0]}: {row[1]}日分, 最新={row[2]}, 価格={row[3]}")
        else:
            logger.warning("特殊ティッカーのデータがロードされていません")
            
            # 通常のLPティッカーの確認
            cursor.execute("""
                SELECT 
                    g.GenericTicker,
                    COUNT(DISTINCT p.TradeDate) as Days
                FROM T_CommodityPrice_V2 p
                INNER JOIN M_GenericFutures g ON p.GenericID = g.GenericID
                WHERE g.ExchangeCode = 'LME'
                    AND p.TradeDate >= DATEADD(day, -7, GETDATE())
                GROUP BY g.GenericTicker
                ORDER BY g.GenericTicker
            """)
            
            logger.info("\n【通常のLMEティッカーのロード結果】")
            for row in cursor.fetchall():
                logger.info(f"{row[0]}: {row[1]}日分")
        
        conn.close()
        
    except Exception as e:
        logger.error(f"エラー: {e}")
        raise

if __name__ == "__main__":
    main()