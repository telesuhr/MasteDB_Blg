"""
全LMEデータを再ロード
"""
import sys
import os
from datetime import datetime

# プロジェクトルートとsrcディレクトリをPythonパスに追加
project_root = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.join(project_root, 'src')
sys.path.insert(0, project_root)
sys.path.insert(0, src_dir)

from final_optimized_loader import OptimizedMultiExchangeLoader
from config.logging_config import logger
import pyodbc
import pandas as pd

def main():
    """LMEデータを再ロード"""
    logger.info("=== LME全データ再ロード ===")
    
    try:
        # ローダーインスタンス作成
        loader = OptimizedMultiExchangeLoader()
        
        # 全取引所データをロード（LMEも含む）
        loader.load_all_exchanges_data(days_back=7)
        
        logger.info("\n=== ロード完了、結果確認 ===")
        
        # 結果確認
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
        
        # LME特殊ティッカーの確認
        query = """
        SELECT 
            g.GenericTicker,
            g.Description,
            p.TradeDate,
            p.LastPrice,
            p.SettlementPrice,
            p.Volume
        FROM T_CommodityPrice_V2 p
        INNER JOIN M_GenericFutures g ON p.GenericID = g.GenericID
        WHERE g.GenericTicker IN ('LMCADY Index', 'CAD TT00 Comdty', 'LMCADS03 Comdty', 'LMCADS 0003 Comdty')
            AND p.TradeDate >= DATEADD(day, -7, GETDATE())
        ORDER BY g.GenericTicker, p.TradeDate DESC
        """
        
        df = pd.read_sql(query, conn)
        
        logger.info("\n【LME特殊ティッカーのデータ】")
        if not df.empty:
            print(df.to_string(index=False))
            
            # サマリー
            summary = df.groupby('GenericTicker').agg({
                'TradeDate': 'count',
                'LastPrice': 'last'
            }).rename(columns={'TradeDate': 'DataDays', 'LastPrice': 'LatestPrice'})
            
            logger.info("\n【サマリー】")
            for ticker, row in summary.iterrows():
                logger.info(f"{ticker}: {row['DataDays']}日分, 最新価格={row['LatestPrice']}")
        else:
            logger.warning("LME特殊ティッカーのデータがありません")
            
            # 通常のLPデータを確認
            query2 = """
            SELECT 
                g.GenericTicker,
                COUNT(DISTINCT p.TradeDate) as Days,
                MAX(p.LastPrice) as LatestPrice
            FROM T_CommodityPrice_V2 p
            INNER JOIN M_GenericFutures g ON p.GenericID = g.GenericID
            WHERE g.ExchangeCode = 'LME'
                AND g.GenericTicker LIKE 'LP%'
                AND p.TradeDate >= DATEADD(day, -7, GETDATE())
            GROUP BY g.GenericTicker
            ORDER BY g.GenericTicker
            """
            
            df2 = pd.read_sql(query2, conn)
            logger.info("\n【通常のLPティッカー（参考）】")
            if not df2.empty:
                print(df2.head(10).to_string(index=False))
        
        conn.close()
        
    except Exception as e:
        logger.error(f"エラー: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()