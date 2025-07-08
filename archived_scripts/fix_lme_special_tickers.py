"""
LME特殊ティッカーのフォーマット修正とデータ取得
"""
import sys
import os
from datetime import datetime, timedelta
import pandas as pd

# プロジェクトルートとsrcディレクトリをPythonパスに追加
project_root = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.join(project_root, 'src')
sys.path.insert(0, project_root)
sys.path.insert(0, src_dir)

from bloomberg_api import BloombergDataFetcher
from config.logging_config import logger
import pyodbc

def update_config_and_load():
    """設定更新とデータロード"""
    logger.info("=== LME特殊ティッカーデータロード ===")
    
    # 既存データ確認
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
    
    # 既存のLP*データをチェック（正しく取得できているティッカーを確認）
    cursor.execute("""
        SELECT TOP 10 
            g.GenericTicker,
            COUNT(DISTINCT p.TradeDate) as DataDays
        FROM T_CommodityPrice_V2 p
        INNER JOIN M_GenericFutures g ON p.GenericID = g.GenericID
        WHERE g.ExchangeCode = 'LME'
            AND p.TradeDate >= DATEADD(day, -7, GETDATE())
        GROUP BY g.GenericTicker
        ORDER BY g.GenericTicker
    """)
    
    logger.info("\n【既存LMEデータ（過去7日）】")
    for row in cursor.fetchall():
        logger.info(f"{row[0]}: {row[1]}日分")
    
    # config/bloomberg_config.pyの定義を確認
    from config.bloomberg_config import BLOOMBERG_TICKERS
    
    lme_config = BLOOMBERG_TICKERS.get('LME_COPPER_PRICES', {})
    existing_tickers = lme_config.get('securities', [])
    
    logger.info(f"\n【設定済みLMEティッカー数】: {len(existing_tickers)}")
    
    # 特殊ティッカーが設定に含まれているか確認
    special_tickers = ['LMCADY Index', 'LMCADS03 Comdty', 'LMCADS 0003 Comdty']
    for ticker in special_tickers:
        if ticker in existing_tickers:
            logger.info(f"✓ {ticker} は設定済み")
        else:
            logger.warning(f"✗ {ticker} は未設定")
    
    # 実際のデータ取得テスト
    bloomberg = BloombergDataFetcher()
    
    try:
        if not bloomberg.connect():
            logger.error("Bloomberg API接続失敗")
            return
        
        # テスト用：各ティッカーを個別に取得
        test_tickers = [
            ('LMCADY Index', 'LME Copper Cash'),
            ('LMCADS03 Comdty', 'LME Copper 3M'),
            ('LMCADS 0003 Comdty', 'LME Copper Cash/3M Spread'),
            ('LP1 Comdty', 'LME Generic 1st (比較用)')
        ]
        
        fields = ['PX_LAST']
        end_date = datetime.now()
        start_date = end_date - timedelta(days=1)
        
        for ticker, desc in test_tickers:
            logger.info(f"\n{ticker} ({desc}) のテスト取得...")
            
            try:
                data = bloomberg.get_historical_data(
                    ticker,
                    fields,
                    start_date.strftime('%Y%m%d'),
                    end_date.strftime('%Y%m%d')
                )
                
                if data is not None and not data.empty:
                    logger.info(f"  ✓ 成功: {len(data)}件")
                    for idx, row in data.iterrows():
                        logger.info(f"    {idx.date() if hasattr(idx, 'date') else idx}: {row.get('PX_LAST')}")
                else:
                    logger.warning(f"  ✗ データなし")
                    
            except Exception as e:
                logger.error(f"  ✗ エラー: {e}")
        
        # 全LMEデータを実際にロード（既存のローダーを使用）
        logger.info("\n\n=== 標準ローダーでLMEデータ更新 ===")
        
        from final_optimized_loader import OptimizedMultiExchangeLoader
        loader = OptimizedMultiExchangeLoader()
        
        # LMEのみロード
        loader.load_exchange_data('LME', days_back=3)
        
    finally:
        bloomberg.disconnect()
    
    # 結果確認
    cursor.execute("""
        SELECT 
            g.GenericTicker,
            g.Description,
            COUNT(DISTINCT p.TradeDate) as DataDays,
            MAX(p.TradeDate) as LatestDate,
            MAX(p.LastPrice) as LatestPrice
        FROM T_CommodityPrice_V2 p
        INNER JOIN M_GenericFutures g ON p.GenericID = g.GenericID
        WHERE g.GenericTicker IN ('LMCADY Index', 'LMCADS03 Comdty', 'LMCADS 0003 Comdty')
            OR g.GenericTicker IN ('LP1 Comdty', 'LP3 Comdty')  -- 比較用
        GROUP BY g.GenericTicker, g.Description
        ORDER BY g.GenericTicker
    """)
    
    logger.info("\n\n【最終結果】")
    results = cursor.fetchall()
    if results:
        for row in results:
            logger.info(f"{row[0]} ({row[1]}): {row[2]}日分, 最新={row[3]}, 価格={row[4]}")
    else:
        logger.warning("特殊ティッカーのデータが見つかりません")
    
    conn.close()

def main():
    """メイン処理"""
    logger.info(f"実行開始: {datetime.now()}")
    update_config_and_load()
    logger.info(f"\n実行完了: {datetime.now()}")

if __name__ == "__main__":
    main()