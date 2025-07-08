"""
Bloomberg APIから直接データを取得してテスト
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), 'src'))

from bloomberg_api import BloombergDataFetcher
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_fetch():
    """Bloomberg APIから直接データを取得"""
    bloomberg = BloombergDataFetcher()
    
    if not bloomberg.connect():
        logger.error("Bloomberg接続失敗")
        return
        
    try:
        # テスト対象のティッカー
        test_tickers = [
            'LMCADY Index',      # Cash
            'CAD TT00 Comdty',   # Tom-Next
            'LMCADS03 Comdty',   # 3M
            'LP1 Comdty',        # Generic 1
            'LP12 Comdty',       # Generic 12
            'LP13 Comdty',       # Generic 13
            'LP36 Comdty'        # Generic 36
        ]
        
        # 各ティッカーを個別に取得
        for ticker in test_tickers:
            logger.info(f"\n{ticker}を取得中...")
            try:
                df = bloomberg.get_historical_data(
                    [ticker],
                    ['PX_LAST'],
                    '20250707',
                    '20250707'
                )
                if df is not None and not df.empty:
                    logger.info(f"  成功: {len(df)}件")
                    logger.info(f"  価格: {df.iloc[0]['PX_LAST']}")
                else:
                    logger.warning(f"  失敗: データなし")
            except Exception as e:
                logger.error(f"  エラー: {e}")
                
        # バッチで取得
        logger.info("\n全ティッカーをバッチ取得...")
        df = bloomberg.batch_request(
            test_tickers,
            ['PX_LAST'],
            '20250707',
            '20250707',
            request_type='historical'
        )
        
        if not df.empty:
            logger.info(f"バッチ取得成功: {len(df)}件")
            logger.info("\n取得されたティッカー:")
            for ticker in df['security'].unique():
                logger.info(f"  - {ticker}")
        else:
            logger.warning("バッチ取得失敗")
            
    finally:
        bloomberg.disconnect()

if __name__ == "__main__":
    test_fetch()