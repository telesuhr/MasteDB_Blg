#!/usr/bin/env python3
"""
LME在庫ティッカーのテストスクリプト
正しいティッカー形式を確認するため
"""
import sys
import os

# プロジェクトルートとsrcディレクトリをPythonパスに追加
project_root = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.join(project_root, 'src')
sys.path.insert(0, project_root)
sys.path.insert(0, src_dir)

from src.bloomberg_api import BloombergAPI
from config.logging_config import logger
from datetime import datetime, timedelta

def test_lme_tickers():
    """様々な形式のLMEティッカーをテスト"""
    
    # テストするティッカーのパターン
    test_patterns = [
        # %パターン
        ['NLSCA Index', 'NLSCA %ASIA Index', 'NLSCA %AMER Index', 'NLSCA %EURO Index'],
        # スペースなしパターン
        ['NLSCA Index', 'NLSCAASIA Index', 'NLSCAAMER Index', 'NLSCAEURO Index'],
        # ハイフンパターン
        ['NLSCA Index', 'NLSCA-ASIA Index', 'NLSCA-AMER Index', 'NLSCA-EURO Index'],
        # アンダースコアパターン
        ['NLSCA Index', 'NLSCA_ASIA Index', 'NLSCA_AMER Index', 'NLSCA_EURO Index'],
        # スペースのみパターン
        ['NLSCA Index', 'NLSCA ASIA Index', 'NLSCA AMER Index', 'NLSCA EURO Index'],
    ]
    
    bloomberg = BloombergAPI()
    
    try:
        bloomberg.connect()
        
        # 最新日付のデータだけ取得
        end_date = datetime.now().strftime('%Y%m%d')
        start_date = (datetime.now() - timedelta(days=7)).strftime('%Y%m%d')
        
        for i, pattern in enumerate(test_patterns):
            logger.info(f"\n=== パターン {i+1} のテスト ===")
            logger.info(f"テストティッカー: {pattern}")
            
            try:
                df = bloomberg.get_historical_data(
                    pattern, 
                    ['PX_LAST'], 
                    start_date, 
                    end_date
                )
                
                if df.empty:
                    logger.warning(f"パターン {i+1}: データなし")
                else:
                    logger.success(f"パターン {i+1}: {len(df)}件のデータ取得成功！")
                    # ティッカー別のデータ件数を表示
                    ticker_counts = df.groupby('security').size()
                    for ticker, count in ticker_counts.items():
                        logger.info(f"  {ticker}: {count}件")
                        
            except Exception as e:
                logger.error(f"パターン {i+1} エラー: {e}")
                
    finally:
        bloomberg.disconnect()

if __name__ == "__main__":
    test_lme_tickers()