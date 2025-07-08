"""
Bloomberg APIのフィールド取得テスト
各取引所・銘柄でどのフィールドが実際に取得できるか調査
"""
import sys
import os
from datetime import datetime, timedelta
import pandas as pd
import pytz

# プロジェクトルートとsrcディレクトリをPythonパスに追加
project_root = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.join(project_root, 'src')
sys.path.insert(0, project_root)
sys.path.insert(0, src_dir)

from bloomberg_api import BloombergDataFetcher
from config.logging_config import logger

def test_bloomberg_fields():
    """Bloombergフィールドテスト"""
    bloomberg = BloombergDataFetcher()
    
    try:
        if not bloomberg.connect():
            raise ConnectionError("Bloomberg API接続失敗")
        
        # テスト対象銘柄（各取引所の代表的な銘柄）
        test_tickers = {
            'LME': ['LP1 Comdty', 'LP3 Comdty', 'LP12 Comdty'],
            'SHFE': ['CU1 Comdty', 'CU3 Comdty'],
            'CMX': ['HG1 Comdty', 'HG3 Comdty', 'HG12 Comdty']
        }
        
        # テストするフィールド（包括的リスト）
        test_fields = [
            # 価格フィールド
            'PX_LAST',          # 終値
            'PX_SETTLE',        # 清算値
            'SETTLE',           # 清算値（別名）
            'PX_OPEN',          # 始値
            'PX_HIGH',          # 高値
            'PX_LOW',           # 安値
            'PX_MID',           # 仲値
            'PX_BID',           # 買値
            'PX_ASK',           # 売値
            'LAST_PRICE',       # 直近価格
            'PRIOR_SETTLE_DT',  # 前営業日清算値
            
            # 出来高フィールド
            'PX_VOLUME',        # 出来高
            'VOLUME',           # 出来高（別名）
            'TURNOVER',         # 売買代金
            'TRADES',           # 取引回数
            
            # 建玉フィールド
            'OPEN_INT',         # 建玉
            'CHG_OPEN_INT',     # 建玉変化
            'PRIOR_OPEN_INT',   # 前日建玉
            
            # 日付フィールド
            'LAST_UPDATE_DT',   # 最終更新日
            'LAST_UPDATE',      # 最終更新
            'TIME'              # 時刻
        ]
        
        # 日付範囲（直近1日）
        end_date = datetime.now().strftime('%Y%m%d')
        start_date = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
        
        print("=== Bloomberg フィールド取得テスト ===")
        print(f"テスト日付: {start_date} - {end_date}")
        
        # 各取引所でテスト
        for exchange, tickers in test_tickers.items():
            print(f"\n--- {exchange} 取引所 ---")
            
            for ticker in tickers:
                print(f"\n{ticker}:")
                
                # データ取得
                try:
                    data = bloomberg.get_historical_data([ticker], test_fields, start_date, end_date)
                    
                    if data.empty:
                        print("  データなし")
                        continue
                    
                    # 取得できたフィールドを確認
                    available_fields = []
                    for field in test_fields:
                        if field in data.columns and data[field].notna().any():
                            sample_value = data[field].iloc[0]
                            available_fields.append(f"{field}: {sample_value}")
                    
                    print(f"  取得可能フィールド数: {len(available_fields)}")
                    for field_info in available_fields:
                        print(f"    {field_info}")
                        
                except Exception as e:
                    print(f"  エラー: {e}")
        
        # 時刻確認
        print("\n=== 時刻確認 ===")
        print(f"現在時刻（UTC）: {datetime.utcnow()}")
        print(f"現在時刻（JST）: {datetime.now(pytz.timezone('Asia/Tokyo'))}")
        print(f"現在時刻（ローカル）: {datetime.now()}")
        
    finally:
        bloomberg.disconnect()

if __name__ == "__main__":
    test_bloomberg_fields()