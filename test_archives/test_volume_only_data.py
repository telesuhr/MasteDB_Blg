"""
Volumeのみのデータがあるか確認
"""
import sys
import os

# プロジェクトルートとsrcディレクトリをPythonパスに追加
project_root = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.join(project_root, 'src')
sys.path.insert(0, project_root)
sys.path.insert(0, src_dir)

from bloomberg_api import BloombergDataFetcher
from datetime import datetime, timedelta
import pandas as pd

def test_volume_only():
    """Volumeのみのデータテスト"""
    bloomberg = BloombergDataFetcher()
    
    if not bloomberg.connect():
        print("Bloomberg接続失敗")
        return
        
    # HG5-HG10 for 2025-07-08
    test_tickers = [f'HG{i} Comdty' for i in range(5, 11)]
    fields = ['PX_LAST', 'PX_SETTLE', 'PX_OPEN', 'PX_HIGH', 'PX_LOW', 'PX_VOLUME', 'VOLUME', 'OPEN_INT']
    
    print("=== Volume-onlyデータテスト ===")
    print(f"対象: {test_tickers}")
    print("日付: 2025-07-08")
    
    data = bloomberg.get_historical_data(test_tickers, fields, '20250708', '20250708')
    
    print(f'\n取得レコード数: {len(data)}')
    
    if not data.empty:
        print('\n受信データ:')
        for _, row in data.iterrows():
            print(f"\n{row['security']}:")
            has_price = False
            has_volume = False
            
            # 価格データチェック
            for price_field in ['PX_LAST', 'PX_SETTLE']:
                val = row.get(price_field)
                if pd.notna(val):
                    has_price = True
                    print(f'  {price_field}: {val}')
                    
            # OHLCデータチェック
            for ohlc_field in ['PX_OPEN', 'PX_HIGH', 'PX_LOW']:
                val = row.get(ohlc_field)
                if pd.notna(val):
                    print(f'  {ohlc_field}: {val}')
                    
            # Volumeデータチェック
            for vol_field in ['PX_VOLUME', 'VOLUME']:
                val = row.get(vol_field)
                if pd.notna(val):
                    has_volume = True
                    print(f'  {vol_field}: {val}')
                    
            # OIデータチェック
            oi = row.get('OPEN_INT')
            if pd.notna(oi):
                print(f'  OPEN_INT: {oi}')
                
            # 分析
            if has_volume and not has_price:
                print('  → Volume-onlyレコード')
            elif has_price and not has_volume:
                print('  → Price-onlyレコード')
            elif has_price and has_volume:
                print('  → 完全レコード')
            else:
                print('  → 空レコード')
    else:
        print("データなし")
    
    bloomberg.disconnect()
    print("\n=== テスト完了 ===")

if __name__ == "__main__":
    test_volume_only()