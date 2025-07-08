"""
CMXの全ティッカーのデータ取得確認
"""
import sys
import os

# プロジェクトルートとsrcディレクトリをPythonパスに追加
project_root = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.join(project_root, 'src')
sys.path.insert(0, project_root)
sys.path.insert(0, src_dir)

from bloomberg_api import BloombergDataFetcher
import pandas as pd

def test_all_cmx():
    """CMX全ティッカーテスト"""
    bloomberg = BloombergDataFetcher()
    
    if not bloomberg.connect():
        print("Bloomberg接続失敗")
        return
        
    # CMXの全ティッカー（HG1-HG36）
    all_tickers = [f'HG{i} Comdty' for i in range(1, 37)]
    fields = ['PX_LAST', 'PX_SETTLE', 'PX_VOLUME', 'VOLUME']
    
    print("=== CMX全ティッカーテスト ===")
    print(f"対象: HG1-HG36 ({len(all_tickers)}銘柄)")
    print("日付: 2025-07-08")
    
    # バッチで取得（25銘柄ずつ）
    all_data = pd.DataFrame()
    for i in range(0, len(all_tickers), 25):
        batch = all_tickers[i:i+25]
        print(f"\nバッチ {i//25 + 1}: {batch[0]} - {batch[-1]}")
        
        data = bloomberg.get_historical_data(batch, fields, '20250708', '20250708')
        print(f"  取得: {len(data)}件")
        
        if not data.empty:
            all_data = pd.concat([all_data, data], ignore_index=True)
    
    print(f"\n合計取得レコード数: {len(all_data)}")
    
    if not all_data.empty:
        # どのティッカーが取得できたか確認
        retrieved_tickers = set(all_data['security'].unique())
        missing_tickers = set(all_tickers) - retrieved_tickers
        
        print(f"\n取得成功: {len(retrieved_tickers)}銘柄")
        print(f"取得失敗: {len(missing_tickers)}銘柄")
        
        if missing_tickers:
            print("\n取得できなかった銘柄:")
            for ticker in sorted(missing_tickers):
                print(f"  {ticker}")
    
    bloomberg.disconnect()
    print("\n=== テスト完了 ===")

if __name__ == "__main__":
    test_all_cmx()