"""
Bloomberg APIレスポンスのデバッグ
実際に何が返ってきているか確認
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

def debug_bloomberg_response():
    """Bloomberg APIレスポンスをデバッグ"""
    bloomberg = BloombergDataFetcher()
    
    try:
        if not bloomberg.connect():
            raise ConnectionError("Bloomberg API接続失敗")
        
        # テスト対象: CMXのHG1-HG10
        test_tickers = [f'HG{i} Comdty' for i in range(1, 11)]
        
        # フィールド
        fields = [
            'PX_LAST',
            'PX_SETTLE', 
            'PX_OPEN',
            'PX_HIGH',
            'PX_LOW',
            'PX_VOLUME',
            'VOLUME',
            'OPEN_INT'
        ]
        
        # 日付（今日と昨日）
        end_date = datetime.now().strftime('%Y%m%d')
        start_date = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
        
        print("=== Bloomberg APIレスポンスデバッグ ===")
        print(f"対象: {test_tickers}")
        print(f"期間: {start_date} - {end_date}")
        print(f"フィールド: {fields}")
        
        # データ取得
        print("\n実行中...")
        raw_data = bloomberg.get_historical_data(test_tickers, fields, start_date, end_date)
        
        print(f"\n取得レコード数: {len(raw_data)}")
        
        if not raw_data.empty:
            # 生データを表示
            print("\n=== 生データ（最初の20行） ===")
            pd.set_option('display.max_columns', None)
            pd.set_option('display.width', None)
            print(raw_data.head(20))
            
            # 日付別・ティッカー別集計
            print("\n=== 日付別・ティッカー別レコード数 ===")
            summary = raw_data.groupby(['date', 'security']).size().reset_index(name='count')
            print(summary.to_string(index=False))
            
            # 各ティッカーのデータ有無確認
            print("\n=== 各ティッカーのフィールド有無 ===")
            for ticker in test_tickers:
                ticker_data = raw_data[raw_data['security'] == ticker]
                if not ticker_data.empty:
                    print(f"\n{ticker}:")
                    for _, row in ticker_data.iterrows():
                        print(f"  {row['date']}:")
                        for field in fields:
                            value = row.get(field)
                            if pd.notna(value):
                                print(f"    {field}: {value}")
                else:
                    print(f"\n{ticker}: データなし")
                    
            # NaNチェック
            print("\n=== NaN値の分析 ===")
            for col in raw_data.columns:
                if col not in ['date', 'security']:
                    nan_count = raw_data[col].isna().sum()
                    total_count = len(raw_data)
                    print(f"{col}: {nan_count}/{total_count} ({nan_count/total_count*100:.1f}%)")
        else:
            print("データが取得できませんでした")
            
    finally:
        bloomberg.disconnect()

if __name__ == "__main__":
    debug_bloomberg_response()