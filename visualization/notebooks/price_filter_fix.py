# アウトライト価格の修正されたフィルタリング

import pandas as pd
import sys
import os
sys.path.append('../config')
from data_utils import DataFetcher

print("=== CORRECTED OUTRIGHT PRICE FILTERING ===")

# データ取得
fetcher = DataFetcher()
historical_data = fetcher.get_copper_prices(days=30)

# 正常な価格範囲のアウトライト価格のみをフィルタリング
# 銅価格として妥当な範囲: 5,000 - 15,000 USD/MT
valid_price_data = historical_data[
    (historical_data['LastPrice'] >= 5000) & 
    (historical_data['LastPrice'] <= 15000)
].copy()

print(f"Original data: {len(historical_data)} records")
print(f"Valid price data: {len(valid_price_data)} records")

# 有効な限月タイプを確認
print(f"Valid tenor types: {valid_price_data['TenorTypeName'].unique()}")

# アウトライト価格として適切なもののみを選択
outright_tenors = ['Cash', '3M Futures']  # Generic Futuresは除外
corrected_outright_data = valid_price_data[
    valid_price_data['TenorTypeName'].isin(outright_tenors)
].copy()

print(f"Corrected outright data: {len(corrected_outright_data)} records")
print(f"Available tenors: {corrected_outright_data['TenorTypeName'].unique()}")

# 価格統計
for tenor in corrected_outright_data['TenorTypeName'].unique():
    tenor_data = corrected_outright_data[corrected_outright_data['TenorTypeName'] == tenor]
    print(f"{tenor}: {tenor_data['LastPrice'].min():.2f} - {tenor_data['LastPrice'].max():.2f} USD/MT")

print("\n✓ Corrected filtering complete!")