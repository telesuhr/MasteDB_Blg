# 修正版: アウトライト価格の時系列チャート作成
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pandas as pd

# 正常な価格範囲でフィルタリング（Generic Futuresの問題を解決）
if not historical_data.empty:
    print("Step 9 (Corrected): Filtering valid outright prices...")
    
    # 銅価格として妥当な範囲: 5,000 - 15,000 USD/MT
    valid_price_data = historical_data[
        (historical_data['LastPrice'] >= 5000) & 
        (historical_data['LastPrice'] <= 15000)
    ].copy()
    
    print(f"Original data: {len(historical_data)} records")
    print(f"Valid price data: {len(valid_price_data)} records")
    
    # アウトライト価格のフィルタリング（問題のあるGeneric Futuresは除外）
    outright_tenors = ['Cash', '3M Futures']  # 正常な価格のみ
    outright_data = valid_price_data[
        valid_price_data['TenorTypeName'].isin(outright_tenors)
    ].copy()
    
    print(f"Corrected outright data: {len(outright_data)} records")
    print(f"Available tenors: {outright_data['TenorTypeName'].unique()}")
    
    if not outright_data.empty:
        print("Step 10 (Corrected): Creating corrected outright price chart...")
        
        # 日付をdatetimeに変換
        outright_data['TradeDate'] = pd.to_datetime(outright_data['TradeDate'])
        
        # 価格チャートの作成
        plt.figure(figsize=(14, 8))
        
        # 限月タイプごとに線を描画
        colors = {'Cash': '#1f77b4', '3M Futures': '#ff7f0e'}
        
        for tenor in outright_data['TenorTypeName'].unique():
            tenor_data = outright_data[outright_data['TenorTypeName'] == tenor]
            if not tenor_data.empty:
                plt.plot(tenor_data['TradeDate'], tenor_data['LastPrice'], 
                        label=tenor, linewidth=2, color=colors.get(tenor, '#333333'), 
                        marker='o', markersize=4)
        
        # チャートの設定
        plt.title('Copper Outright Price Historical Chart (30 Days) - Corrected', 
                 fontsize=16, fontweight='bold', pad=20)
        plt.xlabel('Date', fontsize=12)
        plt.ylabel('Price (USD/MT)', fontsize=12)
        plt.legend(fontsize=11)
        plt.grid(True, alpha=0.3)
        
        # Y軸の範囲を調整（正常な価格帯に集中）
        plt.ylim(9000, 11000)
        
        # 日付軸の書式設定
        plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))
        plt.gca().xaxis.set_major_locator(mdates.DayLocator(interval=3))
        plt.xticks(rotation=45)
        
        # レイアウト調整
        plt.tight_layout()
        plt.show()
        
        print("Step 10 (Corrected): Outright price chart - SUCCESS")
        
        # 価格統計も表示
        print("\n" + "="*50)
        print("CORRECTED OUTRIGHT PRICE STATISTICS")
        print("="*50)
        
        for tenor in outright_data['TenorTypeName'].unique():
            tenor_data = outright_data[outright_data['TenorTypeName'] == tenor]
            if not tenor_data.empty:
                latest_price = tenor_data['LastPrice'].iloc[-1]
                min_price = tenor_data['LastPrice'].min()
                max_price = tenor_data['LastPrice'].max()
                avg_price = tenor_data['LastPrice'].mean()
                
                print(f"\n{tenor}:")
                print(f"  Latest Price: {latest_price:,.2f} USD/MT")
                print(f"  30-Day Range: {min_price:,.2f} - {max_price:,.2f} USD/MT")
                print(f"  30-Day Average: {avg_price:,.2f} USD/MT")
                
                # 30日間の変動計算
                if len(tenor_data) > 1:
                    first_price = tenor_data['LastPrice'].iloc[0]
                    price_change = latest_price - first_price
                    price_change_pct = (price_change / first_price) * 100
                    print(f"  30-Day Change: {price_change:+,.2f} USD/MT ({price_change_pct:+.2f}%)")
        
    else:
        print("Step 10: No valid outright data available for charting")

else:
    print("Step 9-10: No historical data available")