"""
3営業日分のマルチ取引所データ取得プログラム
実際のBloomberg APIを使用してLME、SHFE、CMXのデータを取得
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.main import BloombergSQLIngestor
from datetime import datetime, timedelta

def get_last_3_business_days():
    """直近3営業日を取得"""
    business_days = []
    current_date = datetime.now().date()
    
    while len(business_days) < 3:
        if current_date.weekday() < 5:  # 土日除外
            business_days.append(current_date)
        current_date -= timedelta(days=1)
    
    business_days.reverse()  # 古い順に並べ替え
    return business_days

def main():
    """メイン実行"""
    print("=== 3営業日分のマルチ取引所データ取得開始 ===")
    
    try:
        # Ingestorインスタンス作成
        ingestor = BloombergSQLIngestor()
        
        # 3営業日の取得
        business_days = get_last_3_business_days()
        print(f"対象営業日: {', '.join(str(d) for d in business_days)}")
        
        # 各営業日のデータ取得
        for trade_date in business_days:
            print(f"\n--- {trade_date} のデータ取得開始 ---")
            
            # 日次更新モードで実行（指定日付）
            success = ingestor.run(mode="daily", specific_date=trade_date)
            
            if success:
                print(f"✅ {trade_date} のデータ取得完了")
            else:
                print(f"❌ {trade_date} のデータ取得失敗")
        
        print("\n=== 3営業日分のデータ取得完了 ===")
        
        # データ確認
        ingestor._verify_data_quality()
        
        return True
        
    except Exception as e:
        print(f"エラー: {e}")
        return False

if __name__ == "__main__":
    main()