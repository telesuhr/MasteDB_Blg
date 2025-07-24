"""
データ欠損期間を検出し、自動補完するスクリプト
"""
import sys
import os
from datetime import datetime, timedelta
import pandas as pd

# プロジェクトルートを追加
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.database import DatabaseManager
from config.logging_config import logger

def check_missing_dates(days_back=30):
    """過去N日間のデータ欠損をチェック"""
    
    db_manager = DatabaseManager()
    db_manager.connect()
    
    try:
        # 過去30日間の営業日を生成
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=days_back)
        
        # データベースから既存の日付を取得
        query = """
        SELECT DISTINCT TradeDate
        FROM T_CommodityPrice
        WHERE TradeDate >= ? AND TradeDate <= ?
        AND MetalID = 1  -- Copper
        ORDER BY TradeDate
        """
        
        with db_manager.get_connection() as conn:
            existing_dates = pd.read_sql(query, conn, params=(start_date, end_date))
            existing_dates['TradeDate'] = pd.to_datetime(existing_dates['TradeDate']).dt.date
        
        # 営業日を生成（簡易版：土日を除く）
        business_days = pd.bdate_range(start=start_date, end=end_date, freq='B')
        business_days = [d.date() for d in business_days]
        
        # 欠損日を検出
        existing_set = set(existing_dates['TradeDate'].tolist())
        missing_dates = [d for d in business_days if d not in existing_set]
        
        if missing_dates:
            logger.warning(f"Found {len(missing_dates)} missing dates:")
            for date in missing_dates:
                logger.warning(f"  - {date}")
            
            # 連続する欠損期間を検出
            missing_ranges = []
            if missing_dates:
                current_start = missing_dates[0]
                current_end = missing_dates[0]
                
                for date in missing_dates[1:]:
                    if (date - current_end).days <= 1:
                        current_end = date
                    else:
                        missing_ranges.append((current_start, current_end))
                        current_start = date
                        current_end = date
                
                missing_ranges.append((current_start, current_end))
            
            return missing_ranges
        else:
            logger.info("No missing dates found in the last %d days", days_back)
            return []
            
    finally:
        db_manager.disconnect()

def auto_fill_missing_data(missing_ranges):
    """欠損期間のデータを自動補完"""
    
    for start_date, end_date in missing_ranges:
        logger.info(f"Fetching missing data from {start_date} to {end_date}")
        
        # main.pyを使って欠損データを取得
        from src.main import BloombergSQLIngestor
        
        ingestor = BloombergSQLIngestor()
        try:
            ingestor.initialize()
            
            # 日付範囲を指定してデータ取得
            start_str = start_date.strftime('%Y%m%d')
            end_str = end_date.strftime('%Y%m%d')
            
            # 各カテゴリのデータを取得
            for category_name, ticker_info in ingestor.BLOOMBERG_TICKERS.items():
                if category_name not in ['COTR_DATA']:  # COTRは週次なので除外
                    ingestor.process_category(category_name, ticker_info, start_str, end_str)
            
        finally:
            ingestor.cleanup()

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Check and fill missing data')
    parser.add_argument('--days', type=int, default=30, help='Number of days to check')
    parser.add_argument('--auto-fill', action='store_true', help='Automatically fill missing data')
    
    args = parser.parse_args()
    
    missing_ranges = check_missing_dates(args.days)
    
    if missing_ranges and args.auto_fill:
        auto_fill_missing_data(missing_ranges)
    elif missing_ranges:
        print("\nTo automatically fill missing data, run:")
        print(f"python {__file__} --days {args.days} --auto-fill")