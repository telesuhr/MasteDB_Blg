"""
日付範囲を指定してデータを取得するスクリプト
全カテゴリのデータを指定期間で取得
"""
import argparse
import sys
from datetime import datetime
from bloomberg_api import BloombergDataFetcher
from database import DatabaseManager
from main import BloombergSQLIngestor
from config.bloomberg_config import BLOOMBERG_TICKERS
import logging

# ロギング設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    # 引数パーサーの設定
    parser = argparse.ArgumentParser(
        description='Bloomberg データを指定期間で取得'
    )
    parser.add_argument(
        'start_date',
        help='開始日 (YYYY-MM-DD)'
    )
    parser.add_argument(
        'end_date',
        help='終了日 (YYYY-MM-DD)'
    )
    parser.add_argument(
        '--categories',
        nargs='+',
        help='取得するカテゴリ（指定しない場合は全て）',
        default=None
    )
    
    args = parser.parse_args()
    
    # 日付検証
    try:
        start_dt = datetime.strptime(args.start_date, '%Y-%m-%d')
        end_dt = datetime.strptime(args.end_date, '%Y-%m-%d')
        
        if start_dt > end_dt:
            logger.error("開始日は終了日より前である必要があります")
            return 1
            
    except ValueError:
        logger.error("日付はYYYY-MM-DD形式で指定してください")
        return 1
    
    # Bloomberg形式に変換
    start_date_bloomberg = args.start_date.replace('-', '')
    end_date_bloomberg = args.end_date.replace('-', '')
    
    logger.info(f"データ取得期間: {args.start_date} から {args.end_date}")
    
    # Bloomberg APIとDB接続
    bloomberg_fetcher = BloombergDataFetcher()
    db_manager = DatabaseManager()
    
    if not bloomberg_fetcher.connect():
        logger.error("Bloomberg API接続に失敗しました")
        return 1
        
    try:
        # Ingestorの初期化
        ingestor = BloombergSQLIngestor()
        ingestor.bloomberg = bloomberg_fetcher
        ingestor.db_manager = db_manager
        ingestor.db_manager.load_master_data()
        
        from data_processor import DataProcessor
        ingestor.processor = DataProcessor(ingestor.db_manager)
        
        # カテゴリの選択
        if args.categories:
            # 指定されたカテゴリのみ
            categories_to_process = {
                k: v for k, v in BLOOMBERG_TICKERS.items() 
                if k in args.categories
            }
        else:
            # 全カテゴリ
            categories_to_process = BLOOMBERG_TICKERS
        
        logger.info(f"処理するカテゴリ数: {len(categories_to_process)}")
        
        # 各カテゴリのデータを取得
        total_records = 0
        for category_name, ticker_info in categories_to_process.items():
            logger.info(f"\n=== {category_name} を処理中 ===")
            try:
                record_count = ingestor.process_category(
                    category_name, 
                    ticker_info, 
                    start_date_bloomberg, 
                    end_date_bloomberg
                )
                logger.info(f"{category_name}: {record_count}件のレコードを保存")
                total_records += record_count
                
            except Exception as e:
                logger.error(f"{category_name}の処理でエラー: {e}")
                import traceback
                logger.debug(traceback.format_exc())
                
        logger.info(f"\n=== 完了: 合計 {total_records} 件のレコードを保存 ===")
        
        # サマリー表示
        show_summary(db_manager, args.start_date, args.end_date)
        
        return 0
        
    except Exception as e:
        logger.error(f"エラーが発生しました: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return 1
        
    finally:
        bloomberg_fetcher.disconnect()

def show_summary(db_manager, start_date: str, end_date: str):
    """取得結果のサマリーを表示"""
    try:
        with db_manager.get_connection() as conn:
            cursor = conn.cursor()
            
            # 価格データのサマリー
            cursor.execute("""
                SELECT 
                    COUNT(*) as RecordCount,
                    COUNT(DISTINCT TradeDate) as TradingDays
                FROM T_CommodityPrice
                WHERE TradeDate BETWEEN ? AND ?
            """, (start_date, end_date))
            result = cursor.fetchone()
            if result:
                logger.info(f"\n価格データ: {result[0]}件 ({result[1]}営業日)")
            
            # 在庫データのサマリー
            cursor.execute("""
                SELECT COUNT(*) FROM T_LMEInventory
                WHERE ReportDate BETWEEN ? AND ?
            """, (start_date, end_date))
            inv_count = cursor.fetchone()[0]
            logger.info(f"LME在庫データ: {inv_count}件")
            
            # その他の指標
            cursor.execute("""
                SELECT COUNT(*) FROM T_MarketIndicator
                WHERE ReportDate BETWEEN ? AND ?
            """, (start_date, end_date))
            ind_count = cursor.fetchone()[0]
            logger.info(f"市場指標データ: {ind_count}件")
            
    except Exception as e:
        logger.error(f"サマリー表示でエラー: {e}")

if __name__ == "__main__":
    sys.exit(main())