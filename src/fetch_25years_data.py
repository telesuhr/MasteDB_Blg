"""
25年分のヒストリカルデータを段階的に取得するスクリプト
大量データのため、年単位でバッチ処理を行い、エラー時の再開も可能
"""
import sys
import os
import json
from datetime import datetime, timedelta
import logging
import time
from bloomberg_api import BloombergDataFetcher
from database import DatabaseManager
from main import BloombergSQLIngestor
from historical_mapping_updater import HistoricalMappingUpdater

# ロギング設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('fetch_25years.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# 進捗ファイル
PROGRESS_FILE = 'fetch_25years_progress.json'

def load_progress():
    """進捗状況を読み込む"""
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, 'r') as f:
            return json.load(f)
    return {'completed_years': []}

def save_progress(progress):
    """進捗状況を保存"""
    with open(PROGRESS_FILE, 'w') as f:
        json.dump(progress, f, indent=2)

def fetch_year_data(year: int, bloomberg_fetcher, db_manager):
    """1年分のデータを取得"""
    start_date = f"{year}-01-01"
    end_date = f"{year}-12-31"
    
    # 今年の場合は今日までのデータのみ取得
    if year == datetime.now().year:
        end_date = datetime.now().strftime('%Y-%m-%d')
    
    logger.info(f"\n{'='*60}")
    logger.info(f"{year}年のデータを取得: {start_date} から {end_date}")
    logger.info(f"{'='*60}")
    
    try:
        # 1. マッピングデータを取得
        logger.info(f"[{year}] Generic-Actual契約マッピングを取得中...")
        mapping_updater = HistoricalMappingUpdater(bloomberg_fetcher, db_manager)
        mapping_updater.update_historical_mappings(start_date, end_date)
        
        # 2. 価格データを取得
        logger.info(f"[{year}] 価格データを取得中...")
        ingestor = BloombergSQLIngestor(bloomberg_fetcher, db_manager)
        
        # LME銅価格
        logger.info(f"[{year}] LME銅価格データ...")
        ingestor.ingest_lme_copper_prices(start_date, end_date, is_initial=True)
        
        # LME在庫
        logger.info(f"[{year}] LME在庫データ...")
        ingestor.ingest_lme_inventory(start_date, end_date, is_initial=True)
        
        # 他取引所データ
        logger.info(f"[{year}] SHFE/COMEX価格データ...")
        ingestor.ingest_other_exchange_prices(start_date, end_date, is_initial=True)
        ingestor.ingest_other_exchange_inventory(start_date, end_date, is_initial=True)
        
        # 市場指標
        logger.info(f"[{year}] 市場指標データ...")
        ingestor.ingest_market_indicators(start_date, end_date, is_initial=True)
        
        # 取得結果を確認
        show_year_summary(db_manager, year)
        
        logger.info(f"[{year}] ✓ 完了")
        return True
        
    except Exception as e:
        logger.error(f"[{year}] ✗ エラー: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False

def show_year_summary(db_manager: DatabaseManager, year: int):
    """年次サマリーを表示"""
    start_date = f"{year}-01-01"
    end_date = f"{year}-12-31"
    
    with db_manager.get_connection() as conn:
        cursor = conn.cursor()
        
        # データ件数を取得
        cursor.execute("""
            SELECT 
                'Mapping' as DataType,
                COUNT(*) as RecordCount
            FROM T_GenericContractMapping
            WHERE YEAR(TradeDate) = ?
            UNION ALL
            SELECT 
                'Price' as DataType,
                COUNT(*) as RecordCount
            FROM T_CommodityPrice
            WHERE YEAR(TradeDate) = ?
                AND DataType = 'Generic'
            UNION ALL
            SELECT 
                'Inventory' as DataType,
                COUNT(*) as RecordCount
            FROM T_LMEInventory
            WHERE YEAR(ReportDate) = ?
        """, (year, year, year))
        
        logger.info(f"\n[{year}] 取得データサマリー:")
        for row in cursor.fetchall():
            logger.info(f"  - {row[0]}: {row[1]:,}件")

def main():
    """メイン処理"""
    # 開始年と終了年を設定
    end_year = datetime.now().year
    start_year = end_year - 24  # 25年前から
    
    # 引数で範囲を指定可能
    if len(sys.argv) >= 3:
        start_year = int(sys.argv[1])
        end_year = int(sys.argv[2])
    
    logger.info(f"データ取得期間: {start_year}年 から {end_year}年 (計{end_year - start_year + 1}年間)")
    
    # 進捗を読み込む
    progress = load_progress()
    completed_years = progress.get('completed_years', [])
    
    if completed_years:
        logger.info(f"完了済み年: {completed_years}")
        response = input("\n既存の進捗から続行しますか？ (Y/N): ")
        if response.upper() != 'Y':
            completed_years = []
            progress = {'completed_years': []}
    
    # Bloomberg API接続
    bloomberg_fetcher = BloombergDataFetcher()
    db_manager = DatabaseManager()
    
    if not bloomberg_fetcher.connect():
        logger.error("Bloomberg API接続に失敗しました")
        return 1
    
    try:
        # 年ごとにデータを取得
        for year in range(start_year, end_year + 1):
            if year in completed_years:
                logger.info(f"\n{year}年: スキップ（完了済み）")
                continue
            
            # データ取得
            success = fetch_year_data(year, bloomberg_fetcher, db_manager)
            
            if success:
                # 進捗を保存
                completed_years.append(year)
                progress['completed_years'] = sorted(completed_years)
                save_progress(progress)
                
                # API負荷軽減のため少し待機
                if year < end_year:
                    logger.info("次の年の処理まで10秒待機...")
                    time.sleep(10)
            else:
                logger.error(f"{year}年の処理に失敗しました。処理を中断します。")
                logger.info(f"再開するには同じコマンドを実行してください。")
                break
        
        # 最終サマリー
        if len(completed_years) == (end_year - start_year + 1):
            logger.info("\n" + "="*60)
            logger.info("全ての年のデータ取得が完了しました！")
            show_final_summary(db_manager, start_year, end_year)
            
            # 進捗ファイルを削除
            if os.path.exists(PROGRESS_FILE):
                os.remove(PROGRESS_FILE)
        
        return 0
        
    except Exception as e:
        logger.error(f"エラーが発生しました: {e}")
        return 1
    finally:
        bloomberg_fetcher.disconnect()

def show_final_summary(db_manager: DatabaseManager, start_year: int, end_year: int):
    """最終サマリーを表示"""
    with db_manager.get_connection() as conn:
        cursor = conn.cursor()
        
        # 全体のデータ件数
        cursor.execute("""
            SELECT 
                'T_GenericContractMapping' as TableName,
                COUNT(*) as TotalRecords,
                MIN(TradeDate) as OldestDate,
                MAX(TradeDate) as NewestDate
            FROM T_GenericContractMapping
            UNION ALL
            SELECT 
                'T_CommodityPrice' as TableName,
                COUNT(*) as TotalRecords,
                MIN(TradeDate) as OldestDate,
                MAX(TradeDate) as NewestDate
            FROM T_CommodityPrice
            WHERE DataType = 'Generic'
            UNION ALL
            SELECT 
                'T_LMEInventory' as TableName,
                COUNT(*) as TotalRecords,
                MIN(ReportDate) as OldestDate,
                MAX(ReportDate) as NewestDate
            FROM T_LMEInventory
        """)
        
        logger.info("\n最終データサマリー:")
        logger.info(f"{'テーブル':<30} {'レコード数':>15} {'最古日付':<12} {'最新日付':<12}")
        logger.info("-" * 70)
        
        for row in cursor.fetchall():
            logger.info(f"{row[0]:<30} {row[1]:>15,} {str(row[2]):<12} {str(row[3]):<12}")

if __name__ == "__main__":
    sys.exit(main())