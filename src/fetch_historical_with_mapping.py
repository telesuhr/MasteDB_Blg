"""
ヒストリカルデータ（価格とマッピング）を一括取得するスクリプト
価格データとGeneric-Actual契約マッピングを同時に取得・更新する
"""
import sys
from datetime import datetime, timedelta
import logging
from bloomberg_api import BloombergDataFetcher
from database import DatabaseManager
from main import BloombergSQLIngestor
from historical_mapping_updater import HistoricalMappingUpdater

# ロギング設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def fetch_historical_data_with_mapping(start_date: str, end_date: str):
    """
    指定期間のヒストリカルデータを取得（価格データとマッピング両方）
    
    Args:
        start_date: 開始日 (YYYY-MM-DD)
        end_date: 終了日 (YYYY-MM-DD)
    """
    logger.info(f"=== ヒストリカルデータ取得開始: {start_date} から {end_date} ===")
    
    # Bloomberg APIとDB接続
    bloomberg_fetcher = BloombergDataFetcher()
    db_manager = DatabaseManager()
    
    if not bloomberg_fetcher.connect():
        logger.error("Bloomberg API接続に失敗しました")
        return False
        
    try:
        # 1. まずGeneric-Actual契約マッピングを取得
        logger.info("\n=== ステップ1: Generic-Actual契約マッピングを取得 ===")
        mapping_updater = HistoricalMappingUpdater(bloomberg_fetcher, db_manager)
        mapping_updater.update_historical_mappings(start_date, end_date)
        logger.info("マッピング取得完了")
        
        # 2. 価格データを取得
        logger.info("\n=== ステップ2: 価格データを取得 ===")
        ingestor = BloombergSQLIngestor()
        
        # 既に接続済みのbloomberg_fetcherとdb_managerを使用
        ingestor.bloomberg = bloomberg_fetcher
        ingestor.db_manager = db_manager
        
        # マスタデータのロード
        ingestor.db_manager.load_master_data()
        
        # データプロセッサーの初期化
        from data_processor import DataProcessor
        ingestor.processor = DataProcessor(ingestor.db_manager)
        
        # 各カテゴリのデータを取得
        from config.bloomberg_config import BLOOMBERG_TICKERS
        
        # 主要なカテゴリを処理
        categories_to_process = [
            'LME_COPPER_PRICES',
            'SHFE_COPPER_PRICES', 
            'CMX_COPPER_PRICES',
            'LME_INVENTORY',
            'SHFE_INVENTORY',
            'CMX_INVENTORY',
            'INTEREST_RATES',
            'CURRENCY_RATES',
            'COMMODITY_INDICES',
            'SHANGHAI_PREMIUM'
        ]
        
        for category_name in categories_to_process:
            if category_name in BLOOMBERG_TICKERS:
                logger.info(f"{category_name}を取得中...")
                ticker_info = BLOOMBERG_TICKERS[category_name]
                # 日付フォーマットをYYYYMMDDに変換
                start_date_bloomberg = start_date.replace('-', '')
                end_date_bloomberg = end_date.replace('-', '')
                record_count = ingestor.process_category(
                    category_name, ticker_info, start_date_bloomberg, end_date_bloomberg
                )
                logger.info(f"{category_name}: {record_count}件のレコードを保存")
        
        logger.info("\n=== 全てのデータ取得が完了しました ===")
        
        # 3. 取得結果のサマリーを表示
        show_summary(db_manager, start_date, end_date)
        
        return True
        
    except Exception as e:
        logger.error(f"エラーが発生しました: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False
    finally:
        bloomberg_fetcher.disconnect()

def show_summary(db_manager: DatabaseManager, start_date: str, end_date: str):
    """取得したデータのサマリーを表示"""
    with db_manager.get_connection() as conn:
        cursor = conn.cursor()
        
        # マッピングデータの件数
        cursor.execute("""
            SELECT COUNT(*) 
            FROM T_GenericContractMapping 
            WHERE TradeDate BETWEEN ? AND ?
        """, (start_date, end_date))
        mapping_count = cursor.fetchone()[0]
        
        # 価格データの件数
        cursor.execute("""
            SELECT COUNT(*) 
            FROM T_CommodityPrice 
            WHERE TradeDate BETWEEN ? AND ?
                AND DataType = 'Generic'
        """, (start_date, end_date))
        price_count = cursor.fetchone()[0]
        
        # LP1の契約切り替え履歴
        cursor.execute("""
            SELECT 
                MIN(m.TradeDate) as StartDate,
                MAX(m.TradeDate) as EndDate,
                ac.ContractTicker,
                COUNT(*) as Days
            FROM T_GenericContractMapping m
            INNER JOIN M_GenericFutures gf ON m.GenericID = gf.GenericID
            INNER JOIN M_ActualContract ac ON m.ActualContractID = ac.ActualContractID
            WHERE gf.GenericTicker = 'LP1 Comdty'
                AND m.TradeDate BETWEEN ? AND ?
            GROUP BY ac.ContractTicker
            ORDER BY MIN(m.TradeDate)
        """, (start_date, end_date))
        
        logger.info("\n=== データ取得サマリー ===")
        logger.info(f"期間: {start_date} から {end_date}")
        logger.info(f"マッピングデータ: {mapping_count}件")
        logger.info(f"価格データ: {price_count}件")
        logger.info("\nLP1 Comdtyの契約履歴:")
        
        for row in cursor.fetchall():
            logger.info(f"  {row[0]} - {row[1]}: {row[2]} ({row[3]}日間)")

def main():
    """メイン関数"""
    if len(sys.argv) < 3:
        # デフォルトで過去6か月
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=180)
        start_date_str = start_date.strftime('%Y-%m-%d')
        end_date_str = end_date.strftime('%Y-%m-%d')
        logger.info(f"引数が指定されていないため、デフォルト期間を使用: {start_date_str} から {end_date_str}")
    else:
        start_date_str = sys.argv[1]
        end_date_str = sys.argv[2]
    
    success = fetch_historical_data_with_mapping(start_date_str, end_date_str)
    
    if success:
        logger.info("\n処理が正常に完了しました")
        return 0
    else:
        logger.error("\n処理中にエラーが発生しました")
        return 1

if __name__ == "__main__":
    sys.exit(main())