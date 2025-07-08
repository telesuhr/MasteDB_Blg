"""
LME銅先物データを36か月分に拡張するスクリプト
既存の12か月分のデータはそのまま残し、13-36か月分を追加で取得
"""
import os
import sys
import logging
from datetime import datetime, timedelta

# プロジェクトのルートディレクトリをPythonパスに追加
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)

from src.main import BloombergSQLIngestor
from config.bloomberg_config import BLOOMBERG_TICKERS

# ログディレクトリの作成
if not os.path.exists('logs'):
    os.makedirs('logs')

# ロギング設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'logs/update_lme_36months_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def update_lme_36months():
    """LME銅先物を36か月分に拡張"""
    try:
        logger.info("LME銅先物データ36か月拡張処理を開始")
        
        # BloombergSQLIngestorのインスタンス作成
        ingestor = BloombergSQLIngestor()
        
        # システムの初期化（Bloomberg API接続、DB接続、マスタデータロード）
        ingestor.initialize()
        
        # 13-36か月分のティッカーリストを作成
        extended_tickers = [f'LP{i} Comdty' for i in range(13, 37)]
        logger.info(f"追加ティッカー: {extended_tickers}")
        
        # 既存データの最新日付を確認（実際の実装ではDB確認が必要）
        # ここでは過去5年分を取得する設定にする
        end_date = datetime.now()
        start_date = end_date - timedelta(days=365 * 5)  # 5年分のデータ
        
        # Bloomberg APIでデータ取得
        logger.info(f"データ取得期間: {start_date.strftime('%Y%m%d')} - {end_date.strftime('%Y%m%d')}")
        
        # 拡張したティッカー設定を一時的に作成
        extended_config = {
            'LME_COPPER_EXTENDED': {
                'securities': extended_tickers,
                'fields': BLOOMBERG_TICKERS['LME_COPPER_PRICES']['fields'],
                'table': 'T_CommodityPrice',
                'metal': 'COPPER',
                'exchange': 'LME',
                'tenor_mapping': {
                    f'LP{i} Comdty': f'Generic {i}st Future' if i == 21
                    else f'Generic {i}nd Future' if i == 22
                    else f'Generic {i}rd Future' if i == 23
                    else f'Generic {i}th Future' 
                    for i in range(13, 37)
                }
            }
        }
        
        # データ取得と保存
        logger.info("Bloomberg APIからデータ取得中...")
        for category, config in extended_config.items():
            try:
                # データ取得
                data = ingestor.bloomberg.batch_request(
                    config['securities'],
                    config['fields'],
                    start_date.strftime('%Y%m%d'),
                    end_date.strftime('%Y%m%d')
                )
                
                if data.empty:
                    logger.warning(f"{category}のデータが取得できませんでした")
                    continue
                
                logger.info(f"{category}: {len(data)}件のデータを取得")
                
                # データ処理
                processed_data = ingestor.processor.process_commodity_prices(
                    data, 
                    config
                )
                
                # データベースへ保存
                if not processed_data.empty:
                    unique_columns = ['TradeDate', 'MetalID', 'TenorTypeID', 'SpecificTenorDate']
                    saved_count = ingestor.db_manager.upsert_dataframe(
                        processed_data, 
                        'T_CommodityPrice', 
                        unique_columns
                    )
                    logger.info(f"{category}: {saved_count}件のデータを保存")
                
            except Exception as e:
                logger.error(f"{category}の処理中にエラー: {str(e)}")
                continue
        
        logger.info("LME銅先物データ36か月拡張処理が完了しました")
        
    except Exception as e:
        logger.error(f"処理中にエラーが発生: {str(e)}")
        raise
    finally:
        if 'ingestor' in locals():
            ingestor.cleanup()

if __name__ == "__main__":
    update_lme_36months()