"""
Bloomberg Data Ingestor with Automatic Mapping
自動マッピング機能を統合したメインスクリプト
"""
import sys
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from loguru import logger

# プロジェクトルートをPythonパスに追加
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from bloomberg_api import BloombergDataFetcher
from database import DatabaseManager
from enhanced_data_processor_v2 import EnhancedDataProcessorV2
from config.bloomberg_config import BLOOMBERG_TICKERS
from config.database_config import TABLE_MAPPINGS, TABLE_UNIQUE_KEYS


class BloombergSQLIngestorWithMapping:
    """自動マッピング機能付きBloombergデータ取り込みクラス"""
    
    def __init__(self):
        self.bloomberg = BloombergDataFetcher()
        self.db_manager = DatabaseManager()
        self.processor = None  # 後で初期化
        
    def run(self, mode: str = 'daily'):
        """
        メイン実行メソッド
        
        Args:
            mode: 実行モード ('initial' または 'daily')
        """
        logger.info(f"=== Bloomberg Data Ingestor with Auto Mapping 開始 (mode: {mode}) ===")
        
        try:
            # 接続
            if not self._connect():
                return False
                
            # マスターデータのロード
            self.db_manager.load_master_data()
            
            # Enhanced Data Processorの初期化（自動マッピング機能付き）
            self.processor = EnhancedDataProcessorV2(self.db_manager)
            
            # 実行モードに応じて処理
            if mode == 'initial':
                success = self._run_initial_load()
            else:
                success = self._run_daily_update()
                
            if success:
                logger.info("=== 処理が正常に完了しました ===")
            else:
                logger.error("=== 処理中にエラーが発生しました ===")
                
            return success
            
        except Exception as e:
            logger.error(f"予期しないエラーが発生しました: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False
            
        finally:
            self._disconnect()
            
    def _connect(self) -> bool:
        """Bloomberg APIとデータベースに接続"""
        # Bloomberg API接続
        if not self.bloomberg.connect():
            logger.warning("Bloomberg API接続に失敗しました - モックモードで実行します")
            
        # データベース接続
        try:
            self.db_manager.connect()
            logger.info("データベース接続成功")
            return True
        except Exception as e:
            logger.error(f"データベース接続エラー: {e}")
            return False
            
    def _disconnect(self):
        """接続を切断"""
        self.bloomberg.disconnect()
        self.db_manager.disconnect()
        
    def _run_initial_load(self) -> bool:
        """初回ロード実行"""
        logger.info("初回ロードモードで実行中...")
        
        # カテゴリごとに異なる期間を設定
        category_periods = {
            'LME_COPPER_PRICES': 10,      # 10年
            'SHFE_COPPER_PRICES': 5,      # 5年
            'CMX_COPPER_PRICES': 5,       # 5年
            'LME_INVENTORY': 10,           # 10年
            'SHFE_INVENTORY': 5,           # 5年
            'CMX_INVENTORY': 5,            # 5年
            'INTEREST_RATES': 10,          # 10年
            'CURRENCY_RATES': 10,          # 10年
            'COMMODITY_INDICES': 10,       # 10年
            'SHANGHAI_PREMIUM': 5,         # 5年
            'MACRO_INDICATORS_CHINA': 10,  # 10年
            'MACRO_INDICATORS_US': 10,     # 10年
            'MACRO_INDICATORS_EU': 10,     # 10年
            'MACRO_INDICATORS_JP': 10,     # 10年
            'MACRO_INDICATORS_LATAM': 10,  # 10年
            'COTR_DATA': 3,               # 3年
            'BANDING_DATA': 3,            # 3年
            'COMPANY_PRICES': 5           # 5年
        }
        
        total_records = 0
        
        for category_name, years in category_periods.items():
            if category_name in BLOOMBERG_TICKERS:
                end_date = datetime.now()
                start_date = end_date - timedelta(days=365 * years)
                
                logger.info(f"\n{category_name}を処理中 ({years}年分)...")
                
                ticker_info = BLOOMBERG_TICKERS[category_name]
                record_count = self.process_category(
                    category_name,
                    ticker_info,
                    start_date.strftime('%Y%m%d'),
                    end_date.strftime('%Y%m%d')
                )
                
                total_records += record_count
                logger.info(f"{category_name}: {record_count}件のレコードを保存")
                
        logger.info(f"\n初回ロード完了: 合計 {total_records}件のレコードを保存")
        return True
        
    def _run_daily_update(self) -> bool:
        """日次更新実行"""
        logger.info("日次更新モードで実行中...")
        
        # 過去3日分のデータを取得（週末対応）
        end_date = datetime.now()
        start_date = end_date - timedelta(days=3)
        
        total_records = 0
        
        for category_name, ticker_info in BLOOMBERG_TICKERS.items():
            logger.info(f"\n{category_name}を処理中...")
            
            record_count = self.process_category(
                category_name,
                ticker_info,
                start_date.strftime('%Y%m%d'),
                end_date.strftime('%Y%m%d')
            )
            
            total_records += record_count
            logger.info(f"{category_name}: {record_count}件のレコードを保存")
            
        logger.info(f"\n日次更新完了: 合計 {total_records}件のレコードを保存")
        return True
        
    def process_category(self, category_name: str, ticker_info: Dict[str, Any],
                        start_date: str, end_date: str) -> int:
        """
        カテゴリごとのデータを処理
        
        Returns:
            保存されたレコード数
        """
        try:
            # ティッカーリスト取得
            tickers = ticker_info.get('securities', ticker_info.get('tickers', []))
            fields = ticker_info['fields']
            table_name = ticker_info['table']
            
            logger.info(f"取得中: {len(tickers)}ティッカー, {start_date}から{end_date}")
            
            # データ取得
            df = self.bloomberg.get_historical_data(tickers, fields, start_date, end_date)
            
            if df.empty:
                logger.warning(f"{category_name}: データが取得できませんでした")
                return 0
                
            logger.info(f"取得完了: {len(df)}件のデータ")
            
            # データ処理（自動マッピング機能が統合されている）
            process_method = getattr(self.processor, TABLE_MAPPINGS[table_name])
            processed_df = process_method(df, ticker_info)
            
            if processed_df.empty:
                logger.warning(f"{category_name}: 処理後のデータが空です")
                return 0
                
            # データベースに保存
            unique_columns = TABLE_UNIQUE_KEYS.get(table_name, [])
            record_count = self.db_manager.upsert_dataframe(processed_df, table_name, unique_columns)
            
            # マッピング統計をログ出力
            if table_name == 'T_CommodityPrice' and 'ActualContractID' in processed_df.columns:
                mapped_count = processed_df['ActualContractID'].notna().sum()
                logger.info(f"自動マッピング結果: {mapped_count}/{len(processed_df)}件がマッピング済み")
                
            return record_count
            
        except Exception as e:
            logger.error(f"{category_name}の処理中にエラーが発生しました: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return 0


def main():
    """メインエントリーポイント"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Bloomberg Data Ingestor with Auto Mapping')
    parser.add_argument('--mode', choices=['initial', 'daily'], default='daily',
                       help='実行モード: initial (初回ロード) または daily (日次更新)')
    
    args = parser.parse_args()
    
    # 実行
    ingestor = BloombergSQLIngestorWithMapping()
    success = ingestor.run(mode=args.mode)
    
    # 終了コード
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()