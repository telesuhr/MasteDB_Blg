"""
Bloomberg データ取得・SQL Server格納システム
メインエントリーポイント
"""
import argparse
import sys
import os
from datetime import datetime, timedelta
from typing import Dict, List

# プロジェクトルートをPythonパスに追加
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from bloomberg_api import BloombergDataFetcher
from database import DatabaseManager
from data_processor import DataProcessor
from utils import measure_execution_time, create_summary_report

from config.bloomberg_config import BLOOMBERG_TICKERS, get_date_range
from config.logging_config import logger


class BloombergSQLIngestor:
    """Bloomberg データ取得・SQL Server格納を管理するメインクラス"""
    
    def __init__(self):
        self.bloomberg = BloombergDataFetcher()
        self.db_manager = DatabaseManager()
        self.processor = None
        self.data_counts = {}
        
    def initialize(self):
        """システムの初期化"""
        logger.info("Initializing Bloomberg SQL Ingestor...")
        
        # Bloomberg API接続
        if not self.bloomberg.connect():
            raise ConnectionError("Failed to connect to Bloomberg API")
            
        # データベース接続
        self.db_manager.connect()
        
        # マスタデータのロード
        self.db_manager.load_master_data()
        
        # データプロセッサーの初期化
        self.processor = DataProcessor(self.db_manager)
        
        logger.info("Initialization completed successfully")
        
    def cleanup(self):
        """リソースのクリーンアップ"""
        logger.info("Cleaning up resources...")
        self.bloomberg.disconnect()
        self.db_manager.disconnect()
        
    @measure_execution_time
    def process_category(self, category_name: str, ticker_info: Dict, 
                        start_date: str, end_date: str) -> int:
        """
        カテゴリ別にデータを処理
        
        Args:
            category_name: カテゴリ名
            ticker_info: ティッカー設定情報
            start_date: 開始日
            end_date: 終了日
            
        Returns:
            int: 処理されたレコード数
        """
        logger.info(f"Processing {category_name}...")
        
        try:
            # 証券リストとフィールドの取得
            if isinstance(ticker_info['securities'], dict):
                # 複雑な構造（在庫データなど）
                all_securities = []
                for sec_list in ticker_info['securities'].values():
                    if isinstance(sec_list, list):
                        all_securities.extend(sec_list)
                    elif isinstance(sec_list, dict):
                        for subsec_list in sec_list.values():
                            all_securities.extend(subsec_list)
            else:
                all_securities = ticker_info['securities']
                
            fields = ticker_info['fields']
            
            # データ取得（リファレンスまたはヒストリカル）
            if ticker_info.get('frequency') == 'Weekly':
                # 週次データは最新のみ取得
                df = self.bloomberg.get_reference_data(all_securities, fields)
            else:
                # 日次データはヒストリカル取得
                df = self.bloomberg.batch_request(
                    all_securities, fields, start_date, end_date,
                    request_type='historical'
                )
                
            if df.empty:
                logger.warning(f"No data retrieved for {category_name}")
                return 0
                
            # データ処理
            table_name = ticker_info['table']
            processed_df = pd.DataFrame()
            
            if table_name == 'T_CommodityPrice':
                processed_df = self.processor.process_commodity_prices(df, ticker_info)
            elif table_name == 'T_LMEInventory':
                processed_df = self.processor.process_lme_inventory(df, ticker_info)
            elif table_name == 'T_OtherExchangeInventory':
                processed_df = self._process_other_inventory(df, ticker_info)
            elif table_name == 'T_MarketIndicator':
                processed_df = self.processor.process_market_indicators(df, ticker_info)
            elif table_name == 'T_MacroEconomicIndicator':
                processed_df = self._process_macro_indicators(df, ticker_info)
            elif table_name == 'T_COTR':
                processed_df = self.processor.process_cotr_data(df, ticker_info)
            elif table_name == 'T_BandingReport':
                processed_df = self.processor.process_banding_report(df, ticker_info)
            elif table_name == 'T_CompanyStockPrice':
                processed_df = self.processor.process_company_stocks(df, ticker_info)
                
            # データベースに格納
            if not processed_df.empty:
                unique_columns = self._get_unique_columns(table_name)
                record_count = self.db_manager.upsert_dataframe(
                    processed_df, table_name, unique_columns
                )
                logger.info(f"Stored {record_count} records for {category_name}")
                return record_count
            else:
                logger.warning(f"No processed data for {category_name}")
                return 0
                
        except Exception as e:
            logger.error(f"Error processing {category_name}: {e}")
            return 0
            
    def _process_other_inventory(self, df: pd.DataFrame, ticker_info: Dict) -> pd.DataFrame:
        """他取引所在庫データを処理"""
        if df.empty:
            return pd.DataFrame()
            
        processed_data = []
        
        for _, row in df.iterrows():
            security = row['security']
            report_date = pd.to_datetime(row['date']).date()
            value = row.get('PX_LAST', 0)
            
            # データタイプの識別
            type_mapping = ticker_info.get('type_mapping', {})
            data_type = type_mapping.get(security, 'total_stock')
            
            processed_row = {
                'ReportDate': report_date,
                'MetalID': self.db_manager.get_or_create_master_id('metals', ticker_info['metal']),
                'ExchangeCode': ticker_info['exchange'],
                'TotalStock': value if data_type == 'total_stock' else None,
                'OnWarrant': value if data_type == 'on_warrant' else None
            }
            
            processed_data.append(processed_row)
            
        return pd.DataFrame(processed_data)
        
    def _process_macro_indicators(self, df: pd.DataFrame, ticker_info: Dict) -> pd.DataFrame:
        """マクロ経済指標データを処理"""
        if df.empty:
            return pd.DataFrame()
            
        processed_data = []
        
        for _, row in df.iterrows():
            security = row['security']
            report_date = pd.to_datetime(row['date']).date()
            value = row.get('PX_LAST')
            
            # 国コードの識別
            country_code = None
            for country, securities in ticker_info['securities'].items():
                if security in securities:
                    country_code = country
                    break
                    
            # インジケーターコードの生成
            indicator_code = security.split()[0]
            
            # インジケーターIDの取得
            indicator_id = self.db_manager.get_or_create_master_id(
                'indicators',
                indicator_code,
                name=security,
                additional_fields={
                    'Category': ticker_info.get('category', 'Macro Economic'),
                    'Unit': '%' if 'PMI' in security else 'YoY %',
                    'Freq': self._determine_frequency(security)
                }
            )
            
            processed_row = {
                'ReportDate': report_date,
                'IndicatorID': indicator_id,
                'CountryCode': country_code,
                'Value': value
            }
            
            processed_data.append(processed_row)
            
        return pd.DataFrame(processed_data)
        
    def _determine_frequency(self, security: str) -> str:
        """証券名から更新頻度を推定"""
        if 'PMI' in security:
            return 'Monthly'
        elif 'GDP' in security:
            return 'Yearly'
        else:
            return 'Monthly'
            
    def _get_unique_columns(self, table_name: str) -> List[str]:
        """テーブルのユニークキーカラムを取得"""
        unique_columns_mapping = {
            'T_CommodityPrice': ['TradeDate', 'MetalID', 'TenorTypeID', 'SpecificTenorDate'],
            'T_LMEInventory': ['ReportDate', 'MetalID', 'RegionID'],
            'T_OtherExchangeInventory': ['ReportDate', 'MetalID', 'ExchangeCode'],
            'T_MarketIndicator': ['ReportDate', 'IndicatorID', 'MetalID'],
            'T_MacroEconomicIndicator': ['ReportDate', 'IndicatorID', 'CountryCode'],
            'T_COTR': ['ReportDate', 'MetalID', 'COTRCategoryID'],
            'T_BandingReport': ['ReportDate', 'MetalID', 'ReportType', 'TenorTypeID', 'BandID'],
            'T_CompanyStockPrice': ['TradeDate', 'CompanyTicker']
        }
        
        return unique_columns_mapping.get(table_name, [])
        
    @measure_execution_time
    def run_initial_load(self):
        """初回データロードを実行"""
        logger.info("Starting initial historical data load...")
        
        for category_name, ticker_info in BLOOMBERG_TICKERS.items():
            # カテゴリに応じた期間を取得
            category_type = self._get_category_type(category_name)
            start_date, end_date = get_date_range('initial', category_type)
            
            record_count = self.process_category(
                category_name, ticker_info, start_date, end_date
            )
            
            self.data_counts[category_name] = record_count
            
        logger.info("Initial load completed")
        
    @measure_execution_time
    def run_daily_update(self):
        """日次更新を実行"""
        logger.info("Starting daily data update...")
        
        # 日次更新対象のカテゴリ
        daily_categories = [
            'LME_COPPER_PRICES', 'SHFE_COPPER_PRICES', 'CMX_COPPER_PRICES',
            'LME_INVENTORY', 'SHFE_INVENTORY', 'CMX_INVENTORY',
            'INTEREST_RATES', 'FX_RATES', 'COMMODITY_INDICES', 'EQUITY_INDICES',
            'FUTURES_BANDING', 'WARRANT_BANDING',
            'ENERGY_PRICES', 'PHYSICAL_PREMIUMS', 'OTHER_INDICATORS',
            'COMPANY_STOCKS'
        ]
        
        for category_name in daily_categories:
            if category_name in BLOOMBERG_TICKERS:
                ticker_info = BLOOMBERG_TICKERS[category_name]
                
                # 過去3日分のデータを取得（週末対応）
                end_date = datetime.now().strftime('%Y%m%d')
                start_date = (datetime.now() - timedelta(days=3)).strftime('%Y%m%d')
                
                record_count = self.process_category(
                    category_name, ticker_info, start_date, end_date
                )
                
                self.data_counts[category_name] = record_count
                
        # 週次データ（COTR）の処理
        if datetime.now().weekday() == 4:  # 金曜日
            logger.info("Processing weekly COTR data...")
            if 'COTR_DATA' in BLOOMBERG_TICKERS:
                ticker_info = BLOOMBERG_TICKERS['COTR_DATA']
                # 最新のCOTRデータのみ取得
                df = self.bloomberg.get_reference_data(
                    ticker_info['securities']['investment_funds']['long'] +
                    ticker_info['securities']['investment_funds']['short'] +
                    ticker_info['securities']['commercial']['long'] +
                    ticker_info['securities']['commercial']['short'],
                    ticker_info['fields']
                )
                if not df.empty:
                    processed_df = self.processor.process_cotr_data(df, ticker_info)
                    if not processed_df.empty:
                        record_count = self.db_manager.upsert_dataframe(
                            processed_df, ticker_info['table'], 
                            self._get_unique_columns(ticker_info['table'])
                        )
                        self.data_counts['COTR_DATA'] = record_count
                        
        # 月次データ（マクロ指標）の処理
        if datetime.now().day <= 7:  # 月初の1週間
            logger.info("Checking for monthly macro indicator updates...")
            if 'MACRO_INDICATORS' in BLOOMBERG_TICKERS:
                ticker_info = BLOOMBERG_TICKERS['MACRO_INDICATORS']
                # 過去1ヶ月分のデータを取得
                end_date = datetime.now().strftime('%Y%m%d')
                start_date = (datetime.now() - timedelta(days=30)).strftime('%Y%m%d')
                
                record_count = self.process_category(
                    'MACRO_INDICATORS', ticker_info, start_date, end_date
                )
                
                self.data_counts['MACRO_INDICATORS'] = record_count
                
        logger.info("Daily update completed")
        
    def _get_category_type(self, category_name: str) -> str:
        """カテゴリ名からタイプを判定"""
        if 'PRICE' in category_name:
            return 'prices'
        elif 'INVENTORY' in category_name:
            return 'inventory'
        elif 'MACRO' in category_name:
            return 'macro'
        elif 'COTR' in category_name:
            return 'cotr'
        elif 'BANDING' in category_name:
            return 'banding'
        elif 'STOCK' in category_name:
            return 'stocks'
        else:
            return 'indicators'
            
    def run(self, mode: str = 'daily'):
        """
        メイン実行メソッド
        
        Args:
            mode: 'initial' または 'daily'
        """
        try:
            self.initialize()
            
            if mode == 'initial':
                self.run_initial_load()
            elif mode == 'daily':
                self.run_daily_update()
            else:
                raise ValueError(f"Invalid mode: {mode}. Use 'initial' or 'daily'")
                
            # サマリーレポートの出力
            summary = create_summary_report(self.data_counts)
            logger.info(summary)
            
        except Exception as e:
            logger.error(f"Fatal error during execution: {e}", exc_info=True)
            raise
            
        finally:
            self.cleanup()
            

def main():
    """メイン関数"""
    parser = argparse.ArgumentParser(
        description='Bloomberg Data Ingestion to SQL Server'
    )
    parser.add_argument(
        '--mode',
        choices=['initial', 'daily'],
        default='daily',
        help='Execution mode: initial (historical load) or daily (update)'
    )
    
    args = parser.parse_args()
    
    try:
        ingestor = BloombergSQLIngestor()
        ingestor.run(mode=args.mode)
        logger.info("Process completed successfully")
        sys.exit(0)
        
    except Exception as e:
        logger.error(f"Process failed: {e}")
        sys.exit(1)
        

if __name__ == "__main__":
    import pandas as pd  # ここでインポート（グローバルスコープで必要）
    main()