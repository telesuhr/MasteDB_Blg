"""
Enhanced daily update module with market-aware timing and data validation
"""
import pandas as pd
from datetime import datetime, timedelta, time
import pytz
from typing import Dict, Tuple, Optional
import hashlib
import json

from config.logging_config import logger
from config.bloomberg_config import BLOOMBERG_TICKERS


class MarketTimingManager:
    """市場タイミングを管理するクラス"""
    
    # 主要市場の営業時間（UTC）
    MARKET_HOURS = {
        'LME': {
            'timezone': 'Europe/London',
            'settlement_time': time(17, 0),  # 17:00 London time
            'delay_hours': 1  # Settlement後1時間待機
        },
        'SHFE': {
            'timezone': 'Asia/Shanghai',
            'settlement_time': time(15, 0),  # 15:00 Shanghai time
            'delay_hours': 2  # Settlement後2時間待機
        },
        'CMX': {
            'timezone': 'America/New_York',
            'settlement_time': time(13, 30),  # 13:30 NY time
            'delay_hours': 1  # Settlement後1時間待機
        }
    }
    
    @classmethod
    def get_optimal_update_time(cls, market: str) -> Tuple[datetime, datetime]:
        """
        市場に応じた最適な更新時刻を計算
        
        Returns:
            Tuple[datetime, datetime]: (開始時刻, 終了時刻)
        """
        if market not in cls.MARKET_HOURS:
            # デフォルト：現在時刻から過去3日
            end_date = datetime.now()
            start_date = end_date - timedelta(days=3)
            return start_date, end_date
            
        market_info = cls.MARKET_HOURS[market]
        tz = pytz.timezone(market_info['timezone'])
        now_local = datetime.now(tz)
        
        # Settlement時刻を過ぎているか確認
        settlement_datetime = now_local.replace(
            hour=market_info['settlement_time'].hour,
            minute=market_info['settlement_time'].minute,
            second=0,
            microsecond=0
        )
        
        # 待機時間を考慮
        safe_time = settlement_datetime + timedelta(hours=market_info['delay_hours'])
        
        if now_local < safe_time:
            # まだセトルメント後の安全時間に達していない場合は前日のデータまで
            end_date = (now_local - timedelta(days=1)).replace(hour=23, minute=59)
        else:
            # 安全時間を過ぎていれば当日のデータも取得
            end_date = now_local
            
        # 開始日は5営業日前（週末を考慮）
        start_date = end_date - timedelta(days=7)
        
        return start_date.replace(tzinfo=None), end_date.replace(tzinfo=None)
    
    @classmethod
    def should_update_market(cls, market: str) -> bool:
        """指定された市場のデータを更新すべきか判断"""
        if market not in cls.MARKET_HOURS:
            return True
            
        market_info = cls.MARKET_HOURS[market]
        tz = pytz.timezone(market_info['timezone'])
        now_local = datetime.now(tz)
        
        # 週末は更新しない
        if now_local.weekday() >= 5:  # 土日
            logger.info(f"{market} market update skipped (weekend)")
            return False
            
        return True


class DataValidationManager:
    """データ検証を管理するクラス"""
    
    def __init__(self, db_manager):
        self.db_manager = db_manager
        self.validation_history = {}
        self.logger = logger  # ロガーを追加
        
    def calculate_data_hash(self, data: pd.DataFrame) -> str:
        """データフレームのハッシュ値を計算"""
        if data.empty:
            return ""
            
        # 重要なカラムのみでハッシュを計算
        important_cols = [col for col in data.columns 
                         if col not in ['UpdatedAt', 'CreatedAt']]
        
        data_str = data[important_cols].to_json(orient='records', date_format='iso')
        return hashlib.md5(data_str.encode()).hexdigest()
        
    def get_overlapping_data(self, table_name: str, start_date: datetime, 
                           end_date: datetime, additional_conditions: Dict = None) -> pd.DataFrame:
        """既存データとの重複期間のデータを取得"""
        query = f"""
        SELECT * FROM {table_name}
        WHERE 1=1
        """
        
        params = []
        
        # 日付条件の追加
        date_column = 'TradeDate' if 'Price' in table_name else 'ReportDate'
        query += f" AND {date_column} BETWEEN ? AND ?"
        params.extend([start_date, end_date])
        
        # 追加条件
        if additional_conditions:
            for col, val in additional_conditions.items():
                query += f" AND {col} = ?"
                params.append(val)
                
        try:
            # デバッグ情報を追加
            self.logger.debug(f"Executing query for {table_name}")
            self.logger.debug(f"Query: {query}")
            self.logger.debug(f"Params: {params}")
            
            result = self.db_manager.execute_query(query, params)
            self.logger.debug(f"Query result shape: {result.shape if hasattr(result, 'shape') else 'N/A'}")
            
            return result
        except Exception as e:
            self.logger.error(f"Query execution failed for {table_name}: {e}")
            import traceback
            self.logger.error(f"Full traceback: {traceback.format_exc()}")
            return pd.DataFrame()  # 空のDataFrameを返す
        
    def validate_new_data(self, new_data: pd.DataFrame, existing_data: pd.DataFrame,
                         key_columns: list, value_columns: list) -> Dict:
        """新規データと既存データを比較検証"""
        if new_data.empty or existing_data.empty:
            return {'status': 'no_overlap', 'changes': []}
            
        # キーカラムでマージ
        merged = pd.merge(
            existing_data, 
            new_data, 
            on=key_columns, 
            how='inner',
            suffixes=('_existing', '_new')
        )
        
        if merged.empty:
            return {'status': 'no_overlap', 'changes': []}
            
        changes = []
        for idx, row in merged.iterrows():
            for col in value_columns:
                existing_val = row.get(f'{col}_existing')
                new_val = row.get(f'{col}_new')
                
                # 値の変更をチェック（NaN処理を含む）
                if pd.notna(existing_val) and pd.notna(new_val):
                    if abs(float(existing_val) - float(new_val)) > 0.0001:
                        changes.append({
                            'keys': {k: row[k] for k in key_columns},
                            'column': col,
                            'old_value': existing_val,
                            'new_value': new_val,
                            'change_pct': ((new_val - existing_val) / existing_val * 100) if existing_val != 0 else None
                        })
                        
        return {
            'status': 'validated',
            'total_overlapped': len(merged),
            'changes': changes,
            'change_rate': len(changes) / len(merged) * 100 if len(merged) > 0 else 0
        }
        
    def log_validation_results(self, category: str, validation_result: Dict):
        """検証結果をログに記録"""
        timestamp = datetime.now()
        
        if validation_result['status'] == 'no_overlap':
            logger.info(f"[{category}] No overlapping data to validate")
            return
            
        logger.info(f"[{category}] Validation completed: {validation_result['total_overlapped']} records checked")
        
        if validation_result['changes']:
            logger.warning(f"[{category}] Found {len(validation_result['changes'])} changes ({validation_result['change_rate']:.2f}%)")
            
            # 最初の5件の変更を詳細ログ
            for change in validation_result['changes'][:5]:
                logger.warning(
                    f"  Change detected - {change['keys']} | {change['column']}: "
                    f"{change['old_value']} -> {change['new_value']} "
                    f"({change['change_pct']:.2f}%)" if change['change_pct'] else ""
                )
                
            if len(validation_result['changes']) > 5:
                logger.warning(f"  ... and {len(validation_result['changes']) - 5} more changes")
                
        # 履歴を保存
        self.validation_history[category] = {
            'timestamp': timestamp,
            'result': validation_result
        }


class EnhancedDailyUpdater:
    """拡張版日次更新クラス"""
    
    def __init__(self, bloomberg_sql_ingestor):
        self.ingestor = bloomberg_sql_ingestor
        self.timing_manager = MarketTimingManager()
        self.validation_manager = DataValidationManager(bloomberg_sql_ingestor.db_manager)
        
    def run_enhanced_daily_update(self):
        """拡張版日次更新の実行"""
        logger.info("Starting enhanced daily update with market timing and validation...")
        
        # 自動ロールオーバー処理を実行
        logger.info("=== Executing automatic rollover check ===")
        try:
            from auto_rollover_manager import AutoRolloverManager
            rollover_manager = AutoRolloverManager()
            rollover_success = rollover_manager.execute_auto_rollover()
            if rollover_success:
                logger.info("Automatic rollover completed successfully")
            else:
                logger.warning("Automatic rollover encountered issues")
        except Exception as e:
            logger.error(f"Automatic rollover failed: {e}")
            # ロールオーバーエラーは日次更新を停止しない
        
        # カテゴリーを市場別にグループ化
        market_categories = {
            'LME': ['LME_COPPER_PRICES', 'LME_INVENTORY'],
            'SHFE': ['SHFE_COPPER_PRICES', 'SHFE_INVENTORY'],
            'CMX': ['CMX_COPPER_PRICES', 'CMX_INVENTORY'],
            'GLOBAL': ['INTEREST_RATES', 'FX_RATES', 'COMMODITY_INDICES', 
                      'EQUITY_INDICES', 'ENERGY_PRICES', 'PHYSICAL_PREMIUMS',
                      'OTHER_INDICATORS', 'COMPANY_STOCKS']
        }
        
        update_summary = {}
        
        for market, categories in market_categories.items():
            # 市場タイミングチェック
            if not self.timing_manager.should_update_market(market):
                continue
                
            # 最適な更新時間範囲を取得
            start_date, end_date = self.timing_manager.get_optimal_update_time(market)
            logger.info(f"Processing {market} market data from {start_date} to {end_date}")
            
            for category_name in categories:
                if category_name not in BLOOMBERG_TICKERS:
                    continue
                    
                try:
                    ticker_info = BLOOMBERG_TICKERS[category_name].copy()  # Deep copyを作成
                    
                    # MEST地域を除外（LME在庫の場合）
                    if category_name == 'LME_INVENTORY':
                        # MESTを含むティッカーを除外
                        for data_type, tickers in ticker_info['securities'].items():
                            ticker_info['securities'][data_type] = [
                                t for t in tickers if '%MEST' not in t
                            ]
                        # region_mappingからもMESTを削除
                        if '%MEST Index' in ticker_info.get('region_mapping', {}):
                            del ticker_info['region_mapping']['%MEST Index']
                    
                    # 1. 新規データの取得
                    logger.info(f"Fetching {category_name} data...")
                    new_data_df = self._fetch_category_data(
                        category_name, ticker_info, 
                        start_date.strftime('%Y%m%d'), 
                        end_date.strftime('%Y%m%d')
                    )
                    
                    if new_data_df.empty:
                        logger.warning(f"No new data fetched for {category_name}")
                        continue
                        
                    # 2. 既存データとの重複期間を検証（一時的に無効化）
                    validation_result = {'status': 'skipped', 'changes': []}
                    logger.info(f"[{category_name}] Data validation temporarily disabled")
                    
                    # TODO: データベースクエリの形状問題解決後に再有効化
                    # validation_start = start_date + timedelta(days=2)  # 重複検証は2日分
                    # table_name = self._get_table_name(category_name)
                    # 
                    # if table_name:
                    #     existing_data = self.validation_manager.get_overlapping_data(
                    #         table_name, validation_start, end_date
                    #     )
                    #     
                    #     # データ検証
                    #     key_columns = self._get_key_columns(category_name)
                    #     value_columns = self._get_value_columns(category_name)
                    #     
                    #     validation_result = self.validation_manager.validate_new_data(
                    #         new_data_df, existing_data, key_columns, value_columns
                    #     )
                    #     
                    #     self.validation_manager.log_validation_results(
                    #         category_name, validation_result
                    #     )
                    #     
                    #     # 変更率が高い場合は警告
                    #     if validation_result.get('change_rate', 0) > 10:
                    #         logger.error(f"High change rate detected for {category_name}: {validation_result['change_rate']:.2f}%")
                    #         # 必要に応じて更新を中断するロジックを追加可能
                            
                    # 3. データの保存（UPSERT）
                    record_count = self.ingestor.process_category(
                        category_name, ticker_info,
                        start_date.strftime('%Y%m%d'),
                        end_date.strftime('%Y%m%d')
                    )
                    
                    update_summary[category_name] = {
                        'records': record_count,
                        'validation': validation_result if 'validation_result' in locals() else None
                    }
                    
                except Exception as e:
                    logger.error(f"Failed to update {category_name}: {e}")
                    import traceback
                    logger.error(traceback.format_exc())
                    
        # 更新サマリーをログ出力
        self._log_update_summary(update_summary)
        
        # バンディングレポート（金曜日のみ）
        if datetime.now().weekday() == 4:
            self._update_weekly_data()
            
        return update_summary
        
    def _fetch_category_data(self, category_name: str, ticker_info: Dict,
                           start_date: str, end_date: str) -> pd.DataFrame:
        """カテゴリーのデータを取得（処理せずに生データを返す）"""
        # この部分は実際のBloomberg APIからのデータ取得ロジックになります
        # ここでは簡略化しています
        securities = []
        
        # securitiesが辞書形式の場合
        if isinstance(ticker_info.get('securities'), dict):
            for data_type, tickers in ticker_info.get('securities', {}).items():
                if isinstance(tickers, list):
                    securities.extend(tickers)
                elif isinstance(tickers, dict):
                    for position_type, ticker_list in tickers.items():
                        securities.extend(ticker_list)
        # securitiesがリスト形式の場合
        elif isinstance(ticker_info.get('securities'), list):
            securities = ticker_info.get('securities', [])
        # securitiesがその他の形式の場合
        else:
            securities = ticker_info.get('securities', [])
                    
        if not securities:
            logger.warning(f"No securities found for {category_name}")
            return pd.DataFrame()
            
        logger.debug(f"Fetching data for {len(securities)} securities: {securities[:5]}...")
            
        # データ取得
        df = self.ingestor.bloomberg.get_historical_data(
            securities, ticker_info['fields'], start_date, end_date
        )
        
        return df
        
    def _get_table_name(self, category_name: str) -> Optional[str]:
        """カテゴリー名からテーブル名を取得"""
        table_mapping = {
            'LME_COPPER_PRICES': 'T_CommodityPrice',
            'SHFE_COPPER_PRICES': 'T_CommodityPrice',
            'CMX_COPPER_PRICES': 'T_CommodityPrice',
            'LME_INVENTORY': 'T_LMEInventory',
            'SHFE_INVENTORY': 'T_OtherExchangeInventory',
            'CMX_INVENTORY': 'T_OtherExchangeInventory',
            'INTEREST_RATES': 'T_MarketIndicator',
            'FX_RATES': 'T_MarketIndicator',
            'COMMODITY_INDICES': 'T_MarketIndicator',
            'EQUITY_INDICES': 'T_MarketIndicator',
            'COMPANY_STOCKS': 'T_CompanyStockPrice'
        }
        
        return table_mapping.get(category_name)
        
    def _get_key_columns(self, category_name: str) -> list:
        """カテゴリーのキーカラムを取得"""
        if 'PRICE' in category_name:
            return ['TradeDate', 'MetalID', 'TenorTypeID']
        elif 'LME_INVENTORY' in category_name:
            return ['ReportDate', 'MetalID', 'RegionID']
        elif 'INVENTORY' in category_name:
            return ['ReportDate', 'MetalID', 'ExchangeCode']
        elif 'INDICATOR' in category_name or 'RATES' in category_name:
            return ['ReportDate', 'IndicatorID']
        elif 'STOCKS' in category_name:
            return ['TradeDate', 'CompanyTicker']
        else:
            return []
            
    def _get_value_columns(self, category_name: str) -> list:
        """カテゴリーの値カラムを取得"""
        if 'PRICE' in category_name:
            return ['SettlementPrice', 'OpenPrice', 'HighPrice', 'LowPrice', 'Volume']
        elif 'INVENTORY' in category_name:
            return ['TotalStock', 'OnWarrant', 'CancelledWarrant', 'Inflow', 'Outflow']
        elif 'INDICATOR' in category_name or 'RATES' in category_name:
            return ['Value']
        elif 'STOCKS' in category_name:
            return ['OpenPrice', 'HighPrice', 'LowPrice', 'LastPrice', 'Volume']
        else:
            return []
            
    def _update_weekly_data(self):
        """週次データの更新（COTRなど）"""
        logger.info("Processing weekly data updates...")
        
        weekly_categories = ['COTR_DATA', 'FUTURES_BANDING', 'WARRANT_BANDING']
        
        for category_name in weekly_categories:
            if category_name in BLOOMBERG_TICKERS:
                try:
                    ticker_info = BLOOMBERG_TICKERS[category_name]
                    # 過去1週間分のデータを取得
                    end_date = datetime.now().strftime('%Y%m%d')
                    start_date = (datetime.now() - timedelta(days=7)).strftime('%Y%m%d')
                    
                    self.ingestor.process_category(
                        category_name, ticker_info, start_date, end_date
                    )
                    
                except Exception as e:
                    logger.error(f"Failed to update weekly data {category_name}: {e}")
                    
    def _log_update_summary(self, summary: Dict):
        """更新サマリーをログ出力"""
        logger.info("=" * 60)
        logger.info("Enhanced Daily Update Summary")
        logger.info("=" * 60)
        
        total_records = 0
        total_changes = 0
        
        for category, result in summary.items():
            records = result.get('records', 0)
            total_records += records
            
            validation = result.get('validation')
            if validation and validation.get('changes'):
                changes = len(validation['changes'])
                total_changes += changes
                logger.info(f"{category}: {records} records, {changes} changes detected")
            else:
                logger.info(f"{category}: {records} records")
                
        logger.info(f"Total: {total_records} records processed, {total_changes} changes detected")
        logger.info("=" * 60)