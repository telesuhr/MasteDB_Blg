"""
ヒストリカルなGeneric-Actual契約マッピングを取得・更新するスクリプト
FUT_CUR_GEN_TICKERをヒストリカルデータとして取得し、
過去の正しいマッピングを復元する
"""

import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import logging
from bloomberg_api import BloombergDataFetcher
from database import DatabaseManager
from config.bloomberg_config import BLOOMBERG_TICKERS

# ロガー設定
logger = logging.getLogger(__name__)

class HistoricalMappingUpdater:
    """ヒストリカルなGeneric-Actual契約マッピングを管理"""
    
    def __init__(self, bloomberg_fetcher: BloombergDataFetcher, db_manager: DatabaseManager):
        self.bloomberg = bloomberg_fetcher
        self.db_manager = db_manager
        
    def update_historical_mappings(self, start_date: str, end_date: str, 
                                 generic_tickers: Optional[List[str]] = None):
        """
        指定期間のヒストリカルマッピングを更新
        
        Args:
            start_date: 開始日 (YYYY-MM-DD)
            end_date: 終了日 (YYYY-MM-DD)
            generic_tickers: 更新対象のジェネリックティッカーリスト（Noneの場合は全て）
        """
        logger.info(f"ヒストリカルマッピング更新開始: {start_date} から {end_date}")
        
        # 対象のジェネリック先物を取得
        if generic_tickers is None:
            # デフォルトではLME先物のみを対象とする
            generic_futures = self._get_all_generic_futures()
            generic_futures = generic_futures[generic_futures['ExchangeCode'] == 'LME']
        else:
            generic_futures = self._get_generic_futures_by_tickers(generic_tickers)
            
        if generic_futures.empty:
            logger.warning("更新対象のジェネリック先物が見つかりません")
            return
            
        # ティッカーリスト
        tickers = generic_futures['GenericTicker'].tolist()
        logger.info(f"更新対象: {len(tickers)}件のジェネリック先物")
        
        # FUT_CUR_GEN_TICKERをヒストリカルで取得
        fields = [
            'FUT_CUR_GEN_TICKER',     # その日のジェネリック契約
            'LAST_TRADEABLE_DT',       # 最終取引日
            'FUT_DLV_DT_LAST',         # 最終受渡日
            'FUT_CONTRACT_DT',         # 契約月
            'FUT_CONT_SIZE',           # 契約サイズ
            'FUT_TICK_SIZE'            # ティックサイズ
        ]
        
        # 日付フォーマットをYYYYMMDDに変換
        start_date_bloomberg = start_date.replace('-', '')
        end_date_bloomberg = end_date.replace('-', '')
        
        logger.info("Bloombergからヒストリカルデータを取得中...")
        hist_data = self.bloomberg.get_historical_data(
            tickers,
            fields,
            start_date_bloomberg,
            end_date_bloomberg
        )
        
        if hist_data.empty:
            logger.error("ヒストリカルデータの取得に失敗しました")
            return
            
        # データフレームの構造を確認
        logger.info(f"取得したデータ: {len(hist_data)}件")
        logger.debug(f"データフレームのカラム: {hist_data.columns.tolist()}")
        logger.debug(f"データフレームのインデックス: {hist_data.index.names}")
        
        # 最初の数行を確認
        if len(hist_data) > 0:
            logger.debug(f"データサンプル:\n{hist_data.head()}")
            if 'date' in hist_data.columns:
                logger.debug(f"日付のデータ型: {hist_data['date'].dtype}")
                logger.debug(f"日付の例: {hist_data['date'].iloc[0]}")
        
        # 日付カラムを datetime 型に変換
        if 'date' in hist_data.columns:
            hist_data['date'] = pd.to_datetime(hist_data['date'])
        
        # 日付ごとにマッピングを処理
        for trade_date in pd.date_range(start_date, end_date):
            self._process_date_mappings(trade_date.date(), hist_data, generic_futures)
            
        logger.info("ヒストリカルマッピング更新完了")
        
    def _process_date_mappings(self, trade_date, hist_data: pd.DataFrame, 
                              generic_futures: pd.DataFrame):
        """特定日のマッピングを処理"""
        # データフレームの構造に応じてフィルタリング
        if 'date' in hist_data.columns:
            # dateがカラムの場合 - 日付の比較をdate()同士で行う
            date_data = hist_data[hist_data['date'].dt.date == trade_date]
        elif hist_data.index.nlevels > 1 and 'date' in hist_data.index.names:
            # dateがマルチインデックスの場合
            date_data = hist_data[hist_data.index.get_level_values('date') == pd.Timestamp(trade_date)]
        else:
            # dateがシングルインデックスの場合
            date_data = hist_data[hist_data.index == pd.Timestamp(trade_date)]
        
        if date_data.empty:
            logger.debug(f"{trade_date}: データなし（休場日の可能性）")
            return
            
        logger.info(f"{trade_date}: {len(date_data)}件のマッピングを処理")
        
        # tickerでグループ化（security列を使用）
        if 'security' in date_data.columns:
            grouped = date_data.groupby('security')
        elif 'ticker' in date_data.columns:
            grouped = date_data.groupby('ticker')
        elif date_data.index.nlevels > 1 and 'ticker' in date_data.index.names:
            grouped = date_data.groupby(level='ticker')
        else:
            # tickerがない場合は個別に処理
            grouped = [(None, date_data)]
            
        for ticker, ticker_data in grouped:
            if ticker is None and 'security' in ticker_data.columns:
                ticker = ticker_data['security'].iloc[0]
            
            # FUT_CUR_GEN_TICKERを取得
            if isinstance(ticker_data, pd.DataFrame):
                current_contract = ticker_data['FUT_CUR_GEN_TICKER'].iloc[0] if len(ticker_data) > 0 else None
            else:
                current_contract = ticker_data.get('FUT_CUR_GEN_TICKER')
            
            if pd.isna(current_contract) or current_contract is None:
                logger.warning(f"{trade_date} {ticker}: 現在の契約が取得できません")
                continue
                
            # ジェネリック先物情報を取得
            matching_futures = generic_futures[generic_futures['GenericTicker'] == ticker]
            if matching_futures.empty:
                logger.debug(f"{trade_date} {ticker}: ジェネリック先物マスタに存在しません")
                continue
            generic_info = matching_futures.iloc[0]
            
            # 実契約を確認・作成
            actual_contract_id = self._ensure_actual_contract(
                current_contract, 
                generic_info,
                ticker_data.iloc[0] if isinstance(ticker_data, pd.DataFrame) else ticker_data
            )
            
            if actual_contract_id:
                # マッピングを更新
                self._update_mapping(
                    trade_date,
                    generic_info['GenericID'],
                    actual_contract_id,
                    ticker_data
                )
                
    def _ensure_actual_contract(self, contract_ticker: str, generic_info: pd.Series,
                               bloomberg_data: pd.Series) -> Optional[int]:
        """実契約を確認し、存在しない場合は作成"""
        with self.db_manager.get_connection() as conn:
            cursor = conn.cursor()
            
            # 既存チェック
            cursor.execute(
                "SELECT ActualContractID FROM M_ActualContract WHERE ContractTicker = ?",
                (contract_ticker,)
            )
            existing = cursor.fetchone()
            
            if existing:
                return existing[0]
                
            # 新規契約の場合、Bloomberg APIから正確な情報を取得
            logger.info(f"新しい契約を発見: {contract_ticker}。Bloomberg APIから詳細情報を取得します。")
            
            # 実契約の詳細情報を取得するためのフィールド
            actual_fields = [
                'LAST_TRADEABLE_DT',     # 最終取引日
                'FUT_DLV_DT_LAST',       # 最終受渡日
                'FUT_CONTRACT_DT',       # 契約月
                'FUT_CONT_SIZE',         # 契約サイズ
                'FUT_TICK_SIZE',         # ティックサイズ
                'NAME',                  # 契約名
                'EXCH_CODE'             # 取引所コード
            ]
            
            # 実契約のリファレンスデータを取得
            ref_data = self.bloomberg.get_reference_data([contract_ticker], actual_fields)
            
            if ref_data.empty:
                logger.error(f"Bloomberg APIから{contract_ticker}の情報を取得できませんでした")
                return None
                
            actual_data = ref_data.iloc[0]
            
            # 契約月の解析
            contract_month = None
            contract_year = None
            contract_month_code = None
            
            contract_date = actual_data.get('FUT_CONTRACT_DT')
            if pd.notna(contract_date):
                try:
                    # 日付の処理
                    if hasattr(contract_date, 'date'):
                        contract_dt = contract_date
                    else:
                        contract_dt = pd.to_datetime(contract_date)
                        
                    contract_month = contract_dt.month
                    contract_year = contract_dt.year
                    
                    # 月コード生成
                    month_codes = ['F', 'G', 'H', 'J', 'K', 'M', 'N', 'Q', 'U', 'V', 'X', 'Z']
                    contract_month_code = month_codes[contract_month - 1]
                except Exception as e:
                    logger.warning(f"契約月解析エラー: {e}")
                    
            # 最終取引日の処理
            last_tradeable = actual_data.get('LAST_TRADEABLE_DT')
            if pd.notna(last_tradeable):
                if hasattr(last_tradeable, 'date'):
                    last_tradeable = last_tradeable.date()
                else:
                    last_tradeable = pd.to_datetime(last_tradeable).date()
                    
            # 最終受渡日の処理
            delivery_date = actual_data.get('FUT_DLV_DT_LAST')
            if pd.notna(delivery_date):
                if hasattr(delivery_date, 'date'):
                    delivery_date = delivery_date.date()
                else:
                    delivery_date = pd.to_datetime(delivery_date).date()
                    
            # 挿入
            cursor.execute("""
                INSERT INTO M_ActualContract (
                    ContractTicker, MetalID, ExchangeCode, ContractMonth,
                    ContractYear, ContractMonthCode, LastTradeableDate,
                    DeliveryDate, ContractSize, TickSize
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                contract_ticker,
                int(generic_info['MetalID']),
                generic_info['ExchangeCode'],
                contract_month,
                contract_year,
                contract_month_code,
                last_tradeable,
                delivery_date,
                float(actual_data.get('FUT_CONT_SIZE')) if pd.notna(actual_data.get('FUT_CONT_SIZE')) else None,
                float(actual_data.get('FUT_TICK_SIZE')) if pd.notna(actual_data.get('FUT_TICK_SIZE')) else None
            ))
            
            cursor.execute("SELECT @@IDENTITY")
            actual_contract_id = cursor.fetchone()[0]
            conn.commit()
            
            logger.info(f"新規実契約作成完了: {contract_ticker} (ID: {actual_contract_id})")
            logger.info(f"  LastTradeableDate: {last_tradeable}")
            logger.info(f"  DeliveryDate: {delivery_date}")
            logger.info(f"  ContractMonth: {contract_month}/{contract_year}")
            
            return actual_contract_id
            
    def _update_mapping(self, trade_date, generic_id: int,
                       actual_contract_id: int, bloomberg_data: pd.Series):
        """マッピングを更新（MERGE操作）"""
        with self.db_manager.get_connection() as conn:
            cursor = conn.cursor()
            
            # 残存日数計算
            days_to_expiry = None
            last_tradeable = bloomberg_data.get('LAST_TRADEABLE_DT')
            if pd.notna(last_tradeable):
                try:
                    if hasattr(last_tradeable, 'date'):
                        last_tradeable_dt = last_tradeable.date()
                    else:
                        last_tradeable_dt = pd.to_datetime(last_tradeable).date()
                    days_to_expiry = (last_tradeable_dt - trade_date).days
                except Exception as e:
                    logger.warning(f"残存日数計算エラー: {e}")
                    
            # MERGE操作
            cursor.execute("""
                MERGE T_GenericContractMapping AS target
                USING (SELECT ? as TradeDate, ? as GenericID, ? as ActualContractID, ? as DaysToExpiry) AS source
                ON target.TradeDate = source.TradeDate AND target.GenericID = source.GenericID
                WHEN MATCHED THEN
                    UPDATE SET 
                        ActualContractID = source.ActualContractID,
                        DaysToExpiry = source.DaysToExpiry,
                        CreatedAt = GETDATE()
                WHEN NOT MATCHED THEN
                    INSERT (TradeDate, GenericID, ActualContractID, DaysToExpiry)
                    VALUES (source.TradeDate, source.GenericID, source.ActualContractID, source.DaysToExpiry);
            """, (trade_date, int(generic_id), int(actual_contract_id), days_to_expiry))
            
            conn.commit()
            logger.debug(f"マッピング更新: {trade_date} GenericID {generic_id} -> ContractID {actual_contract_id}")
            
    def _get_all_generic_futures(self) -> pd.DataFrame:
        """全てのアクティブなジェネリック先物を取得"""
        with self.db_manager.get_connection() as conn:
            query = """
                SELECT GenericID, GenericTicker, MetalID, ExchangeCode
                FROM M_GenericFutures
                WHERE IsActive = 1
                ORDER BY ExchangeCode, GenericNumber
            """
            return pd.read_sql(query, conn)
            
    def _get_generic_futures_by_tickers(self, tickers: List[str]) -> pd.DataFrame:
        """指定されたティッカーのジェネリック先物を取得"""
        with self.db_manager.get_connection() as conn:
            placeholders = ','.join(['?' for _ in tickers])
            query = f"""
                SELECT GenericID, GenericTicker, MetalID, ExchangeCode
                FROM M_GenericFutures
                WHERE GenericTicker IN ({placeholders})
                    AND IsActive = 1
                ORDER BY ExchangeCode, GenericNumber
            """
            return pd.read_sql(query, conn, params=tickers)


def main():
    """スタンドアロン実行用"""
    import sys
    from datetime import datetime
    
    # ロギング設定
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # 引数チェック
    if len(sys.argv) < 3:
        print("使用方法: python historical_mapping_updater.py [開始日] [終了日] [ティッカー（オプション）]")
        print("例: python historical_mapping_updater.py 2025-06-01 2025-06-30")
        print("例: python historical_mapping_updater.py 2025-06-01 2025-06-30 'LP1 Comdty'")
        sys.exit(1)
        
    start_date = sys.argv[1]
    end_date = sys.argv[2]
    generic_tickers = None
    
    if len(sys.argv) > 3:
        generic_tickers = [ticker.strip() for ticker in sys.argv[3:]]
        
    # インスタンス作成
    bloomberg_fetcher = BloombergDataFetcher()
    db_manager = DatabaseManager()
    
    # Bloomberg APIに接続
    if not bloomberg_fetcher.connect():
        logger.error("Bloomberg API接続に失敗しました")
        sys.exit(1)
    
    try:
        updater = HistoricalMappingUpdater(bloomberg_fetcher, db_manager)
        
        # 更新実行
        updater.update_historical_mappings(start_date, end_date, generic_tickers)
    finally:
        # 切断
        bloomberg_fetcher.disconnect()
    
    logger.info("処理完了")


if __name__ == "__main__":
    main()