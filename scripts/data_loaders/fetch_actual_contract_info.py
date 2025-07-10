"""
実際の契約（Actual Contract）の詳細情報をBloomberg APIから取得して
M_ActualContractテーブルを更新するスクリプト
"""
import pandas as pd
from datetime import datetime
import logging
from bloomberg_api import BloombergDataFetcher
from database import DatabaseManager
from typing import List, Optional

# ロギング設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ActualContractInfoUpdater:
    """実契約情報の更新クラス"""
    
    def __init__(self, bloomberg_fetcher: BloombergDataFetcher, db_manager: DatabaseManager):
        self.bloomberg = bloomberg_fetcher
        self.db_manager = db_manager
        
    def update_contract_info(self, contract_tickers: Optional[List[str]] = None):
        """
        実契約の情報を更新
        
        Args:
            contract_tickers: 更新対象の契約ティッカーリスト（Noneの場合は全て）
        """
        # 更新対象の契約を取得
        contracts = self._get_contracts_to_update(contract_tickers)
        
        if contracts.empty:
            logger.warning("更新対象の契約が見つかりません")
            return
            
        logger.info(f"更新対象: {len(contracts)}件の実契約")
        
        # バッチ処理（100件ずつ）
        batch_size = 100
        for i in range(0, len(contracts), batch_size):
            batch = contracts.iloc[i:i+batch_size]
            self._update_batch(batch)
            
    def _get_contracts_to_update(self, contract_tickers: Optional[List[str]] = None) -> pd.DataFrame:
        """更新対象の契約を取得"""
        with self.db_manager.get_connection() as conn:
            if contract_tickers:
                placeholders = ','.join(['?' for _ in contract_tickers])
                query = f"""
                    SELECT ActualContractID, ContractTicker, MetalID, ExchangeCode
                    FROM M_ActualContract
                    WHERE ContractTicker IN ({placeholders})
                    ORDER BY ContractTicker
                """
                return pd.read_sql(query, conn, params=contract_tickers)
            else:
                query = """
                    SELECT ActualContractID, ContractTicker, MetalID, ExchangeCode
                    FROM M_ActualContract
                    WHERE (LastTradeableDate IS NULL OR ContractMonth IS NULL)
                    ORDER BY ContractTicker
                """
                return pd.read_sql(query, conn)
                
    def _update_batch(self, contracts: pd.DataFrame):
        """バッチ単位で契約情報を更新"""
        # ティッカーにComdtyサフィックスを追加（LPF25 → LPF25 Comdty）
        tickers = [f"{ticker} Comdty" for ticker in contracts['ContractTicker'].tolist()]
        
        # 必要なフィールドを定義
        fields = [
            'LAST_TRADEABLE_DT',     # 最終取引日
            'FUT_DLV_DT_LAST',       # 最終受渡日
            'FUT_NOTICE_FIRST',      # ファーストノーティス日
            'FUT_CONTRACT_DT',       # 契約月
            'FUT_CONT_SIZE',         # 契約サイズ
            'FUT_TICK_SIZE',         # ティックサイズ
            'EXCH_CODE',             # 取引所コード
            'CRNCY',                 # 通貨
            'NAME'                   # 契約名
        ]
        
        logger.info(f"Bloomberg APIから{len(tickers)}件の契約情報を取得中...")
        
        # リファレンスデータとして取得（静的データ）
        ref_data = self.bloomberg.get_reference_data(tickers, fields)
        
        if ref_data.empty:
            logger.error("契約情報の取得に失敗しました")
            return
            
        # 各契約の情報を更新
        for _, row in ref_data.iterrows():
            self._update_single_contract(row, contracts)
            
    def _update_single_contract(self, bloomberg_data: pd.Series, contracts: pd.DataFrame):
        """個別の契約情報を更新"""
        # Comdtyサフィックスを除去してオリジナルのティッカーに戻す
        contract_ticker_with_suffix = bloomberg_data['security']
        contract_ticker = contract_ticker_with_suffix.replace(' Comdty', '')
        
        # contractsデータフレームから該当する契約情報を取得
        contract_info = contracts[contracts['ContractTicker'] == contract_ticker].iloc[0]
        
        # 契約月の解析
        contract_month = None
        contract_year = None
        contract_month_code = None
        
        contract_dt = bloomberg_data.get('FUT_CONTRACT_DT')
        if pd.notna(contract_dt):
            try:
                if hasattr(contract_dt, 'month'):
                    contract_month = contract_dt.month
                    contract_year = contract_dt.year
                else:
                    contract_dt = pd.to_datetime(contract_dt)
                    contract_month = contract_dt.month
                    contract_year = contract_dt.year
                    
                # 月コード生成
                month_codes = ['F', 'G', 'H', 'J', 'K', 'M', 'N', 'Q', 'U', 'V', 'X', 'Z']
                contract_month_code = month_codes[contract_month - 1]
            except Exception as e:
                logger.warning(f"契約月解析エラー ({contract_ticker}): {e}")
                
        # 最終取引日の処理
        last_tradeable = bloomberg_data.get('LAST_TRADEABLE_DT')
        if pd.notna(last_tradeable):
            if hasattr(last_tradeable, 'date'):
                last_tradeable = last_tradeable.date()
            else:
                last_tradeable = pd.to_datetime(last_tradeable).date()
                
        # 最終受渡日の処理
        delivery_date = bloomberg_data.get('FUT_DLV_DT_LAST')
        if pd.notna(delivery_date):
            if hasattr(delivery_date, 'date'):
                delivery_date = delivery_date.date()
            else:
                delivery_date = pd.to_datetime(delivery_date).date()
                
        # データベース更新
        with self.db_manager.get_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute("""
                UPDATE M_ActualContract
                SET 
                    ContractMonth = COALESCE(?, ContractMonth),
                    ContractYear = COALESCE(?, ContractYear),
                    ContractMonthCode = COALESCE(?, ContractMonthCode),
                    LastTradeableDate = COALESCE(?, LastTradeableDate),
                    DeliveryDate = COALESCE(?, DeliveryDate),
                    ContractSize = COALESCE(?, ContractSize),
                    TickSize = COALESCE(?, TickSize),
                    LastUpdated = GETDATE()
                WHERE ActualContractID = ?
            """, (
                contract_month,
                contract_year,
                contract_month_code,
                last_tradeable,
                delivery_date,
                float(bloomberg_data.get('FUT_CONT_SIZE')) if pd.notna(bloomberg_data.get('FUT_CONT_SIZE')) else None,
                float(bloomberg_data.get('FUT_TICK_SIZE')) if pd.notna(bloomberg_data.get('FUT_TICK_SIZE')) else None,
                int(contract_info['ActualContractID'])
            ))
            
            conn.commit()
            logger.info(f"契約情報更新: {contract_ticker}")


def main():
    """メイン関数"""
    import sys
    
    # Bloomberg APIとDB接続
    bloomberg_fetcher = BloombergDataFetcher()
    db_manager = DatabaseManager()
    
    if not bloomberg_fetcher.connect():
        logger.error("Bloomberg API接続に失敗しました")
        sys.exit(1)
        
    try:
        updater = ActualContractInfoUpdater(bloomberg_fetcher, db_manager)
        
        if len(sys.argv) > 1:
            # 特定の契約を更新
            contract_tickers = sys.argv[1:]
            logger.info(f"指定された契約を更新: {contract_tickers}")
            updater.update_contract_info(contract_tickers)
        else:
            # 全ての不完全な契約を更新
            logger.info("不完全な契約情報を全て更新")
            updater.update_contract_info()
            
    finally:
        bloomberg_fetcher.disconnect()


if __name__ == "__main__":
    main()