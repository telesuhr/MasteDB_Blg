"""
契約日付情報の検証と修正スクリプト
M_ActualContractテーブルの日付情報を確認し、必要に応じて修正する
"""
import sys
import os
from datetime import datetime, date
import pandas as pd

# プロジェクトルートをPythonパスに追加
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)
sys.path.insert(0, os.path.join(project_root, 'src'))

from database import DatabaseManager
from bloomberg_api import BloombergDataFetcher
from config.logging_config import logger


class ContractDateVerifier:
    """契約日付情報の検証と修正を行うクラス"""
    
    def __init__(self):
        self.db_manager = DatabaseManager()
        self.bloomberg = BloombergDataFetcher()
        
    def verify_all_contracts(self):
        """全契約の日付情報を検証"""
        logger.info("=== 契約日付検証開始 ===")
        
        try:
            self.db_manager.connect()
            if not self.bloomberg.connect():
                logger.warning("Bloomberg API接続失敗 - データベース情報のみで検証")
            
            # 1. 現在のM_ActualContract内容を確認
            self._check_current_data()
            
            # 2. 問題のあるデータを特定
            problematic_contracts = self._identify_problematic_contracts()
            
            if not problematic_contracts.empty:
                logger.info(f"問題のある契約: {len(problematic_contracts)}件")
                logger.info(f"\n{problematic_contracts}")
                
                # 3. Bloombergから正しい情報を取得して修正
                if hasattr(self.bloomberg, 'session') and self.bloomberg.session:
                    self._fix_contract_dates(problematic_contracts)
                else:
                    logger.warning("Bloomberg未接続のため、日付修正はスキップされます")
                    
        except Exception as e:
            logger.error(f"検証エラー: {e}")
            
        finally:
            self.bloomberg.disconnect()
            self.db_manager.disconnect()
            
    def _check_current_data(self):
        """現在のデータ状況を確認"""
        with self.db_manager.get_connection() as conn:
            # ContractMonth分布を確認
            query_month_dist = """
                SELECT 
                    ExchangeCode,
                    ContractMonth,
                    COUNT(*) as ContractCount
                FROM M_ActualContract
                GROUP BY ExchangeCode, ContractMonth
                ORDER BY ExchangeCode, ContractMonth
            """
            month_dist = pd.read_sql(query_month_dist, conn)
            logger.info("契約月分布:")
            logger.info(f"\n{month_dist}")
            
            # データ型情報を確認
            query_column_info = """
                SELECT 
                    COLUMN_NAME,
                    DATA_TYPE,
                    IS_NULLABLE
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = 'M_ActualContract'
                AND COLUMN_NAME IN ('ContractMonth', 'LastTradeableDate', 'DeliveryDate')
                ORDER BY ORDINAL_POSITION
            """
            column_info = pd.read_sql(query_column_info, conn)
            logger.info("\nカラムのデータ型:")
            logger.info(f"\n{column_info}")
            
            # 日付データ型とNULL値を確認（ContractMonthがDATE型の場合に対応）
            query_date_check = """
                SELECT 
                    COUNT(*) as TotalContracts,
                    SUM(CASE WHEN LastTradeableDate IS NULL THEN 1 ELSE 0 END) as NullLastTradeable,
                    SUM(CASE WHEN DeliveryDate IS NULL THEN 1 ELSE 0 END) as NullDelivery,
                    SUM(CASE WHEN ContractMonth IS NULL THEN 1 ELSE 0 END) as NullContractMonth
                FROM M_ActualContract
            """
            date_check = pd.read_sql(query_date_check, conn)
            logger.info("\n日付データ状況:")
            logger.info(f"\n{date_check}")
            
    def _identify_problematic_contracts(self) -> pd.DataFrame:
        """問題のある契約を特定"""
        with self.db_manager.get_connection() as conn:
            # まずContractMonthのデータ型を確認
            cursor = conn.cursor()
            cursor.execute("""
                SELECT DATA_TYPE 
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_NAME = 'M_ActualContract' 
                AND COLUMN_NAME = 'ContractMonth'
            """)
            data_type = cursor.fetchone()[0]
            
            if data_type.lower() == 'date':
                # ContractMonthがDATE型の場合
                query = """
                    SELECT 
                        ActualContractID,
                        ContractTicker,
                        MetalID,
                        ExchangeCode,
                        ContractMonth,
                        MONTH(ContractMonth) as ContractMonthNum,
                        ContractYear,
                        ContractMonthCode,
                        LastTradeableDate,
                        DeliveryDate,
                        CreatedDate
                    FROM M_ActualContract
                    WHERE 
                        -- 問題のあるデータを特定
                        ContractMonth IS NULL 
                        OR LastTradeableDate IS NULL
                        OR DeliveryDate IS NULL
                        -- 2025年6月のように手動追加されたデータも確認
                        OR (ContractTicker = 'LPM25')
                    ORDER BY ExchangeCode, ContractYear, ContractMonth
                """
            else:
                # ContractMonthがINT型の場合
                query = """
                    SELECT 
                        ActualContractID,
                        ContractTicker,
                        MetalID,
                        ExchangeCode,
                        ContractMonth,
                        ContractMonth as ContractMonthNum,
                        ContractYear,
                        ContractMonthCode,
                        LastTradeableDate,
                        DeliveryDate,
                        CreatedDate
                    FROM M_ActualContract
                    WHERE 
                        -- 問題のあるデータを特定
                        ContractMonth IS NULL 
                        OR ContractMonth < 1 
                        OR ContractMonth > 12
                        OR LastTradeableDate IS NULL
                        OR DeliveryDate IS NULL
                        -- 2025年6月のように手動追加されたデータも確認
                        OR (ContractTicker = 'LPM25' AND ContractMonth = 6)
                    ORDER BY ExchangeCode, ContractYear, ContractMonth
                """
            return pd.read_sql(query, conn)
            
    def _fix_contract_dates(self, problematic_contracts: pd.DataFrame):
        """Bloombergから正しい日付情報を取得して修正"""
        logger.info("Bloombergから正しい日付情報を取得中...")
        
        # ティッカーリスト
        tickers = problematic_contracts['ContractTicker'].unique().tolist()
        
        # 必要なフィールド
        fields = [
            'LAST_TRADEABLE_DT',    # 最終取引日
            'FUT_DLV_DT_LAST',      # 最終受渡日
            'FUT_CONTRACT_DT',      # 契約月
            'FUT_MONTH_YR'          # 契約年月
        ]
        
        # バッチで取得
        batch_size = 25
        for i in range(0, len(tickers), batch_size):
            batch_tickers = tickers[i:i+batch_size]
            
            try:
                ref_data = self.bloomberg.get_reference_data(batch_tickers, fields)
                
                if not ref_data.empty:
                    for _, row in ref_data.iterrows():
                        ticker = row['security']
                        contract_info = problematic_contracts[
                            problematic_contracts['ContractTicker'] == ticker
                        ].iloc[0]
                        
                        # 日付情報を抽出
                        last_tradeable = row.get('LAST_TRADEABLE_DT')
                        delivery_date = row.get('FUT_DLV_DT_LAST')
                        contract_dt = row.get('FUT_CONTRACT_DT')
                        
                        # 契約月を正しく抽出
                        contract_month = None
                        if pd.notna(contract_dt):
                            try:
                                if hasattr(contract_dt, 'month'):
                                    contract_month = contract_dt.month
                                else:
                                    contract_month = pd.to_datetime(contract_dt).month
                            except:
                                logger.warning(f"{ticker}: 契約月の解析に失敗")
                                
                        # データベースを更新
                        self._update_contract_dates(
                            contract_info['ActualContractID'],
                            ticker,
                            last_tradeable,
                            delivery_date,
                            contract_month
                        )
                        
            except Exception as e:
                logger.error(f"バッチ処理エラー: {e}")
                continue
                
    def _update_contract_dates(self, contract_id: int, ticker: str,
                              last_tradeable, delivery_date, contract_month):
        """契約日付情報を更新"""
        with self.db_manager.get_connection() as conn:
            cursor = conn.cursor()
            
            updates = []
            params = []
            
            # 日付型の処理
            if pd.notna(last_tradeable):
                if hasattr(last_tradeable, 'date'):
                    last_tradeable = last_tradeable.date()
                else:
                    last_tradeable = pd.to_datetime(last_tradeable).date()
                updates.append("LastTradeableDate = ?")
                params.append(last_tradeable)
                
            if pd.notna(delivery_date):
                if hasattr(delivery_date, 'date'):
                    delivery_date = delivery_date.date()
                else:
                    delivery_date = pd.to_datetime(delivery_date).date()
                updates.append("DeliveryDate = ?")
                params.append(delivery_date)
                
            if contract_month and 1 <= contract_month <= 12:
                updates.append("ContractMonth = ?")
                params.append(int(contract_month))
                
            if updates:
                params.append(contract_id)
                sql = f"""
                    UPDATE M_ActualContract 
                    SET {', '.join(updates)}, LastUpdated = GETDATE()
                    WHERE ActualContractID = ?
                """
                cursor.execute(sql, params)
                conn.commit()
                logger.info(f"{ticker}: 日付情報を更新しました")
                
    def verify_specific_contracts(self, tickers: list):
        """特定の契約の日付情報を詳細確認"""
        logger.info(f"=== 特定契約の詳細確認: {tickers} ===")
        
        try:
            self.db_manager.connect()
            
            with self.db_manager.get_connection() as conn:
                placeholders = ','.join(['?' for _ in tickers])
                query = f"""
                    SELECT 
                        ContractTicker,
                        ContractMonth,
                        ContractMonthCode,
                        ContractYear,
                        LastTradeableDate,
                        DeliveryDate,
                        DATEDIFF(day, GETDATE(), LastTradeableDate) as DaysToExpiry
                    FROM M_ActualContract
                    WHERE ContractTicker IN ({placeholders})
                    ORDER BY ContractYear, ContractMonth
                """
                result = pd.read_sql(query, conn, params=tickers)
                
                logger.info(f"\n{result}")
                
                # マッピング状況も確認
                query_mapping = f"""
                    SELECT 
                        gf.GenericTicker,
                        ac.ContractTicker,
                        gcm.TradeDate,
                        gcm.DaysToExpiry
                    FROM T_GenericContractMapping gcm
                    JOIN M_GenericFutures gf ON gcm.GenericID = gf.GenericID
                    JOIN M_ActualContract ac ON gcm.ActualContractID = ac.ActualContractID
                    WHERE ac.ContractTicker IN ({placeholders})
                    AND gcm.TradeDate >= DATEADD(day, -30, GETDATE())
                    ORDER BY gcm.TradeDate DESC
                """
                mapping_result = pd.read_sql(query_mapping, conn, params=tickers)
                
                if not mapping_result.empty:
                    logger.info("\n最近のマッピング状況:")
                    logger.info(f"\n{mapping_result.head(20)}")
                    
        finally:
            self.db_manager.disconnect()


if __name__ == "__main__":
    verifier = ContractDateVerifier()
    
    # 全体検証
    verifier.verify_all_contracts()
    
    # 特定契約の詳細確認（例：2025年6月契約）
    verifier.verify_specific_contracts(['LPM25', 'LPN25', 'LPQ25'])