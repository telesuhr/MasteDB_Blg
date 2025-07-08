"""
自動ロールオーバー管理システム
GenericTickerが指し示す実際の先物契約を自動的に更新する
"""
import sys
import os
from datetime import datetime, date, timedelta
import pandas as pd
from typing import Optional, Dict, List

# プロジェクトルートとsrcディレクトリをPythonパスに追加
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
src_dir = os.path.join(project_root, 'src')
sys.path.insert(0, project_root)
sys.path.insert(0, src_dir)

from bloomberg_api import BloombergDataFetcher
from database import DatabaseManager
from config.logging_config import logger


class AutoRolloverManager:
    """自動ロールオーバー管理クラス"""
    
    def __init__(self):
        self.bloomberg = BloombergDataFetcher()
        self.db_manager = DatabaseManager()
        
    def execute_auto_rollover(self) -> bool:
        """自動ロールオーバーを実行"""
        logger.info("=== 自動ロールオーバー処理開始 ===")
        
        try:
            # 接続
            if not self.bloomberg.connect():
                logger.warning("Bloomberg API接続失敗 - モックモードで実行")
            self.db_manager.connect()
            
            # 1. 満期日情報を更新
            logger.info("ステップ1: 満期日情報を更新")
            self._update_maturity_dates()
            
            # 2. ロールオーバーが必要な契約を確認
            logger.info("ステップ2: ロールオーバー必要性チェック")
            rollover_candidates = self._check_rollover_needed()
            
            if rollover_candidates.empty:
                logger.info("ロールオーバーが必要な契約はありません")
                return True
            
            logger.info(f"ロールオーバー候補: {len(rollover_candidates)}件")
            
            # 3. 各ジェネリック先物のマッピングを更新
            logger.info("ステップ3: ジェネリック先物マッピング更新")
            success_count = self._update_generic_mappings(rollover_candidates)
            
            logger.info(f"=== ロールオーバー処理完了: {success_count}件更新 ===")
            return True
            
        except Exception as e:
            logger.error(f"ロールオーバー処理エラー: {e}")
            return False
            
        finally:
            self.bloomberg.disconnect()
            self.db_manager.disconnect()
            
    def _update_maturity_dates(self):
        """満期日情報を更新"""
        # 全ジェネリック先物のリストを取得
        with self.db_manager.get_connection() as conn:
            query = """
                SELECT GenericID, GenericTicker
                FROM M_GenericFutures 
                WHERE IsActive = 1
                ORDER BY ExchangeCode, GenericNumber
            """
            generic_futures = pd.read_sql(query, conn)
            
        # Bloomberg APIから満期日情報を取得
        tickers = generic_futures['GenericTicker'].tolist()
        fields = ['LAST_TRADEABLE_DT', 'FUT_DLV_DT_LAST']
        
        # バッチで取得
        batch_size = 25
        current_time = datetime.now()
        
        for i in range(0, len(tickers), batch_size):
            batch_tickers = tickers[i:i+batch_size]
            try:
                ref_data = self.bloomberg.get_reference_data(batch_tickers, fields)
                
                if not ref_data.empty:
                    for _, row in ref_data.iterrows():
                        ticker = row['security']
                        generic_id = generic_futures[
                            generic_futures['GenericTicker'] == ticker
                        ]['GenericID'].iloc[0]
                        
                        last_tradeable = row.get('LAST_TRADEABLE_DT')
                        delivery_date = row.get('FUT_DLV_DT_LAST')
                        
                        # 日付変換
                        if pd.notna(last_tradeable):
                            last_tradeable = pd.to_datetime(last_tradeable).date()
                        if pd.notna(delivery_date):
                            delivery_date = pd.to_datetime(delivery_date).date()
                        
                        # DB更新
                        self._update_maturity_in_db(generic_id, last_tradeable, delivery_date, current_time)
                        
            except Exception as e:
                logger.error(f"満期日更新エラー (バッチ {i//batch_size + 1}): {e}")
                
    def _update_maturity_in_db(self, generic_id: int, last_tradeable: date, 
                               delivery_date: date, update_time: datetime):
        """データベースに満期日を更新"""
        with self.db_manager.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE M_GenericFutures
                SET 
                    LastTradeableDate = ?,
                    FutureDeliveryDateLast = ?,
                    LastRefreshDate = ?
                WHERE GenericID = ?
            """, (last_tradeable, delivery_date, update_time, int(generic_id)))
            conn.commit()
            
    def _check_rollover_needed(self) -> pd.DataFrame:
        """ロールオーバーが必要な契約を確認"""
        with self.db_manager.get_connection() as conn:
            # ロールオーバー判定クエリ
            query = """
                WITH RolloverCheck AS (
                    SELECT 
                        gf.GenericID,
                        gf.GenericTicker,
                        gf.ExchangeCode,
                        gf.GenericNumber,
                        gf.LastTradeableDate,
                        gf.RolloverDays,
                        -- 現在のマッピング
                        gcm.ActualContractID as CurrentContractID,
                        ac.ContractTicker as CurrentContract,
                        -- ロールオーバー判定
                        CASE 
                            WHEN gf.LastTradeableDate IS NULL THEN 0
                            WHEN DATEDIFF(day, GETDATE(), gf.LastTradeableDate) <= gf.RolloverDays THEN 1
                            ELSE 0
                        END as NeedsRollover,
                        DATEDIFF(day, GETDATE(), gf.LastTradeableDate) as DaysToExpiry
                    FROM M_GenericFutures gf
                    LEFT JOIN T_GenericContractMapping gcm ON gf.GenericID = gcm.GenericID
                        AND gcm.TradeDate = CAST(GETDATE() AS DATE)
                    LEFT JOIN M_ActualContract ac ON gcm.ActualContractID = ac.ActualContractID
                    WHERE gf.IsActive = 1
                )
                SELECT *
                FROM RolloverCheck
                WHERE NeedsRollover = 1
                OR CurrentContractID IS NULL  -- マッピングがない場合も更新対象
                ORDER BY ExchangeCode, GenericNumber
            """
            
            df = pd.read_sql(query, conn)
            
            # ログ出力
            for _, row in df.iterrows():
                if row['NeedsRollover'] == 1:
                    logger.info(f"{row['GenericTicker']}: 満期まで{row['DaysToExpiry']}日 - ロールオーバー必要")
                else:
                    logger.info(f"{row['GenericTicker']}: 現在のマッピングなし - 新規作成必要")
                    
            return df
            
    def _update_generic_mappings(self, rollover_candidates: pd.DataFrame) -> int:
        """ジェネリック先物のマッピングを更新"""
        success_count = 0
        today = date.today()
        
        # ティッカーリストを作成
        tickers = rollover_candidates['GenericTicker'].tolist()
        
        # Bloombergから現在のジェネリック契約情報を取得
        fields = [
            'FUT_CUR_GEN_TICKER',     # 現在のジェネリック契約
            'LAST_TRADEABLE_DT',      # 最終取引日
            'FUT_DLV_DT_LAST',        # 最終引渡日
            'FUT_CONTRACT_DT',        # 契約月
            'FUT_CONT_SIZE',          # 契約サイズ
            'FUT_TICK_SIZE'           # ティックサイズ
        ]
        
        ref_data = self.bloomberg.get_reference_data(tickers, fields)
        
        if ref_data.empty:
            logger.error("Bloombergからデータを取得できませんでした")
            return 0
            
        # 各ティッカーのマッピングを更新
        for _, row in ref_data.iterrows():
            ticker = row['security']
            current_contract = row.get('FUT_CUR_GEN_TICKER')
            
            if pd.isna(current_contract):
                logger.warning(f"{ticker}: 現在の契約が取得できません")
                continue
                
            # ジェネリック先物情報を取得
            generic_info = rollover_candidates[
                rollover_candidates['GenericTicker'] == ticker
            ].iloc[0]
            
            logger.info(f"{ticker} -> {current_contract} へマッピング更新")
            
            # 実契約を作成または取得
            actual_contract_id = self._ensure_actual_contract(
                current_contract, generic_info, row
            )
            
            if actual_contract_id:
                # マッピングを更新
                self._update_mapping(today, generic_info['GenericID'], actual_contract_id, row)
                success_count += 1
                
        return success_count
        
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
                
            # 新規作成
            # 契約月の解析
            contract_date = bloomberg_data.get('FUT_CONTRACT_DT')
            contract_month = None
            contract_year = None
            contract_month_code = None
            
            if pd.notna(contract_date):
                try:
                    contract_dt = pd.to_datetime(contract_date)
                    contract_month = contract_dt.replace(day=1).date()
                    contract_year = contract_dt.year
                    
                    # 月コード生成
                    month_codes = ['F', 'G', 'H', 'J', 'K', 'M', 'N', 'Q', 'U', 'V', 'X', 'Z']
                    contract_month_code = month_codes[contract_dt.month - 1]
                except Exception as e:
                    logger.warning(f"契約月解析エラー: {e}")
                    
            # MetalIDを取得
            cursor.execute(
                "SELECT MetalID FROM M_GenericFutures WHERE GenericID = ?",
                (int(generic_info['GenericID']),)
            )
            metal_id = cursor.fetchone()[0]
            
            # 挿入
            cursor.execute("""
                INSERT INTO M_ActualContract (
                    ContractTicker, MetalID, ExchangeCode, ContractMonth, 
                    ContractYear, ContractMonthCode, LastTradeableDate, 
                    DeliveryDate, ContractSize, TickSize
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                contract_ticker,
                metal_id,
                generic_info['ExchangeCode'],
                contract_month,
                contract_year,
                contract_month_code,
                bloomberg_data.get('LAST_TRADEABLE_DT'),
                bloomberg_data.get('FUT_DLV_DT_LAST'),
                float(bloomberg_data.get('FUT_CONT_SIZE')) if pd.notna(bloomberg_data.get('FUT_CONT_SIZE')) else None,
                float(bloomberg_data.get('FUT_TICK_SIZE')) if pd.notna(bloomberg_data.get('FUT_TICK_SIZE')) else None
            ))
            
            cursor.execute("SELECT @@IDENTITY")
            actual_contract_id = cursor.fetchone()[0]
            conn.commit()
            
            logger.info(f"新規実契約作成: {contract_ticker} (ID: {actual_contract_id})")
            return actual_contract_id
            
    def _update_mapping(self, trade_date: date, generic_id: int, 
                        actual_contract_id: int, bloomberg_data: pd.Series):
        """ジェネリック・実契約マッピングを更新"""
        with self.db_manager.get_connection() as conn:
            cursor = conn.cursor()
            
            # 残存日数計算
            days_to_expiry = None
            last_tradeable = bloomberg_data.get('LAST_TRADEABLE_DT')
            if pd.notna(last_tradeable):
                try:
                    last_tradeable_dt = pd.to_datetime(last_tradeable).date()
                    days_to_expiry = (last_tradeable_dt - trade_date).days
                except Exception as e:
                    logger.warning(f"残存日数計算エラー: {e}")
                    
            # 既存チェック
            cursor.execute("""
                SELECT MappingID FROM T_GenericContractMapping 
                WHERE TradeDate = ? AND GenericID = ?
            """, (trade_date, int(generic_id)))
            existing = cursor.fetchone()
            
            if existing:
                # 更新
                cursor.execute("""
                    UPDATE T_GenericContractMapping 
                    SET ActualContractID = ?, DaysToExpiry = ?, CreatedAt = ?
                    WHERE TradeDate = ? AND GenericID = ?
                """, (int(actual_contract_id), days_to_expiry, datetime.now(), trade_date, int(generic_id)))
                logger.info(f"マッピング更新: GenericID {generic_id} -> ContractID {actual_contract_id}")
            else:
                # 新規挿入
                cursor.execute("""
                    INSERT INTO T_GenericContractMapping (
                        TradeDate, GenericID, ActualContractID, DaysToExpiry
                    ) VALUES (?, ?, ?, ?)
                """, (trade_date, int(generic_id), int(actual_contract_id), days_to_expiry))
                logger.info(f"マッピング作成: GenericID {generic_id} -> ContractID {actual_contract_id}")
                
            conn.commit()
            
    def verify_rollover_status(self):
        """ロールオーバー状況を確認"""
        logger.info("\n=== ロールオーバー状況確認 ===")
        
        with self.db_manager.get_connection() as conn:
            # 現在のマッピング状況
            query = """
                SELECT 
                    gf.GenericTicker,
                    gf.ExchangeCode,
                    ac.ContractTicker as CurrentContract,
                    gf.LastTradeableDate,
                    gcm.DaysToExpiry,
                    CASE 
                        WHEN gcm.DaysToExpiry <= gf.RolloverDays THEN 'IMMEDIATE'
                        WHEN gcm.DaysToExpiry <= gf.RolloverDays + 5 THEN 'SOON'
                        ELSE 'OK'
                    END as RolloverStatus
                FROM M_GenericFutures gf
                LEFT JOIN T_GenericContractMapping gcm ON gf.GenericID = gcm.GenericID
                    AND gcm.TradeDate = CAST(GETDATE() AS DATE)
                LEFT JOIN M_ActualContract ac ON gcm.ActualContractID = ac.ActualContractID
                WHERE gf.IsActive = 1
                ORDER BY gf.ExchangeCode, gf.GenericNumber
            """
            
            df = pd.read_sql(query, conn)
            
            # 取引所別に表示
            for exchange in df['ExchangeCode'].unique():
                display_name = 'COMEX' if exchange == 'CMX' else exchange
                logger.info(f"\n{display_name}:")
                
                exchange_df = df[df['ExchangeCode'] == exchange]
                for _, row in exchange_df.iterrows():
                    status_icon = '🔴' if row['RolloverStatus'] == 'IMMEDIATE' else \
                                  '🟡' if row['RolloverStatus'] == 'SOON' else '🟢'
                    logger.info(f"  {row['GenericTicker']} -> {row['CurrentContract']} "
                              f"(残存{row['DaysToExpiry']}日) {status_icon}")


def main():
    """メイン実行関数"""
    manager = AutoRolloverManager()
    
    # 自動ロールオーバー実行
    success = manager.execute_auto_rollover()
    
    if success:
        # 状況確認
        manager.verify_rollover_status()
        logger.info("\n自動ロールオーバー処理が正常に完了しました")
    else:
        logger.error("\n自動ロールオーバー処理が失敗しました")
        
    return success


if __name__ == "__main__":
    main()