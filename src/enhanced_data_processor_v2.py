"""
Enhanced Data Processor with Automatic Mapping
価格データ処理時に自動的に正しいGeneric-Actual契約マッピングを適用
"""
import pandas as pd
import numpy as np
from datetime import datetime, date
from typing import Dict, List, Optional, Any, Tuple
from loguru import logger
from database import DatabaseManager
from data_processor import DataProcessor


class EnhancedDataProcessorV2(DataProcessor):
    """自動マッピング機能を持つ拡張データプロセッサー"""
    
    def __init__(self, db_manager: DatabaseManager):
        super().__init__(db_manager)
        self.mapping_cache = {}  # {(GenericID, TradeDate): ActualContractID}
        self.contract_info_cache = {}  # {ActualContractID: contract_info}
        
    def process_commodity_prices(self, df: pd.DataFrame, ticker_info: Dict[str, Any]) -> pd.DataFrame:
        """
        商品価格データを処理（自動マッピング付き）
        """
        if df.empty:
            return df
            
        # 基本処理を実行
        processed_df = super().process_commodity_prices(df, ticker_info)
        
        # ジェネリック先物の場合、自動的にActualContractIDを設定
        if not processed_df.empty and 'DataType' in processed_df.columns:
            generic_mask = processed_df['DataType'] == 'Generic'
            if generic_mask.any():
                logger.info(f"ジェネリック先物 {generic_mask.sum()}件の自動マッピングを実行")
                processed_df = self._apply_automatic_mapping(processed_df, generic_mask)
                
        return processed_df
        
    def _apply_automatic_mapping(self, df: pd.DataFrame, generic_mask: pd.Series) -> pd.DataFrame:
        """ジェネリック先物に対して自動的にActualContractIDを設定"""
        
        # 対象となる日付範囲を取得
        trade_dates = df.loc[generic_mask, 'TradeDate'].unique()
        generic_ids = df.loc[generic_mask, 'GenericID'].unique()
        
        # 必要なマッピングを一括取得
        self._ensure_mappings_loaded(trade_dates, generic_ids)
        
        # 各行にマッピングを適用
        for idx in df[generic_mask].index:
            row = df.loc[idx]
            trade_date = row['TradeDate']
            generic_id = row['GenericID']
            
            # キャッシュからマッピングを取得
            mapping_key = (generic_id, trade_date)
            if mapping_key in self.mapping_cache:
                actual_contract_id = self.mapping_cache[mapping_key]
                # CHECK制約のため、GenericタイプではActualContractIDを設定しない
                # マッピング情報はT_GenericContractMappingテーブルで管理
                # df.loc[idx, 'ActualContractID'] = actual_contract_id
                
                # マッピングの存在を確認できたことをログに記録
                logger.debug(f"マッピング確認: GenericID={generic_id}, Date={trade_date} → ActualContractID={actual_contract_id}")
            else:
                logger.warning(f"マッピングが見つかりません: GenericID={generic_id}, TradeDate={trade_date}")
                
        return df
        
    def _ensure_mappings_loaded(self, trade_dates: np.ndarray, generic_ids: np.ndarray):
        """必要なマッピングをデータベースから一括ロード"""
        
        # 既にキャッシュにあるものを除外
        needed_mappings = []
        for generic_id in generic_ids:
            for trade_date in trade_dates:
                if (generic_id, trade_date) not in self.mapping_cache:
                    needed_mappings.append((generic_id, trade_date))
                    
        if not needed_mappings:
            return
            
        logger.info(f"{len(needed_mappings)}件のマッピングをロード中...")
        
        with self.db_manager.get_connection() as conn:
            cursor = conn.cursor()
            
            # SQL ServerはタプルのINをサポートしないので、ORで結合
            conditions = []
            params = []
            for gid, td in needed_mappings:
                conditions.append("(gcm.GenericID = ? AND gcm.TradeDate = ?)")
                # numpy型をPython標準型に変換
                params.extend([int(gid) if hasattr(gid, 'item') else gid, td])
                
            query = f"""
                SELECT 
                    gcm.GenericID,
                    gcm.TradeDate,
                    gcm.ActualContractID,
                    gcm.DaysToExpiry,
                    ac.ContractTicker,
                    ac.ContractMonth,
                    ac.ContractMonthCode,
                    ac.LastTradeableDate,
                    ac.DeliveryDate
                FROM T_GenericContractMapping gcm
                JOIN M_ActualContract ac ON gcm.ActualContractID = ac.ActualContractID
                WHERE {' OR '.join(conditions)}
            """
            
            cursor.execute(query, params)
            
            rows_loaded = 0
            for row in cursor:
                generic_id = row[0]
                trade_date = row[1]
                actual_contract_id = row[2]
                
                # マッピングをキャッシュ
                self.mapping_cache[(generic_id, trade_date)] = actual_contract_id
                
                # 契約情報もキャッシュ
                if actual_contract_id not in self.contract_info_cache:
                    self.contract_info_cache[actual_contract_id] = {
                        'ContractTicker': row[4],
                        'ContractMonth': row[5],
                        'ContractMonthCode': row[6],
                        'LastTradeableDate': row[7],
                        'DeliveryDate': row[8]
                    }
                rows_loaded += 1
                
            logger.info(f"{rows_loaded}件のマッピングをロードしました")
            
            # マッピングが見つからない日付を特定
            missing_mappings = []
            for generic_id in generic_ids:
                for trade_date in trade_dates:
                    if (generic_id, trade_date) not in self.mapping_cache:
                        missing_mappings.append((generic_id, trade_date))
                        
            if missing_mappings:
                logger.warning(f"{len(missing_mappings)}件のマッピングが不足しています")
                self._create_missing_mappings(missing_mappings)
                
    def _create_missing_mappings(self, missing_mappings: List[Tuple[int, date]]):
        """不足しているマッピングを自動生成"""
        
        logger.info("不足しているマッピングの自動生成を開始...")
        
        # GenericIDごとにグループ化
        mappings_by_generic = {}
        for generic_id, trade_date in missing_mappings:
            if generic_id not in mappings_by_generic:
                mappings_by_generic[generic_id] = []
            mappings_by_generic[generic_id].append(trade_date)
            
        with self.db_manager.get_connection() as conn:
            cursor = conn.cursor()
            
            for generic_id, trade_dates in mappings_by_generic.items():
                # ジェネリック先物の情報を取得
                cursor.execute("""
                    SELECT MetalID, ExchangeCode, GenericNumber
                    FROM M_GenericFutures
                    WHERE GenericID = ?
                """, (generic_id,))
                
                result = cursor.fetchone()
                if not result:
                    continue
                    
                metal_id, exchange_code, generic_number = result
                
                # 各日付に対して適切な契約を特定
                for trade_date in sorted(trade_dates):
                    actual_contract_id = self._find_appropriate_contract(
                        cursor, metal_id, exchange_code, trade_date, generic_number
                    )
                    
                    if actual_contract_id:
                        # マッピングを挿入
                        self._insert_mapping(cursor, generic_id, trade_date, actual_contract_id)
                        # キャッシュに追加
                        self.mapping_cache[(generic_id, trade_date)] = actual_contract_id
                        
            conn.commit()
            
    def _find_appropriate_contract(self, cursor, metal_id: int, exchange_code: str, 
                                 trade_date: date, generic_number: int) -> Optional[int]:
        """指定された日付とジェネリック番号に対応する適切な契約を特定"""
        
        # ロールオーバー日数（LMEは通常0日前）
        rollover_days = 0
        
        # 利用可能な契約を取得（満期日順）
        cursor.execute("""
            SELECT 
                ActualContractID,
                ContractTicker,
                LastTradeableDate,
                DATEDIFF(day, ?, LastTradeableDate) as DaysToExpiry
            FROM M_ActualContract
            WHERE MetalID = ?
            AND ExchangeCode = ?
            AND LastTradeableDate >= DATEADD(day, ?, ?)  -- ロールオーバー考慮
            ORDER BY LastTradeableDate
        """, (trade_date, metal_id, exchange_code, rollover_days, trade_date))
        
        contracts = cursor.fetchall()
        
        if not contracts:
            logger.warning(f"利用可能な契約が見つかりません: {exchange_code} Metal={metal_id} Date={trade_date}")
            return None
            
        # ジェネリック番号に対応する契約を選択（1番限 = 最近月）
        if generic_number <= len(contracts):
            return contracts[generic_number - 1][0]
        else:
            # ジェネリック番号が契約数を超える場合は最遠月を返す
            return contracts[-1][0]
            
    def _insert_mapping(self, cursor, generic_id: int, trade_date: date, actual_contract_id: int):
        """マッピングをデータベースに挿入"""
        
        # 契約の満期日を取得してDaysToExpiryを計算
        cursor.execute("""
            SELECT LastTradeableDate
            FROM M_ActualContract
            WHERE ActualContractID = ?
        """, (actual_contract_id,))
        
        result = cursor.fetchone()
        if result and result[0]:
            days_to_expiry = (result[0] - trade_date).days
        else:
            days_to_expiry = None
            
        # マッピングを挿入（MERGE操作）
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
        """, (trade_date, generic_id, actual_contract_id, days_to_expiry))
        
        logger.debug(f"マッピングを作成: GenericID={generic_id}, Date={trade_date}, ContractID={actual_contract_id}")
        
    def clear_cache(self):
        """キャッシュをクリア"""
        self.mapping_cache.clear()
        self.contract_info_cache.clear()
        logger.info("マッピングキャッシュをクリアしました")