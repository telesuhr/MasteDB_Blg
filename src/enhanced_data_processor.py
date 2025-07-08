"""
拡張データプロセッサー：ジェネリック先物の価格データに実契約情報を付与
"""
import pandas as pd
from typing import Dict, Optional
from datetime import datetime, date
import re

from config.logging_config import logger


class EnhancedDataProcessor:
    """ジェネリック先物データに実契約情報を付与するプロセッサー"""
    
    def __init__(self, db_manager):
        self.db_manager = db_manager
        
    def process_commodity_prices_with_actual_info(self, df: pd.DataFrame, ticker_info: Dict) -> pd.DataFrame:
        """
        商品価格データを処理し、実契約情報を付与
        
        Args:
            df: Bloombergから取得した生データ
            ticker_info: ティッカー設定情報
            
        Returns:
            pd.DataFrame: 実契約情報を含む処理済みデータ
        """
        if df.empty:
            return pd.DataFrame()
            
        processed_data = []
        
        # 日付別にグループ化して処理
        grouped = df.groupby('date')
        
        for trade_date, date_df in grouped:
            trade_date = pd.to_datetime(trade_date).date()
            
            # この日付のマッピングを取得または作成
            self._ensure_mappings_for_date(trade_date, date_df['security'].unique(), ticker_info)
            
            # 各レコードを処理
            for _, row in date_df.iterrows():
                processed_row = self._process_single_row(row, trade_date, ticker_info)
                if processed_row:
                    processed_data.append(processed_row)
                    
        result_df = pd.DataFrame(processed_data)
        
        if not result_df.empty:
            # 実契約情報を付与
            result_df = self._attach_actual_contract_info(result_df)
            logger.info(f"Processed {len(result_df)} records with actual contract info")
        
        return result_df
        
    def _ensure_mappings_for_date(self, trade_date: date, securities: list, ticker_info: Dict):
        """指定日付のマッピングが存在することを確認（なければ作成）"""
        with self.db_manager.get_connection() as conn:
            cursor = conn.cursor()
            
            # 既存マッピングを確認
            cursor.execute("""
                SELECT gf.GenericTicker
                FROM M_GenericFutures gf
                LEFT JOIN T_GenericContractMapping gcm 
                    ON gf.GenericID = gcm.GenericID 
                    AND gcm.TradeDate = ?
                WHERE gf.GenericTicker IN ({})
                AND gcm.MappingID IS NULL
            """.format(','.join(['?' for _ in securities])), 
            [trade_date] + list(securities))
            
            missing_mappings = [row[0] for row in cursor.fetchall()]
            
            if missing_mappings:
                logger.info(f"Creating mappings for {len(missing_mappings)} tickers on {trade_date}")
                # ここで自動ロールオーバーマネージャーを呼び出してマッピング作成
                # （簡略化のため、実装は省略）
                
    def _process_single_row(self, row: pd.Series, trade_date: date, ticker_info: Dict) -> Optional[Dict]:
        """単一行を処理"""
        try:
            security = row['security']
            
            # メタルIDの取得
            metal_code = ticker_info.get('metal', 'COPPER')
            metal_id = self.db_manager.get_or_create_master_id('metals', metal_code)
            
            # GenericIDを取得
            generic_id = self._get_generic_id(security, metal_id, ticker_info)
            
            if not generic_id:
                return None
                
            # 価格データの構築
            processed_row = {
                'TradeDate': trade_date,
                'MetalID': metal_id,
                'DataType': 'Generic',
                'GenericID': generic_id,
                'ActualContractID': None,  # 後で付与
                'SettlementPrice': row.get('PX_LAST'),
                'OpenPrice': row.get('PX_OPEN'),
                'HighPrice': row.get('PX_HIGH'),
                'LowPrice': row.get('PX_LOW'),
                'LastPrice': row.get('PX_LAST'),
                'Volume': row.get('PX_VOLUME'),
                'OpenInterest': row.get('OPEN_INT')
            }
            
            # データ型の変換とクリーニング
            processed_row = self._clean_numeric_fields(processed_row)
            return processed_row
            
        except Exception as e:
            logger.error(f"Error processing row for {security}: {e}")
            return None
            
    def _get_generic_id(self, security: str, metal_id: int, ticker_info: Dict) -> Optional[int]:
        """GenericIDを取得"""
        with self.db_manager.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT GenericID FROM M_GenericFutures 
                WHERE GenericTicker = ? AND IsActive = 1
            """, (security,))
            result = cursor.fetchone()
            
            if result:
                return result[0]
            else:
                # 新規作成（必要に応じて）
                exchange_code = ticker_info.get('exchange', 'LME')
                generic_number = self._extract_generic_number(security)
                description = f"{exchange_code} Copper Generic {generic_number} Future"
                
                cursor.execute("""
                    INSERT INTO M_GenericFutures (
                        GenericTicker, MetalID, ExchangeCode, GenericNumber, 
                        Description, IsActive, CreatedDate
                    ) VALUES (?, ?, ?, ?, ?, 1, ?)
                """, (security, metal_id, exchange_code, generic_number, 
                      description, datetime.now()))
                cursor.execute("SELECT @@IDENTITY")
                generic_id = cursor.fetchone()[0]
                conn.commit()
                logger.info(f"Created new generic future: {security} (ID: {generic_id})")
                return generic_id
                
    def _attach_actual_contract_info(self, df: pd.DataFrame) -> pd.DataFrame:
        """価格データに実契約情報を付与"""
        with self.db_manager.get_connection() as conn:
            # SQLでJOINして実契約情報を取得
            query = """
                SELECT 
                    cp.TradeDate,
                    cp.GenericID,
                    gcm.ActualContractID,
                    ac.ContractTicker,
                    ac.ContractMonth,
                    ac.ContractMonthCode,
                    ac.ContractYear,
                    gcm.DaysToExpiry
                FROM (
                    SELECT DISTINCT TradeDate, GenericID 
                    FROM (VALUES {}) AS cp(TradeDate, GenericID)
                ) cp
                LEFT JOIN T_GenericContractMapping gcm 
                    ON cp.GenericID = gcm.GenericID 
                    AND cp.TradeDate = gcm.TradeDate
                LEFT JOIN M_ActualContract ac 
                    ON gcm.ActualContractID = ac.ActualContractID
            """.format(
                ','.join([f"('{row['TradeDate']}', {row['GenericID']})" 
                         for _, row in df.iterrows()])
            )
            
            mapping_df = pd.read_sql(query, conn)
            
            # 元のデータフレームとマージ
            result_df = df.merge(
                mapping_df[['TradeDate', 'GenericID', 'ActualContractID', 
                           'ContractTicker', 'ContractMonth', 'ContractMonthCode', 
                           'ContractYear', 'DaysToExpiry']],
                on=['TradeDate', 'GenericID'],
                how='left'
            )
            
            # ActualContractIDを更新
            result_df['ActualContractID'] = result_df['ActualContractID_y'].fillna(result_df['ActualContractID_x'])
            result_df.drop(['ActualContractID_x', 'ActualContractID_y'], axis=1, inplace=True)
            
            return result_df
            
    def _extract_generic_number(self, ticker: str) -> int:
        """ティッカーからジェネリック番号を抽出"""
        match = re.search(r'(\d+)', ticker)
        return int(match.group(1)) if match else 1
        
    def _clean_numeric_fields(self, row: Dict) -> Dict:
        """数値フィールドのクリーニング"""
        numeric_fields = ['SettlementPrice', 'OpenPrice', 'HighPrice', 
                         'LowPrice', 'LastPrice', 'Volume', 'OpenInterest']
        
        for field in numeric_fields:
            if field in row and row[field] is not None:
                try:
                    if field in ['Volume', 'OpenInterest']:
                        row[field] = int(float(row[field]))
                    else:
                        row[field] = float(row[field])
                except:
                    row[field] = None
                    
        return row
        
    def create_enhanced_price_view(self):
        """実契約情報を含む拡張価格ビューを作成"""
        view_sql = """
        CREATE OR ALTER VIEW V_CommodityPriceWithActualContract AS
        SELECT 
            cp.PriceID,
            cp.TradeDate,
            m.MetalCode,
            m.MetalName,
            gf.GenericTicker,
            gf.GenericNumber,
            ac.ContractTicker as ActualContract,
            ac.ContractMonth,
            ac.ContractMonthCode,
            ac.ContractYear,
            DATEDIFF(day, cp.TradeDate, ac.LastTradeableDate) as DaysToExpiry,
            cp.LastPrice,
            cp.OpenPrice,
            cp.HighPrice,
            cp.LowPrice,
            cp.Volume,
            cp.OpenInterest,
            gf.ExchangeCode,
            CASE gf.ExchangeCode
                WHEN 'CMX' THEN 'COMEX'
                ELSE gf.ExchangeCode
            END as ExchangeName
        FROM T_CommodityPrice cp
        JOIN M_Metal m ON cp.MetalID = m.MetalID
        JOIN M_GenericFutures gf ON cp.GenericID = gf.GenericID
        LEFT JOIN T_GenericContractMapping gcm 
            ON cp.GenericID = gcm.GenericID 
            AND cp.TradeDate = gcm.TradeDate
        LEFT JOIN M_ActualContract ac 
            ON gcm.ActualContractID = ac.ActualContractID
        WHERE cp.DataType = 'Generic'
        """
        
        with self.db_manager.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(view_sql)
            conn.commit()
            logger.info("Created enhanced price view with actual contract info")