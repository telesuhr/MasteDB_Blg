"""
日次更新プログラム（OpenInterest遅延対応版）
OpenInterestは1営業日遅れで更新される特性を考慮した設計
"""
import sys
import os
from datetime import datetime, date, timedelta
import pandas as pd

# プロジェクトルートとsrcディレクトリをPythonパスに追加
project_root = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.join(project_root, 'src')
sys.path.insert(0, project_root)
sys.path.insert(0, src_dir)

from bloomberg_api import BloombergDataFetcher
from database import DatabaseManager
from config.logging_config import logger

class DailyUpdateWithOIDelay:
    """OpenInterest遅延を考慮した日次更新クラス"""
    
    def __init__(self):
        self.bloomberg = BloombergDataFetcher()
        self.db_manager = DatabaseManager()
        
    def run_daily_update(self):
        """日次更新メイン処理"""
        logger.info("=== OpenInterest遅延対応日次更新開始 ===")
        
        try:
            # Bloomberg API接続
            if not self.bloomberg.connect():
                raise ConnectionError("Bloomberg API接続に失敗しました")
                
            # データベース接続
            self.db_manager.connect()
            
            # 1. 新しい価格データ・マッピング更新
            self._update_latest_price_and_mapping()
            
            # 2. OpenInterest の遅延更新
            self._update_delayed_open_interest()
            
            # 3. データ品質確認
            self._quality_check()
            
            logger.info("=== 日次更新完了 ===")
            return True
            
        except Exception as e:
            logger.error(f"日次更新中にエラーが発生: {e}")
            return False
            
        finally:
            self.bloomberg.disconnect()
            self.db_manager.disconnect()
            
    def _update_latest_price_and_mapping(self):
        """最新の価格データとマッピング更新"""
        logger.info("最新価格データ・マッピング更新開始...")
        
        today = datetime.now().date()
        
        # 1. 今日のマッピングを更新（実契約変更の可能性）
        self._update_today_mapping(today)
        
        # 2. 今日の価格データを取得・更新
        self._update_today_price_data(today)
        
    def _update_today_mapping(self, target_date):
        """今日のマッピング更新"""
        logger.info(f"マッピング更新: {target_date}")
        
        try:
            test_tickers = ['LP1 Comdty', 'LP2 Comdty', 'LP3 Comdty']
            mapping_fields = [
                'FUT_CUR_GEN_TICKER',
                'LAST_TRADEABLE_DT',
                'FUT_DLV_DT_LAST',
                'FUT_CONTRACT_DT',
                'FUT_CONT_SIZE',
                'FUT_TICK_SIZE'
            ]
            
            # 現在のマッピング情報を取得
            ref_data = self.bloomberg.get_reference_data(test_tickers, mapping_fields)
            
            if ref_data.empty:
                logger.warning("マッピングデータ取得失敗")
                return
                
            # マッピングデータ処理
            self._process_mapping_update(target_date, ref_data)
            
        except Exception as e:
            logger.error(f"マッピング更新エラー ({target_date}): {e}")
            
    def _process_mapping_update(self, target_date, ref_data):
        """マッピングデータ処理・更新"""
        try:
            # ジェネリック先物情報を取得
            with self.db_manager.get_connection() as conn:
                query = """
                    SELECT GenericID, GenericTicker, MetalID, ExchangeCode
                    FROM M_GenericFutures 
                    WHERE GenericTicker IN ('LP1 Comdty', 'LP2 Comdty', 'LP3 Comdty')
                    ORDER BY GenericNumber
                """
                generic_info = pd.read_sql(query, conn)
                
            for _, row in ref_data.iterrows():
                security = row['security']
                current_generic = row.get('FUT_CUR_GEN_TICKER')
                
                if pd.isna(current_generic):
                    continue
                    
                logger.info(f"{target_date}: {security} -> {current_generic}")
                
                # ジェネリック情報を取得
                generic_row = generic_info[generic_info['GenericTicker'] == security]
                if len(generic_row) == 0:
                    continue
                    
                generic_row = generic_row.iloc[0]
                
                # 実契約を取得または作成
                actual_contract_id = self._get_or_create_actual_contract(
                    current_generic, generic_row, row
                )
                
                if actual_contract_id:
                    # マッピングを更新または作成
                    self._upsert_mapping(
                        target_date, 
                        int(generic_row['GenericID']), 
                        actual_contract_id, 
                        row
                    )
                    
        except Exception as e:
            logger.error(f"マッピング処理エラー ({target_date}): {e}")
            
    def _get_or_create_actual_contract(self, contract_ticker, generic_info, row):
        """実契約を取得または作成"""
        try:
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
                    
                # 新規作成処理（前回のコードと同じ）
                contract_date = row.get('FUT_CONTRACT_DT')
                contract_month = None
                contract_year = None
                contract_month_code = None
                
                if not pd.isna(contract_date):
                    try:
                        if isinstance(contract_date, str):
                            contract_dt = pd.to_datetime(contract_date)
                        else:
                            contract_dt = contract_date
                        contract_month = contract_dt.replace(day=1).date()
                        contract_year = contract_dt.year
                        
                        month_codes = ['F', 'G', 'H', 'J', 'K', 'M', 'N', 'Q', 'U', 'V', 'X', 'Z']
                        contract_month_code = month_codes[contract_dt.month - 1]
                        
                    except Exception as e:
                        logger.warning(f"契約月解析失敗: {contract_date}")
                
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
                    row.get('LAST_TRADEABLE_DT'),
                    row.get('FUT_DLV_DT_LAST'),
                    float(row.get('FUT_CONT_SIZE')) if not pd.isna(row.get('FUT_CONT_SIZE')) else None,
                    float(row.get('FUT_TICK_SIZE')) if not pd.isna(row.get('FUT_TICK_SIZE')) else None
                ))
                
                cursor.execute("SELECT @@IDENTITY")
                actual_contract_id = cursor.fetchone()[0]
                conn.commit()
                
                logger.info(f"新規実契約作成: {contract_ticker} (ID: {actual_contract_id})")
                return actual_contract_id
                
        except Exception as e:
            logger.error(f"実契約作成エラー ({contract_ticker}): {e}")
            return None
            
    def _upsert_mapping(self, target_date, generic_id, actual_contract_id, row):
        """マッピングをUPSERT"""
        try:
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()
                
                # 残存日数計算
                days_to_expiry = None
                last_tradeable = row.get('LAST_TRADEABLE_DT')
                if not pd.isna(last_tradeable):
                    try:
                        if isinstance(last_tradeable, str):
                            last_tradeable_dt = pd.to_datetime(last_tradeable).date()
                        else:
                            last_tradeable_dt = last_tradeable
                        days_to_expiry = (last_tradeable_dt - target_date).days
                    except Exception:
                        pass
                
                # 既存チェック
                cursor.execute("""
                    SELECT MappingID FROM T_GenericContractMapping 
                    WHERE TradeDate = ? AND GenericID = ?
                """, (target_date, generic_id))
                existing = cursor.fetchone()
                
                if existing:
                    # 更新
                    cursor.execute("""
                        UPDATE T_GenericContractMapping 
                        SET ActualContractID = ?, DaysToExpiry = ?, CreatedAt = ?
                        WHERE TradeDate = ? AND GenericID = ?
                    """, (actual_contract_id, days_to_expiry, datetime.now(), target_date, generic_id))
                    logger.info(f"マッピング更新: {target_date} - ジェネリックID {generic_id} -> 実契約ID {actual_contract_id}")
                else:
                    # 新規作成
                    cursor.execute("""
                        INSERT INTO T_GenericContractMapping (
                            TradeDate, GenericID, ActualContractID, DaysToExpiry
                        ) VALUES (?, ?, ?, ?)
                    """, (target_date, generic_id, actual_contract_id, days_to_expiry))
                    logger.info(f"マッピング作成: {target_date} - ジェネリックID {generic_id} -> 実契約ID {actual_contract_id}")
                
                conn.commit()
                
        except Exception as e:
            logger.error(f"マッピングUPSERTエラー: {e}")
            
    def _update_today_price_data(self, target_date):
        """今日の価格データ更新"""
        logger.info(f"価格データ更新: {target_date}")
        
        try:
            test_tickers = ['LP1 Comdty', 'LP2 Comdty', 'LP3 Comdty']
            price_fields = [
                'PX_LAST', 'PX_OPEN', 'PX_HIGH', 'PX_LOW', 'PX_VOLUME', 'OPEN_INT'
            ]
            
            # 今日の価格データ取得
            date_str = target_date.strftime('%Y%m%d')
            price_data = self.bloomberg.get_historical_data(
                test_tickers, price_fields, date_str, date_str
            )
            
            if price_data.empty:
                logger.warning(f"価格データ取得失敗: {target_date}")
                return
                
            # 価格データ処理
            self._process_price_data_update(price_data, target_date)
            
        except Exception as e:
            logger.error(f"価格データ更新エラー ({target_date}): {e}")
            
    def _process_price_data_update(self, price_data, target_date):
        """価格データ処理・更新"""
        try:
            # マッピング情報を取得
            with self.db_manager.get_connection() as conn:
                query = """
                    SELECT g.GenericID, g.GenericTicker, g.MetalID, m.ActualContractID
                    FROM M_GenericFutures g
                    JOIN T_GenericContractMapping m ON m.GenericID = g.GenericID
                    WHERE m.TradeDate = ?
                        AND g.GenericTicker IN ('LP1 Comdty', 'LP2 Comdty', 'LP3 Comdty')
                """
                mapping_info = pd.read_sql(query, conn, params=[target_date])
                
            mapping_dict = {}
            for _, row in mapping_info.iterrows():
                mapping_dict[row['GenericTicker']] = {
                    'GenericID': row['GenericID'],
                    'MetalID': row['MetalID'],
                    'ActualContractID': row['ActualContractID']
                }
                
            # 価格データUPSERT
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()
                
                for _, row in price_data.iterrows():
                    security = row['security']
                    trade_date = pd.to_datetime(row['date']).date()
                    
                    if security not in mapping_dict:
                        continue
                        
                    mapping = mapping_dict[security]
                    
                    # 既存チェック
                    cursor.execute("""
                        SELECT PriceID FROM T_CommodityPrice_V2 
                        WHERE TradeDate = ? AND GenericID = ? AND DataType = 'Generic'
                    """, (trade_date, mapping['GenericID']))
                    existing = cursor.fetchone()
                    
                    if existing:
                        # 更新（OpenInterestは遅延対応のため特別処理）
                        # OpenInterestがNULL/0でない場合のみ更新
                        open_interest = self._safe_int(row.get('OPEN_INT'))
                        
                        if open_interest is not None and open_interest > 0:
                            # 完全更新
                            cursor.execute("""
                                UPDATE T_CommodityPrice_V2 
                                SET SettlementPrice = ?, OpenPrice = ?, HighPrice = ?, 
                                    LowPrice = ?, LastPrice = ?, Volume = ?, OpenInterest = ?,
                                    LastUpdated = ?
                                WHERE TradeDate = ? AND GenericID = ? AND DataType = 'Generic'
                            """, (
                                self._safe_float(row.get('PX_LAST')),
                                self._safe_float(row.get('PX_OPEN')),
                                self._safe_float(row.get('PX_HIGH')),
                                self._safe_float(row.get('PX_LOW')),
                                self._safe_float(row.get('PX_LAST')),
                                self._safe_int(row.get('PX_VOLUME')),
                                open_interest,
                                datetime.now(),
                                trade_date, mapping['GenericID']
                            ))
                        else:
                            # OpenInterest以外を更新
                            cursor.execute("""
                                UPDATE T_CommodityPrice_V2 
                                SET SettlementPrice = ?, OpenPrice = ?, HighPrice = ?, 
                                    LowPrice = ?, LastPrice = ?, Volume = ?,
                                    LastUpdated = ?
                                WHERE TradeDate = ? AND GenericID = ? AND DataType = 'Generic'
                            """, (
                                self._safe_float(row.get('PX_LAST')),
                                self._safe_float(row.get('PX_OPEN')),
                                self._safe_float(row.get('PX_HIGH')),
                                self._safe_float(row.get('PX_LOW')),
                                self._safe_float(row.get('PX_LAST')),
                                self._safe_int(row.get('PX_VOLUME')),
                                datetime.now(),
                                trade_date, mapping['GenericID']
                            ))
                        
                        logger.info(f"価格データ更新: {security} - {trade_date} (OI: {open_interest})")
                    else:
                        # 新規挿入
                        cursor.execute("""
                            INSERT INTO T_CommodityPrice_V2 (
                                TradeDate, MetalID, DataType, GenericID, ActualContractID,
                                SettlementPrice, OpenPrice, HighPrice, LowPrice, LastPrice,
                                Volume, OpenInterest
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (
                            trade_date,
                            int(mapping['MetalID']),
                            'Generic',
                            int(mapping['GenericID']),
                            int(mapping['ActualContractID']),
                            self._safe_float(row.get('PX_LAST')),
                            self._safe_float(row.get('PX_OPEN')),
                            self._safe_float(row.get('PX_HIGH')),
                            self._safe_float(row.get('PX_LOW')),
                            self._safe_float(row.get('PX_LAST')),
                            self._safe_int(row.get('PX_VOLUME')),
                            self._safe_int(row.get('OPEN_INT'))
                        ))
                        
                        logger.info(f"価格データ作成: {security} - {trade_date}")
                
                conn.commit()
                
        except Exception as e:
            logger.error(f"価格データ処理エラー ({target_date}): {e}")
            
    def _update_delayed_open_interest(self):
        """OpenInterestの遅延更新"""
        logger.info("OpenInterest遅延更新開始...")
        
        try:
            # 昨日の日付を取得
            yesterday = datetime.now().date() - timedelta(days=1)
            
            # 昨日が営業日かチェック（簡易版）
            if yesterday.weekday() >= 5:  # 土日の場合
                logger.info("昨日は非営業日のためOpenInterest更新をスキップ")
                return
                
            # 昨日のOpenInterestを再取得
            self._fetch_delayed_open_interest(yesterday)
            
        except Exception as e:
            logger.error(f"OpenInterest遅延更新エラー: {e}")
            
    def _fetch_delayed_open_interest(self, target_date):
        """特定日のOpenInterest再取得"""
        logger.info(f"OpenInterest再取得: {target_date}")
        
        try:
            test_tickers = ['LP1 Comdty', 'LP2 Comdty', 'LP3 Comdty']
            
            # OpenInterestのみを取得
            date_str = target_date.strftime('%Y%m%d')
            oi_data = self.bloomberg.get_historical_data(
                test_tickers, ['OPEN_INT'], date_str, date_str
            )
            
            if oi_data.empty:
                logger.warning(f"OpenInterest取得失敗: {target_date}")
                return
                
            # OpenInterest更新
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()
                
                for _, row in oi_data.iterrows():
                    security = row['security']
                    open_interest = self._safe_int(row.get('OPEN_INT'))
                    
                    if open_interest is not None and open_interest > 0:
                        # ジェネリックIDを取得
                        cursor.execute("""
                            SELECT g.GenericID FROM M_GenericFutures g
                            WHERE g.GenericTicker = ?
                        """, (security,))
                        result = cursor.fetchone()
                        
                        if result:
                            generic_id = result[0]
                            
                            # OpenInterestのみを更新
                            cursor.execute("""
                                UPDATE T_CommodityPrice_V2 
                                SET OpenInterest = ?, LastUpdated = ?
                                WHERE TradeDate = ? AND GenericID = ? AND DataType = 'Generic'
                            """, (open_interest, datetime.now(), target_date, generic_id))
                            
                            if cursor.rowcount > 0:
                                logger.info(f"OpenInterest更新: {security} - {target_date} -> {open_interest}")
                
                conn.commit()
                
        except Exception as e:
            logger.error(f"OpenInterest再取得エラー ({target_date}): {e}")
            
    def _quality_check(self):
        """データ品質確認"""
        logger.info("データ品質確認開始...")
        
        try:
            with self.db_manager.get_connection() as conn:
                # 最新3日間のデータ品質を確認
                quality_query = """
                    SELECT 
                        p.TradeDate,
                        g.GenericTicker,
                        a.ContractTicker,
                        CASE WHEN p.SettlementPrice IS NULL THEN 'NG' ELSE 'OK' END as 価格,
                        CASE WHEN p.Volume IS NULL THEN 'NG' ELSE 'OK' END as 出来高,
                        CASE WHEN p.OpenInterest IS NULL THEN 'PENDING' ELSE 'OK' END as 建玉,
                        m.DaysToExpiry as 残存日数
                    FROM T_CommodityPrice_V2 p
                    JOIN M_GenericFutures g ON p.GenericID = g.GenericID
                    JOIN M_ActualContract a ON p.ActualContractID = a.ActualContractID
                    JOIN T_GenericContractMapping m ON m.GenericID = g.GenericID 
                        AND m.TradeDate = p.TradeDate
                    WHERE p.DataType = 'Generic'
                        AND p.TradeDate >= DATEADD(day, -3, GETDATE())
                    ORDER BY p.TradeDate DESC, g.GenericNumber
                """
                
                quality_df = pd.read_sql(quality_query, conn)
                
                if not quality_df.empty:
                    print("\n=== データ品質確認結果 ===")
                    print(quality_df.to_string(index=False))
                    
                    # OpenInterest PENDINGの件数
                    pending_count = len(quality_df[quality_df['建玉'] == 'PENDING'])
                    if pending_count > 0:
                        logger.warning(f"OpenInterest未更新: {pending_count}件（翌日更新予定）")
                    else:
                        logger.info("全データが正常に更新されています")
                        
        except Exception as e:
            logger.error(f"データ品質確認エラー: {e}")
            
    def _safe_float(self, value):
        """安全なfloat変換"""
        if value is None or pd.isna(value):
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None
            
    def _safe_int(self, value):
        """安全なint変換"""
        if value is None or pd.isna(value):
            return None
        try:
            return int(float(value))
        except (ValueError, TypeError):
            return None

def main():
    """メイン実行関数"""
    logger.info("OpenInterest遅延対応日次更新開始")
    
    updater = DailyUpdateWithOIDelay()
    success = updater.run_daily_update()
    
    if success:
        print("\n" + "✅ " * 20)
        print("✅ 日次更新完了！")
        print("✅ OpenInterest遅延も考慮済み")
        print("✅ " * 20)
    else:
        logger.error("日次更新失敗")
        
    return success

if __name__ == "__main__":
    main()