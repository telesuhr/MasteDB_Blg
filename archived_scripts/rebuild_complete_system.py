"""
完全システム再構築: 1からBloomberg APIでデータ取得
全データ削除 → 日別マッピング取得 → 価格データ取得 → 実契約ID設定
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

class CompleteSystemRebuilder:
    """完全システム再構築クラス"""
    
    def __init__(self):
        self.bloomberg = BloombergDataFetcher()
        self.db_manager = DatabaseManager()
        
    def rebuild_complete_system(self, days_back=5):
        """完全システム再構築"""
        logger.info("=== 完全システム再構築開始 ===")
        
        try:
            # Bloomberg API接続
            if not self.bloomberg.connect():
                raise ConnectionError("Bloomberg API接続に失敗しました")
                
            # データベース接続
            self.db_manager.connect()
            
            # Step 1: 既存データクリーンアップ
            self._cleanup_existing_data()
            
            # Step 2: 対象期間の決定
            end_date = datetime.now().date()
            start_date = end_date - timedelta(days=days_back)
            business_dates = self._get_business_dates(start_date, end_date)
            
            logger.info(f"対象期間: {start_date} - {end_date}")
            logger.info(f"営業日: {business_dates}")
            
            # Step 3: 各営業日のマッピングデータ取得
            for business_date in business_dates:
                self._build_daily_mapping(business_date)
                
            # Step 4: 各営業日の価格データ取得
            for business_date in business_dates:
                self._fetch_daily_price_data(business_date)
                
            # Step 5: 統合確認
            self._verify_integration()
            
            logger.info("=== 完全システム再構築完了 ===")
            return True
            
        except Exception as e:
            logger.error(f"システム再構築中にエラーが発生: {e}")
            return False
            
        finally:
            self.bloomberg.disconnect()
            self.db_manager.disconnect()
            
    def _cleanup_existing_data(self):
        """既存データクリーンアップ"""
        logger.info("既存データクリーンアップ開始...")
        
        try:
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()
                
                # 外部キー制約のため、逆順で削除
                cleanup_queries = [
                    "DELETE FROM T_CommodityPrice_V2",
                    "DELETE FROM T_GenericContractMapping", 
                    "DELETE FROM M_ActualContract"
                ]
                
                for query in cleanup_queries:
                    cursor.execute(query)
                    deleted_count = cursor.rowcount
                    table_name = query.split()[-1]
                    logger.info(f"{table_name}: {deleted_count}件削除")
                
                conn.commit()
                logger.info("既存データクリーンアップ完了")
                
        except Exception as e:
            logger.error(f"クリーンアップエラー: {e}")
            
    def _get_business_dates(self, start_date, end_date):
        """営業日リストを生成"""
        business_dates = []
        current_date = start_date
        
        while current_date <= end_date:
            # 土日を除外（祝日は簡易的に除外しない）
            if current_date.weekday() < 5:  # 0=月曜, 6=日曜
                business_dates.append(current_date)
            current_date += timedelta(days=1)
            
        return business_dates
        
    def _build_daily_mapping(self, business_date):
        """特定日のマッピングデータ構築"""
        logger.info(f"マッピング構築: {business_date}")
        
        try:
            # ジェネリック先物リスト
            test_tickers = ['LP1 Comdty', 'LP2 Comdty', 'LP3 Comdty']
            
            # Bloomberg APIから現在の実契約を取得
            mapping_fields = [
                'FUT_CUR_GEN_TICKER',     # 現在の実契約
                'LAST_TRADEABLE_DT',      # 最終取引日
                'FUT_DLV_DT_LAST',        # 最終引渡日
                'FUT_CONTRACT_DT',        # 契約月
                'FUT_CONT_SIZE',          # 契約サイズ
                'FUT_TICK_SIZE'           # ティックサイズ
            ]
            
            # リファレンスデータ取得（現在時点）
            ref_data = self.bloomberg.get_reference_data(test_tickers, mapping_fields)
            
            if ref_data.empty:
                logger.warning(f"マッピングデータ取得失敗: {business_date}")
                return
                
            # データ処理と格納
            self._process_daily_mapping(business_date, ref_data)
            
        except Exception as e:
            logger.error(f"マッピング構築エラー ({business_date}): {e}")
            
    def _process_daily_mapping(self, business_date, ref_data):
        """日別マッピングデータ処理"""
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
                    logger.warning(f"実契約取得失敗: {security}")
                    continue
                    
                logger.info(f"{business_date}: {security} -> {current_generic}")
                
                # ジェネリック情報を取得
                generic_row = generic_info[generic_info['GenericTicker'] == security]
                if len(generic_row) == 0:
                    continue
                    
                generic_row = generic_row.iloc[0]
                
                # 実契約をM_ActualContractに格納
                actual_contract_id = self._create_actual_contract(current_generic, generic_row, row)
                
                if actual_contract_id:
                    # マッピングを作成
                    self._create_mapping(
                        business_date, 
                        int(generic_row['GenericID']), 
                        actual_contract_id, 
                        row
                    )
                    
        except Exception as e:
            logger.error(f"マッピング処理エラー ({business_date}): {e}")
            
    def _create_actual_contract(self, contract_ticker, generic_info, row):
        """実契約を作成または取得"""
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
                    
                # 契約月の解析
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
                        
                        # 月コード生成
                        month_codes = ['F', 'G', 'H', 'J', 'K', 'M', 'N', 'Q', 'U', 'V', 'X', 'Z']
                        contract_month_code = month_codes[contract_dt.month - 1]
                        
                    except Exception as e:
                        logger.warning(f"契約月解析失敗: {contract_date}, エラー: {e}")
                
                # 新規作成
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
                
                logger.info(f"実契約作成: {contract_ticker} (ID: {actual_contract_id})")
                return actual_contract_id
                
        except Exception as e:
            logger.error(f"実契約作成エラー ({contract_ticker}): {e}")
            return None
            
    def _create_mapping(self, business_date, generic_id, actual_contract_id, row):
        """マッピングを作成"""
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
                        days_to_expiry = (last_tradeable_dt - business_date).days
                    except Exception as e:
                        logger.warning(f"残存日数計算エラー: {e}")
                
                # マッピング作成
                cursor.execute("""
                    INSERT INTO T_GenericContractMapping (
                        TradeDate, GenericID, ActualContractID, DaysToExpiry
                    ) VALUES (?, ?, ?, ?)
                """, (business_date, generic_id, actual_contract_id, days_to_expiry))
                
                conn.commit()
                logger.info(f"マッピング作成: {business_date} - ジェネリックID {generic_id} -> 実契約ID {actual_contract_id}")
                
        except Exception as e:
            logger.error(f"マッピング作成エラー: {e}")
            
    def _fetch_daily_price_data(self, business_date):
        """特定日の価格データ取得"""
        logger.info(f"価格データ取得: {business_date}")
        
        try:
            test_tickers = ['LP1 Comdty', 'LP2 Comdty', 'LP3 Comdty']
            price_fields = [
                'PX_LAST', 'PX_OPEN', 'PX_HIGH', 'PX_LOW', 'PX_VOLUME', 'OPEN_INT'
            ]
            
            # 単日のヒストリカルデータ取得
            date_str = business_date.strftime('%Y%m%d')
            price_data = self.bloomberg.get_historical_data(
                test_tickers, price_fields, date_str, date_str
            )
            
            if price_data.empty:
                logger.warning(f"価格データ取得失敗: {business_date}")
                return
                
            # 価格データ処理と格納
            self._process_price_data(price_data, business_date)
            
        except Exception as e:
            logger.error(f"価格データ取得エラー ({business_date}): {e}")
            
    def _process_price_data(self, price_data, business_date):
        """価格データ処理と格納"""
        try:
            # ジェネリック・マッピング情報を取得
            with self.db_manager.get_connection() as conn:
                query = """
                    SELECT g.GenericID, g.GenericTicker, g.MetalID, m.ActualContractID
                    FROM M_GenericFutures g
                    JOIN T_GenericContractMapping m ON m.GenericID = g.GenericID
                    WHERE m.TradeDate = ?
                        AND g.GenericTicker IN ('LP1 Comdty', 'LP2 Comdty', 'LP3 Comdty')
                """
                mapping_info = pd.read_sql(query, conn, params=[business_date])
                
            mapping_dict = {}
            for _, row in mapping_info.iterrows():
                mapping_dict[row['GenericTicker']] = {
                    'GenericID': row['GenericID'],
                    'MetalID': row['MetalID'],
                    'ActualContractID': row['ActualContractID']
                }
                
            # 価格データ格納
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()
                
                for _, row in price_data.iterrows():
                    security = row['security']
                    trade_date = pd.to_datetime(row['date']).date()
                    
                    if security not in mapping_dict:
                        continue
                        
                    mapping = mapping_dict[security]
                    
                    # 価格レコード作成
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
                    
                    logger.info(f"価格データ格納: {security} - {trade_date}")
                
                conn.commit()
                
        except Exception as e:
            logger.error(f"価格データ処理エラー ({business_date}): {e}")
            
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
            
    def _verify_integration(self):
        """統合確認"""
        logger.info("統合確認開始...")
        
        try:
            with self.db_manager.get_connection() as conn:
                # 統合クエリテスト
                integration_query = """
                    SELECT 
                        p.TradeDate,
                        g.GenericTicker,
                        a.ContractTicker,
                        m.DaysToExpiry,
                        p.SettlementPrice,
                        p.Volume
                    FROM T_CommodityPrice_V2 p
                    JOIN M_GenericFutures g ON p.GenericID = g.GenericID
                    JOIN M_ActualContract a ON p.ActualContractID = a.ActualContractID
                    JOIN T_GenericContractMapping m ON m.GenericID = g.GenericID 
                        AND m.TradeDate = p.TradeDate
                    WHERE p.DataType = 'Generic'
                    ORDER BY p.TradeDate, g.GenericNumber
                """
                
                result_df = pd.read_sql(integration_query, conn)
                
                if not result_df.empty:
                    logger.info(f"統合確認成功: {len(result_df)}件のデータが正常に連携")
                    print("\n=== 統合確認結果 ===")
                    print(result_df.to_string(index=False))
                else:
                    logger.error("統合確認失敗: データが連携されていません")
                    
        except Exception as e:
            logger.error(f"統合確認エラー: {e}")

def main():
    """メイン実行関数"""
    logger.info("完全システム再構築開始")
    
    rebuilder = CompleteSystemRebuilder()
    success = rebuilder.rebuild_complete_system(days_back=5)
    
    if success:
        print("\n" + "🎉 " * 20)
        print("🎉 完全システム再構築成功！")
        print("🎉 ジェネリック・実契約・価格データが完全に連携されました！")
        print("🎉 " * 20)
    else:
        logger.error("完全システム再構築失敗")
        
    return success

if __name__ == "__main__":
    main()