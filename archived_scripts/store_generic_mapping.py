"""
Phase 2完了版: ジェネリック先物マッピングデータのデータベース格納
LP1-LP3のマッピングデータを M_ActualContract と T_GenericContractMapping に格納
"""
import sys
import os
from datetime import datetime, date
import pandas as pd

# プロジェクトルートとsrcディレクトリをPythonパスに追加
project_root = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.join(project_root, 'src')
sys.path.insert(0, project_root)
sys.path.insert(0, src_dir)

from bloomberg_api import BloombergDataFetcher
from database import DatabaseManager
from config.logging_config import logger

class GenericMappingStore:
    """ジェネリック先物マッピングデータ格納クラス"""
    
    def __init__(self):
        self.bloomberg = BloombergDataFetcher()
        self.db_manager = DatabaseManager()
        
    def store_mapping_data(self, test_mode=True):
        """マッピングデータの取得と格納"""
        logger.info("=== ジェネリック先物マッピングデータ格納開始 ===")
        
        try:
            # Bloomberg API接続
            if not self.bloomberg.connect():
                raise ConnectionError("Bloomberg API接続に失敗しました")
                
            # データベース接続
            self.db_manager.connect()
            
            # テスト対象のジェネリック先物（Phase 2ではLP1-LP3のみ）
            test_tickers = ['LP1 Comdty', 'LP2 Comdty', 'LP3 Comdty']
            
            # 取得フィールド
            fields = [
                'FUT_CUR_GEN_TICKER',     # 現在のジェネリック契約
                'LAST_TRADEABLE_DT',      # 最終取引日
                'FUT_DLV_DT_LAST',        # 最終引渡日
                'FUT_CONTRACT_DT',        # 契約月
                'FUT_CONT_SIZE',          # 契約サイズ
                'FUT_TICK_SIZE'           # ティックサイズ
            ]
            
            logger.info(f"処理対象: {test_tickers}")
            
            # リファレンスデータ取得
            ref_data = self.bloomberg.get_reference_data(test_tickers, fields)
            
            if ref_data.empty:
                logger.error("リファレンスデータが取得できませんでした")
                return False
                
            # データベースから既存のジェネリック先物情報を取得
            generic_futures = self._get_generic_futures_info(test_tickers)
            
            # データ処理とデータベース格納
            success_count = self._process_and_store_data(ref_data, generic_futures)
            
            logger.info(f"=== 格納完了: {success_count}件 ===")
            return True
            
        except Exception as e:
            logger.error(f"格納処理中にエラーが発生: {e}")
            return False
            
        finally:
            self.bloomberg.disconnect()
            self.db_manager.disconnect()
            
    def _get_generic_futures_info(self, tickers):
        """データベースからジェネリック先物情報を取得"""
        with self.db_manager.get_connection() as conn:
            placeholders = ','.join(['?' for _ in tickers])
            query = f"""
                SELECT GenericID, GenericTicker, MetalID, ExchangeCode, GenericNumber
                FROM M_GenericFutures 
                WHERE GenericTicker IN ({placeholders})
                ORDER BY GenericNumber
            """
            df = pd.read_sql(query, conn, params=tickers)
            logger.info(f"データベースから取得したジェネリック先物: {len(df)}件")
            return df
            
    def _process_and_store_data(self, ref_data, generic_futures):
        """マッピングデータの処理とデータベース格納"""
        today = date.today()
        success_count = 0
        
        for _, row in ref_data.iterrows():
            try:
                security = row['security']
                logger.info(f"処理中: {security}")
                
                # ジェネリック先物情報を取得
                generic_info = generic_futures[
                    generic_futures['GenericTicker'] == security
                ]
                
                if len(generic_info) == 0:
                    logger.warning(f"ジェネリック先物情報が見つかりません: {security}")
                    continue
                    
                generic_info = generic_info.iloc[0]
                
                # 現在のジェネリック契約を取得
                current_generic = row.get('FUT_CUR_GEN_TICKER')
                if pd.isna(current_generic):
                    logger.warning(f"FUT_CUR_GEN_TICKERが取得できません: {security}")
                    continue
                    
                logger.info(f"{security} -> 現在の実契約: {current_generic}")
                
                # 実契約をM_ActualContractに格納（または更新）
                actual_contract_id = self._store_actual_contract(
                    current_generic, generic_info, row
                )
                
                if actual_contract_id:
                    # ジェネリック・実契約マッピングを格納
                    self._store_mapping(
                        today, generic_info['GenericID'], actual_contract_id, row
                    )
                    success_count += 1
                    
            except Exception as e:
                logger.error(f"データ処理エラー ({security}): {e}")
                continue
                
        return success_count
        
    def _store_actual_contract(self, contract_ticker, generic_info, row):
        """実契約データをM_ActualContractに格納"""
        try:
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()
                
                # 契約月の解析（YYYY-MM-DD形式への変換）
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
                        
                        # 月コードの生成（F=1月, G=2月, ..., Z=12月）
                        month_codes = ['F', 'G', 'H', 'J', 'K', 'M', 'N', 'Q', 'U', 'V', 'X', 'Z']
                        contract_month_code = month_codes[contract_dt.month - 1]
                        
                    except Exception as e:
                        logger.warning(f"契約月の解析に失敗: {contract_date}, エラー: {e}")
                
                # 既存チェック
                cursor.execute(
                    "SELECT ActualContractID FROM M_ActualContract WHERE ContractTicker = ?",
                    (contract_ticker,)
                )
                existing = cursor.fetchone()
                
                if existing:
                    actual_contract_id = existing[0]
                    logger.info(f"既存の実契約を使用: {contract_ticker} (ID: {actual_contract_id})")
                else:
                    # 新規挿入
                    cursor.execute("""
                        INSERT INTO M_ActualContract (
                            ContractTicker, MetalID, ExchangeCode, ContractMonth, 
                            ContractYear, ContractMonthCode, LastTradeableDate, 
                            DeliveryDate, ContractSize, TickSize
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        contract_ticker,
                        int(generic_info['MetalID']),  # numpy.int64 -> int変換
                        generic_info['ExchangeCode'],
                        contract_month,
                        contract_year,
                        contract_month_code,
                        row.get('LAST_TRADEABLE_DT'),
                        row.get('FUT_DLV_DT_LAST'),
                        float(row.get('FUT_CONT_SIZE')) if not pd.isna(row.get('FUT_CONT_SIZE')) else None,
                        float(row.get('FUT_TICK_SIZE')) if not pd.isna(row.get('FUT_TICK_SIZE')) else None
                    ))
                    
                    # 新規IDを取得
                    cursor.execute("SELECT @@IDENTITY")
                    actual_contract_id = cursor.fetchone()[0]
                    
                    conn.commit()
                    logger.info(f"新規実契約を作成: {contract_ticker} (ID: {actual_contract_id})")
                
                return actual_contract_id
                
        except Exception as e:
            logger.error(f"実契約格納エラー ({contract_ticker}): {e}")
            return None
            
    def _store_mapping(self, trade_date, generic_id, actual_contract_id, row):
        """ジェネリック・実契約マッピングを格納"""
        try:
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()
                
                # 残存日数の計算
                days_to_expiry = None
                last_tradeable = row.get('LAST_TRADEABLE_DT')
                if not pd.isna(last_tradeable):
                    try:
                        if isinstance(last_tradeable, str):
                            last_tradeable_dt = pd.to_datetime(last_tradeable).date()
                        else:
                            last_tradeable_dt = last_tradeable
                        days_to_expiry = (last_tradeable_dt - trade_date).days
                    except Exception as e:
                        logger.warning(f"残存日数計算エラー: {e}")
                
                # 既存チェック（同じ日付・ジェネリックの組み合わせ）
                cursor.execute("""
                    SELECT MappingID FROM T_GenericContractMapping 
                    WHERE TradeDate = ? AND GenericID = ?
                """, (trade_date, int(generic_id)))  # numpy.int64 -> int変換
                existing = cursor.fetchone()
                
                if existing:
                    # 更新
                    cursor.execute("""
                        UPDATE T_GenericContractMapping 
                        SET ActualContractID = ?, DaysToExpiry = ?, CreatedAt = ?
                        WHERE TradeDate = ? AND GenericID = ?
                    """, (int(actual_contract_id), days_to_expiry, datetime.now(), trade_date, int(generic_id)))
                    logger.info(f"マッピング更新: ジェネリックID {generic_id} -> 実契約ID {actual_contract_id}")
                else:
                    # 新規挿入
                    cursor.execute("""
                        INSERT INTO T_GenericContractMapping (
                            TradeDate, GenericID, ActualContractID, DaysToExpiry
                        ) VALUES (?, ?, ?, ?)
                    """, (trade_date, int(generic_id), int(actual_contract_id), days_to_expiry))
                    logger.info(f"マッピング作成: ジェネリックID {generic_id} -> 実契約ID {actual_contract_id}")
                
                conn.commit()
                
        except Exception as e:
            logger.error(f"マッピング格納エラー (ジェネリックID: {generic_id}): {e}")

def main():
    """メイン実行関数"""
    logger.info("ジェネリック先物マッピングデータ格納開始")
    
    store = GenericMappingStore()
    success = store.store_mapping_data()
    
    if success:
        logger.info("データ格納正常完了")
        
        # 格納結果の確認
        print("\n=== 格納結果確認 ===")
        with store.db_manager.get_connection() as conn:
            # 実契約テーブル確認
            actual_df = pd.read_sql("""
                SELECT ContractTicker, ContractMonth, LastTradeableDate, ContractSize
                FROM M_ActualContract 
                ORDER BY ContractMonth
            """, conn)
            print("\n【M_ActualContract】")
            print(actual_df.to_string())
            
            # マッピングテーブル確認
            mapping_df = pd.read_sql("""
                SELECT m.TradeDate, g.GenericTicker, a.ContractTicker, m.DaysToExpiry
                FROM T_GenericContractMapping m
                JOIN M_GenericFutures g ON m.GenericID = g.GenericID
                JOIN M_ActualContract a ON m.ActualContractID = a.ActualContractID
                ORDER BY g.GenericNumber
            """, conn)
            print("\n【T_GenericContractMapping】")
            print(mapping_df.to_string())
    else:
        logger.error("データ格納失敗")
        
    return success

if __name__ == "__main__":
    main()