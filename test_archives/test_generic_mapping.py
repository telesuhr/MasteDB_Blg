"""
Phase 2: ジェネリック先物マッピングデータ取得テスト
LP1-LP3のみで小規模テスト実行
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

class GenericMappingTester:
    """ジェネリック先物マッピングのテストクラス"""
    
    def __init__(self):
        self.bloomberg = BloombergDataFetcher()
        self.db_manager = DatabaseManager()
        
    def test_generic_mapping(self):
        """LP1-LP3のマッピングテスト"""
        logger.info("=== ジェネリック先物マッピングテスト開始 ===")
        
        try:
            # Bloomberg API接続
            if not self.bloomberg.connect():
                raise ConnectionError("Bloomberg API接続に失敗しました")
                
            # データベース接続
            self.db_manager.connect()
            
            # テスト対象のジェネリック先物
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
            
            logger.info(f"テスト対象: {test_tickers}")
            logger.info(f"取得フィールド: {fields}")
            
            # リファレンスデータ取得（現在の属性）
            ref_data = self.bloomberg.get_reference_data(test_tickers, fields)
            
            if ref_data.empty:
                logger.error("リファレンスデータが取得できませんでした")
                return False
                
            logger.info("=== 取得データ ===")
            print(ref_data.to_string())
            
            # データベースから既存のジェネリック先物情報を取得
            generic_futures = self._get_generic_futures_info()
            
            # データ処理とデータベース格納
            self._process_and_store_mapping_data(ref_data, generic_futures)
            
            logger.info("=== テスト完了 ===")
            return True
            
        except Exception as e:
            logger.error(f"テスト中にエラーが発生: {e}")
            return False
            
        finally:
            self.bloomberg.disconnect()
            self.db_manager.disconnect()
            
    def _get_generic_futures_info(self):
        """データベースからジェネリック先物情報を取得"""
        with self.db_manager.get_connection() as conn:
            query = """
                SELECT GenericID, GenericTicker, MetalID, ExchangeCode, GenericNumber
                FROM M_GenericFutures 
                WHERE GenericTicker IN ('LP1 Comdty', 'LP2 Comdty', 'LP3 Comdty')
                ORDER BY GenericNumber
            """
            df = pd.read_sql(query, conn)
            logger.info(f"データベースから取得したジェネリック先物: {len(df)}件")
            return df
            
    def _process_and_store_mapping_data(self, ref_data, generic_futures):
        """マッピングデータの処理とデータベース格納"""
        today = date.today()
        
        for _, row in ref_data.iterrows():
            try:
                security = row['security']
                logger.info(f"処理中: {security}")
                
                # ジェネリック先物情報を取得
                generic_info = generic_futures[
                    generic_futures['GenericTicker'] == security
                ].iloc[0] if len(generic_futures[generic_futures['GenericTicker'] == security]) > 0 else None
                
                if generic_info is None:
                    logger.warning(f"ジェネリック先物情報が見つかりません: {security}")
                    continue
                    
                # 現在のジェネリック契約を取得
                current_generic = row.get('FUT_CUR_GEN_TICKER')
                if pd.isna(current_generic):
                    logger.warning(f"FUT_CUR_GEN_TICKERが取得できません: {security}")
                    continue
                    
                logger.info(f"{security} -> 現在の実契約: {current_generic}")
                
                # 実契約の属性データ
                contract_attributes = {
                    'ContractTicker': current_generic,
                    'MetalID': generic_info['MetalID'],
                    'ExchangeCode': generic_info['ExchangeCode'],
                    'LastTradeableDate': row.get('LAST_TRADEABLE_DT'),
                    'DeliveryDate': row.get('FUT_DLV_DT_LAST'),
                    'ContractMonth': row.get('FUT_CONTRACT_DT'),
                    'ContractSize': row.get('FUT_CONT_SIZE'),
                    'TickSize': row.get('FUT_TICK_SIZE')
                }
                
                logger.info(f"契約属性: {contract_attributes}")
                
                # ここでは表示のみ（Phase 2では実際のDB格納は次のステップ）
                logger.info(f"【マッピング情報】")
                logger.info(f"  ジェネリック: {security} (ID: {generic_info['GenericID']})")
                logger.info(f"  実契約: {current_generic}")
                logger.info(f"  最終取引日: {contract_attributes['LastTradeableDate']}")
                logger.info(f"  契約月: {contract_attributes['ContractMonth']}")
                
            except Exception as e:
                logger.error(f"データ処理エラー ({security}): {e}")
                continue

def main():
    """メイン実行関数"""
    logger.info("ジェネリック先物マッピングテスト開始")
    
    tester = GenericMappingTester()
    success = tester.test_generic_mapping()
    
    if success:
        logger.info("テスト正常完了")
    else:
        logger.error("テスト失敗")
        
    return success

if __name__ == "__main__":
    main()