"""
main.pyをT_CommodityPrice_V2対応に更新するための分析と準備
"""
import os
import shutil
from datetime import datetime
from config.logging_config import logger

def analyze_main_py():
    """main.pyの現在の実装を分析"""
    logger.info("=== main.py V2移行分析 ===")
    
    # 現在のmain.pyをバックアップ
    src_file = "src/main.py"
    backup_file = f"src/main_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.py"
    
    if os.path.exists(src_file):
        shutil.copy2(src_file, backup_file)
        logger.info(f"バックアップ作成: {backup_file}")
    
    # data_processor.pyも確認
    logger.info("\n【現在の実装】")
    logger.info("1. main.py → data_processor.py → database.py")
    logger.info("2. データフロー:")
    logger.info("   - process_commodity_prices() がT_CommodityPriceに保存")
    logger.info("   - TenorTypeIDとSpecificTenorDateを使用（旧設計）")
    
    logger.info("\n【V2への変更点】")
    logger.info("1. T_CommodityPrice → T_CommodityPrice_V2")
    logger.info("2. TenorTypeID → GenericID")
    logger.info("3. SpecificTenorDate → 削除（GenericIDで管理）")
    logger.info("4. M_TenorType → M_GenericFutures")
    
    logger.info("\n【影響範囲】")
    logger.info("- data_processor.py: process_commodity_prices()の変更")
    logger.info("- database.py: save_commodity_prices()の変更")
    logger.info("- テナーマッピングロジックの変更")
    
    logger.info("\n【推奨アプローチ】")
    logger.info("1. 新しいdata_processor_v2.pyを作成")
    logger.info("2. 並行稼働でテスト")
    logger.info("3. 問題なければmain.pyを切り替え")
    
    return backup_file

def create_v2_compatible_processor():
    """V2互換のデータプロセッサーを作成"""
    logger.info("\n=== V2互換プロセッサー作成 ===")
    
    v2_processor_code = '''"""
T_CommodityPrice_V2対応のデータプロセッサー
"""
import pandas as pd
from typing import Dict, List, Optional
from datetime import datetime, date
from config.logging_config import logger


class DataProcessorV2:
    """T_CommodityPrice_V2用のデータプロセッサー"""
    
    def __init__(self, db_manager):
        self.db_manager = db_manager
        
    def process_commodity_prices(self, df: pd.DataFrame, ticker_info: Dict) -> pd.DataFrame:
        """
        商品価格データを処理（V2版）
        
        Args:
            df: Bloombergから取得した生データ
            ticker_info: ティッカー設定情報
            
        Returns:
            pd.DataFrame: 処理済みデータ（T_CommodityPrice_V2形式）
        """
        if df.empty:
            return pd.DataFrame()
            
        processed_data = []
        
        for _, row in df.iterrows():
            try:
                # 基本情報の取得
                security = row['security']
                trade_date = pd.to_datetime(row['date']).date()
                
                # GenericIDの取得（M_GenericFuturesから）
                generic_id = self.db_manager.get_generic_id(security)
                if not generic_id:
                    logger.warning(f"GenericID not found for {security}")
                    continue
                
                # MetalIDの取得
                metal_id = self.db_manager.get_metal_id_for_generic(generic_id)
                
                # 価格データの処理
                record = {
                    'TradeDate': trade_date,
                    'MetalID': metal_id,
                    'DataType': 'Generic',  # 汎用先物
                    'GenericID': generic_id,
                    'ActualContractID': None,  # 汎用先物なのでNULL
                    'SettlementPrice': self._get_price_value(row, 'PX_SETTLE'),
                    'OpenPrice': self._get_price_value(row, 'PX_OPEN'),
                    'HighPrice': self._get_price_value(row, 'PX_HIGH'),
                    'LowPrice': self._get_price_value(row, 'PX_LOW'),
                    'LastPrice': self._get_price_value(row, 'PX_LAST'),
                    'Volume': self._get_numeric_value(row, 'PX_VOLUME'),
                    'OpenInterest': self._get_numeric_value(row, 'OPEN_INT'),
                    'LastUpdated': datetime.now()
                }
                
                processed_data.append(record)
                
            except Exception as e:
                logger.error(f"Error processing row for {security}: {e}")
                continue
                
        return pd.DataFrame(processed_data)
    
    def _get_price_value(self, row: pd.Series, field: str) -> Optional[float]:
        """価格値を取得（NULL処理付き）"""
        value = row.get(field)
        if pd.isna(value) or value == '#N/A N/A':
            return None
        try:
            return float(value)
        except:
            return None
            
    def _get_numeric_value(self, row: pd.Series, field: str) -> Optional[int]:
        """数値を取得（NULL処理付き）"""
        value = row.get(field)
        if pd.isna(value) or value == '#N/A N/A':
            return None
        try:
            return int(float(value))
        except:
            return None
'''
    
    # ファイル作成
    with open('src/data_processor_v2.py', 'w', encoding='utf-8') as f:
        f.write(v2_processor_code)
    
    logger.info("✓ src/data_processor_v2.py を作成しました")
    
def create_database_v2_methods():
    """DatabaseManagerにV2用メソッドを追加"""
    logger.info("\n=== DatabaseManager V2メソッド追加 ===")
    
    v2_methods = '''
# 以下のメソッドをDatabaseManagerクラスに追加

def get_generic_id(self, ticker: str) -> Optional[int]:
    """ティッカーからGenericIDを取得"""
    if ticker not in self._generic_cache:
        query = """
        SELECT GenericID 
        FROM M_GenericFutures 
        WHERE GenericTicker = ?
        """
        result = self.execute_query(query, (ticker,))
        if result:
            self._generic_cache[ticker] = result[0][0]
        else:
            return None
    return self._generic_cache.get(ticker)

def get_metal_id_for_generic(self, generic_id: int) -> int:
    """GenericIDからMetalIDを取得"""
    query = """
    SELECT MetalID 
    FROM M_GenericFutures 
    WHERE GenericID = ?
    """
    result = self.execute_query(query, (generic_id,))
    return result[0][0] if result else None

def save_commodity_prices_v2(self, df: pd.DataFrame) -> int:
    """商品価格データを保存（T_CommodityPrice_V2用）"""
    if df.empty:
        return 0
        
    merge_query = """
    MERGE T_CommodityPrice_V2 AS target
    USING (
        SELECT ? as TradeDate, ? as MetalID, ? as DataType, 
               ? as GenericID, ? as ActualContractID
    ) AS source
    ON target.TradeDate = source.TradeDate
        AND target.MetalID = source.MetalID
        AND target.DataType = source.DataType
        AND (target.GenericID = source.GenericID OR 
             (target.GenericID IS NULL AND source.GenericID IS NULL))
        AND (target.ActualContractID = source.ActualContractID OR 
             (target.ActualContractID IS NULL AND source.ActualContractID IS NULL))
    WHEN MATCHED THEN
        UPDATE SET 
            SettlementPrice = ?,
            OpenPrice = ?,
            HighPrice = ?,
            LowPrice = ?,
            LastPrice = ?,
            Volume = ?,
            OpenInterest = ?,
            LastUpdated = ?
    WHEN NOT MATCHED THEN
        INSERT (TradeDate, MetalID, DataType, GenericID, ActualContractID,
                SettlementPrice, OpenPrice, HighPrice, LowPrice, LastPrice,
                Volume, OpenInterest, LastUpdated)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    """
    
    count = 0
    for _, row in df.iterrows():
        try:
            params = (
                # USING句のパラメータ
                row['TradeDate'], row['MetalID'], row['DataType'], 
                row['GenericID'], row['ActualContractID'],
                # UPDATE句のパラメータ
                row['SettlementPrice'], row['OpenPrice'], row['HighPrice'],
                row['LowPrice'], row['LastPrice'], row['Volume'],
                row['OpenInterest'], row['LastUpdated'],
                # INSERT句のパラメータ
                row['TradeDate'], row['MetalID'], row['DataType'], 
                row['GenericID'], row['ActualContractID'],
                row['SettlementPrice'], row['OpenPrice'], row['HighPrice'],
                row['LowPrice'], row['LastPrice'], row['Volume'],
                row['OpenInterest'], row['LastUpdated']
            )
            
            self.execute_query(merge_query, params)
            count += 1
            
        except Exception as e:
            logger.error(f"Error saving price data: {e}")
            continue
            
    self.commit()
    return count
'''
    
    # メソッド定義をファイルに保存
    with open('database_v2_methods.txt', 'w', encoding='utf-8') as f:
        f.write(v2_methods)
    
    logger.info("✓ database_v2_methods.txt を作成しました")
    logger.info("  → DatabaseManagerクラスに手動で追加してください")

def main():
    """メイン処理"""
    logger.info(f"実行開始: {datetime.now()}")
    
    # 1. 現状分析
    backup_file = analyze_main_py()
    
    # 2. V2互換プロセッサー作成
    create_v2_compatible_processor()
    
    # 3. DatabaseManagerメソッド準備
    create_database_v2_methods()
    
    logger.info("\n【次のステップ】")
    logger.info("1. database.pyにV2メソッドを追加")
    logger.info("2. main.pyでDataProcessorV2を使用するようにテスト")
    logger.info("3. 並行稼働で動作確認")
    logger.info("4. 問題なければ完全移行")
    
    logger.info(f"\nバックアップ: {backup_file}")

if __name__ == "__main__":
    main()