"""
マルチ取引所対応のT_CommodityPrice_V2データロードプログラム
LME、SHFE、CMXの全銘柄データを取得して格納
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

class MultiExchangeDataLoader:
    """マルチ取引所データローダー"""
    
    def __init__(self):
        self.bloomberg = BloombergDataFetcher()
        self.db_manager = DatabaseManager()
        
    def load_all_exchanges_data(self, days_back=3):
        """全取引所のデータをロード"""
        logger.info("=== マルチ取引所データロード開始 ===")
        
        try:
            # Bloomberg API接続
            if not self.bloomberg.connect():
                raise ConnectionError("Bloomberg API接続に失敗しました")
                
            # データベース接続
            self.db_manager.connect()
            
            # 全取引所のジェネリック先物情報を取得
            generic_futures = self._get_all_generic_futures()
            logger.info(f"対象銘柄数: {len(generic_futures)}")
            
            # 取引所別にティッカーを整理
            exchange_tickers = {}
            for _, row in generic_futures.iterrows():
                exchange = row['ExchangeCode']
                if exchange not in exchange_tickers:
                    exchange_tickers[exchange] = []
                exchange_tickers[exchange].append(row['GenericTicker'])
            
            # 価格フィールド
            price_fields = [
                'PX_LAST',          # 終値
                'PX_OPEN',          # 始値
                'PX_HIGH',          # 高値
                'PX_LOW',           # 安値
                'PX_VOLUME',        # 出来高
                'OPEN_INT'          # 建玉残高
            ]
            
            # 日付範囲
            end_date = datetime.now().strftime('%Y%m%d')
            start_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y%m%d')
            
            logger.info(f"取得期間: {start_date} - {end_date}")
            
            total_success = 0
            
            # 取引所別にデータ取得・格納
            for exchange, tickers in exchange_tickers.items():
                display_name = 'COMEX' if exchange == 'CMX' else exchange
                logger.info(f"\n--- {display_name} 取引所処理開始 ---")
                logger.info(f"対象銘柄: {len(tickers)}本")
                
                # バッチ処理（Bloomberg APIの制限考慮）
                batch_size = 50
                for i in range(0, len(tickers), batch_size):
                    batch_tickers = tickers[i:i+batch_size]
                    
                    # ヒストリカルデータ取得
                    price_data = self.bloomberg.get_historical_data(
                        batch_tickers, price_fields, start_date, end_date
                    )
                    
                    if price_data.empty:
                        logger.warning(f"{display_name} バッチ {i//batch_size + 1}: データなし")
                        continue
                        
                    logger.info(f"{display_name} バッチ {i//batch_size + 1}: {len(price_data)}件取得")
                    
                    # データ処理と格納
                    success_count = self._process_and_store_price_data(
                        price_data, generic_futures, exchange
                    )
                    total_success += success_count
                
                logger.info(f"{display_name} 取引所処理完了")
            
            logger.info(f"\n=== 全取引所データロード完了: 合計{total_success}件 ===")
            
            # 結果確認
            self._verify_loaded_data()
            
            return True
            
        except Exception as e:
            logger.error(f"データロード中にエラーが発生: {e}")
            return False
            
        finally:
            self.bloomberg.disconnect()
            self.db_manager.disconnect()
            
    def _get_all_generic_futures(self):
        """全ジェネリック先物情報を取得"""
        with self.db_manager.get_connection() as conn:
            query = """
                SELECT GenericID, GenericTicker, MetalID, ExchangeCode, GenericNumber
                FROM M_GenericFutures 
                ORDER BY ExchangeCode, GenericNumber
            """
            df = pd.read_sql(query, conn)
            return df
            
    def _process_and_store_price_data(self, price_data, generic_futures, exchange):
        """価格データの処理と格納"""
        success_count = 0
        
        # ジェネリック先物情報をマップに変換
        generic_map = {}
        for _, row in generic_futures[generic_futures['ExchangeCode'] == exchange].iterrows():
            generic_map[row['GenericTicker']] = {
                'GenericID': row['GenericID'],
                'MetalID': row['MetalID'],
                'GenericNumber': row['GenericNumber']
            }
        
        for _, row in price_data.iterrows():
            try:
                security = row['security']
                trade_date = pd.to_datetime(row['date']).date()
                
                # ジェネリック先物情報を取得
                if security not in generic_map:
                    continue
                    
                generic_data = generic_map[security]
                
                # 実契約IDの取得（マッピングがあれば）
                actual_contract_id = self._get_actual_contract_id(
                    generic_data['GenericID'], trade_date
                )
                
                # 価格データの抽出と変換
                price_record = {
                    'TradeDate': trade_date,
                    'MetalID': int(generic_data['MetalID']),
                    'DataType': 'Generic',
                    'GenericID': int(generic_data['GenericID']),
                    'ActualContractID': actual_contract_id,
                    'SettlementPrice': self._safe_float(row.get('PX_LAST')),
                    'OpenPrice': self._safe_float(row.get('PX_OPEN')),
                    'HighPrice': self._safe_float(row.get('PX_HIGH')),
                    'LowPrice': self._safe_float(row.get('PX_LOW')),
                    'LastPrice': self._safe_float(row.get('PX_LAST')),
                    'Volume': self._safe_int(row.get('PX_VOLUME')),
                    'OpenInterest': self._safe_int(row.get('OPEN_INT'))
                }
                
                # データベースに格納
                if self._store_price_record(price_record):
                    success_count += 1
                
            except Exception as e:
                logger.error(f"価格データ処理エラー ({security}): {e}")
                continue
                
        return success_count
        
    def _get_actual_contract_id(self, generic_id, trade_date):
        """実契約IDを取得（存在する場合）"""
        try:
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT ActualContractID 
                    FROM T_GenericContractMapping
                    WHERE GenericID = ? AND TradeDate = ?
                """, (generic_id, trade_date))
                result = cursor.fetchone()
                return result[0] if result else None
        except:
            return None
            
    def _store_price_record(self, price_record):
        """価格レコードをデータベースに格納"""
        try:
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()
                
                # MERGE文でUPSERT実行
                cursor.execute("""
                    MERGE T_CommodityPrice_V2 AS target
                    USING (SELECT ? AS TradeDate, ? AS GenericID) AS source
                    ON target.TradeDate = source.TradeDate 
                        AND target.GenericID = source.GenericID 
                        AND target.DataType = 'Generic'
                    WHEN MATCHED THEN
                        UPDATE SET 
                            ActualContractID = ?,
                            SettlementPrice = ?, 
                            OpenPrice = ?, 
                            HighPrice = ?, 
                            LowPrice = ?, 
                            LastPrice = ?, 
                            Volume = ?, 
                            OpenInterest = ?,
                            LastUpdated = GETDATE()
                    WHEN NOT MATCHED THEN
                        INSERT (TradeDate, MetalID, DataType, GenericID, ActualContractID,
                                SettlementPrice, OpenPrice, HighPrice, LowPrice, LastPrice,
                                Volume, OpenInterest)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                """, (
                    # USING句のパラメータ
                    price_record['TradeDate'], price_record['GenericID'],
                    # UPDATE句のパラメータ
                    price_record['ActualContractID'],
                    price_record['SettlementPrice'], price_record['OpenPrice'],
                    price_record['HighPrice'], price_record['LowPrice'],
                    price_record['LastPrice'], price_record['Volume'],
                    price_record['OpenInterest'],
                    # INSERT句のパラメータ
                    price_record['TradeDate'], price_record['MetalID'],
                    price_record['DataType'], price_record['GenericID'],
                    price_record['ActualContractID'], price_record['SettlementPrice'],
                    price_record['OpenPrice'], price_record['HighPrice'],
                    price_record['LowPrice'], price_record['LastPrice'],
                    price_record['Volume'], price_record['OpenInterest']
                ))
                
                conn.commit()
                return True
                
        except Exception as e:
            logger.error(f"価格データ格納エラー: {e}")
            return False
            
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
            
    def _verify_loaded_data(self):
        """ロードされたデータの検証"""
        logger.info("\n=== データロード結果確認 ===")
        
        with self.db_manager.get_connection() as conn:
            # 取引所別サマリー
            summary_df = pd.read_sql("""
                SELECT 
                    g.ExchangeCode,
                    COUNT(DISTINCT p.TradeDate) as 営業日数,
                    COUNT(DISTINCT g.GenericNumber) as 銘柄数,
                    COUNT(*) as レコード数,
                    MIN(p.TradeDate) as 最古日付,
                    MAX(p.TradeDate) as 最新日付
                FROM T_CommodityPrice_V2 p
                JOIN M_GenericFutures g ON p.GenericID = g.GenericID
                WHERE p.DataType = 'Generic'
                GROUP BY g.ExchangeCode
                ORDER BY g.ExchangeCode
            """, conn)
            
            logger.info("\n【取引所別サマリー】")
            for _, row in summary_df.iterrows():
                exchange = 'COMEX' if row['ExchangeCode'] == 'CMX' else row['ExchangeCode']
                logger.info(f"{exchange}: {row['営業日数']}日間, {row['銘柄数']}銘柄, {row['レコード数']}件 ({row['最古日付']} - {row['最新日付']})")

def main():
    """メイン実行関数"""
    logger.info("マルチ取引所データロード開始")
    
    loader = MultiExchangeDataLoader()
    success = loader.load_all_exchanges_data(days_back=3)  # 過去3営業日分
    
    if success:
        logger.info("データロード正常完了")
    else:
        logger.error("データロード失敗")
        
    return success

if __name__ == "__main__":
    main()