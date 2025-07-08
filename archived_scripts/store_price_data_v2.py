"""
Phase 3: LP1-LP3の価格データを新しいT_CommodityPrice_V2テーブルに格納
ジェネリック先物の価格データ取得と格納のテスト
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

class PriceDataStoreV2:
    """新しい価格データテーブルへの格納クラス"""
    
    def __init__(self):
        self.bloomberg = BloombergDataFetcher()
        self.db_manager = DatabaseManager()
        
    def store_generic_price_data(self, days_back=5):
        """ジェネリック先物の価格データを取得・格納"""
        logger.info("=== LP1-LP3 価格データ格納開始 ===")
        
        try:
            # Bloomberg API接続
            if not self.bloomberg.connect():
                raise ConnectionError("Bloomberg API接続に失敗しました")
                
            # データベース接続
            self.db_manager.connect()
            
            # テスト対象のジェネリック先物
            test_tickers = ['LP1 Comdty', 'LP2 Comdty', 'LP3 Comdty']
            
            # 価格フィールド
            price_fields = [
                'PX_LAST',          # 終値
                'PX_OPEN',          # 始値
                'PX_HIGH',          # 高値
                'PX_LOW',           # 安値
                'PX_VOLUME',        # 出来高
                'OPEN_INT'          # 建玉残高
            ]
            
            # 過去数日分のデータを取得
            end_date = datetime.now().strftime('%Y%m%d')
            start_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y%m%d')
            
            logger.info(f"取得期間: {start_date} - {end_date}")
            logger.info(f"対象銘柄: {test_tickers}")
            
            # ヒストリカルデータ取得
            price_data = self.bloomberg.get_historical_data(
                test_tickers, price_fields, start_date, end_date
            )
            
            if price_data.empty:
                logger.error("価格データが取得できませんでした")
                return False
                
            logger.info(f"取得データ件数: {len(price_data)}")
            
            # ジェネリック先物情報を取得
            generic_info = self._get_generic_futures_info(test_tickers)
            
            # データ処理と格納
            success_count = self._process_and_store_price_data(price_data, generic_info)
            
            logger.info(f"=== 価格データ格納完了: {success_count}件 ===")
            return True
            
        except Exception as e:
            logger.error(f"価格データ格納中にエラーが発生: {e}")
            return False
            
        finally:
            self.bloomberg.disconnect()
            self.db_manager.disconnect()
            
    def _get_generic_futures_info(self, tickers):
        """ジェネリック先物情報を取得"""
        with self.db_manager.get_connection() as conn:
            placeholders = ','.join(['?' for _ in tickers])
            query = f"""
                SELECT GenericID, GenericTicker, MetalID, ExchangeCode
                FROM M_GenericFutures 
                WHERE GenericTicker IN ({placeholders})
                ORDER BY GenericNumber
            """
            df = pd.read_sql(query, conn, params=tickers)
            logger.info(f"ジェネリック先物情報取得: {len(df)}件")
            return df
            
    def _process_and_store_price_data(self, price_data, generic_info):
        """価格データの処理と格納"""
        success_count = 0
        
        # ジェネリック先物情報をマップに変換
        generic_map = {}
        for _, row in generic_info.iterrows():
            generic_map[row['GenericTicker']] = {
                'GenericID': row['GenericID'],
                'MetalID': row['MetalID']
            }
        
        for _, row in price_data.iterrows():
            try:
                security = row['security']
                trade_date = pd.to_datetime(row['date']).date()
                
                logger.info(f"処理中: {security} - {trade_date}")
                
                # ジェネリック先物情報を取得
                if security not in generic_map:
                    logger.warning(f"ジェネリック先物情報が見つかりません: {security}")
                    continue
                    
                generic_data = generic_map[security]
                
                # 価格データの抽出と変換
                price_record = {
                    'TradeDate': trade_date,
                    'MetalID': int(generic_data['MetalID']),
                    'DataType': 'Generic',
                    'GenericID': int(generic_data['GenericID']),
                    'ActualContractID': None,
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
                    logger.info(f"格納完了: {security} - {trade_date}")
                
            except Exception as e:
                logger.error(f"価格データ処理エラー ({security} - {trade_date}): {e}")
                continue
                
        return success_count
        
    def _store_price_record(self, price_record):
        """価格レコードをデータベースに格納"""
        try:
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()
                
                # 既存チェック（同じ日付・ジェネリックの組み合わせ）
                cursor.execute("""
                    SELECT PriceID FROM T_CommodityPrice_V2 
                    WHERE TradeDate = ? AND GenericID = ? AND DataType = 'Generic'
                """, (price_record['TradeDate'], price_record['GenericID']))
                existing = cursor.fetchone()
                
                if existing:
                    # 更新
                    cursor.execute("""
                        UPDATE T_CommodityPrice_V2 
                        SET SettlementPrice = ?, OpenPrice = ?, HighPrice = ?, 
                            LowPrice = ?, LastPrice = ?, Volume = ?, OpenInterest = ?,
                            LastUpdated = ?
                        WHERE TradeDate = ? AND GenericID = ? AND DataType = 'Generic'
                    """, (
                        price_record['SettlementPrice'], price_record['OpenPrice'],
                        price_record['HighPrice'], price_record['LowPrice'],
                        price_record['LastPrice'], price_record['Volume'],
                        price_record['OpenInterest'], datetime.now(),
                        price_record['TradeDate'], price_record['GenericID']
                    ))
                    logger.debug(f"価格データ更新: ジェネリックID {price_record['GenericID']}")
                else:
                    # 新規挿入
                    cursor.execute("""
                        INSERT INTO T_CommodityPrice_V2 (
                            TradeDate, MetalID, DataType, GenericID, ActualContractID,
                            SettlementPrice, OpenPrice, HighPrice, LowPrice, LastPrice,
                            Volume, OpenInterest
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        price_record['TradeDate'], price_record['MetalID'],
                        price_record['DataType'], price_record['GenericID'],
                        price_record['ActualContractID'], price_record['SettlementPrice'],
                        price_record['OpenPrice'], price_record['HighPrice'],
                        price_record['LowPrice'], price_record['LastPrice'],
                        price_record['Volume'], price_record['OpenInterest']
                    ))
                    logger.debug(f"価格データ作成: ジェネリックID {price_record['GenericID']}")
                
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

def main():
    """メイン実行関数"""
    logger.info("LP1-LP3 価格データ格納開始")
    
    store = PriceDataStoreV2()
    success = store.store_generic_price_data(days_back=5)  # 過去5日分
    
    if success:
        logger.info("価格データ格納正常完了")
        
        # 格納結果の確認
        print("\n=== 価格データ格納結果確認 ===")
        with store.db_manager.get_connection() as conn:
            # 格納された価格データ確認
            price_df = pd.read_sql("""
                SELECT 
                    p.TradeDate,
                    g.GenericTicker,
                    p.SettlementPrice,
                    p.Volume,
                    p.OpenInterest
                FROM T_CommodityPrice_V2 p
                JOIN M_GenericFutures g ON p.GenericID = g.GenericID
                WHERE p.DataType = 'Generic'
                ORDER BY p.TradeDate DESC, g.GenericNumber
            """, conn)
            print("\n【格納された価格データ】")
            print(price_df.to_string())
            
            # 件数サマリー
            summary_df = pd.read_sql("""
                SELECT 
                    g.GenericTicker,
                    COUNT(*) as データ件数,
                    MIN(p.TradeDate) as 最古日付,
                    MAX(p.TradeDate) as 最新日付
                FROM T_CommodityPrice_V2 p
                JOIN M_GenericFutures g ON p.GenericID = g.GenericID
                WHERE p.DataType = 'Generic'
                GROUP BY g.GenericTicker, g.GenericNumber
                ORDER BY g.GenericNumber
            """, conn)
            print("\n【データ件数サマリー】")
            print(summary_df.to_string())
    else:
        logger.error("価格データ格納失敗")
        
    return success

if __name__ == "__main__":
    main()