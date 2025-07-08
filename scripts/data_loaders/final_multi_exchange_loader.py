"""
最終版マルチ取引所データローダー
全フィールドを確実に取得し、日本時間で記録
"""
import sys
import os
from datetime import datetime, date, timedelta
import pandas as pd
import pytz

# プロジェクトルートとsrcディレクトリをPythonパスに追加
project_root = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.join(project_root, 'src')
sys.path.insert(0, project_root)
sys.path.insert(0, src_dir)

from bloomberg_api import BloombergDataFetcher
from database import DatabaseManager
from config.logging_config import logger

class FinalMultiExchangeLoader:
    """最終版マルチ取引所データローダー"""
    
    def __init__(self):
        self.bloomberg = BloombergDataFetcher()
        self.db_manager = DatabaseManager()
        
        # 日本時間設定
        self.jst = pytz.timezone('Asia/Tokyo')
        
        # 実証済みの利用可能フィールド
        self.bloomberg_fields = [
            'PX_LAST',          # 終値
            'PX_SETTLE',        # 清算値
            'PX_OPEN',          # 始値
            'PX_HIGH',          # 高値
            'PX_LOW',           # 安値
            'PX_MID',           # 仲値
            'PX_BID',           # 買値
            'PX_ASK',           # 売値
            'LAST_PRICE',       # 直近価格
            'PX_VOLUME',        # 出来高
            'VOLUME',           # 出来高（別名）
            'OPEN_INT',         # 建玉
        ]
        
    def load_all_exchanges_data(self, days_back=3):
        """全取引所のデータをロード（最終版）"""
        logger.info("=== 最終版マルチ取引所データロード開始 ===")
        logger.info(f"実行時刻（JST）: {datetime.now(self.jst)}")
        
        try:
            # Bloomberg API接続
            if not self.bloomberg.connect():
                raise ConnectionError("Bloomberg API接続に失敗しました")
                
            # データベース接続
            self.db_manager.connect()
            
            # 既存データをクリア（テスト用）
            self._clear_existing_data()
            
            # 全取引所のジェネリック先物情報を取得
            generic_futures = self._get_all_generic_futures()
            logger.info(f"対象銘柄数: {len(generic_futures)}")
            
            # 取引所別にティッカーを整理
            exchange_tickers = {}
            for _, row in generic_futures.iterrows():
                exchange = row['ExchangeCode']
                if exchange not in exchange_tickers:
                    exchange_tickers[exchange] = []
                exchange_tickers[exchange].append({
                    'ticker': row['GenericTicker'],
                    'generic_id': row['GenericID'],
                    'metal_id': row['MetalID'],
                    'generic_number': row['GenericNumber']
                })
            
            # 日付範囲
            end_date = datetime.now().strftime('%Y%m%d')
            start_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y%m%d')
            
            logger.info(f"取得期間: {start_date} - {end_date}")
            
            total_success = 0
            exchange_results = {}
            
            # 取引所別にデータ取得・格納
            for exchange, ticker_info_list in exchange_tickers.items():
                display_name = 'COMEX' if exchange == 'CMX' else exchange
                logger.info(f"\n--- {display_name} 取引所処理開始 ---")
                
                success_count = self._process_exchange_data(
                    exchange, ticker_info_list, start_date, end_date
                )
                
                exchange_results[display_name] = success_count
                total_success += success_count
                logger.info(f"{display_name} 取引所処理完了: {success_count}件")
            
            logger.info(f"\n=== 全取引所データロード完了 ===")
            for exchange, count in exchange_results.items():
                logger.info(f"{exchange}: {count}件")
            logger.info(f"合計: {total_success}件")
            
            # 結果確認
            self._verify_loaded_data()
            
            return True
            
        except Exception as e:
            logger.error(f"データロード中にエラーが発生: {e}")
            return False
            
        finally:
            self.bloomberg.disconnect()
            self.db_manager.disconnect()
            
    def _clear_existing_data(self):
        """既存データクリア（テスト用）"""
        logger.info("既存データをクリア中...")
        with self.db_manager.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM T_CommodityPrice_V2 WHERE DataType = 'Generic'")
            conn.commit()
            logger.info("クリア完了")
            
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
            
    def _process_exchange_data(self, exchange, ticker_info_list, start_date, end_date):
        """取引所データ処理（統一版）"""
        success_count = 0
        
        # ティッカーリストを作成
        tickers = [info['ticker'] for info in ticker_info_list]
        
        # バッチサイズ設定（取引所別）
        batch_size = 12 if exchange == 'SHFE' else 25
        
        for i in range(0, len(tickers), batch_size):
            batch_tickers = tickers[i:i+batch_size]
            batch_info = ticker_info_list[i:i+batch_size]
            
            # データ取得（全フィールド）
            try:
                price_data = self.bloomberg.get_historical_data(
                    batch_tickers, self.bloomberg_fields, start_date, end_date
                )
                
                if price_data.empty:
                    logger.warning(f"{exchange} バッチ {i//batch_size + 1}: データなし")
                    continue
                    
                logger.info(f"{exchange} バッチ {i//batch_size + 1}: {len(price_data)}件取得")
                
                # データ処理
                for _, row in price_data.iterrows():
                    try:
                        # 対応するティッカー情報を取得
                        security = row['security']
                        ticker_info = next((info for info in batch_info if info['ticker'] == security), None)
                        
                        if not ticker_info:
                            continue
                        
                        processed_data = self._process_record(row, ticker_info, exchange)
                        if processed_data and self._store_price_record(processed_data):
                            success_count += 1
                            
                    except Exception as e:
                        logger.error(f"{exchange} レコード処理エラー: {e}")
                        
            except Exception as e:
                logger.error(f"{exchange} データ取得エラー: {e}")
                continue
                
        return success_count
        
    def _process_record(self, row, ticker_info, exchange):
        """レコード処理（統一版）"""
        
        # 価格データ処理
        # SettlementPriceとLastPriceの優先順位
        settlement_price = self._get_first_valid_value(row, ['PX_SETTLE', 'PX_LAST', 'LAST_PRICE'])
        last_price = self._get_first_valid_value(row, ['PX_LAST', 'LAST_PRICE', 'PX_SETTLE'])
        
        # OHLC データ
        open_price = self._safe_float(row.get('PX_OPEN'))
        high_price = self._safe_float(row.get('PX_HIGH'))
        low_price = self._safe_float(row.get('PX_LOW'))
        
        # 出来高（複数フィールドから取得）
        volume = self._get_first_valid_value(row, ['PX_VOLUME', 'VOLUME'], as_int=True)
        
        # 建玉（LMEはなし）
        open_interest = None
        if exchange != 'LME':
            open_interest = self._safe_int(row.get('OPEN_INT'))
        
        return {
            'TradeDate': pd.to_datetime(row['date']).date(),
            'MetalID': int(ticker_info['metal_id']),
            'DataType': 'Generic',
            'GenericID': int(ticker_info['generic_id']),
            'ActualContractID': None,
            'SettlementPrice': settlement_price,
            'OpenPrice': open_price,
            'HighPrice': high_price,
            'LowPrice': low_price,
            'LastPrice': last_price,
            'Volume': volume,
            'OpenInterest': open_interest
        }
        
    def _get_first_valid_value(self, row, field_list, as_int=False):
        """複数フィールドから最初の有効な値を取得"""
        for field in field_list:
            value = row.get(field)
            if value is not None and not pd.isna(value):
                if as_int:
                    return self._safe_int(value)
                else:
                    return self._safe_float(value)
        return None
        
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
            
    def _store_price_record(self, price_record):
        """価格レコードをデータベースに格納（日本時間で記録）"""
        try:
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()
                
                # 現在時刻を日本時間で取得
                current_time_jst = datetime.now(self.jst)
                
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
                            LastUpdated = ?
                    WHEN NOT MATCHED THEN
                        INSERT (TradeDate, MetalID, DataType, GenericID, ActualContractID,
                                SettlementPrice, OpenPrice, HighPrice, LowPrice, LastPrice,
                                Volume, OpenInterest, LastUpdated)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                """, (
                    # USING句のパラメータ
                    price_record['TradeDate'], price_record['GenericID'],
                    # UPDATE句のパラメータ
                    price_record['ActualContractID'],
                    price_record['SettlementPrice'], price_record['OpenPrice'],
                    price_record['HighPrice'], price_record['LowPrice'],
                    price_record['LastPrice'], price_record['Volume'],
                    price_record['OpenInterest'],
                    current_time_jst,  # 日本時間で記録
                    # INSERT句のパラメータ
                    price_record['TradeDate'], price_record['MetalID'],
                    price_record['DataType'], price_record['GenericID'],
                    price_record['ActualContractID'], price_record['SettlementPrice'],
                    price_record['OpenPrice'], price_record['HighPrice'],
                    price_record['LowPrice'], price_record['LastPrice'],
                    price_record['Volume'], price_record['OpenInterest'],
                    current_time_jst  # 日本時間で記録
                ))
                
                conn.commit()
                return True
                
        except Exception as e:
            logger.error(f"価格データ格納エラー: {e}")
            return False
            
    def _verify_loaded_data(self):
        """ロードされたデータの検証"""
        logger.info("\n=== データロード結果確認 ===")
        
        with self.db_manager.get_connection() as conn:
            # 取引所別品質チェック
            quality_df = pd.read_sql("""
                SELECT 
                    g.ExchangeCode,
                    COUNT(*) as 総レコード数,
                    AVG(CASE WHEN p.SettlementPrice IS NOT NULL THEN 1.0 ELSE 0.0 END) * 100 as Settlement有効率,
                    AVG(CASE WHEN p.OpenPrice IS NOT NULL THEN 1.0 ELSE 0.0 END) * 100 as OHLC有効率,
                    AVG(CASE WHEN p.Volume IS NOT NULL THEN 1.0 ELSE 0.0 END) * 100 as Volume有効率,
                    AVG(CASE WHEN p.OpenInterest IS NOT NULL THEN 1.0 ELSE 0.0 END) * 100 as OI有効率
                FROM T_CommodityPrice_V2 p
                JOIN M_GenericFutures g ON p.GenericID = g.GenericID
                WHERE p.DataType = 'Generic'
                GROUP BY g.ExchangeCode
                ORDER BY g.ExchangeCode
            """, conn)
            
            logger.info("\n【取引所別データ品質】")
            for _, row in quality_df.iterrows():
                exchange = 'COMEX' if row['ExchangeCode'] == 'CMX' else row['ExchangeCode']
                logger.info(f"{exchange}: {int(row['総レコード数'])}件")
                logger.info(f"  Settlement: {row['Settlement有効率']:.1f}%")
                logger.info(f"  OHLC: {row['OHLC有効率']:.1f}%")
                logger.info(f"  Volume: {row['Volume有効率']:.1f}%")
                logger.info(f"  OI: {row['OI有効率']:.1f}%")
                
            # サンプルデータ確認（CMXのみ）
            sample_df = pd.read_sql("""
                SELECT TOP 5
                    p.TradeDate,
                    g.GenericTicker,
                    p.SettlementPrice,
                    p.OpenPrice,
                    p.HighPrice,
                    p.LowPrice,
                    p.Volume,
                    p.OpenInterest,
                    p.LastUpdated
                FROM T_CommodityPrice_V2 p
                JOIN M_GenericFutures g ON p.GenericID = g.GenericID
                WHERE p.DataType = 'Generic' AND g.ExchangeCode = 'CMX'
                ORDER BY p.TradeDate DESC, g.GenericNumber
            """, conn)
            
            logger.info("\n【CMX/COMEXサンプルデータ】")
            for _, row in sample_df.iterrows():
                logger.info(f"{row['GenericTicker']} ({row['TradeDate']}): "
                           f"OHLC={row['OpenPrice']}/{row['HighPrice']}/{row['LowPrice']}, "
                           f"Vol={row['Volume']}, OI={row['OpenInterest']}, "
                           f"更新={row['LastUpdated']}")

def main():
    """メイン実行関数"""
    logger.info("最終版マルチ取引所データロード開始")
    
    loader = FinalMultiExchangeLoader()
    success = loader.load_all_exchanges_data(days_back=3)  # 過去3営業日分
    
    if success:
        logger.info("データロード正常完了")
    else:
        logger.error("データロード失敗")
        
    return success

if __name__ == "__main__":
    main()