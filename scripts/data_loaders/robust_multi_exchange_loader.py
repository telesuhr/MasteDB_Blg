"""
改良版マルチ取引所データローダー
各取引所の特性に応じた適切なフィールドマッピングと品質チェック付き
"""
import sys
import os
from datetime import datetime, date, timedelta
import pandas as pd
import time

# プロジェクトルートとsrcディレクトリをPythonパスに追加
project_root = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.join(project_root, 'src')
sys.path.insert(0, project_root)
sys.path.insert(0, src_dir)

from bloomberg_api import BloombergDataFetcher
from database import DatabaseManager
from config.logging_config import logger

class RobustMultiExchangeLoader:
    """改良版マルチ取引所データローダー"""
    
    def __init__(self):
        self.bloomberg = BloombergDataFetcher()
        self.db_manager = DatabaseManager()
        
        # 取引所別のフィールドマッピング
        # 各取引所で利用可能なフィールドを定義
        self.exchange_fields = {
            'LME': {
                'price_fields': ['PX_LAST', 'PX_SETTLE', 'PX_BID', 'PX_ASK'],
                'volume_fields': ['VOLUME', 'PX_VOLUME'],
                'oi_fields': []  # LMEはOIデータなし
            },
            'SHFE': {
                'price_fields': ['PX_LAST', 'PX_OPEN', 'PX_HIGH', 'PX_LOW', 'PX_SETTLE'],
                'volume_fields': ['PX_VOLUME', 'VOLUME'],
                'oi_fields': ['OPEN_INT']
            },
            'CMX': {
                'price_fields': ['PX_LAST', 'PX_SETTLE', 'PX_OPEN', 'PX_HIGH', 'PX_LOW'],
                'volume_fields': ['PX_VOLUME', 'VOLUME'],
                'oi_fields': ['OPEN_INT']
            }
        }
        
        # 標準フィールドセット（全取引所共通で試す）
        self.standard_fields = [
            'PX_LAST',      # 終値
            'PX_SETTLE',    # 清算値
            'PX_OPEN',      # 始値
            'PX_HIGH',      # 高値
            'PX_LOW',       # 安値
            'PX_VOLUME',    # 出来高
            'VOLUME',       # 出来高（別名）
            'OPEN_INT',     # 建玉
            'PX_BID',       # 買値
            'PX_ASK'        # 売値
        ]
        
    def load_all_exchanges_data(self, days_back=3):
        """全取引所のデータをロード（改良版）"""
        logger.info("=== 改良版マルチ取引所データロード開始 ===")
        
        try:
            # Bloomberg API接続
            if not self.bloomberg.connect():
                raise ConnectionError("Bloomberg API接続に失敗しました")
                
            # データベース接続
            self.db_manager.connect()
            
            # まず既存データをクリア（テスト用）
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
                exchange_tickers[exchange].append(row['GenericTicker'])
            
            # 日付範囲
            end_date = datetime.now().strftime('%Y%m%d')
            start_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y%m%d')
            
            logger.info(f"取得期間: {start_date} - {end_date}")
            
            total_success = 0
            exchange_results = {}
            
            # 取引所別にデータ取得・格納
            for exchange, tickers in exchange_tickers.items():
                display_name = 'COMEX' if exchange == 'CMX' else exchange
                logger.info(f"\n--- {display_name} 取引所処理開始 ---")
                
                # 取引所別処理
                if exchange == 'LME':
                    success_count = self._process_lme_data(tickers, start_date, end_date, generic_futures)
                elif exchange == 'SHFE':
                    success_count = self._process_shfe_data(tickers, start_date, end_date, generic_futures)
                elif exchange == 'CMX':
                    success_count = self._process_cmx_data(tickers, start_date, end_date, generic_futures)
                else:
                    logger.warning(f"未対応の取引所: {exchange}")
                    continue
                
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
            
    def _process_lme_data(self, tickers, start_date, end_date, generic_futures):
        """LME専用処理"""
        logger.info("LME専用処理開始")
        success_count = 0
        
        # LMEは流動性の高い限月のみOHLCデータがある
        # LP1-LP12は通常データあり、LP13以降は清算値のみの場合が多い
        
        # バッチ処理
        batch_size = 30
        for i in range(0, len(tickers), batch_size):
            batch_tickers = tickers[i:i+batch_size]
            
            # LME用フィールド
            lme_fields = ['PX_LAST', 'PX_SETTLE', 'PX_OPEN', 'PX_HIGH', 'PX_LOW', 'PX_VOLUME']
            
            # データ取得（複数回試行）
            price_data = None
            for attempt in range(3):
                try:
                    price_data = self.bloomberg.get_historical_data(
                        batch_tickers, lme_fields, start_date, end_date
                    )
                    if not price_data.empty:
                        break
                except Exception as e:
                    logger.warning(f"LME データ取得試行 {attempt+1} 失敗: {e}")
                    time.sleep(2)
            
            if price_data is None or price_data.empty:
                logger.warning(f"LME バッチ {i//batch_size + 1}: データ取得失敗")
                continue
                
            logger.info(f"LME バッチ {i//batch_size + 1}: {len(price_data)}件取得")
            
            # データ処理
            for _, row in price_data.iterrows():
                try:
                    processed_data = self._process_lme_record(row, generic_futures)
                    if processed_data and self._store_price_record(processed_data):
                        success_count += 1
                except Exception as e:
                    logger.error(f"LME レコード処理エラー: {e}")
                    
        return success_count
        
    def _process_shfe_data(self, tickers, start_date, end_date, generic_futures):
        """SHFE専用処理"""
        logger.info("SHFE専用処理開始")
        success_count = 0
        
        # SHFEは全フィールドが通常利用可能
        shfe_fields = ['PX_LAST', 'PX_SETTLE', 'PX_OPEN', 'PX_HIGH', 'PX_LOW', 
                       'PX_VOLUME', 'OPEN_INT']
        
        # 全ティッカー一括取得（SHFEは12銘柄のみ）
        price_data = self.bloomberg.get_historical_data(
            tickers, shfe_fields, start_date, end_date
        )
        
        if price_data.empty:
            logger.warning("SHFE: データなし")
            return 0
            
        logger.info(f"SHFE: {len(price_data)}件取得")
        
        # データ処理
        for _, row in price_data.iterrows():
            try:
                processed_data = self._process_shfe_record(row, generic_futures)
                if processed_data and self._store_price_record(processed_data):
                    success_count += 1
            except Exception as e:
                logger.error(f"SHFE レコード処理エラー: {e}")
                
        return success_count
        
    def _process_cmx_data(self, tickers, start_date, end_date, generic_futures):
        """CMX/COMEX専用処理"""
        logger.info("CMX/COMEX専用処理開始")
        success_count = 0
        
        # CMXは活発な限月（HG1-HG6）のみOHLCデータがある場合が多い
        active_tickers = [t for t in tickers if int(t[2:-7]) <= 12]  # HG1-HG12
        other_tickers = [t for t in tickers if int(t[2:-7]) > 12]   # HG13以降
        
        # アクティブ限月の処理
        if active_tickers:
            cmx_fields = ['PX_LAST', 'PX_SETTLE', 'PX_OPEN', 'PX_HIGH', 'PX_LOW', 
                          'PX_VOLUME', 'OPEN_INT']
            
            price_data = self.bloomberg.get_historical_data(
                active_tickers, cmx_fields, start_date, end_date
            )
            
            if not price_data.empty:
                logger.info(f"CMX アクティブ限月: {len(price_data)}件取得")
                
                for _, row in price_data.iterrows():
                    try:
                        processed_data = self._process_cmx_record(row, generic_futures)
                        if processed_data and self._store_price_record(processed_data):
                            success_count += 1
                    except Exception as e:
                        logger.error(f"CMX レコード処理エラー: {e}")
        
        # 非アクティブ限月の処理（清算値のみ）
        if other_tickers:
            simple_fields = ['PX_LAST', 'PX_SETTLE']
            
            # バッチ処理
            batch_size = 20
            for i in range(0, len(other_tickers), batch_size):
                batch_tickers = other_tickers[i:i+batch_size]
                
                price_data = self.bloomberg.get_historical_data(
                    batch_tickers, simple_fields, start_date, end_date
                )
                
                if not price_data.empty:
                    logger.info(f"CMX 非アクティブ限月バッチ: {len(price_data)}件取得")
                    
                    for _, row in price_data.iterrows():
                        try:
                            processed_data = self._process_cmx_record(row, generic_futures, simple=True)
                            if processed_data and self._store_price_record(processed_data):
                                success_count += 1
                        except Exception as e:
                            logger.error(f"CMX レコード処理エラー: {e}")
                            
        return success_count
        
    def _process_lme_record(self, row, generic_futures):
        """LMEレコード処理"""
        security = row['security']
        generic_info = generic_futures[generic_futures['GenericTicker'] == security].iloc[0]
        
        # 価格データ処理
        settlement_price = self._get_value(row, ['PX_SETTLE', 'PX_LAST'])
        last_price = self._get_value(row, ['PX_LAST', 'PX_SETTLE'])
        
        return {
            'TradeDate': pd.to_datetime(row['date']).date(),
            'MetalID': int(generic_info['MetalID']),
            'DataType': 'Generic',
            'GenericID': int(generic_info['GenericID']),
            'ActualContractID': None,
            'SettlementPrice': settlement_price,
            'OpenPrice': self._safe_float(row.get('PX_OPEN')),
            'HighPrice': self._safe_float(row.get('PX_HIGH')),
            'LowPrice': self._safe_float(row.get('PX_LOW')),
            'LastPrice': last_price,
            'Volume': self._safe_int(row.get('PX_VOLUME')),
            'OpenInterest': None  # LMEはOIなし
        }
        
    def _process_shfe_record(self, row, generic_futures):
        """SHFEレコード処理"""
        security = row['security']
        generic_info = generic_futures[generic_futures['GenericTicker'] == security].iloc[0]
        
        # 価格データ処理
        settlement_price = self._get_value(row, ['PX_SETTLE', 'PX_LAST'])
        last_price = self._get_value(row, ['PX_LAST', 'PX_SETTLE'])
        
        return {
            'TradeDate': pd.to_datetime(row['date']).date(),
            'MetalID': int(generic_info['MetalID']),
            'DataType': 'Generic',
            'GenericID': int(generic_info['GenericID']),
            'ActualContractID': None,
            'SettlementPrice': settlement_price,
            'OpenPrice': self._safe_float(row.get('PX_OPEN')),
            'HighPrice': self._safe_float(row.get('PX_HIGH')),
            'LowPrice': self._safe_float(row.get('PX_LOW')),
            'LastPrice': last_price,
            'Volume': self._safe_int(row.get('PX_VOLUME')),
            'OpenInterest': self._safe_int(row.get('OPEN_INT'))
        }
        
    def _process_cmx_record(self, row, generic_futures, simple=False):
        """CMXレコード処理"""
        security = row['security']
        generic_info = generic_futures[generic_futures['GenericTicker'] == security].iloc[0]
        
        # 価格データ処理
        settlement_price = self._get_value(row, ['PX_SETTLE', 'PX_LAST'])
        last_price = self._get_value(row, ['PX_LAST', 'PX_SETTLE'])
        
        if simple:
            # 非アクティブ限月（清算値のみ）
            return {
                'TradeDate': pd.to_datetime(row['date']).date(),
                'MetalID': int(generic_info['MetalID']),
                'DataType': 'Generic',
                'GenericID': int(generic_info['GenericID']),
                'ActualContractID': None,
                'SettlementPrice': settlement_price,
                'OpenPrice': None,
                'HighPrice': None,
                'LowPrice': None,
                'LastPrice': last_price,
                'Volume': None,
                'OpenInterest': None
            }
        else:
            # アクティブ限月（全データ）
            return {
                'TradeDate': pd.to_datetime(row['date']).date(),
                'MetalID': int(generic_info['MetalID']),
                'DataType': 'Generic',
                'GenericID': int(generic_info['GenericID']),
                'ActualContractID': None,
                'SettlementPrice': settlement_price,
                'OpenPrice': self._safe_float(row.get('PX_OPEN')),
                'HighPrice': self._safe_float(row.get('PX_HIGH')),
                'LowPrice': self._safe_float(row.get('PX_LOW')),
                'LastPrice': last_price,
                'Volume': self._safe_int(row.get('PX_VOLUME')),
                'OpenInterest': self._safe_int(row.get('OPEN_INT'))
            }
            
    def _get_value(self, row, field_list):
        """複数フィールドから最初の有効な値を取得"""
        for field in field_list:
            value = row.get(field)
            if value is not None and not pd.isna(value):
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

def main():
    """メイン実行関数"""
    logger.info("改良版マルチ取引所データロード開始")
    
    loader = RobustMultiExchangeLoader()
    success = loader.load_all_exchanges_data(days_back=3)  # 過去3営業日分
    
    if success:
        logger.info("データロード正常完了")
    else:
        logger.error("データロード失敗")
        
    return success

if __name__ == "__main__":
    main()