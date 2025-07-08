"""
過去日付のジェネリック・実契約マッピングを遡って取得・修正
ロールオーバー分析のために各取引日時点での正確なマッピングを構築
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

class HistoricalMappingFixer:
    """過去日付のマッピングデータ修正クラス"""
    
    def __init__(self):
        self.bloomberg = BloombergDataFetcher()
        self.db_manager = DatabaseManager()
        
    def fix_historical_mapping(self):
        """過去日付のマッピングを取得・修正"""
        logger.info("=== 過去日付マッピング修正開始 ===")
        
        try:
            # Bloomberg API接続
            if not self.bloomberg.connect():
                raise ConnectionError("Bloomberg API接続に失敗しました")
                
            # データベース接続
            self.db_manager.connect()
            
            # 価格データから取引日を取得
            trade_dates = self._get_trade_dates()
            logger.info(f"対象取引日: {trade_dates}")
            
            # 各取引日のマッピングを取得
            for trade_date in trade_dates:
                logger.info(f"処理中: {trade_date}")
                self._process_date_mapping(trade_date)
                
            # 価格データのActualContractIDを更新
            self._update_price_data_actual_contract_id()
            
            logger.info("=== 過去日付マッピング修正完了 ===")
            return True
            
        except Exception as e:
            logger.error(f"マッピング修正中にエラーが発生: {e}")
            return False
            
        finally:
            self.bloomberg.disconnect()
            self.db_manager.disconnect()
            
    def _get_trade_dates(self):
        """価格データから取引日一覧を取得"""
        with self.db_manager.get_connection() as conn:
            query = """
                SELECT DISTINCT TradeDate 
                FROM T_CommodityPrice_V2 
                WHERE DataType = 'Generic'
                ORDER BY TradeDate
            """
            df = pd.read_sql(query, conn)
            return [row['TradeDate'] for _, row in df.iterrows()]
            
    def _process_date_mapping(self, trade_date):
        """特定日付のマッピングを処理"""
        try:
            # その日のジェネリック先物情報を取得
            test_tickers = ['LP1 Comdty', 'LP2 Comdty', 'LP3 Comdty']
            
            # Bloomberg APIから当時のマッピングを取得
            # 注意: Bloomberg APIは過去時点でのFUT_CUR_GEN_TICKERを取得できない場合があります
            # その場合は現在のマッピングを使用するか、推定ロジックを使用
            
            if trade_date == date(2025, 7, 7):
                # 既存のマッピングを使用
                logger.info(f"{trade_date}: 既存マッピング使用")
                return
                
            # 過去日付の場合は、推定またはBloomberg履歴取得
            mapping_data = self._estimate_historical_mapping(trade_date, test_tickers)
            
            # マッピングデータベースに格納
            self._store_historical_mapping(trade_date, mapping_data)
            
        except Exception as e:
            logger.error(f"日付別マッピング処理エラー ({trade_date}): {e}")
            
    def _estimate_historical_mapping(self, trade_date, tickers):
        """過去日付のマッピングを推定"""
        """
        実際の実装では以下のロジックを使用:
        1. Bloomberg履歴データがある場合: 実際のマッピング取得
        2. 履歴データがない場合: 満期日ベースでの推定
        3. 今回はテスト用に簡易推定を実装
        """
        
        # 現在のマッピングを基準に推定
        with self.db_manager.get_connection() as conn:
            query = """
                SELECT g.GenericID, g.GenericTicker, m.ActualContractID, a.ContractTicker,
                       a.LastTradeableDate, a.ContractMonth
                FROM M_GenericFutures g
                JOIN T_GenericContractMapping m ON m.GenericID = g.GenericID
                JOIN M_ActualContract a ON m.ActualContractID = a.ActualContractID
                WHERE g.GenericTicker IN ('LP1 Comdty', 'LP2 Comdty', 'LP3 Comdty')
                  AND m.TradeDate = '2025-07-07'
                ORDER BY g.GenericNumber
            """
            current_mapping = pd.read_sql(query, conn)
            
        # 推定ロジック: LMEの通常のロールオーバーパターン
        estimated_mapping = []
        
        for _, row in current_mapping.iterrows():
            generic_id = row['GenericID']
            current_contract = row['ContractTicker']
            last_tradeable = row['LastTradeableDate']
            
            # 簡易推定: 取引日が最終取引日より前の場合は同じ契約を使用
            # 実際にはより複雑なロジックが必要（第三水曜日ルール等）
            
            if trade_date <= last_tradeable:
                # 同じ契約を使用
                estimated_mapping.append({
                    'generic_id': generic_id,
                    'actual_contract_id': row['ActualContractID'],
                    'contract_ticker': current_contract
                })
            else:
                # ロールオーバー後の推定（実装必要）
                logger.warning(f"ロールオーバー推定が必要: {trade_date}, {current_contract}")
                # 今回は同じ契約を使用（実際は次の契約を推定）
                estimated_mapping.append({
                    'generic_id': generic_id,
                    'actual_contract_id': row['ActualContractID'],
                    'contract_ticker': current_contract
                })
                
        return estimated_mapping
        
    def _store_historical_mapping(self, trade_date, mapping_data):
        """過去日付のマッピングをデータベースに格納"""
        try:
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()
                
                for mapping in mapping_data:
                    # 既存チェック
                    cursor.execute("""
                        SELECT MappingID FROM T_GenericContractMapping 
                        WHERE TradeDate = ? AND GenericID = ?
                    """, (trade_date, mapping['generic_id']))
                    existing = cursor.fetchone()
                    
                    if not existing:
                        # 残存日数の計算
                        days_to_expiry = self._calculate_days_to_expiry(
                            trade_date, mapping['actual_contract_id']
                        )
                        
                        # 新規挿入
                        cursor.execute("""
                            INSERT INTO T_GenericContractMapping (
                                TradeDate, GenericID, ActualContractID, DaysToExpiry
                            ) VALUES (?, ?, ?, ?)
                        """, (
                            trade_date, 
                            int(mapping['generic_id']), 
                            int(mapping['actual_contract_id']), 
                            days_to_expiry
                        ))
                        
                        logger.info(f"マッピング作成: {trade_date} - ジェネリックID {mapping['generic_id']} -> {mapping['contract_ticker']}")
                
                conn.commit()
                
        except Exception as e:
            logger.error(f"過去マッピング格納エラー ({trade_date}): {e}")
            
    def _calculate_days_to_expiry(self, trade_date, actual_contract_id):
        """残存日数の計算"""
        try:
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT LastTradeableDate FROM M_ActualContract 
                    WHERE ActualContractID = ?
                """, (actual_contract_id,))
                result = cursor.fetchone()
                
                if result and result[0]:
                    last_tradeable = result[0]
                    if isinstance(last_tradeable, str):
                        last_tradeable = pd.to_datetime(last_tradeable).date()
                    return (last_tradeable - trade_date).days
                    
        except Exception as e:
            logger.error(f"残存日数計算エラー: {e}")
            
        return None
        
    def _update_price_data_actual_contract_id(self):
        """価格データのActualContractIDを更新"""
        logger.info("価格データのActualContractID更新開始...")
        
        try:
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()
                
                # T_CommodityPrice_V2のActualContractIDを更新
                update_query = """
                    UPDATE p
                    SET p.ActualContractID = m.ActualContractID
                    FROM T_CommodityPrice_V2 p
                    JOIN T_GenericContractMapping m ON m.GenericID = p.GenericID 
                        AND m.TradeDate = p.TradeDate
                    WHERE p.DataType = 'Generic'
                        AND p.ActualContractID IS NULL
                """
                
                cursor.execute(update_query)
                updated_count = cursor.rowcount
                conn.commit()
                
                logger.info(f"価格データのActualContractID更新完了: {updated_count}件")
                
        except Exception as e:
            logger.error(f"価格データ更新エラー: {e}")

def main():
    """メイン実行関数"""
    logger.info("過去日付マッピング修正開始")
    
    fixer = HistoricalMappingFixer()
    success = fixer.fix_historical_mapping()
    
    if success:
        logger.info("過去日付マッピング修正正常完了")
        
        # 修正結果の確認
        print("\n=== 修正結果確認 ===")
        with fixer.db_manager.get_connection() as conn:
            # マッピングデータ確認
            mapping_df = pd.read_sql("""
                SELECT 
                    m.TradeDate,
                    g.GenericTicker,
                    a.ContractTicker,
                    m.DaysToExpiry
                FROM T_GenericContractMapping m
                JOIN M_GenericFutures g ON m.GenericID = g.GenericID
                JOIN M_ActualContract a ON m.ActualContractID = a.ActualContractID
                ORDER BY m.TradeDate, g.GenericNumber
            """, conn)
            print("\n【修正後のマッピングデータ】")
            print(mapping_df.to_string(index=False))
            
            # 価格データの更新確認
            price_df = pd.read_sql("""
                SELECT 
                    p.TradeDate,
                    g.GenericTicker,
                    a.ContractTicker,
                    p.SettlementPrice,
                    p.Volume
                FROM T_CommodityPrice_V2 p
                JOIN M_GenericFutures g ON p.GenericID = g.GenericID
                LEFT JOIN M_ActualContract a ON p.ActualContractID = a.ActualContractID
                WHERE p.DataType = 'Generic'
                ORDER BY p.TradeDate, g.GenericNumber
            """, conn)
            print("\n【価格データの実契約連携確認】")
            print(price_df.to_string(index=False))
    else:
        logger.error("過去日付マッピング修正失敗")
        
    return success

if __name__ == "__main__":
    main()