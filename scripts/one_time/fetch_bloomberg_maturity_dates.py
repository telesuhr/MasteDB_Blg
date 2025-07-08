"""
BloombergからLAST_TRADEABLE_DTとFUT_DLV_DT_LASTを取得してM_GenericFuturesを更新
"""
import sys
import os
from datetime import datetime
import pandas as pd
import pyodbc

# プロジェクトルートとsrcディレクトリをPythonパスに追加
project_root = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.join(project_root, 'src')
sys.path.insert(0, project_root)
sys.path.insert(0, src_dir)

from bloomberg_api import BloombergDataFetcher
from database import DatabaseManager
from config.logging_config import logger

class MaturityDateUpdater:
    """満期日情報更新クラス"""
    
    def __init__(self):
        self.bloomberg = BloombergDataFetcher()
        self.db_manager = DatabaseManager()
        
    def fetch_and_update_maturity_dates(self):
        """Bloombergから満期日情報を取得してデータベースを更新"""
        logger.info("=== 満期日情報更新開始 ===")
        
        try:
            # Bloomberg API接続
            if not self.bloomberg.connect():
                raise ConnectionError("Bloomberg API接続に失敗しました")
                
            # データベース接続
            self.db_manager.connect()
            
            # 全ジェネリック先物情報を取得
            generic_futures = self._get_all_generic_futures()
            logger.info(f"対象銘柄数: {len(generic_futures)}")
            
            # 取引所別に整理
            exchange_tickers = {}
            ticker_to_id = {}
            
            for _, row in generic_futures.iterrows():
                exchange = row['ExchangeCode']
                ticker = row['GenericTicker']
                
                if exchange not in exchange_tickers:
                    exchange_tickers[exchange] = []
                    
                exchange_tickers[exchange].append(ticker)
                ticker_to_id[ticker] = row['GenericID']
            
            # リファレンスデータフィールド
            ref_fields = ['LAST_TRADEABLE_DT', 'FUT_DLV_DT_LAST']
            
            total_updated = 0
            current_time = datetime.now()
            
            # 取引所別にデータ取得・更新
            for exchange, tickers in exchange_tickers.items():
                display_name = 'COMEX' if exchange == 'CMX' else exchange
                logger.info(f"\n--- {display_name} 取引所処理開始 ---")
                
                # バッチサイズ設定
                batch_size = 25
                
                for i in range(0, len(tickers), batch_size):
                    batch_tickers = tickers[i:i+batch_size]
                    
                    try:
                        # リファレンスデータ取得
                        logger.info(f"{display_name} バッチ {i//batch_size + 1}: {len(batch_tickers)}銘柄")
                        ref_data = self.bloomberg.get_reference_data(batch_tickers, ref_fields)
                        
                        if ref_data.empty:
                            logger.warning(f"  データなし")
                            continue
                            
                        # データ更新
                        for _, row in ref_data.iterrows():
                            ticker = row['security']
                            generic_id = ticker_to_id.get(ticker)
                            
                            if generic_id:
                                last_tradeable = row.get('LAST_TRADEABLE_DT')
                                delivery_date = row.get('FUT_DLV_DT_LAST')
                                
                                # 日付の変換（BloombergはdatetimeまたはNaTを返す可能性がある）
                                if pd.notna(last_tradeable):
                                    last_tradeable = pd.to_datetime(last_tradeable).date()
                                else:
                                    last_tradeable = None
                                    
                                if pd.notna(delivery_date):
                                    delivery_date = pd.to_datetime(delivery_date).date()
                                else:
                                    delivery_date = None
                                
                                # データベース更新
                                if self._update_maturity_dates(generic_id, last_tradeable, delivery_date, current_time):
                                    total_updated += 1
                                    
                    except Exception as e:
                        logger.error(f"{display_name} バッチ処理エラー: {e}")
                        
                logger.info(f"{display_name} 取引所処理完了")
            
            logger.info(f"\n=== 満期日情報更新完了: {total_updated}件更新 ===")
            
            # 更新結果の確認
            self._verify_update_results()
            
            return True
            
        except Exception as e:
            logger.error(f"満期日更新中にエラーが発生: {e}")
            return False
            
        finally:
            self.bloomberg.disconnect()
            self.db_manager.disconnect()
            
    def _get_all_generic_futures(self):
        """全ジェネリック先物情報を取得"""
        with self.db_manager.get_connection() as conn:
            query = """
                SELECT GenericID, GenericTicker, ExchangeCode, GenericNumber
                FROM M_GenericFutures 
                WHERE IsActive = 1
                ORDER BY ExchangeCode, GenericNumber
            """
            df = pd.read_sql(query, conn)
            return df
            
    def _update_maturity_dates(self, generic_id, last_tradeable, delivery_date, update_time):
        """満期日情報を更新"""
        try:
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()
                
                cursor.execute("""
                    UPDATE M_GenericFutures
                    SET 
                        LastTradeableDate = ?,
                        FutureDeliveryDateLast = ?,
                        LastRefreshDate = ?
                    WHERE GenericID = ?
                """, (last_tradeable, delivery_date, update_time, generic_id))
                
                conn.commit()
                return True
                
        except Exception as e:
            logger.error(f"GenericID {generic_id} の更新エラー: {e}")
            return False
            
    def _verify_update_results(self):
        """更新結果の確認"""
        logger.info("\n=== 更新結果確認 ===")
        
        with self.db_manager.get_connection() as conn:
            # 取引所別の更新状況
            query = """
                SELECT 
                    ExchangeCode,
                    COUNT(*) as TotalContracts,
                    COUNT(LastTradeableDate) as WithLastTradeable,
                    COUNT(FutureDeliveryDateLast) as WithDeliveryDate,
                    FORMAT(MIN(LastRefreshDate), 'yyyy-MM-dd HH:mm') as OldestRefresh,
                    FORMAT(MAX(LastRefreshDate), 'yyyy-MM-dd HH:mm') as NewestRefresh
                FROM M_GenericFutures
                GROUP BY ExchangeCode
                ORDER BY ExchangeCode
            """
            
            df = pd.read_sql(query, conn)
            logger.info("\n取引所別更新状況:")
            for _, row in df.iterrows():
                exchange = 'COMEX' if row['ExchangeCode'] == 'CMX' else row['ExchangeCode']
                logger.info(f"{exchange}: {row['WithLastTradeable']}/{row['TotalContracts']}件 "
                          f"(最終取引日: {row['WithLastTradeable']}件, 満期日: {row['WithDeliveryDate']}件)")
                          
            # サンプルデータ表示
            sample_query = """
                SELECT TOP 10
                    GenericTicker,
                    ExchangeCode,
                    GenericNumber,
                    LastTradeableDate,
                    FutureDeliveryDateLast,
                    DATEDIFF(day, LastTradeableDate, FutureDeliveryDateLast) as SettlementDays
                FROM M_GenericFutures
                WHERE LastTradeableDate IS NOT NULL
                ORDER BY ExchangeCode, GenericNumber
            """
            
            df_sample = pd.read_sql(sample_query, conn)
            logger.info("\nサンプルデータ:")
            logger.info(df_sample.to_string(index=False))

def execute_sql_updates():
    """SQLファイルを実行"""
    print("=== データベース構造更新 ===")
    
    conn_str = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=jcz.database.windows.net;"
        "DATABASE=JCL;"
        "UID=TKJCZ01;"
        "PWD=P@ssw0rdmbkazuresql;"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )
    
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    
    try:
        with open('sql/alter_generic_futures_add_bloomberg_dates.sql', 'r', encoding='utf-8') as f:
            sql_content = f.read()
            
        # GOステートメントで分割
        sql_statements = sql_content.split('\nGO\n')
        
        for i, stmt in enumerate(sql_statements):
            if stmt.strip() and not stmt.strip().startswith('-- 使用例'):
                print(f"\nステートメント {i+1} を実行中...")
                try:
                    cursor.execute(stmt)
                    conn.commit()
                    print("   完了")
                except Exception as e:
                    if "Column names in each table must be unique" in str(e):
                        print("   カラムは既に存在します")
                    else:
                        print(f"   エラー: {e}")
                    conn.rollback()
                    
    except Exception as e:
        print(f"ファイル読み込みエラー: {e}")
        
    conn.close()

def main():
    """メイン処理"""
    logger.info(f"実行開始時刻: {datetime.now()}")
    
    # 1. データベース構造更新
    execute_sql_updates()
    
    # 2. Bloombergから満期日情報を取得して更新
    updater = MaturityDateUpdater()
    success = updater.fetch_and_update_maturity_dates()
    
    if success:
        logger.info("満期日情報更新正常完了")
    else:
        logger.error("満期日情報更新失敗")
        
    return success

if __name__ == "__main__":
    main()