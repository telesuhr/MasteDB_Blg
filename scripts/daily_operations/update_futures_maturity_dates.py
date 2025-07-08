"""
CMXとSHFEの先物満期日情報を更新
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), 'src'))

from bloomberg_api import BloombergDataFetcher
from database import DatabaseManager
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def update_futures_maturity_dates():
    """全ての先物の満期日情報を更新"""
    
    bloomberg = BloombergDataFetcher()
    db_manager = DatabaseManager()
    
    if not bloomberg.connect():
        logger.error("Bloomberg API接続失敗")
        return False
        
    try:
        db_manager.connect()
        
        # 更新対象の先物を取得
        with db_manager.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT GenericID, GenericTicker, ExchangeCode
                FROM M_GenericFutures
                WHERE IsActive = 1
                ORDER BY ExchangeCode, GenericNumber
            """)
            
            futures = cursor.fetchall()
            logger.info(f"更新対象: {len(futures)}件の先物")
            
            # 取引所別に集計
            exchange_counts = {}
            for _, _, exchange in futures:
                exchange_counts[exchange] = exchange_counts.get(exchange, 0) + 1
            
            for exchange, count in exchange_counts.items():
                logger.info(f"  {exchange}: {count}件")
            
            # バッチでリファレンスデータを取得
            tickers = [f[1] for f in futures]
            fields = ['LAST_TRADEABLE_DT', 'FUT_DLV_DT_LAST', 'FUT_CONTRACT_DT']
            
            logger.info("Bloombergからリファレンスデータ取得中...")
            df = bloomberg.get_reference_data(tickers, fields)
            
            if df.empty:
                logger.warning("データが取得できませんでした")
                return False
                
            # データベースを更新
            update_count = 0
            for _, row in df.iterrows():
                ticker = row['security']
                last_tradeable_dt = row.get('LAST_TRADEABLE_DT')
                fut_dlv_dt_last = row.get('FUT_DLV_DT_LAST')
                fut_contract_dt = row.get('FUT_CONTRACT_DT')
                
                # GenericIDを取得
                generic_id = next((f[0] for f in futures if f[1] == ticker), None)
                if not generic_id:
                    continue
                    
                # 日付の変換
                def convert_to_date(date_value):
                    if date_value is None:
                        return None
                    if hasattr(date_value, 'date'):
                        return date_value.date()
                    elif isinstance(date_value, str):
                        # 複数の日付フォーマットに対応
                        for fmt in ['%Y-%m-%d', '%m/%Y', '%Y/%m/%d', '%m/%d/%Y']:
                            try:
                                return datetime.strptime(date_value, fmt).date()
                            except ValueError:
                                continue
                        logger.warning(f"Unknown date format: {date_value}")
                        return None
                    return date_value
                
                last_tradeable_dt = convert_to_date(last_tradeable_dt)
                fut_dlv_dt_last = convert_to_date(fut_dlv_dt_last)
                
                # FUT_CONTRACT_DTは月/年のフォーマット（例：07/2027）なので特別処理
                if fut_contract_dt and isinstance(fut_contract_dt, str) and '/' in fut_contract_dt:
                    # 月/年を年-月-01の日付に変換
                    try:
                        parts = fut_contract_dt.split('/')
                        if len(parts) == 2:
                            month, year = parts
                            fut_contract_dt = datetime(int(year), int(month), 1).date()
                    except:
                        logger.warning(f"Could not parse contract date: {fut_contract_dt}")
                        fut_contract_dt = None
                else:
                    fut_contract_dt = convert_to_date(fut_contract_dt)
                    
                # データベース更新
                try:
                    cursor.execute("""
                        UPDATE M_GenericFutures
                        SET LastTradeableDate = ?,
                            FutureDeliveryDateLast = ?
                        WHERE GenericID = ?
                    """, (last_tradeable_dt, fut_dlv_dt_last, generic_id))
                except Exception as e:
                    logger.error(f"Failed to update {ticker}: {e}")
                    continue
                      
                update_count += 1
                
                # ログ出力
                exchange = next((f[2] for f in futures if f[0] == generic_id), '')
                logger.info(f"{ticker} ({exchange}): 最終取引日={last_tradeable_dt}, 受渡日={fut_dlv_dt_last}")
                
            conn.commit()
            logger.info(f"{update_count}件の先物情報を更新しました")
            
            # 更新結果の確認
            cursor.execute("""
                SELECT 
                    ExchangeCode,
                    COUNT(*) as Total,
                    COUNT(LastTradeableDate) as HasLastTradeable,
                    COUNT(FutureDeliveryDateLast) as HasDelivery
                FROM M_GenericFutures
                WHERE IsActive = 1
                GROUP BY ExchangeCode
                ORDER BY ExchangeCode
            """)
            
            logger.info("\n更新結果:")
            logger.info("取引所 | 総数 | 最終取引日 | 受渡日")
            logger.info("-" * 40)
            for row in cursor.fetchall():
                logger.info(f"{row[0]:6} | {row[1]:4} | {row[2]:10} | {row[3]:6}")
                
        return True
        
    except Exception as e:
        logger.error(f"エラー: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False
        
    finally:
        bloomberg.disconnect()
        db_manager.disconnect()

if __name__ == "__main__":
    if update_futures_maturity_dates():
        logger.info("正常に完了しました")
    else:
        logger.error("エラーが発生しました")