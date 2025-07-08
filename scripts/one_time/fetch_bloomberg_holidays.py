"""
BloombergからEXCH_MARKET_STATUS（取引所営業状況）を取得して休日カレンダーを更新
"""
import sys
import os
from datetime import datetime, date, timedelta
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

class TradingCalendarUpdater:
    """取引所営業日カレンダー更新クラス"""
    
    def __init__(self):
        self.bloomberg = BloombergDataFetcher()
        self.db_manager = DatabaseManager()
        
        # 取引所コードとBloombergティッカーのマッピング
        self.exchange_mapping = {
            'CMX': 'CMX Index',     # COMEX
            'LME': 'LME Index',     # LME
            'SHFE': 'SHFE Index'    # Shanghai Futures Exchange
        }
        
    def create_calendar_structure(self):
        """カレンダーテーブル構造を作成"""
        logger.info("=== カレンダーテーブル作成 ===")
        
        try:
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()
                
                # SQLファイルを実行
                with open('sql/create_trading_calendar.sql', 'r', encoding='utf-8') as f:
                    sql_content = f.read()
                    
                # GOステートメントで分割
                sql_statements = sql_content.split('\nGO\n')
                
                for stmt in sql_statements:
                    if stmt.strip() and not stmt.strip().startswith('-- 使用例'):
                        try:
                            cursor.execute(stmt)
                            conn.commit()
                        except Exception as e:
                            if "There is already an object named" in str(e):
                                logger.info("  オブジェクトは既に存在します")
                            else:
                                logger.error(f"  エラー: {e}")
                                
                # 初期カレンダー作成
                cursor.execute("EXEC sp_InitializeTradingCalendar")
                conn.commit()
                logger.info("初期カレンダー作成完了")
                
        except Exception as e:
            logger.error(f"カレンダー構造作成エラー: {e}")
            
    def fetch_and_update_holidays(self, start_date=None, end_date=None):
        """Bloombergから休日情報を取得してカレンダーを更新"""
        logger.info("=== 取引所休日情報更新開始 ===")
        
        try:
            # Bloomberg API接続
            if not self.bloomberg.connect():
                raise ConnectionError("Bloomberg API接続に失敗しました")
                
            # データベース接続
            self.db_manager.connect()
            
            # 日付範囲設定（デフォルト: 過去1年から未来2年）
            if not start_date:
                start_date = (datetime.now() - timedelta(days=365)).strftime('%Y%m%d')
            if not end_date:
                end_date = (datetime.now() + timedelta(days=730)).strftime('%Y%m%d')
                
            logger.info(f"取得期間: {start_date} - {end_date}")
            
            # 各取引所の休日情報を取得
            for exchange_code, bloomberg_ticker in self.exchange_mapping.items():
                logger.info(f"\n--- {exchange_code} 取引所処理開始 ---")
                
                try:
                    # EXCH_MARKET_STATUSフィールドで市場状態を取得
                    # 1 = Open, 0 = Closed
                    fields = ['EXCH_MARKET_STATUS']
                    
                    # ヒストリカルデータとして取得
                    market_status = self.bloomberg.get_historical_data(
                        [bloomberg_ticker], 
                        fields, 
                        start_date, 
                        end_date
                    )
                    
                    if not market_status.empty:
                        # 休日を特定して更新
                        self._update_holidays_from_status(exchange_code, market_status)
                    else:
                        logger.warning(f"{exchange_code}: 市場状態データなし")
                        
                    # 代替方法: 価格データから推測
                    self._infer_holidays_from_price_data(exchange_code, start_date, end_date)
                        
                except Exception as e:
                    logger.error(f"{exchange_code} 処理エラー: {e}")
                    
            logger.info("\n=== 休日情報更新完了 ===")
            
            # 更新結果の確認
            self._verify_calendar_update()
            
            return True
            
        except Exception as e:
            logger.error(f"休日更新中にエラーが発生: {e}")
            return False
            
        finally:
            self.bloomberg.disconnect()
            self.db_manager.disconnect()
            
    def _update_holidays_from_status(self, exchange_code, market_status_df):
        """市場状態データから休日を更新"""
        try:
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()
                
                for _, row in market_status_df.iterrows():
                    trade_date = pd.to_datetime(row['date']).date()
                    status = row.get('EXCH_MARKET_STATUS')
                    
                    if pd.notna(status):
                        is_trading = int(status) == 1
                        
                        # カレンダー更新
                        cursor.execute("""
                            UPDATE M_TradingCalendar
                            SET 
                                IsTradingDay = ?,
                                IsHoliday = ?,
                                HolidayType = CASE WHEN ? = 0 THEN 'ExchangeHoliday' ELSE HolidayType END,
                                LastUpdated = GETDATE()
                            WHERE ExchangeCode = ? AND CalendarDate = ?
                        """, (is_trading, not is_trading, is_trading, exchange_code, trade_date))
                        
                conn.commit()
                logger.info(f"{exchange_code}: {len(market_status_df)}日分の市場状態を更新")
                
        except Exception as e:
            logger.error(f"市場状態更新エラー: {e}")
            
    def _infer_holidays_from_price_data(self, exchange_code, start_date, end_date):
        """価格データから休日を推測"""
        try:
            # 主要な銘柄の価格データを取得
            test_tickers = {
                'CMX': ['HG1 Comdty'],  # COMEX銅
                'LME': ['LP1 Comdty'],  # LME銅
                'SHFE': ['CU1 Comdty']  # SHFE銅
            }
            
            if exchange_code not in test_tickers:
                return
                
            # 価格データ取得
            price_data = self.bloomberg.get_historical_data(
                test_tickers[exchange_code],
                ['PX_LAST', 'VOLUME'],
                start_date,
                end_date
            )
            
            if price_data.empty:
                return
                
            # 取引があった日を特定
            trading_dates = set(pd.to_datetime(price_data['date']).dt.date)
            
            # 期間内の全日付
            start_dt = datetime.strptime(start_date, '%Y%m%d').date()
            end_dt = datetime.strptime(end_date, '%Y%m%d').date()
            all_dates = set()
            current_dt = start_dt
            while current_dt <= end_dt:
                if current_dt.weekday() < 5:  # 平日のみ
                    all_dates.add(current_dt)
                current_dt += timedelta(days=1)
                
            # 取引がなかった平日を休日として推測
            inferred_holidays = all_dates - trading_dates
            
            if inferred_holidays:
                with self.db_manager.get_connection() as conn:
                    cursor = conn.cursor()
                    
                    for holiday_date in inferred_holidays:
                        cursor.execute("""
                            UPDATE M_TradingCalendar
                            SET 
                                IsTradingDay = 0,
                                IsHoliday = 1,
                                HolidayType = 'InferredHoliday',
                                HolidayName = 'Inferred from no trading activity',
                                LastUpdated = GETDATE()
                            WHERE ExchangeCode = ? AND CalendarDate = ?
                        """, (exchange_code, holiday_date))
                        
                    conn.commit()
                    logger.info(f"{exchange_code}: {len(inferred_holidays)}日分の休日を推測")
                    
        except Exception as e:
            logger.error(f"休日推測エラー: {e}")
            
    def _verify_calendar_update(self):
        """カレンダー更新結果の確認"""
        logger.info("\n=== カレンダー更新結果確認 ===")
        
        with self.db_manager.get_connection() as conn:
            # 取引所別の休日数
            query = """
                SELECT 
                    ExchangeCode,
                    COUNT(*) as TotalDays,
                    SUM(CASE WHEN IsTradingDay = 1 THEN 1 ELSE 0 END) as TradingDays,
                    SUM(CASE WHEN IsHoliday = 1 THEN 1 ELSE 0 END) as Holidays,
                    SUM(CASE WHEN HolidayType = 'Weekend' THEN 1 ELSE 0 END) as Weekends,
                    SUM(CASE WHEN HolidayType = 'ExchangeHoliday' THEN 1 ELSE 0 END) as ExchangeHolidays,
                    SUM(CASE WHEN HolidayType = 'InferredHoliday' THEN 1 ELSE 0 END) as InferredHolidays
                FROM M_TradingCalendar
                WHERE CalendarDate >= DATEADD(YEAR, -1, GETDATE())
                    AND CalendarDate <= DATEADD(YEAR, 1, GETDATE())
                GROUP BY ExchangeCode
                ORDER BY ExchangeCode
            """
            
            df = pd.read_sql(query, conn)
            logger.info("\n取引所別カレンダー統計（過去1年～未来1年）:")
            for _, row in df.iterrows():
                logger.info(f"\n{row['ExchangeCode']}:")
                logger.info(f"  総日数: {row['TotalDays']}")
                logger.info(f"  営業日: {row['TradingDays']}")
                logger.info(f"  休日: {row['Holidays']} (週末: {row['Weekends']}, "
                          f"取引所休日: {row['ExchangeHolidays']}, 推測: {row['InferredHolidays']})")
                          
            # 最近の休日サンプル
            holiday_query = """
                SELECT TOP 10
                    ExchangeCode,
                    CalendarDate,
                    HolidayName,
                    HolidayType
                FROM M_TradingCalendar
                WHERE IsHoliday = 1
                    AND HolidayType != 'Weekend'
                    AND CalendarDate >= DATEADD(MONTH, -6, GETDATE())
                ORDER BY CalendarDate DESC
            """
            
            df_holidays = pd.read_sql(holiday_query, conn)
            if not df_holidays.empty:
                logger.info("\n最近の取引所休日:")
                logger.info(df_holidays.to_string(index=False))

def execute_full_update():
    """完全な営業日カレンダー更新を実行"""
    logger.info(f"実行開始時刻: {datetime.now()}")
    
    updater = TradingCalendarUpdater()
    
    # 1. カレンダー構造作成
    updater.create_calendar_structure()
    
    # 2. データベース接続（構造作成用）
    updater.db_manager.connect()
    
    # 3. 休日情報取得・更新
    success = updater.fetch_and_update_holidays()
    
    if success:
        logger.info("営業日カレンダー更新正常完了")
        
        # 4. 営業日ベースの計算テスト
        test_trading_days_calculation()
    else:
        logger.error("営業日カレンダー更新失敗")
        
    return success

def test_trading_days_calculation():
    """営業日ベースの計算テスト"""
    logger.info("\n=== 営業日ベース計算テスト ===")
    
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
    
    # テストクエリ
    query = """
    SELECT TOP 10
        ExchangeDisplayName,
        GenericTicker,
        TradeDate,
        LastTradeableDate,
        CalendarDaysRemaining as 暦日残,
        TradingDaysRemaining as 営業日残,
        CalendarDaysRemaining - TradingDaysRemaining as 休日数,
        TradingDayRolloverDate as ロール推奨日,
        TradingDayShouldRollover as ロール要否
    FROM V_CommodityPriceWithTradingDays
    WHERE TradeDate = (SELECT MAX(TradeDate) FROM V_CommodityPriceWithTradingDays)
        AND TradingDaysRemaining IS NOT NULL
        AND Volume > 0
    ORDER BY TradingDaysRemaining
    """
    
    df = pd.read_sql(query, conn)
    logger.info("\n営業日ベースの満期情報:")
    logger.info(df.to_string(index=False))
    
    conn.close()

def main():
    """メイン処理"""
    return execute_full_update()

if __name__ == "__main__":
    main()