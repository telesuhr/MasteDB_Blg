"""
取引カレンダーを2000年から2039年まで拡張
"""
import pyodbc
import pandas as pd
from datetime import datetime, date, timedelta
from config.logging_config import logger

# データベース接続文字列
CONNECTION_STRING = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=jcz.database.windows.net;"
    "DATABASE=JCL;"
    "UID=TKJCZ01;"
    "PWD=P@ssw0rdmbkazuresql;"
    "Encrypt=yes;"
    "TrustServerCertificate=no;"
    "Connection Timeout=30;"
)

def expand_calendar_range():
    """カレンダー範囲を2000-2039年に拡張"""
    logger.info("=== カレンダー範囲拡張開始 ===")
    
    conn = pyodbc.connect(CONNECTION_STRING)
    cursor = conn.cursor()
    
    try:
        # 現在のデータ範囲を確認
        cursor.execute("""
            SELECT 
                MIN(CalendarDate) as MinDate,
                MAX(CalendarDate) as MaxDate,
                COUNT(DISTINCT ExchangeCode) as ExchangeCount
            FROM M_TradingCalendar
        """)
        
        result = cursor.fetchone()
        current_min = result[0]
        current_max = result[1]
        exchange_count = result[2]
        
        logger.info(f"現在の範囲: {current_min} ～ {current_max}")
        logger.info(f"取引所数: {exchange_count}")
        
        # 対象取引所
        exchanges = ['CMX', 'LME', 'SHFE']
        
        # 拡張する期間
        new_start = date(2000, 1, 1)
        new_end = date(2039, 12, 31)
        
        # バッチ処理用の変数
        batch_size = 365  # 1年分ずつ処理
        total_inserted = 0
        
        for exchange in exchanges:
            logger.info(f"\n--- {exchange} 取引所処理開始 ---")
            exchange_inserted = 0
            
            # 開始日より前のデータを追加
            if new_start < current_min:
                logger.info(f"過去データ追加: {new_start} ～ {current_min - timedelta(days=1)}")
                current_date = new_start
                
                while current_date < current_min:
                    batch_end = min(current_date + timedelta(days=batch_size-1), current_min - timedelta(days=1))
                    inserted = insert_calendar_batch(cursor, exchange, current_date, batch_end)
                    exchange_inserted += inserted
                    current_date = batch_end + timedelta(days=1)
                    
                    # 定期的にコミット
                    if exchange_inserted % 1000 == 0:
                        conn.commit()
                        logger.info(f"  {exchange_inserted}件追加済み...")
            
            # 終了日より後のデータを追加
            if new_end > current_max:
                logger.info(f"将来データ追加: {current_max + timedelta(days=1)} ～ {new_end}")
                current_date = current_max + timedelta(days=1)
                
                while current_date <= new_end:
                    batch_end = min(current_date + timedelta(days=batch_size-1), new_end)
                    inserted = insert_calendar_batch(cursor, exchange, current_date, batch_end)
                    exchange_inserted += inserted
                    current_date = batch_end + timedelta(days=1)
                    
                    # 定期的にコミット
                    if exchange_inserted % 1000 == 0:
                        conn.commit()
                        logger.info(f"  {exchange_inserted}件追加済み...")
            
            conn.commit()
            logger.info(f"{exchange}: {exchange_inserted}件追加完了")
            total_inserted += exchange_inserted
        
        logger.info(f"\n=== 合計 {total_inserted}件のカレンダーデータを追加 ===")
        
        # 最終的なデータ範囲を確認
        cursor.execute("""
            SELECT 
                ExchangeCode,
                MIN(CalendarDate) as MinDate,
                MAX(CalendarDate) as MaxDate,
                COUNT(*) as TotalDays,
                SUM(CAST(IsTradingDay as INT)) as TradingDays
            FROM M_TradingCalendar
            GROUP BY ExchangeCode
            ORDER BY ExchangeCode
        """)
        
        logger.info("\n【拡張後のカレンダー範囲】")
        for row in cursor.fetchall():
            logger.info(f"{row[0]}: {row[1]} ～ {row[2]} ({row[3]}日間、営業日{row[4]}日)")
        
        # 年別の統計
        cursor.execute("""
            SELECT 
                YEAR(CalendarDate) as Year,
                COUNT(DISTINCT ExchangeCode) as Exchanges,
                COUNT(*) as TotalRecords,
                SUM(CAST(IsTradingDay as INT)) as TradingDays
            FROM M_TradingCalendar
            WHERE YEAR(CalendarDate) IN (2000, 2010, 2020, 2030, 2039)
            GROUP BY YEAR(CalendarDate)
            ORDER BY Year
        """)
        
        logger.info("\n【主要年の統計】")
        for row in cursor.fetchall():
            avg_trading_days = row[3] / row[1]  # 取引所あたりの平均営業日
            logger.info(f"{row[0]}年: {row[1]}取引所、{row[2]}レコード、平均{avg_trading_days:.0f}営業日/取引所")
        
    except Exception as e:
        logger.error(f"カレンダー拡張エラー: {e}")
        conn.rollback()
        raise
        
    finally:
        conn.close()

def insert_calendar_batch(cursor, exchange_code, start_date, end_date):
    """カレンダーデータをバッチ挿入"""
    inserted_count = 0
    current_date = start_date
    
    # バッチ挿入用のデータを準備
    values = []
    
    while current_date <= end_date:
        # 既存レコードの確認は省略（UNIQUE制約で重複は自動的に防がれる）
        is_weekend = current_date.weekday() in [5, 6]  # 土曜(5)、日曜(6)
        
        values.append((
            exchange_code,
            current_date,
            0 if is_weekend else 1,  # 平日は営業日と仮定
            1 if is_weekend else 0,  # 週末は休日
            'Weekend' if is_weekend else None
        ))
        
        current_date += timedelta(days=1)
        
        # 100件ごとにバッチ挿入
        if len(values) >= 100:
            inserted_count += insert_values_batch(cursor, values)
            values = []
    
    # 残りのデータを挿入
    if values:
        inserted_count += insert_values_batch(cursor, values)
    
    return inserted_count

def insert_values_batch(cursor, values):
    """値のバッチ挿入"""
    if not values:
        return 0
        
    try:
        # バッチ挿入のSQLを構築
        placeholders = ','.join(['(?, ?, ?, ?, ?, NULL, GETDATE())'] * len(values))
        sql = f"""
            INSERT INTO M_TradingCalendar 
            (ExchangeCode, CalendarDate, IsTradingDay, IsHoliday, HolidayType, HolidayName, LastUpdated)
            VALUES {placeholders}
        """
        
        # フラット化したパラメータリスト
        params = []
        for v in values:
            params.extend(v)
        
        cursor.execute(sql, params)
        return len(values)
        
    except Exception as e:
        # 重複エラーは無視
        if "Violation of UNIQUE KEY constraint" in str(e):
            return 0
        else:
            raise

def update_historical_holidays():
    """過去の取引データから休日を更新"""
    logger.info("\n=== 過去の休日情報更新 ===")
    
    conn = pyodbc.connect(CONNECTION_STRING)
    cursor = conn.cursor()
    
    try:
        # 各取引所の取引実績がある日を特定
        cursor.execute("""
            WITH TradingDates AS (
                SELECT DISTINCT
                    g.ExchangeCode,
                    p.TradeDate
                FROM T_CommodityPrice_V2 p
                INNER JOIN M_GenericFutures g ON p.GenericID = g.GenericID
                WHERE p.DataType = 'Generic'
                    AND p.Volume > 0
            )
            UPDATE tc
            SET 
                IsTradingDay = 1,
                IsHoliday = 0,
                HolidayType = NULL,
                LastUpdated = GETDATE()
            FROM M_TradingCalendar tc
            INNER JOIN TradingDates td 
                ON tc.ExchangeCode = td.ExchangeCode 
                AND tc.CalendarDate = td.TradeDate
            WHERE tc.IsTradingDay = 0
        """)
        
        updated_count = cursor.rowcount
        conn.commit()
        
        logger.info(f"取引実績から{updated_count}件の営業日を更新")
        
        # 年別の営業日数を再計算
        cursor.execute("""
            SELECT 
                ExchangeCode,
                YEAR(CalendarDate) as Year,
                COUNT(*) as TotalDays,
                SUM(CAST(IsTradingDay as INT)) as TradingDays,
                CAST(SUM(CAST(IsTradingDay as INT)) * 100.0 / COUNT(*) as DECIMAL(5,2)) as TradingDayRate
            FROM M_TradingCalendar
            WHERE YEAR(CalendarDate) BETWEEN 2020 AND 2025
            GROUP BY ExchangeCode, YEAR(CalendarDate)
            ORDER BY ExchangeCode, Year
        """)
        
        logger.info("\n【2020-2025年の営業日率】")
        for row in cursor.fetchall():
            logger.info(f"{row[0]} {row[1]}年: {row[3]}/{row[2]}日 ({row[4]}%)")
            
    except Exception as e:
        logger.error(f"休日更新エラー: {e}")
        conn.rollback()
        
    finally:
        conn.close()

def test_expanded_calendar():
    """拡張されたカレンダーのテスト"""
    logger.info("\n=== 拡張カレンダーテスト ===")
    
    conn = pyodbc.connect(CONNECTION_STRING)
    
    # テストクエリ
    query = """
    SELECT 
        ExchangeCode,
        -- 2000年代の営業日
        SUM(CASE WHEN YEAR(CalendarDate) BETWEEN 2000 AND 2009 AND IsTradingDay = 1 THEN 1 ELSE 0 END) as Days_2000s,
        -- 2010年代の営業日
        SUM(CASE WHEN YEAR(CalendarDate) BETWEEN 2010 AND 2019 AND IsTradingDay = 1 THEN 1 ELSE 0 END) as Days_2010s,
        -- 2020年代の営業日
        SUM(CASE WHEN YEAR(CalendarDate) BETWEEN 2020 AND 2029 AND IsTradingDay = 1 THEN 1 ELSE 0 END) as Days_2020s,
        -- 2030年代の営業日
        SUM(CASE WHEN YEAR(CalendarDate) BETWEEN 2030 AND 2039 AND IsTradingDay = 1 THEN 1 ELSE 0 END) as Days_2030s,
        -- 全期間の営業日
        SUM(CAST(IsTradingDay as INT)) as TotalTradingDays
    FROM M_TradingCalendar
    GROUP BY ExchangeCode
    ORDER BY ExchangeCode
    """
    
    df = pd.read_sql(query, conn)
    
    logger.info("\n【年代別営業日数】")
    print(df.to_string(index=False))
    
    # 営業日計算のテスト
    cursor = conn.cursor()
    cursor.execute("""
        SELECT 
            dbo.GetTradingDaysBetween('2000-01-01', '2000-12-31', 'CMX') as Y2000_CMX,
            dbo.GetTradingDaysBetween('2020-01-01', '2020-12-31', 'CMX') as Y2020_CMX,
            dbo.GetTradingDaysBetween('2039-01-01', '2039-12-31', 'CMX') as Y2039_CMX
    """)
    
    result = cursor.fetchone()
    logger.info("\n【営業日計算テスト】")
    logger.info(f"2000年のCMX営業日: {result[0]}日")
    logger.info(f"2020年のCMX営業日: {result[1]}日")
    logger.info(f"2039年のCMX営業日: {result[2]}日")
    
    conn.close()

def main():
    """メイン処理"""
    logger.info(f"実行開始時刻: {datetime.now()}")
    
    try:
        # 1. カレンダー範囲拡張
        expand_calendar_range()
        
        # 2. 過去の休日情報更新
        update_historical_holidays()
        
        # 3. テスト実行
        test_expanded_calendar()
        
        logger.info("\nカレンダー拡張正常完了")
        
    except Exception as e:
        logger.error(f"カレンダー拡張失敗: {e}")
        
    logger.info(f"\n実行終了時刻: {datetime.now()}")

if __name__ == "__main__":
    main()