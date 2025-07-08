"""
営業日カレンダーのテストと結果確認
"""
import pyodbc
import pandas as pd
from datetime import datetime

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

def create_simple_views():
    """簡易版ビューを作成"""
    print("=== 簡易版ビュー作成 ===")
    
    conn = pyodbc.connect(CONNECTION_STRING)
    cursor = conn.cursor()
    
    try:
        with open('sql/create_trading_calendar_simple.sql', 'r', encoding='utf-8') as f:
            sql_content = f.read()
            
        # GOステートメントで分割
        sql_statements = sql_content.split('\nGO\n')
        
        for i, stmt in enumerate(sql_statements):
            if stmt.strip():
                print(f"\nステートメント {i+1} を実行中...")
                try:
                    cursor.execute(stmt)
                    conn.commit()
                    print("   完了")
                except Exception as e:
                    print(f"   エラー: {e}")
                    conn.rollback()
                    
    except Exception as e:
        print(f"ファイル読み込みエラー: {e}")
        
    conn.close()

def test_trading_days():
    """営業日ベースの計算テスト"""
    print("\n=== 営業日ベース計算テスト ===")
    
    conn = pyodbc.connect(CONNECTION_STRING)
    
    # 1. カレンダー統計
    query1 = """
    SELECT 
        ExchangeCode,
        COUNT(DISTINCT Year) as 年数,
        SUM(TotalDays) as 総日数,
        SUM(TradingDays) as 営業日数,
        SUM(Holidays) as 休日数,
        CAST(SUM(TradingDays) * 100.0 / SUM(TotalDays) as DECIMAL(5,2)) as 営業日率
    FROM V_TradingCalendarSummary
    WHERE Year IN (2025, 2026)
    GROUP BY ExchangeCode
    ORDER BY ExchangeCode
    """
    
    df1 = pd.read_sql(query1, conn)
    print("\n【取引所別カレンダー統計（2025-2026年）】")
    print(df1.to_string(index=False))
    
    # 2. 営業日ベースの満期情報
    query2 = """
    SELECT TOP 15
        ExchangeDisplayName,
        GenericTicker,
        TradeDate,
        LastTradeableDate,
        CalendarDaysRemaining as 暦日残,
        TradingDaysRemaining as 営業日残,
        HolidaysInPeriod as 期間内休日,
        CASE 
            WHEN CalendarDaysRemaining > 0 
            THEN CAST(TradingDaysRemaining * 100.0 / CalendarDaysRemaining as DECIMAL(5,2))
            ELSE NULL 
        END as 営業日率,
        Volume
    FROM V_CommodityPriceWithTradingDays
    WHERE TradeDate = (SELECT MAX(TradeDate) FROM V_CommodityPriceWithTradingDays)
        AND TradingDaysRemaining IS NOT NULL
        AND Volume > 0
    ORDER BY TradingDaysRemaining
    """
    
    df2 = pd.read_sql(query2, conn)
    print("\n\n【営業日ベース満期情報（最も近い15件）】")
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    print(df2.to_string(index=False))
    
    # 3. 取引所別の営業日率比較
    query3 = """
    SELECT 
        ExchangeCode,
        COUNT(DISTINCT GenericTicker) as 銘柄数,
        AVG(CalendarDaysRemaining) as 平均暦日,
        AVG(TradingDaysRemaining) as 平均営業日,
        AVG(HolidaysInPeriod) as 平均休日数,
        CAST(AVG(TradingDaysRemaining * 100.0 / NULLIF(CalendarDaysRemaining, 0)) as DECIMAL(5,2)) as 平均営業日率
    FROM V_CommodityPriceWithTradingDays
    WHERE TradeDate = (SELECT MAX(TradeDate) FROM V_CommodityPriceWithTradingDays)
        AND TradingDaysRemaining IS NOT NULL
        AND CalendarDaysRemaining > 0
        AND Volume > 0
    GROUP BY ExchangeCode
    ORDER BY ExchangeCode
    """
    
    df3 = pd.read_sql(query3, conn)
    print("\n\n【取引所別営業日率分析】")
    print(df3.to_string(index=False))
    
    # 4. 直近の休日確認
    query4 = """
    SELECT TOP 20
        ExchangeCode,
        CalendarDate,
        DayOfWeek,
        HolidayName,
        HolidayType
    FROM V_UpcomingHolidays
    WHERE HolidayType != 'Weekend'
    ORDER BY CalendarDate
    """
    
    df4 = pd.read_sql(query4, conn)
    if not df4.empty:
        print("\n\n【今後の取引所休日（週末除く）】")
        print(df4.to_string(index=False))
    else:
        print("\n\n今後3ヶ月間に週末以外の休日はありません。")
    
    # 5. 具体的な日数計算例
    query5 = """
    SELECT TOP 5
        GenericTicker,
        FORMAT(TradeDate, 'yyyy-MM-dd') as 取引日,
        FORMAT(LastTradeableDate, 'yyyy-MM-dd') as 最終取引日,
        CalendarDaysRemaining as 暦日,
        TradingDaysRemaining as 営業日,
        HolidaysInPeriod as 休日,
        CONCAT(
            '暦日: ', CalendarDaysRemaining, '日 = ',
            '営業日: ', TradingDaysRemaining, '日 + ',
            '休日: ', HolidaysInPeriod, '日'
        ) as 計算式
    FROM V_CommodityPriceWithTradingDays
    WHERE TradeDate = (SELECT MAX(TradeDate) FROM V_CommodityPriceWithTradingDays)
        AND TradingDaysRemaining IS NOT NULL
        AND Volume > 0
    ORDER BY TradingDaysRemaining
    """
    
    df5 = pd.read_sql(query5, conn)
    print("\n\n【営業日計算の具体例】")
    print(df5.to_string(index=False))
    
    conn.close()

def main():
    """メイン処理"""
    print(f"実行時刻: {datetime.now()}")
    
    # ビュー作成
    create_simple_views()
    
    # テスト実行
    test_trading_days()
    
    print("\n=== 完了 ===")

if __name__ == "__main__":
    main()