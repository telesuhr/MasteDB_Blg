"""
営業日計算の問題を修正
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

def diagnose_calendar_issue():
    """カレンダーの問題を診断"""
    print("=== カレンダー問題診断 ===")
    
    conn = pyodbc.connect(CONNECTION_STRING)
    
    # 1. カレンダーテーブルのデータ範囲確認
    query1 = """
    SELECT 
        ExchangeCode,
        MIN(CalendarDate) as MinDate,
        MAX(CalendarDate) as MaxDate,
        COUNT(*) as TotalRecords,
        SUM(CAST(IsTradingDay as INT)) as TradingDays
    FROM M_TradingCalendar
    GROUP BY ExchangeCode
    """
    
    df1 = pd.read_sql(query1, conn)
    print("\n【カレンダーテーブルのデータ範囲】")
    print(df1.to_string(index=False))
    
    # 2. 特定期間のカレンダー確認（例：2025-07-08から2025-07-29）
    query2 = """
    SELECT TOP 30
        ExchangeCode,
        CalendarDate,
        DATENAME(WEEKDAY, CalendarDate) as DayOfWeek,
        IsTradingDay,
        HolidayType
    FROM M_TradingCalendar
    WHERE ExchangeCode = 'CMX'
        AND CalendarDate BETWEEN '2025-07-08' AND '2025-07-29'
    ORDER BY CalendarDate
    """
    
    df2 = pd.read_sql(query2, conn)
    print("\n\n【CMX 2025-07-08～2025-07-29のカレンダー】")
    print(df2.to_string(index=False))
    
    # 3. GetTradingDaysBetween関数の動作確認
    cursor = conn.cursor()
    cursor.execute("""
        SELECT 
            dbo.GetTradingDaysBetween('2025-07-08', '2025-07-15', 'SHFE') as SHFE_7Days,
            dbo.GetTradingDaysBetween('2025-07-08', '2025-07-29', 'CMX') as CMX_21Days,
            dbo.GetTradingDaysBetween('2025-07-08', '2025-09-26', 'CMX') as CMX_80Days
    """)
    
    result = cursor.fetchone()
    print("\n\n【関数テスト結果】")
    print(f"SHFE 7/8-7/15 (7日間): {result[0]}営業日")
    print(f"CMX 7/8-7/29 (21日間): {result[1]}営業日")
    print(f"CMX 7/8-9/26 (80日間): {result[2]}営業日")
    
    # 4. 関数のロジック修正版を作成
    print("\n\n【修正が必要な点】")
    print("現在の関数は > と <= を使っているため、開始日を含まない計算になっています。")
    print("正しくは開始日を含めて計算する必要があります。")
    
    conn.close()

def fix_trading_days_function():
    """営業日計算関数を修正"""
    print("\n=== 営業日計算関数の修正 ===")
    
    conn = pyodbc.connect(CONNECTION_STRING)
    cursor = conn.cursor()
    
    try:
        # 既存の関数を削除
        cursor.execute("DROP FUNCTION IF EXISTS dbo.GetTradingDaysBetween")
        conn.commit()
        
        # 修正版の関数を作成（開始日を含めない、終了日を含める）
        cursor.execute("""
        CREATE FUNCTION dbo.GetTradingDaysBetween
        (
            @StartDate DATE,
            @EndDate DATE,
            @ExchangeCode NVARCHAR(10)
        )
        RETURNS INT
        AS
        BEGIN
            DECLARE @TradingDays INT;
            
            -- カレンダーテーブルから営業日数を計算
            -- 開始日は含めない、終了日は含める
            SELECT @TradingDays = COUNT(*)
            FROM M_TradingCalendar
            WHERE ExchangeCode = @ExchangeCode
                AND CalendarDate > @StartDate
                AND CalendarDate <= @EndDate
                AND IsTradingDay = 1;
            
            -- データがない場合は簡易計算（土日除外）
            IF @TradingDays = 0 OR @TradingDays IS NULL
            BEGIN
                DECLARE @CurrentDate DATE = DATEADD(DAY, 1, @StartDate);
                SET @TradingDays = 0;
                
                WHILE @CurrentDate <= @EndDate
                BEGIN
                    IF DATEPART(WEEKDAY, @CurrentDate) NOT IN (1, 7)
                        SET @TradingDays = @TradingDays + 1;
                    SET @CurrentDate = DATEADD(DAY, 1, @CurrentDate);
                END
            END
            
            RETURN @TradingDays;
        END;
        """)
        conn.commit()
        print("関数を修正しました（簡易計算ロジック付き）")
        
        # 修正後のテスト
        cursor.execute("""
            SELECT 
                dbo.GetTradingDaysBetween('2025-07-08', '2025-07-15', 'SHFE') as SHFE_7Days,
                dbo.GetTradingDaysBetween('2025-07-08', '2025-07-29', 'CMX') as CMX_21Days,
                dbo.GetTradingDaysBetween('2025-07-08', '2025-09-26', 'CMX') as CMX_80Days
        """)
        
        result = cursor.fetchone()
        print("\n【修正後の関数テスト結果】")
        print(f"SHFE 7/8-7/15 (7暦日): {result[0]}営業日")
        print(f"CMX 7/8-7/29 (21暦日): {result[1]}営業日")
        print(f"CMX 7/8-9/26 (80暦日): {result[2]}営業日")
        
    except Exception as e:
        print(f"エラー: {e}")
        conn.rollback()
        
    conn.close()

def test_fixed_calculation():
    """修正後の計算をテスト"""
    print("\n=== 修正後の営業日計算テスト ===")
    
    conn = pyodbc.connect(CONNECTION_STRING)
    
    # 営業日ベースの満期情報を再確認
    query = """
    SELECT TOP 10
        ExchangeDisplayName,
        GenericTicker,
        FORMAT(TradeDate, 'MM/dd') as 取引日,
        FORMAT(LastTradeableDate, 'MM/dd') as 最終取引日,
        CalendarDaysRemaining as 暦日,
        TradingDaysRemaining as 営業日,
        HolidaysInPeriod as 休日数,
        CASE 
            WHEN TradingDaysRemaining > 0 THEN
                CAST(CAST(TradingDaysRemaining as FLOAT) / CalendarDaysRemaining * 100 as INT)
            ELSE 0
        END as 営業日率,
        Volume
    FROM V_CommodityPriceWithTradingDays
    WHERE TradeDate = (SELECT MAX(TradeDate) FROM V_CommodityPriceWithTradingDays)
        AND CalendarDaysRemaining IS NOT NULL
        AND Volume > 0
    ORDER BY CalendarDaysRemaining
    """
    
    df = pd.read_sql(query, conn)
    print("\n【営業日ベース満期情報（修正版）】")
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    print(df.to_string(index=False))
    
    # 具体的な計算例
    print("\n\n【計算例】")
    print("・SHFE CU1: 7/8(火)→7/15(火) = 7暦日 = 5営業日 + 2休日（土日）")
    print("・CMX HG1: 7/8(火)→7/29(火) = 21暦日 = 15営業日 + 6休日（土日3週分）")
    print("・営業日率 = 営業日 ÷ 暦日 × 100%")
    
    conn.close()

def main():
    """メイン処理"""
    print(f"実行時刻: {datetime.now()}")
    
    # 1. 問題診断
    diagnose_calendar_issue()
    
    # 2. 関数修正
    fix_trading_days_function()
    
    # 3. 修正後テスト
    test_fixed_calculation()
    
    print("\n=== 完了 ===")

if __name__ == "__main__":
    main()