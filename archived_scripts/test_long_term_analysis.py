"""
2000年以降の長期分析テスト
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

def test_long_term_calendar():
    """長期カレンダーの分析"""
    print("=== 2000-2039年カレンダー分析 ===")
    
    conn = pyodbc.connect(CONNECTION_STRING)
    
    # 1. 10年ごとの営業日統計
    query1 = """
    SELECT 
        CASE 
            WHEN YEAR(CalendarDate) BETWEEN 2000 AND 2009 THEN '2000年代'
            WHEN YEAR(CalendarDate) BETWEEN 2010 AND 2019 THEN '2010年代'
            WHEN YEAR(CalendarDate) BETWEEN 2020 AND 2029 THEN '2020年代'
            WHEN YEAR(CalendarDate) BETWEEN 2030 AND 2039 THEN '2030年代'
        END as 年代,
        ExchangeCode,
        COUNT(*) as 総日数,
        SUM(CAST(IsTradingDay as INT)) as 営業日数,
        CAST(AVG(CAST(IsTradingDay as FLOAT)) * 100 as DECIMAL(5,2)) as 営業日率
    FROM M_TradingCalendar
    WHERE YEAR(CalendarDate) BETWEEN 2000 AND 2039
    GROUP BY 
        CASE 
            WHEN YEAR(CalendarDate) BETWEEN 2000 AND 2009 THEN '2000年代'
            WHEN YEAR(CalendarDate) BETWEEN 2010 AND 2019 THEN '2010年代'
            WHEN YEAR(CalendarDate) BETWEEN 2020 AND 2029 THEN '2020年代'
            WHEN YEAR(CalendarDate) BETWEEN 2030 AND 2039 THEN '2030年代'
        END,
        ExchangeCode
    ORDER BY 年代, ExchangeCode
    """
    
    df1 = pd.read_sql(query1, conn)
    print("\n【10年ごとの営業日統計】")
    pd.set_option('display.max_columns', None)
    print(df1.to_string(index=False))
    
    # 2. 長期営業日計算テスト
    cursor = conn.cursor()
    cursor.execute("""
        SELECT 
            -- 1年間
            dbo.GetTradingDaysBetween('2025-01-01', '2025-12-31', 'CMX') as Y2025_CMX,
            -- 5年間
            dbo.GetTradingDaysBetween('2020-01-01', '2024-12-31', 'CMX') as Y2020_2024_CMX,
            -- 10年間
            dbo.GetTradingDaysBetween('2010-01-01', '2019-12-31', 'CMX') as Y2010s_CMX,
            -- 20年間
            dbo.GetTradingDaysBetween('2000-01-01', '2019-12-31', 'CMX') as Y2000_2019_CMX,
            -- 40年間
            dbo.GetTradingDaysBetween('2000-01-01', '2039-12-31', 'CMX') as Y2000_2039_CMX
    """)
    
    result = cursor.fetchone()
    print("\n\n【長期営業日計算テスト（CMX）】")
    print(f"2025年（1年）: {result[0]}営業日")
    print(f"2020-2024年（5年）: {result[1]}営業日 (年平均{result[1]/5:.0f}日)")
    print(f"2010年代（10年）: {result[2]}営業日 (年平均{result[2]/10:.0f}日)")
    print(f"2000-2019年（20年）: {result[3]}営業日 (年平均{result[3]/20:.0f}日)")
    print(f"2000-2039年（40年）: {result[4]}営業日 (年平均{result[4]/40:.0f}日)")
    
    # 3. 将来の満期計算例（2030年代）
    query3 = """
    -- 2030年の仮想的な取引データで営業日計算
    SELECT 
        '2030-01-15' as 仮想取引日,
        '2030-12-31' as 仮想満期日,
        DATEDIFF(day, '2030-01-15', '2030-12-31') as 暦日数,
        dbo.GetTradingDaysBetween('2030-01-15', '2030-12-31', 'CMX') as CMX営業日,
        dbo.GetTradingDaysBetween('2030-01-15', '2030-12-31', 'LME') as LME営業日,
        dbo.GetTradingDaysBetween('2030-01-15', '2030-12-31', 'SHFE') as SHFE営業日
    """
    
    df3 = pd.read_sql(query3, conn)
    print("\n\n【2030年の営業日計算例】")
    print(df3.to_string(index=False))
    
    # 4. 月別営業日数の長期トレンド
    query4 = """
    SELECT 
        MONTH(CalendarDate) as 月,
        AVG(CASE WHEN ExchangeCode = 'CMX' AND IsTradingDay = 1 THEN 1.0 ELSE 0.0 END) * 
            DAY(EOMONTH(DATEFROMPARTS(2020, MONTH(CalendarDate), 1))) as CMX平均営業日,
        AVG(CASE WHEN ExchangeCode = 'LME' AND IsTradingDay = 1 THEN 1.0 ELSE 0.0 END) * 
            DAY(EOMONTH(DATEFROMPARTS(2020, MONTH(CalendarDate), 1))) as LME平均営業日,
        AVG(CASE WHEN ExchangeCode = 'SHFE' AND IsTradingDay = 1 THEN 1.0 ELSE 0.0 END) * 
            DAY(EOMONTH(DATEFROMPARTS(2020, MONTH(CalendarDate), 1))) as SHFE平均営業日
    FROM M_TradingCalendar
    WHERE YEAR(CalendarDate) BETWEEN 2000 AND 2039
    GROUP BY MONTH(CalendarDate)
    ORDER BY 月
    """
    
    df4 = pd.read_sql(query4, conn)
    print("\n\n【月別平均営業日数（40年間平均）】")
    print(df4.to_string(index=False))
    
    conn.close()

def main():
    """メイン処理"""
    print(f"実行時刻: {datetime.now()}")
    test_long_term_calendar()
    print("\n=== 完了 ===")

if __name__ == "__main__":
    main()