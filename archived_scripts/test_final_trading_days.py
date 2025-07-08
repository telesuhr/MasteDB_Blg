"""
最終的な営業日計算の確認
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

def test_trading_days_calculation():
    """営業日計算の最終確認"""
    print("=== 営業日計算の最終確認 ===")
    
    conn = pyodbc.connect(CONNECTION_STRING)
    
    # 1. 正しく営業日が計算されているか確認
    query1 = """
    SELECT TOP 15
        GenericTicker,
        FORMAT(TradeDate, 'yyyy-MM-dd') as 取引日,
        FORMAT(LastTradeableDate, 'yyyy-MM-dd') as 最終取引日,
        CalendarDaysRemaining as 暦日数,
        TradingDaysRemaining as 営業日数,
        HolidaysInPeriod as 休日数,
        TradingDayRate as '営業日率(%)',
        CASE 
            WHEN CalendarDaysRemaining = TradingDaysRemaining + HolidaysInPeriod 
            THEN '✓'
            ELSE '✗'
        END as 検証,
        Volume
    FROM V_CommodityPriceWithMaturityEx
    WHERE TradeDate = (SELECT MAX(TradeDate) FROM V_CommodityPriceWithMaturityEx)
        AND TradingDaysRemaining IS NOT NULL
        AND Volume > 0
    ORDER BY TradingDaysRemaining
    """
    
    df1 = pd.read_sql(query1, conn)
    print("\n【営業日計算結果（最新取引日）】")
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    print(df1.to_string(index=False))
    
    # 2. 取引所別の営業日率
    query2 = """
    SELECT 
        ExchangeCode,
        COUNT(*) as 銘柄数,
        AVG(TradingDayRate) as '平均営業日率(%)',
        MIN(TradingDayRate) as '最小営業日率(%)',
        MAX(TradingDayRate) as '最大営業日率(%)'
    FROM V_CommodityPriceWithMaturityEx
    WHERE TradeDate = (SELECT MAX(TradeDate) FROM V_CommodityPriceWithMaturityEx)
        AND TradingDaysRemaining IS NOT NULL
        AND CalendarDaysRemaining > 0
        AND Volume > 0
    GROUP BY ExchangeCode
    ORDER BY ExchangeCode
    """
    
    df2 = pd.read_sql(query2, conn)
    print("\n\n【取引所別営業日率統計】")
    print(df2.to_string(index=False))
    
    # 3. 期間別の営業日率
    query3 = """
    SELECT 
        CASE 
            WHEN CalendarDaysRemaining <= 7 THEN '1週間以内'
            WHEN CalendarDaysRemaining <= 30 THEN '1ヶ月以内'
            WHEN CalendarDaysRemaining <= 90 THEN '3ヶ月以内'
            WHEN CalendarDaysRemaining <= 180 THEN '6ヶ月以内'
            WHEN CalendarDaysRemaining <= 365 THEN '1年以内'
            ELSE '1年超'
        END as 期間区分,
        COUNT(*) as 契約数,
        AVG(CalendarDaysRemaining) as 平均暦日,
        AVG(TradingDaysRemaining) as 平均営業日,
        AVG(TradingDayRate) as '平均営業日率(%)'
    FROM V_CommodityPriceWithMaturityEx
    WHERE TradeDate = (SELECT MAX(TradeDate) FROM V_CommodityPriceWithMaturityEx)
        AND TradingDaysRemaining IS NOT NULL
        AND Volume > 0
    GROUP BY 
        CASE 
            WHEN CalendarDaysRemaining <= 7 THEN '1週間以内'
            WHEN CalendarDaysRemaining <= 30 THEN '1ヶ月以内'
            WHEN CalendarDaysRemaining <= 90 THEN '3ヶ月以内'
            WHEN CalendarDaysRemaining <= 180 THEN '6ヶ月以内'
            WHEN CalendarDaysRemaining <= 365 THEN '1年以内'
            ELSE '1年超'
        END
    ORDER BY 
        CASE 
            WHEN CalendarDaysRemaining <= 7 THEN 1
            WHEN CalendarDaysRemaining <= 30 THEN 2
            WHEN CalendarDaysRemaining <= 90 THEN 3
            WHEN CalendarDaysRemaining <= 180 THEN 4
            WHEN CalendarDaysRemaining <= 365 THEN 5
            ELSE 6
        END
    """
    
    df3 = pd.read_sql(query3, conn)
    print("\n\n【期間別営業日率】")
    print(df3.to_string(index=False))
    
    # 4. 具体的な計算例
    print("\n\n【営業日計算の具体例】")
    print("暦日数 = 営業日数 + 休日数")
    print("営業日率(%) = 営業日数 ÷ 暦日数 × 100")
    print("\n例：")
    if not df1.empty:
        row = df1.iloc[0]
        print(f"{row['GenericTicker']}: {row['取引日']} → {row['最終取引日']}")
        print(f"  暦日数: {row['暦日数']}日")
        print(f"  営業日数: {row['営業日数']}日")
        print(f"  休日数: {row['休日数']}日")
        print(f"  営業日率: {row['営業日率(%)']}%")
        print(f"  検証: {row['暦日数']} = {row['営業日数']} + {row['休日数']} = {row['営業日数'] + row['休日数']} {row['検証']}")
    
    # 5. カレンダー精度の確認
    cursor = conn.cursor()
    cursor.execute("""
        SELECT 
            ExchangeCode,
            COUNT(*) as TotalDays,
            SUM(CAST(IsTradingDay as INT)) as TradingDays,
            CAST(SUM(CAST(IsTradingDay as INT)) * 100.0 / COUNT(*) as DECIMAL(5,2)) as TradingDayRate
        FROM M_TradingCalendar
        WHERE CalendarDate BETWEEN '2025-01-01' AND '2025-12-31'
        GROUP BY ExchangeCode
        ORDER BY ExchangeCode
    """)
    
    print("\n\n【2025年カレンダー統計】")
    for row in cursor.fetchall():
        print(f"{row[0]}: {row[2]}営業日 / {row[1]}日 = {row[3]}%")
    
    conn.close()
    print("\n=== 確認完了 ===")

def main():
    """メイン処理"""
    print(f"実行時刻: {datetime.now()}")
    test_trading_days_calculation()

if __name__ == "__main__":
    main()