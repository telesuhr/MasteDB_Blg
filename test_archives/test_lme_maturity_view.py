"""
LMEの満期ビューテスト
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

def test_lme_maturity():
    """LMEの満期計算テスト"""
    print("=== LME満期計算テスト ===")
    
    conn = pyodbc.connect(CONNECTION_STRING)
    
    # LMEの満期情報を含むデータ確認
    query1 = """
    SELECT 
        ExchangeDisplayName,
        GenericNumber,
        GenericTicker,
        GenericDescription,
        TradeDate,
        MaturityMonth,
        LastTradingDay,
        TradingDaysRemaining,
        RolloverDays,
        ShouldRollover,
        PriceForAnalysis,
        Volume,
        DataQuality
    FROM V_CommodityPriceWithMaturity
    WHERE ExchangeCode = 'LME'
        AND TradeDate >= '2025-07-07'
    ORDER BY GenericNumber
    LIMIT 15
    """
    
    # SQL Server用にLIMITをTOPに変更
    query1 = query1.replace("LIMIT 15", "")
    query1 = query1.replace("SELECT", "SELECT TOP 15")
    
    df1 = pd.read_sql(query1, conn)
    print("\n【LME満期情報（第1-15限月）】")
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    print(df1.to_string(index=False))
    
    # LMEの満期計算ロジック確認
    query2 = """
    SELECT 
        g.GenericTicker,
        g.GenericNumber,
        g.MaturityRule,
        '2025-07-08' as TestDate,
        dbo.CalculateMaturityDate('2025-07-08', g.MaturityRule) as MaturityMonth,
        dbo.CalculateLastTradingDay(
            dbo.CalculateMaturityDate('2025-07-08', g.MaturityRule),
            g.LastTradingDayRule
        ) as LastTradingDay,
        DATEDIFF(day, '2025-07-08', 
            dbo.CalculateLastTradingDay(
                dbo.CalculateMaturityDate('2025-07-08', g.MaturityRule),
                g.LastTradingDayRule
            )
        ) as DaysToExpiry
    FROM M_GenericFutures g
    WHERE g.ExchangeCode = 'LME'
        AND g.GenericNumber <= 12
    ORDER BY g.GenericNumber
    """
    
    df2 = pd.read_sql(query2, conn)
    print("\n\n【LME満期計算ロジック確認（2025-07-08基準）】")
    print(df2.to_string(index=False))
    
    # 第3水曜日の計算確認
    query3 = """
    WITH MonthlyDates AS (
        SELECT 
            DATEFROMPARTS(2025, 7, 1) as MonthStart,
            'July 2025' as MonthName
        UNION ALL
        SELECT 
            DATEFROMPARTS(2025, 8, 1),
            'August 2025'
        UNION ALL
        SELECT 
            DATEFROMPARTS(2025, 9, 1),
            'September 2025'
        UNION ALL
        SELECT 
            DATEFROMPARTS(2025, 10, 1),
            'October 2025'
    )
    SELECT 
        MonthName,
        MonthStart,
        dbo.CalculateLastTradingDay(MonthStart, '3rdWednesday') as ThirdWednesday,
        DATENAME(WEEKDAY, dbo.CalculateLastTradingDay(MonthStart, '3rdWednesday')) as DayOfWeek
    FROM MonthlyDates
    """
    
    df3 = pd.read_sql(query3, conn)
    print("\n\n【第3水曜日計算確認】")
    print(df3.to_string(index=False))
    
    # 全取引所の満期情報サマリー
    query4 = """
    SELECT 
        ExchangeDisplayName,
        COUNT(DISTINCT GenericTicker) as 銘柄数,
        COUNT(CASE WHEN TradingDaysRemaining IS NOT NULL THEN 1 END) as 満期計算可能数,
        MIN(TradingDaysRemaining) as 最短残存日数,
        MAX(TradingDaysRemaining) as 最長残存日数,
        COUNT(CASE WHEN ShouldRollover = 'Yes' THEN 1 END) as ロール必要数
    FROM V_CommodityPriceWithMaturity
    WHERE TradeDate = (SELECT MAX(TradeDate) FROM V_CommodityPriceWithMaturity)
    GROUP BY ExchangeDisplayName
    ORDER BY ExchangeDisplayName
    """
    
    df4 = pd.read_sql(query4, conn)
    print("\n\n【全取引所満期情報サマリー】")
    print(df4.to_string(index=False))
    
    conn.close()

def main():
    """メイン処理"""
    print(f"実行時刻: {datetime.now()}")
    test_lme_maturity()
    print("\n=== 完了 ===")

if __name__ == "__main__":
    main()