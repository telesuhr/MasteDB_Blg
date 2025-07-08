"""
実際の満期日情報を使用したビューのテスト
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

def test_actual_maturity():
    """実際の満期日情報テスト"""
    print("=== 実際の満期日情報テスト ===")
    
    conn = pyodbc.connect(CONNECTION_STRING)
    
    # 1. 実際の満期日情報を含むデータ確認
    query1 = """
    SELECT TOP 20
        ExchangeDisplayName,
        GenericNumber,
        GenericTicker,
        TradeDate,
        LastTradeableDate,
        FutureDeliveryDateLast,
        ActualTradingDaysRemaining,
        ActualRolloverDate,
        ActualShouldRollover,
        RolloverDays,
        Volume,
        PriceForAnalysis
    FROM V_CommodityPriceWithMaturityEx
    WHERE TradeDate = (SELECT MAX(TradeDate) FROM V_CommodityPriceWithMaturityEx)
        AND MetalCode LIKE 'CU%'
        AND LastTradeableDate IS NOT NULL
    ORDER BY ExchangeDisplayName, GenericNumber
    """
    
    df1 = pd.read_sql(query1, conn)
    print("\n【実際の満期日情報（Bloombergデータ）】")
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    print(df1.to_string(index=False))
    
    # 2. ロールオーバーが必要な契約
    query2 = """
    SELECT 
        ExchangeDisplayName,
        GenericTicker,
        LastTradeableDate,
        ActualTradingDaysRemaining,
        ActualRolloverDate,
        Volume,
        TradingActivity
    FROM V_CommodityPriceWithMaturityEx
    WHERE TradeDate = (SELECT MAX(TradeDate) FROM V_CommodityPriceWithMaturityEx)
        AND ActualShouldRollover = 'Yes'
        AND Volume > 0
    ORDER BY ActualTradingDaysRemaining
    """
    
    df2 = pd.read_sql(query2, conn)
    if not df2.empty:
        print("\n\n【ロールオーバーが必要な契約（実際の日付ベース）】")
        print(df2.to_string(index=False))
    else:
        print("\n\n現在ロールオーバーが必要な契約はありません。")
    
    # 3. 取引所別の実際の残存日数分析
    query3 = """
    SELECT 
        ExchangeDisplayName,
        COUNT(DISTINCT GenericTicker) as 銘柄数,
        AVG(ActualTradingDaysRemaining) as 平均残存日数,
        MIN(ActualTradingDaysRemaining) as 最短残存日数,
        MAX(ActualTradingDaysRemaining) as 最長残存日数,
        AVG(CAST(SettlementPeriodDays as FLOAT)) as 平均決済期間
    FROM V_CommodityPriceWithMaturityEx
    WHERE TradeDate = (SELECT MAX(TradeDate) FROM V_CommodityPriceWithMaturityEx)
        AND ActualTradingDaysRemaining IS NOT NULL
        AND Volume > 0
    GROUP BY ExchangeDisplayName
    ORDER BY ExchangeDisplayName
    """
    
    df3 = pd.read_sql(query3, conn)
    print("\n\n【取引所別実際の残存日数分析】")
    print(df3.to_string(index=False))
    
    # 4. 残存日数別の出来高分布（実際の日付ベース）
    query4 = """
    SELECT 
        ExchangeDisplayName,
        CASE 
            WHEN ActualTradingDaysRemaining <= 30 THEN '1. 30日以内'
            WHEN ActualTradingDaysRemaining <= 60 THEN '2. 31-60日'
            WHEN ActualTradingDaysRemaining <= 90 THEN '3. 61-90日'
            WHEN ActualTradingDaysRemaining <= 180 THEN '4. 91-180日'
            ELSE '5. 180日超'
        END as 残存期間グループ,
        COUNT(*) as 銘柄数,
        SUM(Volume) as 総出来高,
        AVG(Volume) as 平均出来高
    FROM V_CommodityPriceWithMaturityEx
    WHERE TradeDate = (SELECT MAX(TradeDate) FROM V_CommodityPriceWithMaturityEx)
        AND ActualTradingDaysRemaining IS NOT NULL
        AND Volume > 0
    GROUP BY ExchangeDisplayName,
        CASE 
            WHEN ActualTradingDaysRemaining <= 30 THEN '1. 30日以内'
            WHEN ActualTradingDaysRemaining <= 60 THEN '2. 31-60日'
            WHEN ActualTradingDaysRemaining <= 90 THEN '3. 61-90日'
            WHEN ActualTradingDaysRemaining <= 180 THEN '4. 91-180日'
            ELSE '5. 180日超'
        END
    ORDER BY ExchangeDisplayName, 残存期間グループ
    """
    
    df4 = pd.read_sql(query4, conn)
    print("\n\n【残存期間別の出来高分布（実際の日付ベース）】")
    print(df4.to_string(index=False))
    
    # 5. 近い満期の契約詳細
    query5 = """
    SELECT TOP 10
        ExchangeDisplayName,
        GenericTicker,
        LastTradeableDate,
        FutureDeliveryDateLast,
        ActualTradingDaysRemaining,
        SettlementPeriodDays,
        ActualRolloverDate,
        Volume,
        PriceForAnalysis
    FROM V_CommodityPriceWithMaturityEx
    WHERE TradeDate = (SELECT MAX(TradeDate) FROM V_CommodityPriceWithMaturityEx)
        AND ActualTradingDaysRemaining IS NOT NULL
        AND ActualTradingDaysRemaining > 0
        AND Volume > 0
    ORDER BY ActualTradingDaysRemaining
    """
    
    df5 = pd.read_sql(query5, conn)
    print("\n\n【最も満期が近い契約TOP10】")
    print(df5.to_string(index=False))
    
    conn.close()

def main():
    """メイン処理"""
    print(f"実行時刻: {datetime.now()}")
    test_actual_maturity()
    print("\n=== 完了 ===")

if __name__ == "__main__":
    main()