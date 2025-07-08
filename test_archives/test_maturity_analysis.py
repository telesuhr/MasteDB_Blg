"""
満期分析のテスト
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

def analyze_maturity():
    """満期分析"""
    print("=== 満期分析 ===")
    
    conn = pyodbc.connect(CONNECTION_STRING)
    
    # 1. 取引所別の平均残存日数
    query1 = """
    SELECT 
        ExchangeDisplayName,
        COUNT(DISTINCT GenericTicker) as 銘柄数,
        AVG(TradingDaysRemaining) as 平均残存日数,
        MIN(TradingDaysRemaining) as 最短残存日数,
        MAX(TradingDaysRemaining) as 最長残存日数
    FROM V_CommodityPriceWithMaturity
    WHERE TradeDate = (SELECT MAX(TradeDate) FROM V_CommodityPriceWithMaturity)
        AND TradingDaysRemaining IS NOT NULL
        AND Volume > 0
    GROUP BY ExchangeDisplayName
    ORDER BY ExchangeDisplayName
    """
    
    df1 = pd.read_sql(query1, conn)
    print("\n【取引所別残存日数サマリー】")
    print(df1.to_string(index=False))
    
    # 2. 満期までの日数別の出来高分布
    query2 = """
    SELECT 
        ExchangeDisplayName,
        CASE 
            WHEN TradingDaysRemaining <= 30 THEN '1. 30日以内'
            WHEN TradingDaysRemaining <= 60 THEN '2. 31-60日'
            WHEN TradingDaysRemaining <= 90 THEN '3. 61-90日'
            WHEN TradingDaysRemaining <= 180 THEN '4. 91-180日'
            ELSE '5. 180日超'
        END as 残存期間グループ,
        COUNT(*) as 銘柄数,
        SUM(Volume) as 総出来高,
        AVG(Volume) as 平均出来高
    FROM V_CommodityPriceWithMaturity
    WHERE TradeDate = (SELECT MAX(TradeDate) FROM V_CommodityPriceWithMaturity)
        AND TradingDaysRemaining IS NOT NULL
        AND Volume > 0
    GROUP BY ExchangeDisplayName,
        CASE 
            WHEN TradingDaysRemaining <= 30 THEN '1. 30日以内'
            WHEN TradingDaysRemaining <= 60 THEN '2. 31-60日'
            WHEN TradingDaysRemaining <= 90 THEN '3. 61-90日'
            WHEN TradingDaysRemaining <= 180 THEN '4. 91-180日'
            ELSE '5. 180日超'
        END
    ORDER BY ExchangeDisplayName, 残存期間グループ
    """
    
    df2 = pd.read_sql(query2, conn)
    print("\n\n【残存期間別の出来高分布】")
    print(df2.to_string(index=False))
    
    # 3. 最も活発な限月の特定
    query3 = """
    SELECT TOP 10
        ExchangeDisplayName,
        GenericTicker,
        GenericDescription,
        FORMAT(MaturityMonth, 'yyyy-MM') as 満期月,
        TradingDaysRemaining as 残存日数,
        Volume as 出来高,
        PriceForAnalysis as 価格,
        CASE 
            WHEN TradingDaysRemaining <= 60 THEN '流動性高'
            WHEN TradingDaysRemaining <= 180 THEN '標準'
            ELSE '流動性低'
        END as 流動性評価
    FROM V_CommodityPriceWithMaturity
    WHERE TradeDate = (SELECT MAX(TradeDate) FROM V_CommodityPriceWithMaturity)
        AND Volume > 0
    ORDER BY Volume DESC
    """
    
    df3 = pd.read_sql(query3, conn)
    print("\n\n【最も活発な限月TOP10】")
    print(df3.to_string(index=False))
    
    # 4. ロールオーバータイミングの分析
    query4 = """
    WITH RolloverAnalysis AS (
        SELECT 
            ExchangeDisplayName,
            GenericNumber,
            GenericTicker,
            TradingDaysRemaining,
            RolloverDays,
            Volume,
            LAG(Volume) OVER (PARTITION BY ExchangeDisplayName ORDER BY GenericNumber) as PrevVolume,
            LEAD(Volume) OVER (PARTITION BY ExchangeDisplayName ORDER BY GenericNumber) as NextVolume
        FROM V_CommodityPriceWithMaturity
        WHERE TradeDate = (SELECT MAX(TradeDate) FROM V_CommodityPriceWithMaturity)
            AND MetalCode LIKE 'CU%'
    )
    SELECT 
        ExchangeDisplayName,
        GenericNumber,
        GenericTicker,
        TradingDaysRemaining,
        Volume,
        NextVolume,
        CASE 
            WHEN Volume > 0 AND NextVolume > 0 AND NextVolume > Volume THEN '次限月が活発'
            WHEN Volume > 0 AND NextVolume > 0 AND Volume > NextVolume THEN '当限月が活発'
            ELSE '判定不能'
        END as 流動性移行状況
    FROM RolloverAnalysis
    WHERE GenericNumber <= 3
    ORDER BY ExchangeDisplayName, GenericNumber
    """
    
    df4 = pd.read_sql(query4, conn)
    print("\n\n【ロールオーバー分析（第1-3限月）】")
    print(df4.to_string(index=False))
    
    conn.close()

def main():
    """メイン処理"""
    print(f"実行時刻: {datetime.now()}")
    analyze_maturity()
    print("\n=== 完了 ===")

if __name__ == "__main__":
    main()