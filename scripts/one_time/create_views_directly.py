"""
ビューを直接作成
"""
import pyodbc

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

def create_views():
    """ビューを作成"""
    conn = pyodbc.connect(CONNECTION_STRING)
    cursor = conn.cursor()
    
    print("=== ビュー作成開始 ===")
    
    # V_CommodityPriceSimple作成
    print("\n1. V_CommodityPriceSimple を作成中...")
    try:
        cursor.execute("""
CREATE OR ALTER VIEW V_CommodityPriceSimple AS
SELECT 
    -- 基本情報
    p.TradeDate,
    DATENAME(WEEKDAY, p.TradeDate) as TradeDayOfWeek,
    
    -- 取引所情報
    g.ExchangeCode,
    CASE 
        WHEN g.ExchangeCode = 'CMX' THEN 'COMEX'
        WHEN g.ExchangeCode = 'LME' THEN 'LME'
        WHEN g.ExchangeCode = 'SHFE' THEN 'SHFE'
        ELSE g.ExchangeCode
    END AS ExchangeDisplayName,
    
    -- 金属情報
    m.MetalID,
    m.MetalCode,
    m.MetalName,
    m.CurrencyCode,
    
    -- ジェネリック先物情報
    g.GenericID,
    g.GenericNumber,
    g.GenericTicker,
    CASE 
        WHEN g.GenericNumber = 1 THEN '1番限'
        WHEN g.GenericNumber = 2 THEN '2番限'
        WHEN g.GenericNumber = 3 THEN '3番限'
        ELSE CAST(g.GenericNumber AS NVARCHAR(10)) + '番限'
    END AS GenericDescription,
    
    -- 価格データ
    p.SettlementPrice,
    p.OpenPrice,
    p.HighPrice,
    p.LowPrice,
    p.LastPrice,
    COALESCE(p.SettlementPrice, p.LastPrice) as PriceForAnalysis,
    p.Volume,
    p.OpenInterest,
    
    -- 日中変動率
    CASE 
        WHEN p.HighPrice IS NOT NULL AND p.LowPrice IS NOT NULL AND p.LowPrice > 0 THEN
            ROUND((p.HighPrice - p.LowPrice) / p.LowPrice * 100, 2)
        ELSE NULL
    END AS IntradayRange,
    
    -- データ品質
    CASE 
        WHEN p.SettlementPrice IS NULL AND p.LastPrice IS NULL THEN 'Volume Only'
        WHEN p.OpenPrice IS NULL OR p.HighPrice IS NULL OR p.LowPrice IS NULL THEN 'Settlement Only'
        WHEN p.Volume IS NULL OR p.Volume = 0 THEN 'Price Only'
        ELSE 'Complete'
    END AS DataQuality,
    
    -- 取引活発度
    CASE 
        WHEN p.Volume > 1000 THEN 'Very Active'
        WHEN p.Volume > 100 THEN 'Active'
        WHEN p.Volume > 10 THEN 'Moderate'
        WHEN p.Volume > 0 THEN 'Low'
        ELSE 'No Trading'
    END AS TradingActivity,
    
    -- 更新情報
    p.LastUpdated,
    DATEDIFF(MINUTE, p.LastUpdated, GETDATE()) AS MinutesSinceUpdate
    
FROM T_CommodityPrice_V2 p
INNER JOIN M_GenericFutures g ON p.GenericID = g.GenericID
INNER JOIN M_Metal m ON g.MetalID = m.MetalID
WHERE p.DataType = 'Generic'
        """)
        conn.commit()
        print("   作成完了")
    except Exception as e:
        print(f"   エラー: {e}")
        
    # V_CommodityPriceWithChange作成
    print("\n2. V_CommodityPriceWithChange を作成中...")
    try:
        cursor.execute("""
CREATE OR ALTER VIEW V_CommodityPriceWithChange AS
WITH PreviousPrices AS (
    SELECT 
        p1.GenericID,
        p1.TradeDate,
        p2.SettlementPrice as PrevSettlementPrice,
        p2.LastPrice as PrevLastPrice,
        p2.TradeDate as PrevTradeDate
    FROM T_CommodityPrice_V2 p1
    CROSS APPLY (
        SELECT TOP 1 
            SettlementPrice,
            LastPrice,
            TradeDate
        FROM T_CommodityPrice_V2 p2
        WHERE p2.GenericID = p1.GenericID 
            AND p2.TradeDate < p1.TradeDate
            AND p2.DataType = 'Generic'
        ORDER BY p2.TradeDate DESC
    ) p2
    WHERE p1.DataType = 'Generic'
)
SELECT 
    s.*,
    pp.PrevTradeDate,
    pp.PrevSettlementPrice,
    pp.PrevLastPrice,
    
    -- 価格変化
    CASE 
        WHEN s.SettlementPrice IS NOT NULL AND pp.PrevSettlementPrice IS NOT NULL THEN
            s.SettlementPrice - pp.PrevSettlementPrice
        WHEN s.LastPrice IS NOT NULL AND pp.PrevLastPrice IS NOT NULL THEN
            s.LastPrice - pp.PrevLastPrice
        ELSE NULL
    END AS PriceChange,
    
    -- 価格変化率
    CASE 
        WHEN s.SettlementPrice IS NOT NULL AND pp.PrevSettlementPrice IS NOT NULL AND pp.PrevSettlementPrice > 0 THEN
            ROUND((s.SettlementPrice - pp.PrevSettlementPrice) / pp.PrevSettlementPrice * 100, 2)
        WHEN s.LastPrice IS NOT NULL AND pp.PrevLastPrice IS NOT NULL AND pp.PrevLastPrice > 0 THEN
            ROUND((s.LastPrice - pp.PrevLastPrice) / pp.PrevLastPrice * 100, 2)
        ELSE NULL
    END AS PriceChangePercent
    
FROM V_CommodityPriceSimple s
LEFT JOIN PreviousPrices pp ON s.GenericID = pp.GenericID AND s.TradeDate = pp.TradeDate
        """)
        conn.commit()
        print("   作成完了")
    except Exception as e:
        print(f"   エラー: {e}")
        
    conn.close()
    print("\n=== ビュー作成完了 ===")

if __name__ == "__main__":
    create_views()