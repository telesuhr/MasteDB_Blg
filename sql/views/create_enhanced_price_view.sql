-- より詳細な属性情報を含む拡張ビュー
-- 取引可能残日数、価格変化率、テナー情報なども含む

CREATE OR ALTER VIEW V_CommodityPriceEnhanced AS
WITH LatestContractMapping AS (
    -- 各ジェネリック先物の最新の実際の契約を取得
    SELECT 
        m.GenericID,
        m.ActualContractID,
        m.TradeDate,
        ac.ContractMonth,
        ac.LastTradeableDate as LastTradingDate,
        ac.DeliveryDate as FirstNoticeDate,  -- 代替としてDeliveryDateを使用
        ROW_NUMBER() OVER (PARTITION BY m.GenericID ORDER BY m.TradeDate DESC) as rn
    FROM T_GenericContractMapping m
    INNER JOIN M_ActualContract ac ON m.ActualContractID = ac.ActualContractID
),
PreviousPrices AS (
    -- 前日の価格を取得（変化率計算用）
    SELECT 
        p1.GenericID,
        p1.TradeDate,
        p2.SettlementPrice as PrevSettlementPrice,
        p2.LastPrice as PrevLastPrice
    FROM T_CommodityPrice_V2 p1
    INNER JOIN T_CommodityPrice_V2 p2 
        ON p1.GenericID = p2.GenericID 
        AND p2.TradeDate = (
            SELECT MAX(TradeDate) 
            FROM T_CommodityPrice_V2 p3 
            WHERE p3.GenericID = p1.GenericID 
                AND p3.TradeDate < p1.TradeDate
        )
    WHERE p1.DataType = 'Generic' AND p2.DataType = 'Generic'
)
SELECT 
    -- 基本情報
    p.TradeDate,
    DATENAME(WEEKDAY, p.TradeDate) as TradeDayOfWeek,
    
    -- 取引所情報
    g.ExchangeCode,
    CASE 
        WHEN g.ExchangeCode = 'CMX' THEN 'COMEX'
        WHEN g.ExchangeCode = 'LME' THEN 'London Metal Exchange'
        WHEN g.ExchangeCode = 'SHFE' THEN 'Shanghai Futures Exchange'
        ELSE g.ExchangeCode
    END AS ExchangeFullName,
    
    -- 金属情報
    m.MetalID,
    m.MetalCode,
    m.MetalName,
    m.Unit AS MetalUnit,
    m.CurrencyCode,
    
    -- ジェネリック先物情報
    g.GenericID,
    g.GenericNumber,
    g.GenericTicker,
    CASE 
        WHEN g.GenericNumber = 1 THEN '1st Month'
        WHEN g.GenericNumber = 2 THEN '2nd Month'
        WHEN g.GenericNumber = 3 THEN '3rd Month'
        ELSE CAST(g.GenericNumber AS VARCHAR(10)) + 'th Month'
    END AS GenericDescription,
    
    -- 実際の契約情報
    lcm.ActualContractID,
    FORMAT(lcm.ContractMonth, 'yyyyMM') as ContractMonth,
    lcm.FirstNoticeDate,
    lcm.LastTradingDate,
    
    -- 残存期間の計算
    CASE 
        WHEN lcm.LastTradingDate IS NOT NULL THEN
            DATEDIFF(day, p.TradeDate, lcm.LastTradingDate)
        WHEN lcm.ContractMonth IS NOT NULL THEN
            -- LastTradingDateがない場合は契約月末までの日数
            DATEDIFF(day, p.TradeDate, EOMONTH(lcm.ContractMonth))
        ELSE NULL
    END AS DaysToExpiry,
    
    CASE 
        WHEN lcm.FirstNoticeDate IS NOT NULL THEN
            DATEDIFF(day, p.TradeDate, lcm.FirstNoticeDate)
        ELSE NULL
    END AS DaysToFirstNotice,
    
    -- 価格データ
    p.SettlementPrice,
    p.OpenPrice,
    p.HighPrice,
    p.LowPrice,
    p.LastPrice,
    p.Volume,
    p.OpenInterest,
    
    -- 価格変化率
    pp.PrevSettlementPrice,
    CASE 
        WHEN pp.PrevSettlementPrice IS NOT NULL AND pp.PrevSettlementPrice <> 0 THEN
            ROUND((p.SettlementPrice - pp.PrevSettlementPrice) / pp.PrevSettlementPrice * 100, 2)
        ELSE NULL
    END AS SettlementChangePercent,
    
    CASE 
        WHEN pp.PrevLastPrice IS NOT NULL AND pp.PrevLastPrice <> 0 THEN
            ROUND((p.LastPrice - pp.PrevLastPrice) / pp.PrevLastPrice * 100, 2)
        ELSE NULL
    END AS LastPriceChangePercent,
    
    -- 日中変動率（High-Low）
    CASE 
        WHEN p.LowPrice IS NOT NULL AND p.LowPrice <> 0 THEN
            ROUND((p.HighPrice - p.LowPrice) / p.LowPrice * 100, 2)
        ELSE NULL
    END AS IntradayVolatility,
    
    -- データ品質情報
    CASE 
        WHEN p.SettlementPrice IS NULL AND p.LastPrice IS NULL THEN 'Volume Only'
        WHEN p.OpenPrice IS NULL OR p.HighPrice IS NULL OR p.LowPrice IS NULL THEN 'Settlement Only'
        WHEN p.Volume IS NULL OR p.Volume = 0 THEN 'Price Only (No Volume)'
        ELSE 'Complete'
    END AS DataCompleteness,
    
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
LEFT JOIN LatestContractMapping lcm ON g.GenericID = lcm.GenericID AND lcm.rn = 1
LEFT JOIN PreviousPrices pp ON p.GenericID = pp.GenericID AND p.TradeDate = pp.TradeDate
WHERE p.DataType = 'Generic'

GO

-- 使用例1: 最新の取引状況サマリー
SELECT 
    ExchangeFullName,
    MetalCode,
    GenericDescription,
    ContractMonth,
    DaysToExpiry,
    SettlementPrice,
    SettlementChangePercent,
    Volume,
    TradingActivity,
    DataCompleteness
FROM V_CommodityPriceEnhanced
WHERE TradeDate = (SELECT MAX(TradeDate) FROM V_CommodityPriceEnhanced)
    AND MetalCode = 'CU'  -- 銅のみ
ORDER BY ExchangeFullName, GenericNumber

-- 使用例2: 満期が近い契約（30日以内）
SELECT 
    ExchangeFullName,
    GenericTicker,
    ContractMonth,
    DaysToExpiry,
    DaysToFirstNotice,
    SettlementPrice,
    Volume,
    OpenInterest,
    TradingActivity
FROM V_CommodityPriceEnhanced
WHERE TradeDate = (SELECT MAX(TradeDate) FROM V_CommodityPriceEnhanced)
    AND DaysToExpiry BETWEEN 0 AND 30
ORDER BY DaysToExpiry

-- 使用例3: 価格変動が大きい契約（±2%以上）
SELECT 
    TradeDate,
    ExchangeFullName,
    GenericTicker,
    SettlementPrice,
    PrevSettlementPrice,
    SettlementChangePercent,
    Volume,
    IntradayVolatility
FROM V_CommodityPriceEnhanced
WHERE TradeDate >= DATEADD(day, -7, GETDATE())
    AND ABS(SettlementChangePercent) >= 2
ORDER BY TradeDate DESC, ABS(SettlementChangePercent) DESC