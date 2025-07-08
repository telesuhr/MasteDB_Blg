-- 属性情報とヒストリカルデータを結合したビュー
-- 取引可能残日数も計算

CREATE OR ALTER VIEW V_CommodityPriceWithAttributes AS
WITH LatestContractMapping AS (
    -- 各ジェネリック先物の最新の実際の契約を取得
    SELECT 
        m.GenericID,
        m.ActualContractID,
        m.TradeDate,
        ac.ContractMonth,
        ROW_NUMBER() OVER (PARTITION BY m.GenericID ORDER BY m.TradeDate DESC) as rn
    FROM T_GenericContractMapping m
    INNER JOIN M_ActualContract ac ON m.ActualContractID = ac.ActualContractID
)
SELECT 
    -- 基本情報
    p.TradeDate,
    g.ExchangeCode,
    CASE 
        WHEN g.ExchangeCode = 'CMX' THEN 'COMEX'
        ELSE g.ExchangeCode
    END AS ExchangeDisplayName,
    
    -- 金属情報
    m.MetalCode,
    m.MetalName,
    m.Unit AS MetalUnit,
    m.CurrencyCode,
    
    -- ジェネリック先物情報
    g.GenericNumber,
    g.GenericTicker,
    
    -- 実際の契約情報（ある場合）
    FORMAT(lcm.ContractMonth, 'yyyyMM') as ContractMonth,
    
    -- 取引可能残日数の計算
    CASE 
        WHEN lcm.ContractMonth IS NOT NULL THEN
            DATEDIFF(day, p.TradeDate, lcm.ContractMonth)
        ELSE NULL
    END AS DaysToExpiry,
    
    -- 価格データ
    p.SettlementPrice,
    p.OpenPrice,
    p.HighPrice,
    p.LowPrice,
    p.LastPrice,
    p.Volume,
    p.OpenInterest,
    
    -- データ品質フラグ
    CASE 
        WHEN p.SettlementPrice IS NULL AND p.LastPrice IS NULL THEN 'Volume Only'
        WHEN p.OpenPrice IS NULL OR p.HighPrice IS NULL OR p.LowPrice IS NULL THEN 'Settlement Only'
        ELSE 'Full OHLC'
    END AS DataQuality,
    
    -- 更新情報
    p.LastUpdated
    
FROM T_CommodityPrice_V2 p
INNER JOIN M_GenericFutures g ON p.GenericID = g.GenericID
INNER JOIN M_Metal m ON g.MetalID = m.MetalID
LEFT JOIN LatestContractMapping lcm ON g.GenericID = lcm.GenericID AND lcm.rn = 1
WHERE p.DataType = 'Generic'

GO

-- ビューの使用例

-- 1. 最新データの確認（取引可能残日数付き）
SELECT 
    ExchangeDisplayName,
    MetalCode,
    GenericNumber,
    GenericTicker,
    ContractMonth,
    DaysToExpiry,
    TradeDate,
    SettlementPrice,
    Volume,
    DataQuality
FROM V_CommodityPriceWithAttributes
WHERE TradeDate = (SELECT MAX(TradeDate) FROM V_CommodityPriceWithAttributes)
ORDER BY ExchangeDisplayName, GenericNumber

-- 2. 残存期間が30日以内の活発な契約
SELECT 
    ExchangeDisplayName,
    GenericTicker,
    ContractMonth,
    DaysToExpiry,
    TradeDate,
    SettlementPrice,
    Volume,
    OpenInterest
FROM V_CommodityPriceWithAttributes
WHERE DaysToExpiry BETWEEN 1 AND 30
    AND Volume > 0
    AND TradeDate = (SELECT MAX(TradeDate) FROM V_CommodityPriceWithAttributes)
ORDER BY DaysToExpiry

-- 3. 取引所別の平均残存期間
SELECT 
    ExchangeDisplayName,
    AVG(CAST(DaysToExpiry AS FLOAT)) as AvgDaysToExpiry,
    COUNT(DISTINCT GenericTicker) as ActiveContracts
FROM V_CommodityPriceWithAttributes
WHERE TradeDate = (SELECT MAX(TradeDate) FROM V_CommodityPriceWithAttributes)
    AND DaysToExpiry IS NOT NULL
GROUP BY ExchangeDisplayName

-- 4. データ品質の分析
SELECT 
    ExchangeDisplayName,
    DataQuality,
    COUNT(*) as RecordCount,
    COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY ExchangeDisplayName) as Percentage
FROM V_CommodityPriceWithAttributes
WHERE TradeDate >= DATEADD(day, -7, GETDATE())
GROUP BY ExchangeDisplayName, DataQuality
ORDER BY ExchangeDisplayName, DataQuality