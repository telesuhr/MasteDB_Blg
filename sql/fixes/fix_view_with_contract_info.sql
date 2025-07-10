-- V_CommodityPriceWithMaturityExビューを修正
-- CMX/SHFEの契約月情報も表示

-- 既存のビューを削除
DROP VIEW IF EXISTS V_CommodityPriceWithMaturityEx;
GO

-- ビューを再作成
CREATE VIEW V_CommodityPriceWithMaturityEx AS
WITH LatestMapping AS (
    -- LME用のマッピング情報
    SELECT 
        m.GenericID,
        m.TradeDate,
        m.ActualContractID,
        ac.ContractTicker as ActualContract,
        ac.ContractMonth,
        ac.ContractMonthCode,
        ac.ContractYear,
        ac.LastTradeableDate as MappingLastTradeableDate,
        ac.DeliveryDate as MappingDeliveryDate,
        ROW_NUMBER() OVER (PARTITION BY m.GenericID, m.TradeDate ORDER BY m.TradeDate DESC) as rn
    FROM T_GenericContractMapping m
    INNER JOIN M_ActualContract ac ON m.ActualContractID = ac.ActualContractID
)
SELECT 
    -- 基本情報
    cp.PriceID,
    cp.TradeDate,
    cp.GenericID,
    gf.GenericTicker,
    gf.GenericNumber,
    
    -- 金属情報
    m.MetalCode,
    m.MetalName,
    
    -- 取引所情報
    gf.ExchangeCode,
    CASE gf.ExchangeCode
        WHEN 'LME' THEN 'London Metal Exchange'
        WHEN 'CMX' THEN 'COMEX'
        WHEN 'SHFE' THEN 'Shanghai Futures Exchange'
        ELSE gf.ExchangeCode
    END as ExchangeName,
    
    -- 価格情報
    cp.OpenPrice,
    cp.HighPrice,
    cp.LowPrice,
    cp.LastPrice,
    cp.SettlementPrice,
    cp.Volume,
    cp.OpenInterest,
    
    -- 満期日情報
    CASE 
        WHEN gf.ExchangeCode = 'LME' THEN COALESCE(lm.MappingLastTradeableDate, gf.LastTradeableDate)
        ELSE gf.LastTradeableDate
    END as LastTradeableDate,
    
    CASE 
        WHEN gf.ExchangeCode = 'LME' THEN COALESCE(lm.MappingDeliveryDate, gf.FutureDeliveryDateLast)
        ELSE gf.FutureDeliveryDateLast
    END as FutureDeliveryDate,
    
    -- 契約情報
    -- LME: 実際の契約ティッカー（例：LPN25）
    -- CMX/SHFE: ジェネリックティッカー自体（例：HG1、CU1）
    CASE 
        WHEN gf.ExchangeCode = 'LME' THEN lm.ActualContract
        WHEN gf.ExchangeCode IN ('CMX', 'SHFE') THEN gf.GenericTicker
        ELSE NULL
    END as ActualContract,
    
    -- 契約月情報
    -- LME: マッピングからの契約月
    -- CMX/SHFE: 最終取引日から導出した契約月（YYYY-MM形式）
    CASE 
        WHEN gf.ExchangeCode = 'LME' THEN lm.ContractMonth
        WHEN gf.ExchangeCode IN ('CMX', 'SHFE') AND gf.LastTradeableDate IS NOT NULL 
            THEN FORMAT(gf.LastTradeableDate, 'yyyy-MM')
        ELSE NULL
    END as ContractMonth
    
FROM T_CommodityPrice cp
INNER JOIN M_GenericFutures gf ON cp.GenericID = gf.GenericID
INNER JOIN M_Metal m ON gf.MetalID = m.MetalID
LEFT JOIN LatestMapping lm ON cp.GenericID = lm.GenericID 
    AND cp.TradeDate = lm.TradeDate 
    AND lm.rn = 1
WHERE cp.DataType = 'Generic';
GO

-- 各取引所のサンプルデータを確認
SELECT TOP 5
    TradeDate,
    GenericTicker,
    ExchangeCode,
    LastPrice,
    LastTradeableDate,
    ActualContract,
    ContractMonth
FROM V_CommodityPriceWithMaturityEx
WHERE TradeDate = '2025-06-06'
    AND ExchangeCode = 'LME'
ORDER BY GenericNumber;

SELECT TOP 5
    TradeDate,
    GenericTicker,
    ExchangeCode,
    LastPrice,
    LastTradeableDate,
    ActualContract,
    ContractMonth
FROM V_CommodityPriceWithMaturityEx
WHERE TradeDate = '2025-06-06'
    AND ExchangeCode = 'CMX'
ORDER BY GenericNumber;

SELECT TOP 5
    TradeDate,
    GenericTicker,
    ExchangeCode,
    LastPrice,
    LastTradeableDate,
    ActualContract,
    ContractMonth
FROM V_CommodityPriceWithMaturityEx
WHERE TradeDate = '2025-06-06'
    AND ExchangeCode = 'SHFE'
ORDER BY GenericNumber;