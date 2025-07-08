-- V_CommodityPriceWithMaturityExビューを修正して全取引所の満期日情報を表示

-- 既存のビューを削除
DROP VIEW IF EXISTS V_CommodityPriceWithMaturityEx;
GO

-- ビューを再作成（全取引所の満期日情報を含む）
CREATE VIEW V_CommodityPriceWithMaturityEx AS
WITH LatestMapping AS (
    -- 各GenericID・TradeDateごとの最新マッピング情報を取得
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
    
    -- 満期日情報（優先順位：1.価格データのMaturityDate、2.マッピングからの日付、3.M_GenericFuturesからの日付）
    COALESCE(
        cp.MaturityDate,
        lm.MappingLastTradeableDate,
        gf.LastTradeableDate
    ) as LastTradeableDate,
    
    COALESCE(
        lm.MappingDeliveryDate,
        gf.FutureDeliveryDateLast
    ) as FutureDeliveryDate,
    
    -- マッピング情報（LMEのみ）
    CASE 
        WHEN gf.ExchangeCode = 'LME' THEN lm.ActualContract
        ELSE NULL
    END as ActualContract,
    
    CASE 
        WHEN gf.ExchangeCode = 'LME' THEN lm.ContractMonth
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

-- 結果を確認
SELECT TOP 10
    TradeDate,
    GenericTicker,
    ExchangeCode,
    LastPrice,
    LastTradeableDate,
    FutureDeliveryDate
FROM V_CommodityPriceWithMaturityEx
WHERE TradeDate = '2025-06-06'
    AND ExchangeCode IN ('CMX', 'SHFE')
ORDER BY ExchangeCode, GenericTicker;