-- V_CommodityPriceWithMaturityExビューを修正（簡易営業日計算版）
-- 動的にGenericContractMappingとActualContractから満期情報を取得

-- 既存のビューを削除
IF EXISTS (SELECT * FROM sys.objects WHERE name = 'V_CommodityPriceWithMaturityEx' AND type = 'V')
    DROP VIEW V_CommodityPriceWithMaturityEx;
GO

-- 修正版ビューを作成
CREATE VIEW V_CommodityPriceWithMaturityEx AS
WITH LatestMapping AS (
    -- 各GenericIDの最新の契約マッピングを取得
    SELECT 
        m.GenericID,
        m.TradeDate,
        m.ActualContractID,
        ac.ContractTicker as ActualContract,
        ac.ContractMonth,
        ac.ContractMonthCode,
        ac.ContractYear,
        ac.LastTradeableDate,
        ac.DeliveryDate as FutureDeliveryDateLast,
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
    m.MetalCode,
    m.MetalName,
    gf.ExchangeCode,
    CASE 
        WHEN gf.ExchangeCode = 'CMX' THEN 'COMEX'
        WHEN gf.ExchangeCode = 'LME' THEN 'London Metal Exchange'
        WHEN gf.ExchangeCode = 'SHFE' THEN 'Shanghai Futures Exchange'
        ELSE gf.ExchangeCode
    END AS ExchangeName,
    
    -- 価格データ
    cp.OpenPrice,
    cp.HighPrice,
    cp.LowPrice,
    cp.LastPrice,
    cp.SettlementPrice,
    cp.Volume,
    cp.OpenInterest,
    
    -- 動的に取得する満期情報（T_GenericContractMappingから）
    lm.LastTradeableDate,
    lm.FutureDeliveryDateLast,
    gf.LastRefreshDate,
    
    -- 暦日ベースの計算
    CASE 
        WHEN lm.LastTradeableDate IS NOT NULL THEN
            DATEDIFF(day, cp.TradeDate, lm.LastTradeableDate)
        ELSE NULL
    END as CalendarDaysToExpiry,
    
    -- 簡易営業日計算（土日を除外）
    CASE 
        WHEN lm.LastTradeableDate IS NOT NULL THEN
            DATEDIFF(day, cp.TradeDate, lm.LastTradeableDate) - 
            (DATEDIFF(week, cp.TradeDate, lm.LastTradeableDate) * 2) -
            CASE 
                WHEN DATEPART(weekday, cp.TradeDate) = 1 THEN 1 
                WHEN DATEPART(weekday, cp.TradeDate) = 7 THEN 1
                ELSE 0 
            END -
            CASE 
                WHEN DATEPART(weekday, lm.LastTradeableDate) = 1 THEN 1 
                WHEN DATEPART(weekday, lm.LastTradeableDate) = 7 THEN 1
                ELSE 0 
            END
        ELSE NULL
    END as TradingDaysToExpiry,
    
    -- ロールオーバー推奨
    CASE 
        WHEN lm.LastTradeableDate IS NOT NULL AND DATEDIFF(day, cp.TradeDate, lm.LastTradeableDate) <= 30 THEN
            CASE
                WHEN DATEDIFF(day, cp.TradeDate, lm.LastTradeableDate) <= 5 THEN 'URGENT'
                WHEN DATEDIFF(day, cp.TradeDate, lm.LastTradeableDate) <= 10 THEN 'SOON'
                ELSE 'OK'
            END
        ELSE 'OK'
    END as RolloverRecommendation,
    
    -- 期間計算
    CASE 
        WHEN lm.LastTradeableDate IS NOT NULL THEN
            DATEDIFF(week, cp.TradeDate, lm.LastTradeableDate)
        ELSE NULL
    END as WeeksToExpiry,
    
    CASE 
        WHEN lm.LastTradeableDate IS NOT NULL THEN
            DATEDIFF(month, cp.TradeDate, lm.LastTradeableDate)
        ELSE NULL
    END as MonthsToExpiry,
    
    -- 満期から決済までの期間
    CASE 
        WHEN lm.LastTradeableDate IS NOT NULL AND lm.FutureDeliveryDateLast IS NOT NULL THEN
            DATEDIFF(day, lm.LastTradeableDate, lm.FutureDeliveryDateLast)
        ELSE NULL
    END as SettlementPeriodDays,
    
    -- 実際の契約情報
    lm.ActualContractID,
    lm.ActualContract,
    lm.ContractMonth,
    lm.ContractMonthCode,
    lm.ContractYear,
    
    -- データ品質フラグ
    CASE 
        WHEN cp.SettlementPrice IS NOT NULL AND cp.OpenPrice IS NOT NULL 
             AND cp.HighPrice IS NOT NULL AND cp.LowPrice IS NOT NULL THEN 1
        ELSE 0
    END as HasCompletePriceData,
    
    CASE 
        WHEN cp.Volume IS NOT NULL AND cp.Volume > 0 THEN 1
        ELSE 0
    END as HasVolumeData,
    
    -- 日中変動率
    CASE 
        WHEN cp.LastPrice IS NOT NULL AND cp.OpenPrice IS NOT NULL AND cp.OpenPrice <> 0 THEN
            ROUND((cp.LastPrice - cp.OpenPrice) / cp.OpenPrice * 100, 6)
        ELSE NULL
    END as IntradayChangePercent,
    
    -- 日中レンジ
    CASE 
        WHEN cp.HighPrice IS NOT NULL AND cp.LowPrice IS NOT NULL AND cp.LowPrice <> 0 THEN
            ROUND((cp.HighPrice - cp.LowPrice) / cp.LowPrice * 100, 6)
        ELSE NULL
    END as IntradayRangePercent,
    
    cp.LastUpdated
    
FROM T_CommodityPrice cp
INNER JOIN M_GenericFutures gf ON cp.GenericID = gf.GenericID
INNER JOIN M_Metal m ON gf.MetalID = m.MetalID
LEFT JOIN LatestMapping lm ON cp.GenericID = lm.GenericID 
    AND cp.TradeDate = lm.TradeDate 
    AND lm.rn = 1
WHERE cp.DataType = 'Generic';

GO

-- 確認用クエリ
-- 動的な満期情報が正しく取得されているか確認
SELECT TOP 20
    TradeDate,
    GenericTicker,
    ActualContractID,
    ActualContract,
    ContractMonth,
    LastTradeableDate,
    FutureDeliveryDateLast,
    CalendarDaysToExpiry,
    TradingDaysToExpiry,
    RolloverRecommendation
FROM V_CommodityPriceWithMaturityEx
WHERE GenericTicker = 'LP1 Comdty'
    AND TradeDate >= '2025-06-09'
ORDER BY TradeDate DESC;