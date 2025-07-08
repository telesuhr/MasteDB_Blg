-- T_GenericContractMappingのマッピングロジックを修正
-- 取引最終日まではLP1は現在の限月を維持する

-- 1. 2025年6月16日のLP1のマッピングをLPM25に修正
UPDATE T_GenericContractMapping
SET ActualContractID = 177  -- LPM25のID
WHERE GenericID = (SELECT GenericID FROM M_GenericFutures WHERE GenericTicker = 'LP1 Comdty')
    AND TradeDate = '2025-06-16';

-- 2. 確認クエリ
SELECT 
    m.TradeDate,
    gf.GenericTicker,
    m.ActualContractID,
    ac.ContractTicker,
    ac.LastTradeableDate,
    CASE 
        WHEN m.TradeDate <= ac.LastTradeableDate THEN 'Current Month'
        ELSE 'Rolled Over'
    END as Status
FROM T_GenericContractMapping m
INNER JOIN M_GenericFutures gf ON m.GenericID = gf.GenericID
INNER JOIN M_ActualContract ac ON m.ActualContractID = ac.ActualContractID
WHERE gf.GenericTicker = 'LP1 Comdty'
    AND m.TradeDate BETWEEN '2025-06-10' AND '2025-06-20'
ORDER BY m.TradeDate;

-- 3. より一般的な修正：全てのGeneric Futuresについて
-- 取引最終日まではロールオーバーしないようにする更新クエリ
WITH CurrentMapping AS (
    -- 各GenericIDと日付に対して、適切な契約を特定
    SELECT 
        gf.GenericID,
        dates.TradeDate,
        ac.ActualContractID,
        ac.LastTradeableDate,
        ROW_NUMBER() OVER (
            PARTITION BY gf.GenericID, dates.TradeDate 
            ORDER BY 
                CASE 
                    WHEN dates.TradeDate <= ac.LastTradeableDate THEN 0  -- 取引最終日前なら優先
                    ELSE 1 
                END,
                ac.ContractMonth  -- 同じ条件なら早い限月を優先
        ) as rn
    FROM M_GenericFutures gf
    CROSS JOIN (
        SELECT DISTINCT TradeDate 
        FROM T_GenericContractMapping
    ) dates
    INNER JOIN M_ActualContract ac 
        ON gf.MetalID = ac.MetalID 
        AND gf.ExchangeCode = ac.ExchangeCode
    WHERE ac.IsActive = 1
)
-- 更新実行（コメントアウト中 - 実行前に確認してください）
-- UPDATE m
-- SET m.ActualContractID = cm.ActualContractID
-- FROM T_GenericContractMapping m
-- INNER JOIN CurrentMapping cm 
--     ON m.GenericID = cm.GenericID 
--     AND m.TradeDate = cm.TradeDate
--     AND cm.rn = 1
-- WHERE m.ActualContractID <> cm.ActualContractID;

-- 4. ビューの修正版：取引最終日当日はCalendarDaysToExpiryを0にする
CREATE OR ALTER VIEW V_CommodityPriceWithMaturityEx AS
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
    
    -- 暦日ベースの計算（取引最終日当日は0にする）
    CASE 
        WHEN lm.LastTradeableDate IS NOT NULL THEN
            CASE 
                WHEN cp.TradeDate >= lm.LastTradeableDate THEN 0  -- 取引最終日以降は0
                ELSE DATEDIFF(day, cp.TradeDate, lm.LastTradeableDate)
            END
        ELSE NULL
    END as CalendarDaysToExpiry,
    
    -- 営業日ベースの計算（取引最終日当日は0にする）
    CASE 
        WHEN lm.LastTradeableDate IS NOT NULL THEN
            CASE 
                WHEN cp.TradeDate >= lm.LastTradeableDate THEN 0  -- 取引最終日以降は0
                ELSE dbo.GetTradingDaysBetween(cp.TradeDate, lm.LastTradeableDate, gf.ExchangeCode)
            END
        ELSE NULL
    END as TradingDaysToExpiry,
    
    -- ロールオーバー推奨
    CASE 
        WHEN lm.LastTradeableDate IS NOT NULL THEN
            CASE
                WHEN cp.TradeDate >= lm.LastTradeableDate THEN 'EXPIRED'  -- 取引最終日以降
                WHEN DATEDIFF(day, cp.TradeDate, lm.LastTradeableDate) <= 5 THEN 'URGENT'
                WHEN DATEDIFF(day, cp.TradeDate, lm.LastTradeableDate) <= 10 THEN 'SOON'
                WHEN DATEDIFF(day, cp.TradeDate, lm.LastTradeableDate) <= 30 THEN 'OK'
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
SELECT TOP 20
    TradeDate,
    GenericTicker,
    ActualContractID,
    ActualContract,
    LastTradeableDate,
    CalendarDaysToExpiry,
    TradingDaysToExpiry,
    RolloverRecommendation
FROM V_CommodityPriceWithMaturityEx
WHERE GenericTicker = 'LP1 Comdty'
    AND TradeDate BETWEEN '2025-06-13' AND '2025-06-20'
ORDER BY TradeDate DESC;