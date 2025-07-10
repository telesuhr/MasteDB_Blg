-- カスタムマッピングロジックを実装したビュー
-- 取引最終日まで現在の限月を維持し、取引最終日翌日から次の限月に切り替える

-- 既存のビューを削除
IF EXISTS (SELECT * FROM sys.objects WHERE name = 'V_CommodityPriceWithMaturityEx' AND type = 'V')
    DROP VIEW V_CommodityPriceWithMaturityEx;
GO

-- カスタムマッピングロジックを持つビューを作成
CREATE VIEW V_CommodityPriceWithMaturityEx AS
WITH CustomMapping AS (
    -- 各取引日において、適切な契約を選択するロジック
    SELECT 
        gf.GenericID,
        p.TradeDate,
        ac.ActualContractID,
        ac.ContractTicker as ActualContract,
        ac.ContractMonth,
        ac.ContractMonthCode,
        ac.ContractYear,
        ac.LastTradeableDate,
        ac.DeliveryDate as FutureDeliveryDateLast,
        -- 契約選択の優先順位
        ROW_NUMBER() OVER (
            PARTITION BY gf.GenericID, p.TradeDate 
            ORDER BY 
                -- 1. 取引最終日前の契約を優先（取引最終日当日も含む）
                CASE 
                    WHEN p.TradeDate <= ac.LastTradeableDate THEN 0
                    ELSE 1
                END,
                -- 2. GenericNumber番目の契約を選択
                ROW_NUMBER() OVER (
                    PARTITION BY gf.GenericID, p.TradeDate,
                        CASE WHEN p.TradeDate <= ac.LastTradeableDate THEN 0 ELSE 1 END
                    ORDER BY ac.ContractMonth
                )
        ) as rn
    FROM T_CommodityPrice p
    CROSS JOIN M_GenericFutures gf
    INNER JOIN M_ActualContract ac 
        ON gf.MetalID = ac.MetalID 
        AND gf.ExchangeCode = ac.ExchangeCode
        AND ac.IsActive = 1
    WHERE p.DataType = 'Generic'
        AND p.GenericID = gf.GenericID
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
    
    -- カスタムマッピングから取得する満期情報
    cm.LastTradeableDate,
    cm.FutureDeliveryDateLast,
    gf.LastRefreshDate,
    
    -- 暦日ベースの計算（取引最終日当日は0）
    CASE 
        WHEN cm.LastTradeableDate IS NOT NULL THEN
            CASE 
                WHEN cp.TradeDate >= cm.LastTradeableDate THEN 0
                ELSE DATEDIFF(day, cp.TradeDate, cm.LastTradeableDate)
            END
        ELSE NULL
    END as CalendarDaysToExpiry,
    
    -- 営業日ベースの計算（取引最終日当日は0）
    CASE 
        WHEN cm.LastTradeableDate IS NOT NULL THEN
            CASE 
                WHEN cp.TradeDate >= cm.LastTradeableDate THEN 0
                ELSE dbo.GetTradingDaysBetween(cp.TradeDate, cm.LastTradeableDate, gf.ExchangeCode)
            END
        ELSE NULL
    END as TradingDaysToExpiry,
    
    -- ロールオーバー推奨
    CASE 
        WHEN cm.LastTradeableDate IS NOT NULL THEN
            CASE
                WHEN cp.TradeDate >= cm.LastTradeableDate THEN 'EXPIRED'
                WHEN DATEDIFF(day, cp.TradeDate, cm.LastTradeableDate) <= 5 THEN 'URGENT'
                WHEN DATEDIFF(day, cp.TradeDate, cm.LastTradeableDate) <= 10 THEN 'SOON'
                WHEN DATEDIFF(day, cp.TradeDate, cm.LastTradeableDate) <= 30 THEN 'OK'
                ELSE 'OK'
            END
        ELSE 'OK'
    END as RolloverRecommendation,
    
    -- 期間計算
    CASE 
        WHEN cm.LastTradeableDate IS NOT NULL AND cp.TradeDate < cm.LastTradeableDate THEN
            DATEDIFF(week, cp.TradeDate, cm.LastTradeableDate)
        ELSE 0
    END as WeeksToExpiry,
    
    CASE 
        WHEN cm.LastTradeableDate IS NOT NULL AND cp.TradeDate < cm.LastTradeableDate THEN
            DATEDIFF(month, cp.TradeDate, cm.LastTradeableDate)
        ELSE 0
    END as MonthsToExpiry,
    
    -- 満期から決済までの期間
    CASE 
        WHEN cm.LastTradeableDate IS NOT NULL AND cm.FutureDeliveryDateLast IS NOT NULL THEN
            DATEDIFF(day, cm.LastTradeableDate, cm.FutureDeliveryDateLast)
        ELSE NULL
    END as SettlementPeriodDays,
    
    -- 実際の契約情報
    cm.ActualContractID,
    cm.ActualContract,
    cm.ContractMonth,
    cm.ContractMonthCode,
    cm.ContractYear,
    
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
INNER JOIN CustomMapping cm ON cp.GenericID = cm.GenericID 
    AND cp.TradeDate = cm.TradeDate 
    AND cm.rn = gf.GenericNumber  -- GenericNumberに対応する順位の契約を選択
WHERE cp.DataType = 'Generic';

GO

-- 確認用クエリ
SELECT TOP 30
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
    AND TradeDate BETWEEN '2025-06-10' AND '2025-06-20'
ORDER BY TradeDate DESC;

-- 注意: このビューは既存のT_GenericContractMappingテーブルを使用せず、
-- 独自のロジックで契約を選択します。
-- LP1は常に「取引可能な最も近い限月」を参照し、
-- 取引最終日の翌日から次の限月に切り替わります。