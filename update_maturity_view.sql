-- ============================================================
-- V_CommodityPriceWithMaturityEx ビューの更新
-- 現在のテーブル構造に合わせて修正
-- ============================================================

USE [JCL];
GO

-- 既存のビューを削除
IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_NAME = 'V_CommodityPriceWithMaturityEx')
BEGIN
    DROP VIEW V_CommodityPriceWithMaturityEx;
END
GO

-- 営業日ベースの満期情報を含む拡張ビュー
CREATE VIEW V_CommodityPriceWithMaturityEx AS
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
    CASE gf.ExchangeCode
        WHEN 'CMX' THEN 'COMEX'
        ELSE gf.ExchangeCode
    END as ExchangeName,
    
    -- 価格データ
    cp.OpenPrice,
    cp.HighPrice,
    cp.LowPrice,
    cp.LastPrice,
    cp.SettlementPrice,
    cp.Volume,
    cp.OpenInterest,
    
    -- Bloombergから取得した満期情報
    gf.LastTradeableDate,
    gf.FutureDeliveryDateLast,
    gf.LastRefreshDate,
    
    -- 満期関連の計算フィールド
    DATEDIFF(day, cp.TradeDate, gf.LastTradeableDate) as CalendarDaysToExpiry,
    
    -- 営業日計算（シンプル版：週末のみ除外）
    CASE 
        WHEN gf.LastTradeableDate IS NULL THEN NULL
        ELSE 
            DATEDIFF(day, cp.TradeDate, gf.LastTradeableDate) 
            - (DATEDIFF(week, cp.TradeDate, gf.LastTradeableDate) * 2)
            - CASE WHEN DATEPART(weekday, cp.TradeDate) = 1 THEN 1 ELSE 0 END
            - CASE WHEN DATEPART(weekday, gf.LastTradeableDate) = 7 THEN 1 ELSE 0 END
    END as TradingDaysToExpiry,
    
    -- ロールオーバー推奨
    CASE 
        WHEN gf.LastTradeableDate IS NULL THEN 'UNKNOWN'
        WHEN DATEDIFF(day, cp.TradeDate, gf.LastTradeableDate) <= gf.RolloverDays THEN 'ROLLOVER_NOW'
        WHEN DATEDIFF(day, cp.TradeDate, gf.LastTradeableDate) <= gf.RolloverDays + 5 THEN 'ROLLOVER_SOON'
        ELSE 'OK'
    END as RolloverRecommendation,
    
    -- 満期までの週数と月数
    DATEDIFF(week, cp.TradeDate, gf.LastTradeableDate) as WeeksToExpiry,
    DATEDIFF(month, cp.TradeDate, gf.LastTradeableDate) as MonthsToExpiry,
    
    -- 決済期間情報
    DATEDIFF(day, gf.LastTradeableDate, gf.FutureDeliveryDateLast) as SettlementPeriodDays,
    
    -- 実契約情報（マッピングがある場合）
    gcm.ActualContractID,
    ac.ContractTicker as ActualContract,
    ac.ContractMonth,
    ac.ContractMonthCode,
    ac.ContractYear,
    
    -- データ品質フラグ
    CASE 
        WHEN cp.OpenPrice IS NULL OR cp.HighPrice IS NULL OR cp.LowPrice IS NULL OR cp.LastPrice IS NULL THEN 0
        ELSE 1
    END as HasCompletePriceData,
    
    CASE
        WHEN cp.Volume IS NULL OR cp.OpenInterest IS NULL THEN 0
        ELSE 1
    END as HasVolumeData,
    
    -- 価格変動率（日中）
    CASE 
        WHEN cp.OpenPrice IS NOT NULL AND cp.OpenPrice > 0 
        THEN ((cp.LastPrice - cp.OpenPrice) / cp.OpenPrice) * 100
        ELSE NULL
    END as IntradayChangePercent,
    
    -- 価格レンジ（ハイ・ロー）
    CASE 
        WHEN cp.HighPrice IS NOT NULL AND cp.LowPrice IS NOT NULL AND cp.LowPrice > 0
        THEN ((cp.HighPrice - cp.LowPrice) / cp.LowPrice) * 100
        ELSE NULL
    END as IntradayRangePercent,
    
    -- 最終更新時刻
    cp.LastUpdated
    
FROM T_CommodityPrice cp
INNER JOIN M_Metal m ON cp.MetalID = m.MetalID
INNER JOIN M_GenericFutures gf ON cp.GenericID = gf.GenericID
LEFT JOIN T_GenericContractMapping gcm 
    ON cp.GenericID = gcm.GenericID 
    AND cp.TradeDate = gcm.TradeDate
LEFT JOIN M_ActualContract ac 
    ON gcm.ActualContractID = ac.ActualContractID
WHERE cp.DataType = 'Generic';
GO

PRINT 'V_CommodityPriceWithMaturityEx view has been updated successfully';
GO