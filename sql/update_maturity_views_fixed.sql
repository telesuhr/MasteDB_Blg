-- 満期ビューを営業日計算に更新（修正版）
-- 実際のテーブル構造に合わせて修正

-- 既存のビューを削除
IF EXISTS (SELECT * FROM sys.objects WHERE name = 'V_CommodityPriceWithMaturityEx' AND type = 'V')
    DROP VIEW V_CommodityPriceWithMaturityEx;
IF EXISTS (SELECT * FROM sys.objects WHERE name = 'V_MaturitySummaryWithTradingDays' AND type = 'V')
    DROP VIEW V_MaturitySummaryWithTradingDays;
IF EXISTS (SELECT * FROM sys.objects WHERE name = 'V_RolloverAlertsWithTradingDays' AND type = 'V')
    DROP VIEW V_RolloverAlertsWithTradingDays;
IF EXISTS (SELECT * FROM sys.objects WHERE name = 'V_TradingDaysCalculationDetail' AND type = 'V')
    DROP VIEW V_TradingDaysCalculationDetail;
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
    
    -- 暦日ベースの計算（従来）
    CASE 
        WHEN gf.LastTradeableDate IS NOT NULL THEN
            DATEDIFF(day, cp.TradeDate, gf.LastTradeableDate)
        ELSE NULL
    END as CalendarDaysRemaining,
    
    -- 営業日ベースの計算（新規）
    CASE 
        WHEN gf.LastTradeableDate IS NOT NULL THEN
            dbo.GetTradingDaysBetween(cp.TradeDate, gf.LastTradeableDate, gf.ExchangeCode)
        ELSE NULL
    END as TradingDaysRemaining,
    
    -- 期間内の休日数
    CASE 
        WHEN gf.LastTradeableDate IS NOT NULL THEN
            DATEDIFF(day, cp.TradeDate, gf.LastTradeableDate) - 
            dbo.GetTradingDaysBetween(cp.TradeDate, gf.LastTradeableDate, gf.ExchangeCode)
        ELSE NULL
    END as HolidaysInPeriod,
    
    -- 営業日率（％）
    CASE 
        WHEN gf.LastTradeableDate IS NOT NULL AND DATEDIFF(day, cp.TradeDate, gf.LastTradeableDate) > 0 THEN
            CAST(dbo.GetTradingDaysBetween(cp.TradeDate, gf.LastTradeableDate, gf.ExchangeCode) * 100.0 / 
                 DATEDIFF(day, cp.TradeDate, gf.LastTradeableDate) as DECIMAL(5,2))
        ELSE NULL
    END as TradingDayRate,
    
    -- 満期までの日数（営業日ベース）
    CASE 
        WHEN gf.FutureDeliveryDateLast IS NOT NULL THEN
            dbo.GetTradingDaysBetween(cp.TradeDate, gf.FutureDeliveryDateLast, gf.ExchangeCode)
        ELSE NULL
    END as TradingDaysToMaturity,
    
    -- ロールオーバー推奨日（最終取引日の指定営業日前）
    CASE 
        WHEN gf.LastTradeableDate IS NOT NULL AND gf.RolloverDays IS NOT NULL THEN
            -- 最終取引日からRolloverDays営業日前を計算（簡易版）
            DATEADD(day, -CAST(gf.RolloverDays * 1.4 as INT), gf.LastTradeableDate)
        ELSE NULL
    END as SuggestedRolloverDate,
    
    -- ロールオーバーまでの営業日数
    CASE 
        WHEN gf.LastTradeableDate IS NOT NULL AND gf.RolloverDays IS NOT NULL THEN
            dbo.GetTradingDaysBetween(
                cp.TradeDate, 
                DATEADD(day, -CAST(gf.RolloverDays * 1.4 as INT), gf.LastTradeableDate),
                gf.ExchangeCode
            )
        ELSE NULL
    END as TradingDaysToRollover,
    
    -- その他のフィールド
    gf.RolloverDays,
    cp.DataType,
    cp.LastUpdated
    
FROM T_CommodityPrice_V2 cp
INNER JOIN M_GenericFutures gf ON cp.GenericID = gf.GenericID
INNER JOIN M_Metal m ON gf.MetalID = m.MetalID
WHERE cp.DataType = 'Generic';

GO

-- 営業日情報のサマリービュー
CREATE VIEW V_MaturitySummaryWithTradingDays AS
SELECT 
    ExchangeCode,
    COUNT(DISTINCT GenericTicker) as ActiveContracts,
    
    -- 暦日ベースの統計
    AVG(CalendarDaysRemaining) as AvgCalendarDays,
    MIN(CalendarDaysRemaining) as MinCalendarDays,
    MAX(CalendarDaysRemaining) as MaxCalendarDays,
    
    -- 営業日ベースの統計
    AVG(TradingDaysRemaining) as AvgTradingDays,
    MIN(TradingDaysRemaining) as MinTradingDays,
    MAX(TradingDaysRemaining) as MaxTradingDays,
    
    -- 営業日率
    AVG(TradingDayRate) as AvgTradingDayRate,
    
    -- 平均休日数
    AVG(HolidaysInPeriod) as AvgHolidays
    
FROM V_CommodityPriceWithMaturityEx
WHERE TradeDate = (SELECT MAX(TradeDate) FROM V_CommodityPriceWithMaturityEx)
    AND TradingDaysRemaining IS NOT NULL
    AND Volume > 0
GROUP BY ExchangeCode;

GO

-- 営業日ベースのロールオーバー警告ビュー
CREATE VIEW V_RolloverAlertsWithTradingDays AS
SELECT TOP 100 PERCENT
    ExchangeCode,
    GenericTicker,
    TradeDate,
    LastTradeableDate,
    TradingDaysRemaining,
    TradingDaysToRollover,
    Volume,
    OpenInterest,
    CASE 
        WHEN TradingDaysToRollover <= 0 THEN 'IMMEDIATE'
        WHEN TradingDaysToRollover <= 5 THEN 'URGENT'
        WHEN TradingDaysToRollover <= 10 THEN 'SOON'
        ELSE 'OK'
    END as RolloverStatus,
    CASE 
        WHEN TradingDaysToRollover <= 0 THEN '即時ロールオーバー必要'
        WHEN TradingDaysToRollover <= 5 THEN '5営業日以内にロールオーバー推奨'
        WHEN TradingDaysToRollover <= 10 THEN '10営業日以内にロールオーバー検討'
        ELSE '正常'
    END as StatusMessage
FROM V_CommodityPriceWithMaturityEx
WHERE TradeDate = (SELECT MAX(TradeDate) FROM V_CommodityPriceWithMaturityEx)
    AND TradingDaysToRollover IS NOT NULL
    AND Volume > 0
    AND TradingDaysToRollover <= 20  -- 20営業日以内のもののみ表示
ORDER BY TradingDaysToRollover;

GO

-- デバッグ用：特定銘柄の営業日計算詳細
CREATE VIEW V_TradingDaysCalculationDetail AS
SELECT TOP 100
    GenericTicker,
    TradeDate,
    LastTradeableDate,
    CONCAT(
        '取引日: ', FORMAT(TradeDate, 'yyyy-MM-dd (ddd)'), ' → ',
        '最終取引日: ', FORMAT(LastTradeableDate, 'yyyy-MM-dd (ddd)'), ' = ',
        '暦日: ', CalendarDaysRemaining, '日 (',
        '営業日: ', TradingDaysRemaining, '日 + ',
        '休日: ', HolidaysInPeriod, '日)'
    ) as CalculationDetail,
    TradingDayRate as '営業日率(%)',
    Volume
FROM V_CommodityPriceWithMaturityEx
WHERE TradeDate = (SELECT MAX(TradeDate) FROM V_CommodityPriceWithMaturityEx)
    AND TradingDaysRemaining IS NOT NULL
    AND Volume > 0
ORDER BY TradingDaysRemaining;

GO