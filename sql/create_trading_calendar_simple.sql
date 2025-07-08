-- Azure SQL Database対応版：取引所別営業日カレンダー

-- 既存のオブジェクトを削除
IF EXISTS (SELECT * FROM sys.objects WHERE name = 'GetTradingDaysBetween' AND type = 'FN')
    DROP FUNCTION dbo.GetTradingDaysBetween;
IF EXISTS (SELECT * FROM sys.objects WHERE name = 'V_CommodityPriceWithTradingDays' AND type = 'V')
    DROP VIEW V_CommodityPriceWithTradingDays;
GO

-- 1. 営業日計算関数（簡易版）
CREATE FUNCTION dbo.GetTradingDaysBetween
(
    @StartDate DATE,
    @EndDate DATE,
    @ExchangeCode NVARCHAR(10)
)
RETURNS INT
AS
BEGIN
    DECLARE @TradingDays INT;
    
    -- カレンダーテーブルから営業日数を計算
    SELECT @TradingDays = COUNT(*)
    FROM M_TradingCalendar
    WHERE ExchangeCode = @ExchangeCode
        AND CalendarDate > @StartDate
        AND CalendarDate <= @EndDate
        AND IsTradingDay = 1;
    
    RETURN ISNULL(@TradingDays, 0);
END;

GO

-- 2. 営業日ベースの満期情報ビュー（簡易版）
CREATE VIEW V_CommodityPriceWithTradingDays AS
SELECT 
    s.*,
    
    -- Bloombergから取得した実際の日付
    g.LastTradeableDate,
    g.FutureDeliveryDateLast,
    g.LastRefreshDate,
    
    -- 暦日ベースの日数（既存）
    CASE 
        WHEN g.LastTradeableDate IS NOT NULL THEN
            DATEDIFF(day, s.TradeDate, g.LastTradeableDate)
        ELSE NULL
    END as CalendarDaysRemaining,
    
    -- 営業日ベースの残取引可能日数
    CASE 
        WHEN g.LastTradeableDate IS NOT NULL THEN
            dbo.GetTradingDaysBetween(s.TradeDate, g.LastTradeableDate, g.ExchangeCode)
        ELSE NULL
    END as TradingDaysRemaining,
    
    -- 暦日と営業日の差（休日数）
    CASE 
        WHEN g.LastTradeableDate IS NOT NULL THEN
            DATEDIFF(day, s.TradeDate, g.LastTradeableDate) - 
            dbo.GetTradingDaysBetween(s.TradeDate, g.LastTradeableDate, g.ExchangeCode)
        ELSE NULL
    END as HolidaysInPeriod,
    
    g.RolloverDays
    
FROM V_CommodityPriceSimple s
INNER JOIN M_GenericFutures g ON s.GenericID = g.GenericID;

GO

-- 3. カレンダー統計ビュー
CREATE OR ALTER VIEW V_TradingCalendarSummary AS
SELECT 
    ExchangeCode,
    YEAR(CalendarDate) as Year,
    MONTH(CalendarDate) as Month,
    COUNT(*) as TotalDays,
    SUM(CAST(IsTradingDay as INT)) as TradingDays,
    SUM(CAST(IsHoliday as INT)) as Holidays,
    SUM(CASE WHEN HolidayType = 'Weekend' THEN 1 ELSE 0 END) as Weekends,
    SUM(CASE WHEN HolidayType = 'ExchangeHoliday' THEN 1 ELSE 0 END) as ExchangeHolidays,
    SUM(CASE WHEN HolidayType = 'InferredHoliday' THEN 1 ELSE 0 END) as InferredHolidays
FROM M_TradingCalendar
GROUP BY ExchangeCode, YEAR(CalendarDate), MONTH(CalendarDate);

GO

-- 4. 今後の休日一覧ビュー
CREATE OR ALTER VIEW V_UpcomingHolidays AS
SELECT 
    ExchangeCode,
    CalendarDate,
    DATENAME(WEEKDAY, CalendarDate) as DayOfWeek,
    HolidayName,
    HolidayType,
    DATEDIFF(DAY, GETDATE(), CalendarDate) as DaysFromToday
FROM M_TradingCalendar
WHERE IsHoliday = 1
    AND CalendarDate > GETDATE()
    AND CalendarDate <= DATEADD(MONTH, 3, GETDATE());

GO