-- 取引所別営業日カレンダーテーブルと関連機能の作成

-- 1. 営業日カレンダーマスターテーブル
CREATE TABLE M_TradingCalendar (
    CalendarID BIGINT IDENTITY(1,1) PRIMARY KEY,
    ExchangeCode NVARCHAR(10) NOT NULL,
    CalendarDate DATE NOT NULL,
    IsTradingDay BIT NOT NULL DEFAULT 1,
    IsHoliday BIT NOT NULL DEFAULT 0,
    HolidayName NVARCHAR(100) NULL,
    HolidayType NVARCHAR(50) NULL, -- 'Weekend', 'PublicHoliday', 'ExchangeHoliday'
    LastUpdated DATETIME2 DEFAULT GETDATE(),
    CONSTRAINT UQ_Exchange_Date UNIQUE (ExchangeCode, CalendarDate)
);

-- インデックス作成
CREATE NONCLUSTERED INDEX IX_TradingCalendar_Exchange_Date 
ON M_TradingCalendar (ExchangeCode, CalendarDate, IsTradingDay);

CREATE NONCLUSTERED INDEX IX_TradingCalendar_Date 
ON M_TradingCalendar (CalendarDate, ExchangeCode);

GO

-- 2. 営業日計算関数（カレンダーテーブル使用版）
CREATE OR ALTER FUNCTION dbo.GetTradingDaysBetween
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
    
    -- データがない場合は簡易計算（土日除外）
    IF @TradingDays IS NULL OR @TradingDays = 0
    BEGIN
        SELECT @TradingDays = COUNT(*)
        FROM (
            SELECT DATEADD(DAY, number, @StartDate) AS CalendarDate
            FROM master..spt_values
            WHERE type = 'P' 
                AND number <= DATEDIFF(DAY, @StartDate, @EndDate)
        ) dates
        WHERE DATEPART(WEEKDAY, CalendarDate) NOT IN (1, 7); -- 日曜、土曜を除外
    END
    
    RETURN @TradingDays;
END;

GO

-- 3. 次の営業日を取得する関数
CREATE OR ALTER FUNCTION dbo.GetNextTradingDay
(
    @CurrentDate DATE,
    @ExchangeCode NVARCHAR(10)
)
RETURNS DATE
AS
BEGIN
    DECLARE @NextTradingDay DATE;
    
    -- カレンダーテーブルから次の営業日を取得
    SELECT TOP 1 @NextTradingDay = CalendarDate
    FROM M_TradingCalendar
    WHERE ExchangeCode = @ExchangeCode
        AND CalendarDate > @CurrentDate
        AND IsTradingDay = 1
    ORDER BY CalendarDate;
    
    -- データがない場合は翌平日
    IF @NextTradingDay IS NULL
    BEGIN
        SET @NextTradingDay = DATEADD(DAY, 1, @CurrentDate);
        WHILE DATEPART(WEEKDAY, @NextTradingDay) IN (1, 7)
        BEGIN
            SET @NextTradingDay = DATEADD(DAY, 1, @NextTradingDay);
        END
    END
    
    RETURN @NextTradingDay;
END;

GO

-- 4. N営業日前/後を計算する関数
CREATE OR ALTER FUNCTION dbo.AddTradingDays
(
    @StartDate DATE,
    @DaysToAdd INT,
    @ExchangeCode NVARCHAR(10)
)
RETURNS DATE
AS
BEGIN
    DECLARE @ResultDate DATE;
    DECLARE @Direction INT = CASE WHEN @DaysToAdd < 0 THEN -1 ELSE 1 END;
    DECLARE @DaysCount INT = ABS(@DaysToAdd);
    DECLARE @CurrentCount INT = 0;
    
    SET @ResultDate = @StartDate;
    
    -- カレンダーテーブルを使用
    IF EXISTS (SELECT 1 FROM M_TradingCalendar WHERE ExchangeCode = @ExchangeCode)
    BEGIN
        WITH TradingDays AS (
            SELECT 
                CalendarDate,
                ROW_NUMBER() OVER (ORDER BY CalendarDate) as RowNum
            FROM M_TradingCalendar
            WHERE ExchangeCode = @ExchangeCode
                AND IsTradingDay = 1
                AND CalendarDate > @StartDate
        )
        SELECT @ResultDate = CalendarDate
        FROM TradingDays
        WHERE RowNum = @DaysCount;
    END
    ELSE
    BEGIN
        -- 簡易計算（土日除外）
        WHILE @CurrentCount < @DaysCount
        BEGIN
            SET @ResultDate = DATEADD(DAY, @Direction, @ResultDate);
            IF DATEPART(WEEKDAY, @ResultDate) NOT IN (1, 7)
                SET @CurrentCount = @CurrentCount + 1;
        END
    END
    
    RETURN @ResultDate;
END;

GO

-- 5. 営業日ベースの満期情報ビュー
CREATE OR ALTER VIEW V_CommodityPriceWithTradingDays AS
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
    
    -- 営業日ベースのロールオーバー推奨日
    CASE 
        WHEN g.LastTradeableDate IS NOT NULL AND g.RolloverDays IS NOT NULL THEN
            dbo.AddTradingDays(g.LastTradeableDate, -g.RolloverDays, g.ExchangeCode)
        ELSE NULL
    END as TradingDayRolloverDate,
    
    -- 営業日ベースのロールオーバーフラグ
    CASE 
        WHEN g.LastTradeableDate IS NOT NULL 
            AND g.RolloverDays IS NOT NULL
            AND dbo.GetTradingDaysBetween(s.TradeDate, g.LastTradeableDate, g.ExchangeCode) <= g.RolloverDays
        THEN 'Yes'
        ELSE 'No'
    END as TradingDayShouldRollover,
    
    -- 満期までの営業日数
    CASE 
        WHEN g.FutureDeliveryDateLast IS NOT NULL THEN
            dbo.GetTradingDaysBetween(s.TradeDate, g.FutureDeliveryDateLast, g.ExchangeCode)
        ELSE NULL
    END as TradingDaysToDelivery,
    
    g.RolloverDays,
    g.ExchangeCode
    
FROM V_CommodityPriceSimple s
INNER JOIN M_GenericFutures g ON s.GenericID = g.GenericID

GO

-- 6. カレンダーデータ初期化（基本的な土日設定）
CREATE OR ALTER PROCEDURE sp_InitializeTradingCalendar
    @StartDate DATE = '2020-01-01',
    @EndDate DATE = '2030-12-31'
AS
BEGIN
    SET NOCOUNT ON;
    
    -- 各取引所のカレンダーを初期化
    DECLARE @Exchanges TABLE (ExchangeCode NVARCHAR(10));
    INSERT INTO @Exchanges VALUES ('CMX'), ('LME'), ('SHFE');
    
    DECLARE @CurrentDate DATE = @StartDate;
    DECLARE @ExchangeCode NVARCHAR(10);
    
    -- 各取引所ごとにカレンダーを作成
    DECLARE exchange_cursor CURSOR FOR SELECT ExchangeCode FROM @Exchanges;
    OPEN exchange_cursor;
    FETCH NEXT FROM exchange_cursor INTO @ExchangeCode;
    
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SET @CurrentDate = @StartDate;
        
        WHILE @CurrentDate <= @EndDate
        BEGIN
            -- 既存レコードがない場合のみ挿入
            IF NOT EXISTS (
                SELECT 1 FROM M_TradingCalendar 
                WHERE ExchangeCode = @ExchangeCode AND CalendarDate = @CurrentDate
            )
            BEGIN
                INSERT INTO M_TradingCalendar (ExchangeCode, CalendarDate, IsTradingDay, IsHoliday, HolidayType)
                VALUES (
                    @ExchangeCode,
                    @CurrentDate,
                    CASE WHEN DATEPART(WEEKDAY, @CurrentDate) IN (1, 7) THEN 0 ELSE 1 END,
                    CASE WHEN DATEPART(WEEKDAY, @CurrentDate) IN (1, 7) THEN 1 ELSE 0 END,
                    CASE WHEN DATEPART(WEEKDAY, @CurrentDate) IN (1, 7) THEN 'Weekend' ELSE NULL END
                );
            END
            
            SET @CurrentDate = DATEADD(DAY, 1, @CurrentDate);
        END
        
        FETCH NEXT FROM exchange_cursor INTO @ExchangeCode;
    END
    
    CLOSE exchange_cursor;
    DEALLOCATE exchange_cursor;
    
    SELECT 'Trading calendar initialized' as Result;
END;

GO

-- 使用例

-- 1. 営業日ベースの満期情報
SELECT 
    ExchangeDisplayName,
    GenericTicker,
    TradeDate,
    LastTradeableDate,
    CalendarDaysRemaining as 暦日残,
    TradingDaysRemaining as 営業日残,
    TradingDayRolloverDate as ロール推奨日,
    TradingDayShouldRollover as ロール要否
FROM V_CommodityPriceWithTradingDays
WHERE TradeDate = (SELECT MAX(TradeDate) FROM V_CommodityPriceWithTradingDays)
    AND MetalCode LIKE 'CU%'
    AND TradingDaysRemaining IS NOT NULL
ORDER BY TradingDaysRemaining

-- 2. 営業日数の比較
SELECT 
    ExchangeCode,
    COUNT(*) as 銘柄数,
    AVG(CalendarDaysRemaining) as 平均暦日,
    AVG(TradingDaysRemaining) as 平均営業日,
    AVG(CalendarDaysRemaining - TradingDaysRemaining) as 平均休日数
FROM V_CommodityPriceWithTradingDays
WHERE TradeDate = (SELECT MAX(TradeDate) FROM V_CommodityPriceWithTradingDays)
    AND TradingDaysRemaining IS NOT NULL
GROUP BY ExchangeCode