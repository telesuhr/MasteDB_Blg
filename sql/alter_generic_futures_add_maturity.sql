-- M_GenericFuturesテーブルに満期関連情報を追加
-- ロールオーバーを考慮した残取引可能日数の計算に必要

-- 1. M_GenericFuturesに満期関連カラムを追加
ALTER TABLE M_GenericFutures
ADD 
    -- 満期月の計算ルール（例: CurrentMonth, NextMonth, CurrentMonth+2, etc）
    MaturityRule NVARCHAR(50) NULL,
    
    -- ロールオーバー日数（満期何日前にロールオーバーするか）
    RolloverDays INT NULL DEFAULT 5,
    
    -- 最終取引日の計算ルール（例: 15th, LastBusinessDay, 3rdWednesday）
    LastTradingDayRule NVARCHAR(50) NULL,
    
    -- First Notice Day（現物受渡しがある場合）
    FirstNoticeDayRule NVARCHAR(50) NULL,
    
    -- 備考
    MaturityNotes NVARCHAR(500) NULL;

GO

-- 2. 取引所別のデフォルトルールを設定
UPDATE M_GenericFutures
SET 
    MaturityRule = CASE 
        WHEN ExchangeCode = 'LME' THEN
            CASE GenericNumber
                WHEN 0 THEN 'CurrentDay'      -- 現物
                WHEN -1 THEN 'NextDay'        -- トムネクスト
                WHEN 1 THEN 'CurrentMonth+3'  -- 3ヶ月物
                WHEN 2 THEN 'CurrentMonth+4'
                WHEN 3 THEN 'CurrentMonth+5'
                WHEN 4 THEN 'CurrentMonth+6'
                WHEN 5 THEN 'CurrentMonth+7'
                WHEN 6 THEN 'CurrentMonth+8'
                WHEN 7 THEN 'CurrentMonth+9'
                WHEN 8 THEN 'CurrentMonth+10'
                WHEN 9 THEN 'CurrentMonth+11'
                WHEN 10 THEN 'CurrentMonth+12'
                WHEN 11 THEN 'CurrentMonth+13'
                WHEN 12 THEN 'CurrentMonth+14'
                WHEN 13 THEN 'CurrentMonth+15'
                WHEN 14 THEN 'CurrentMonth+16'
                WHEN 15 THEN 'CurrentMonth+17'
                ELSE 'CurrentMonth+' + CAST(GenericNumber + 2 AS NVARCHAR(10))
            END
        WHEN ExchangeCode = 'CMX' THEN
            'NextActiveMonth+' + CAST(GenericNumber - 1 AS NVARCHAR(10))
        WHEN ExchangeCode = 'SHFE' THEN
            'CurrentMonth+' + CAST(GenericNumber AS NVARCHAR(10))
        ELSE NULL
    END,
    
    RolloverDays = CASE 
        WHEN ExchangeCode = 'LME' THEN 0     -- LMEは満期日まで取引
        WHEN ExchangeCode = 'CMX' THEN 5     -- COMEXは5営業日前にロール
        WHEN ExchangeCode = 'SHFE' THEN 3    -- SHFEは3営業日前にロール
        ELSE 5
    END,
    
    LastTradingDayRule = CASE 
        WHEN ExchangeCode = 'LME' THEN '3rdWednesday'      -- 第3水曜日
        WHEN ExchangeCode = 'CMX' THEN '3rdLastBusinessDay' -- 月末から3営業日前
        WHEN ExchangeCode = 'SHFE' THEN '15th'             -- 15日
        ELSE NULL
    END,
    
    FirstNoticeDayRule = CASE 
        WHEN ExchangeCode = 'CMX' THEN 'LastBusinessDayOfPrevMonth'
        ELSE NULL
    END
WHERE MaturityRule IS NULL;

GO

-- 3. 満期日を計算する関数を作成
CREATE OR ALTER FUNCTION dbo.CalculateMaturityDate
(
    @TradeDate DATE,
    @MaturityRule NVARCHAR(50)
)
RETURNS DATE
AS
BEGIN
    DECLARE @MaturityDate DATE;
    DECLARE @MonthsToAdd INT;
    
    -- ルールがNULLの場合はNULLを返す
    IF @MaturityRule IS NULL
        RETURN NULL;
    
    -- 現物・トムネクストの特殊ケース
    IF @MaturityRule = 'CurrentDay'
        RETURN @TradeDate;
    
    IF @MaturityRule = 'NextDay'
        RETURN DATEADD(DAY, 1, @TradeDate);
    
    -- CurrentMonth+N のパターン
    IF @MaturityRule LIKE 'CurrentMonth+%'
    BEGIN
        SET @MonthsToAdd = CAST(SUBSTRING(@MaturityRule, 14, 10) AS INT);
        SET @MaturityDate = DATEADD(MONTH, @MonthsToAdd, @TradeDate);
        -- 月初に設定
        SET @MaturityDate = DATEFROMPARTS(YEAR(@MaturityDate), MONTH(@MaturityDate), 1);
        RETURN @MaturityDate;
    END
    
    -- NextActiveMonth+N のパターン（COMEX用）
    IF @MaturityRule LIKE 'NextActiveMonth+%'
    BEGIN
        SET @MonthsToAdd = CAST(SUBSTRING(@MaturityRule, 17, 10) AS INT);
        -- 次のアクティブ月を計算（簡易版：翌月とする）
        SET @MaturityDate = DATEADD(MONTH, @MonthsToAdd + 1, @TradeDate);
        SET @MaturityDate = DATEFROMPARTS(YEAR(@MaturityDate), MONTH(@MaturityDate), 1);
        RETURN @MaturityDate;
    END
    
    RETURN NULL;
END;

GO

-- 4. 最終取引日を計算する関数
CREATE OR ALTER FUNCTION dbo.CalculateLastTradingDay
(
    @MaturityMonth DATE,
    @LastTradingDayRule NVARCHAR(50)
)
RETURNS DATE
AS
BEGIN
    DECLARE @LastTradingDay DATE;
    DECLARE @LastDayOfMonth DATE;
    DECLARE @DayOfWeek INT;
    
    IF @LastTradingDayRule IS NULL OR @MaturityMonth IS NULL
        RETURN NULL;
    
    -- 月末を取得
    SET @LastDayOfMonth = EOMONTH(@MaturityMonth);
    
    -- 15日ルール（SHFE）
    IF @LastTradingDayRule = '15th'
    BEGIN
        SET @LastTradingDay = DATEFROMPARTS(YEAR(@MaturityMonth), MONTH(@MaturityMonth), 15);
        -- 15日が週末の場合は前営業日に調整
        SET @DayOfWeek = DATEPART(WEEKDAY, @LastTradingDay);
        IF @DayOfWeek = 1 -- 日曜日
            SET @LastTradingDay = DATEADD(DAY, -2, @LastTradingDay);
        ELSE IF @DayOfWeek = 7 -- 土曜日
            SET @LastTradingDay = DATEADD(DAY, -1, @LastTradingDay);
        RETURN @LastTradingDay;
    END
    
    -- 第3水曜日ルール（LME）
    IF @LastTradingDayRule = '3rdWednesday'
    BEGIN
        -- 月初から第3水曜日を探す
        SET @LastTradingDay = DATEFROMPARTS(YEAR(@MaturityMonth), MONTH(@MaturityMonth), 1);
        DECLARE @WednesdayCount INT = 0;
        
        WHILE @WednesdayCount < 3
        BEGIN
            IF DATEPART(WEEKDAY, @LastTradingDay) = 4 -- 水曜日
                SET @WednesdayCount = @WednesdayCount + 1;
            
            IF @WednesdayCount < 3
                SET @LastTradingDay = DATEADD(DAY, 1, @LastTradingDay);
        END
        
        RETURN @LastTradingDay;
    END
    
    -- 月末から3営業日前ルール（COMEX）
    IF @LastTradingDayRule = '3rdLastBusinessDay'
    BEGIN
        SET @LastTradingDay = @LastDayOfMonth;
        DECLARE @BusinessDayCount INT = 0;
        
        WHILE @BusinessDayCount < 3
        BEGIN
            SET @LastTradingDay = DATEADD(DAY, -1, @LastTradingDay);
            SET @DayOfWeek = DATEPART(WEEKDAY, @LastTradingDay);
            
            -- 平日（月～金）の場合のみカウント
            IF @DayOfWeek NOT IN (1, 7)
                SET @BusinessDayCount = @BusinessDayCount + 1;
        END
        
        RETURN @LastTradingDay;
    END
    
    RETURN NULL;
END;

GO

-- 5. 拡張ビューを作成（残取引可能日数付き）
CREATE OR ALTER VIEW V_CommodityPriceWithMaturity AS
SELECT 
    s.*,
    
    -- 満期月
    dbo.CalculateMaturityDate(s.TradeDate, g.MaturityRule) as MaturityMonth,
    
    -- 最終取引日
    dbo.CalculateLastTradingDay(
        dbo.CalculateMaturityDate(s.TradeDate, g.MaturityRule),
        g.LastTradingDayRule
    ) as LastTradingDay,
    
    -- 残取引可能日数（ロールオーバー考慮）
    CASE 
        WHEN g.MaturityRule IS NOT NULL AND g.LastTradingDayRule IS NOT NULL THEN
            DATEDIFF(day, s.TradeDate, 
                dbo.CalculateLastTradingDay(
                    dbo.CalculateMaturityDate(s.TradeDate, g.MaturityRule),
                    g.LastTradingDayRule
                )
            ) - ISNULL(g.RolloverDays, 0)
        ELSE NULL
    END as TradingDaysRemaining,
    
    -- ロールオーバー推奨日
    CASE 
        WHEN g.MaturityRule IS NOT NULL AND g.LastTradingDayRule IS NOT NULL THEN
            DATEADD(day, -ISNULL(g.RolloverDays, 0),
                dbo.CalculateLastTradingDay(
                    dbo.CalculateMaturityDate(s.TradeDate, g.MaturityRule),
                    g.LastTradingDayRule
                )
            )
        ELSE NULL
    END as RolloverDate,
    
    -- ロールオーバーフラグ
    CASE 
        WHEN g.MaturityRule IS NOT NULL AND g.LastTradingDayRule IS NOT NULL 
            AND s.TradeDate >= DATEADD(day, -ISNULL(g.RolloverDays, 0),
                dbo.CalculateLastTradingDay(
                    dbo.CalculateMaturityDate(s.TradeDate, g.MaturityRule),
                    g.LastTradingDayRule
                )
            )
        THEN 'Yes'
        ELSE 'No'
    END as ShouldRollover,
    
    g.RolloverDays,
    g.MaturityRule,
    g.LastTradingDayRule
    
FROM V_CommodityPriceSimple s
INNER JOIN M_GenericFutures g ON s.GenericID = g.GenericID

GO

-- 使用例

-- 1. 残取引可能日数の確認
SELECT 
    ExchangeDisplayName,
    GenericTicker,
    GenericDescription,
    TradeDate,
    MaturityMonth,
    LastTradingDay,
    TradingDaysRemaining,
    RolloverDate,
    ShouldRollover,
    PriceForAnalysis,
    Volume
FROM V_CommodityPriceWithMaturity
WHERE TradeDate = (SELECT MAX(TradeDate) FROM V_CommodityPriceWithMaturity)
    AND MetalCode = 'CU'
ORDER BY ExchangeDisplayName, GenericNumber

-- 2. ロールオーバーが必要な契約
SELECT 
    ExchangeDisplayName,
    GenericTicker,
    LastTradingDay,
    TradingDaysRemaining,
    Volume,
    TradingActivity
FROM V_CommodityPriceWithMaturity
WHERE TradeDate = (SELECT MAX(TradeDate) FROM V_CommodityPriceWithMaturity)
    AND ShouldRollover = 'Yes'
ORDER BY TradingDaysRemaining