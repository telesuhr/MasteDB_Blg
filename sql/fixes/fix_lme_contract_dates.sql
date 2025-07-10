-- ============================================================
-- LME契約の LastTradeableDate を修正
-- LME契約は月の第3水曜日の2営業日前が最終取引日
-- ============================================================

USE [JCL];
GO

-- 修正前のデータを確認
PRINT '=== 修正前のLME契約データ（2025年） ==='
SELECT 
    ContractTicker,
    ContractMonth,
    ContractYear,
    LastTradeableDate,
    DeliveryDate,
    DATENAME(MONTH, DATEFROMPARTS(ContractYear, ContractMonth, 1)) as MonthName
FROM M_ActualContract
WHERE ExchangeCode = 'LME'
    AND ContractYear = 2025
    AND MetalID = 770
ORDER BY ContractYear, ContractMonth;

-- LME契約の正しい日付を計算する関数
-- 各月の第3水曜日を計算し、その2営業日前を返す
PRINT ''
PRINT '=== LME契約の日付を修正 ==='

-- 一時テーブルで正しい日付を計算
CREATE TABLE #LMEDates (
    ContractTicker VARCHAR(20),
    ContractMonth INT,
    ContractYear INT,
    ThirdWednesday DATE,
    LastTradeableDate DATE,
    DeliveryDate DATE
);

-- 2024年と2025年の契約について計算
DECLARE @Year INT = 2024;
DECLARE @Month INT;
DECLARE @ContractTicker VARCHAR(20);
DECLARE @ThirdWed DATE;
DECLARE @LastTrade DATE;

WHILE @Year <= 2025
BEGIN
    SET @Month = 1;
    WHILE @Month <= 12
    BEGIN
        -- 契約ティッカーを生成
        SET @ContractTicker = 'LP' + 
            CASE @Month
                WHEN 1 THEN 'F' WHEN 2 THEN 'G' WHEN 3 THEN 'H'
                WHEN 4 THEN 'J' WHEN 5 THEN 'K' WHEN 6 THEN 'M'
                WHEN 7 THEN 'N' WHEN 8 THEN 'Q' WHEN 9 THEN 'U'
                WHEN 10 THEN 'V' WHEN 11 THEN 'X' WHEN 12 THEN 'Z'
            END + RIGHT(CAST(@Year AS VARCHAR), 2);
        
        -- 第3水曜日を計算
        -- 月の最初の日を取得
        DECLARE @FirstDay DATE = DATEFROMPARTS(@Year, @Month, 1);
        -- 最初の水曜日を見つける
        DECLARE @FirstWed DATE = DATEADD(DAY, (4 - DATEPART(WEEKDAY, @FirstDay) + 7) % 7, @FirstDay);
        -- 第3水曜日は最初の水曜日から14日後
        SET @ThirdWed = DATEADD(DAY, 14, @FirstWed);
        
        -- 最終取引日は第3水曜日の2営業日前（月曜日）
        SET @LastTrade = DATEADD(DAY, -2, @ThirdWed);
        
        -- 週末の場合は調整
        IF DATEPART(WEEKDAY, @LastTrade) = 1 -- 日曜日
            SET @LastTrade = DATEADD(DAY, -2, @LastTrade);
        ELSE IF DATEPART(WEEKDAY, @LastTrade) = 7 -- 土曜日
            SET @LastTrade = DATEADD(DAY, -1, @LastTrade);
        
        INSERT INTO #LMEDates VALUES (
            @ContractTicker,
            @Month,
            @Year,
            @ThirdWed,
            @LastTrade,
            DATEADD(DAY, 2, @ThirdWed) -- DeliveryDateは第3水曜日の2営業日後
        );
        
        SET @Month = @Month + 1;
    END
    SET @Year = @Year + 1;
END

-- 計算結果を確認
SELECT * FROM #LMEDates
WHERE ContractYear = 2025
ORDER BY ContractYear, ContractMonth;

-- M_ActualContract を更新
UPDATE ac
SET 
    LastTradeableDate = ld.LastTradeableDate,
    DeliveryDate = ld.DeliveryDate
FROM M_ActualContract ac
INNER JOIN #LMEDates ld ON ac.ContractTicker = ld.ContractTicker
WHERE ac.ExchangeCode = 'LME'
    AND ac.MetalID = 770;

-- 修正後のデータを確認
PRINT ''
PRINT '=== 修正後のLME契約データ（2025年） ==='
SELECT 
    ContractTicker,
    ContractMonth,
    ContractYear,
    LastTradeableDate,
    DeliveryDate,
    DATENAME(WEEKDAY, LastTradeableDate) as LastTradeWeekday
FROM M_ActualContract
WHERE ExchangeCode = 'LME'
    AND ContractYear = 2025
    AND MetalID = 770
ORDER BY ContractYear, ContractMonth;

-- クリーンアップ
DROP TABLE #LMEDates;

PRINT ''
PRINT '修正が完了しました。'
PRINT 'ビューを再度実行して、正しいマッピングが表示されることを確認してください。'