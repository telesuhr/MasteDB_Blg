-- ============================================================
-- 2025年のLME契約の LastTradeableDate を手動で修正
-- LME契約は月の第3水曜日の2営業日前が最終取引日
-- ============================================================

USE [JCL];
GO

-- 修正前の状態確認
PRINT '=== 修正前の2025年LME契約 ==='
SELECT 
    ActualContractID,
    ContractTicker,
    ContractMonth,
    ContractYear,
    LastTradeableDate,
    DeliveryDate
FROM M_ActualContract
WHERE ContractTicker IN ('LPF25', 'LPG25', 'LPH25', 'LPJ25')
ORDER BY ContractMonth;

-- 2025年の各月の正しい日付を設定
-- LME銅先物の最終取引日（第3水曜日の2営業日前）
UPDATE M_ActualContract
SET LastTradeableDate = '2025-01-13',  -- 2025年1月13日（月）
    DeliveryDate = '2025-01-17'        -- 2025年1月17日（金）
WHERE ContractTicker = 'LPF25';

UPDATE M_ActualContract
SET LastTradeableDate = '2025-02-17',  -- 2025年2月17日（月）
    DeliveryDate = '2025-02-21'        -- 2025年2月21日（金）
WHERE ContractTicker = 'LPG25';

UPDATE M_ActualContract
SET LastTradeableDate = '2025-03-17',  -- 2025年3月17日（月）
    DeliveryDate = '2025-03-21'        -- 2025年3月21日（金）
WHERE ContractTicker = 'LPH25';

UPDATE M_ActualContract
SET LastTradeableDate = '2025-04-14',  -- 2025年4月14日（月）
    DeliveryDate = '2025-04-18'        -- 2025年4月18日（金）
WHERE ContractTicker = 'LPJ25';

-- 修正後の確認
PRINT ''
PRINT '=== 修正後の2025年LME契約 ==='
SELECT 
    ActualContractID,
    ContractTicker,
    ContractMonth,
    ContractYear,
    LastTradeableDate,
    DeliveryDate,
    DATENAME(WEEKDAY, LastTradeableDate) as LastTradeWeekday
FROM M_ActualContract
WHERE ContractTicker IN ('LPF25', 'LPG25', 'LPH25', 'LPJ25')
ORDER BY ContractMonth;

-- 他のNULL値を持つ契約も確認
PRINT ''
PRINT '=== 他の不完全な契約データ ==='
SELECT 
    ActualContractID,
    ContractTicker,
    ContractMonth,
    LastTradeableDate
FROM M_ActualContract
WHERE LastTradeableDate IS NULL 
   OR ContractMonth IS NULL
ORDER BY ContractTicker;

GO