-- ContractMonthカラムのデータ型を確認
USE [JCL];
GO

-- 1. カラムのデータ型確認
SELECT 
    COLUMN_NAME,
    DATA_TYPE,
    CHARACTER_MAXIMUM_LENGTH,
    NUMERIC_PRECISION,
    NUMERIC_SCALE,
    DATETIME_PRECISION,
    IS_NULLABLE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'M_ActualContract'
AND COLUMN_NAME = 'ContractMonth';
GO

-- 2. サンプルデータ確認
SELECT TOP 10
    ActualContractID,
    ContractTicker,
    ContractMonth,
    ContractMonthCode,
    ContractYear,
    LastTradeableDate,
    DeliveryDate
FROM M_ActualContract
WHERE ExchangeCode = 'LME'
ORDER BY ContractYear DESC, ContractMonth DESC;
GO

-- 3. ContractMonthの値分布確認（DATE型の場合）
SELECT 
    YEAR(ContractMonth) as Year,
    MONTH(ContractMonth) as Month,
    COUNT(*) as ContractCount
FROM M_ActualContract
WHERE ContractMonth IS NOT NULL
GROUP BY YEAR(ContractMonth), MONTH(ContractMonth)
ORDER BY Year DESC, Month DESC;
GO

-- 4. 問題のあるデータを特定
SELECT 
    ContractTicker,
    ContractMonth,
    ContractMonthCode,
    LastTradeableDate,
    DeliveryDate,
    CASE 
        WHEN ContractMonth IS NULL THEN 'NULL ContractMonth'
        WHEN LastTradeableDate IS NULL THEN 'NULL LastTradeableDate'
        WHEN DeliveryDate IS NULL THEN 'NULL DeliveryDate'
        ELSE 'Other'
    END as Issue
FROM M_ActualContract
WHERE ContractMonth IS NULL 
   OR LastTradeableDate IS NULL
   OR DeliveryDate IS NULL
ORDER BY ContractTicker;
GO