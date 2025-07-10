-- ============================================================
-- 契約メタデータの問題を分析
-- ============================================================

USE [JCL];
GO

-- 1. LPK25の現在の状態を確認
PRINT '=== LPK25（問題の契約）の現在の状態 ===';
SELECT 
    ActualContractID,
    ContractTicker,
    ContractMonth,
    ContractMonthCode,
    ContractYear,
    LastTradeableDate,
    DeliveryDate,
    CASE 
        WHEN ContractMonthCode = 'K' THEN '5月のはず'
        ELSE 'コード不一致'
    END as ExpectedMonth
FROM M_ActualContract
WHERE ContractTicker = 'LPK25';
GO

-- 2. 全2025年LME契約の状態確認
PRINT '';
PRINT '=== 2025年LME銅契約の一覧（月コードと実際の月の対応） ===';
SELECT 
    ContractTicker,
    ContractMonth,
    ContractMonthCode,
    CASE ContractMonthCode
        WHEN 'F' THEN '1月'
        WHEN 'G' THEN '2月'
        WHEN 'H' THEN '3月'
        WHEN 'J' THEN '4月'
        WHEN 'K' THEN '5月'
        WHEN 'M' THEN '6月'
        WHEN 'N' THEN '7月'
        WHEN 'Q' THEN '8月'
        WHEN 'U' THEN '9月'
        WHEN 'V' THEN '10月'
        WHEN 'X' THEN '11月'
        WHEN 'Z' THEN '12月'
    END as ExpectedMonth,
    LastTradeableDate,
    MONTH(LastTradeableDate) as LastTradeMonth,
    CASE 
        WHEN ContractMonth IS NULL THEN 'ContractMonth NULL'
        WHEN ContractMonthCode = 'K' AND ContractMonth != 5 THEN '月が不一致'
        WHEN MONTH(LastTradeableDate) != ContractMonth AND ContractMonth IS NOT NULL THEN '満期月が不一致'
        ELSE 'OK'
    END as Issue
FROM M_ActualContract
WHERE ExchangeCode = 'LME'
AND ContractYear = 2025
AND MetalID = 770
ORDER BY LastTradeableDate;
GO

-- 3. 2025年4月15日時点での正しいLP1契約候補
PRINT '';
PRINT '=== 2025年4月15日時点でのLP1契約候補（正しい順序） ===';
SELECT 
    ContractTicker,
    ContractMonth,
    ContractMonthCode,
    LastTradeableDate,
    DATEDIFF(day, '2025-04-15', LastTradeableDate) as DaysToExpiry,
    CASE 
        WHEN DATEDIFF(day, '2025-04-15', LastTradeableDate) < 0 THEN '満期済み'
        WHEN DATEDIFF(day, '2025-04-15', LastTradeableDate) <= 3 THEN 'ロールオーバー間近'
        ELSE '取引可能'
    END as Status
FROM M_ActualContract
WHERE ExchangeCode = 'LME'
AND ContractYear IN (2025, 2026)
AND MetalID = 770
AND LastTradeableDate >= '2025-04-01'  -- 4月以降に満期
ORDER BY LastTradeableDate;
GO

-- 4. 修正が必要な契約の特定
PRINT '';
PRINT '=== 修正が必要な契約 ===';
SELECT 
    ActualContractID,
    ContractTicker,
    ContractMonth as CurrentMonth,
    ContractMonthCode,
    CASE ContractMonthCode
        WHEN 'F' THEN 1 WHEN 'G' THEN 2 WHEN 'H' THEN 3 WHEN 'J' THEN 4
        WHEN 'K' THEN 5 WHEN 'M' THEN 6 WHEN 'N' THEN 7 WHEN 'Q' THEN 8
        WHEN 'U' THEN 9 WHEN 'V' THEN 10 WHEN 'X' THEN 11 WHEN 'Z' THEN 12
    END as CorrectMonth,
    LastTradeableDate,
    CASE ContractMonthCode
        WHEN 'K' THEN '2025-05-19'  -- 5月第3月曜日
    END as CorrectLastTradeable
FROM M_ActualContract
WHERE ContractTicker = 'LPK25';
GO