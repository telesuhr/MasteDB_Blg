-- ============================================================
-- 最終的なマッピング状況の確認
-- ============================================================

USE [JCL];
GO

-- 1. ContractMonth NULL値の確認
PRINT '=== ContractMonth NULL値を持つ契約 ===';
SELECT 
    ActualContractID,
    ContractTicker,
    ContractMonth,
    ContractMonthCode,
    ContractYear,
    LastTradeableDate,
    DeliveryDate
FROM M_ActualContract
WHERE ContractMonth IS NULL
ORDER BY ContractTicker;
GO

-- 2. 過去データのマッピング例（2024年6月）
PRINT '';
PRINT '=== 2024年6月のマッピング状況 ===';
SELECT 
    gf.GenericTicker,
    ac.ContractTicker,
    gcm.TradeDate,
    gcm.DaysToExpiry,
    ac.LastTradeableDate,
    ac.ContractMonth
FROM T_GenericContractMapping gcm
JOIN M_GenericFutures gf ON gcm.GenericID = gf.GenericID
JOIN M_ActualContract ac ON gcm.ActualContractID = ac.ActualContractID
WHERE gcm.TradeDate BETWEEN '2024-06-01' AND '2024-06-30'
AND gf.GenericTicker IN ('LP1 Comdty', 'LP2 Comdty')
ORDER BY gcm.TradeDate, gf.GenericTicker;
GO

-- 3. DaysToExpiry統計
PRINT '';
PRINT '=== DaysToExpiry統計 ===';
SELECT 
    COUNT(*) as TotalMappings,
    SUM(CASE WHEN DaysToExpiry IS NULL THEN 1 ELSE 0 END) as NullDaysToExpiry,
    SUM(CASE WHEN DaysToExpiry IS NOT NULL THEN 1 ELSE 0 END) as ValidDaysToExpiry,
    MIN(DaysToExpiry) as MinDays,
    MAX(DaysToExpiry) as MaxDays
FROM T_GenericContractMapping
WHERE TradeDate >= '2024-01-01';
GO

-- 4. 実契約の日付情報統計
PRINT '';
PRINT '=== 実契約の日付情報統計 ===';
SELECT 
    ExchangeCode,
    COUNT(*) as TotalContracts,
    SUM(CASE WHEN ContractMonth IS NULL THEN 1 ELSE 0 END) as NullContractMonth,
    SUM(CASE WHEN LastTradeableDate IS NULL THEN 1 ELSE 0 END) as NullLastTradeable,
    SUM(CASE WHEN DeliveryDate IS NULL THEN 1 ELSE 0 END) as NullDelivery
FROM M_ActualContract
GROUP BY ExchangeCode
ORDER BY ExchangeCode;
GO

PRINT '';
PRINT '確認完了';
GO