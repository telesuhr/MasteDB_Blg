-- ============================================================
-- 2025年4月のマッピング問題を確認
-- ============================================================

USE [JCL];
GO

-- 1. 2025年4月15日時点で利用可能な契約を確認
PRINT '=== 2025年4月15日時点で利用可能なLME銅契約 ===';
SELECT 
    ContractTicker,
    ContractMonth,
    ContractMonthCode,
    ContractYear,
    LastTradeableDate,
    DATEDIFF(day, '2025-04-15', LastTradeableDate) as DaysToExpiry,
    CASE 
        WHEN LastTradeableDate >= '2025-04-15' THEN '取引可能'
        ELSE '満期済み'
    END as Status
FROM M_ActualContract
WHERE ExchangeCode = 'LME'
AND MetalID = 770  -- Copper
AND ContractYear IN (2025, 2026)
ORDER BY LastTradeableDate;
GO

-- 2. 2025年4月のLP1マッピング状況
PRINT '';
PRINT '=== 2025年4月のLP1マッピング（現在の状態） ===';
SELECT 
    gcm.TradeDate,
    gf.GenericTicker,
    ac.ContractTicker,
    ac.ContractMonth,
    ac.ContractMonthCode,
    ac.LastTradeableDate,
    gcm.DaysToExpiry
FROM T_GenericContractMapping gcm
JOIN M_GenericFutures gf ON gcm.GenericID = gf.GenericID
JOIN M_ActualContract ac ON gcm.ActualContractID = ac.ActualContractID
WHERE gcm.TradeDate BETWEEN '2025-04-01' AND '2025-04-30'
AND gf.GenericTicker = 'LP1 Comdty'
ORDER BY gcm.TradeDate;
GO

-- 3. 正しいマッピングの計算（LP1は最近月契約）
PRINT '';
PRINT '=== 2025年4月15日時点での正しいLP1マッピング ===';
WITH AvailableContracts AS (
    SELECT 
        ActualContractID,
        ContractTicker,
        ContractMonth,
        ContractMonthCode,
        LastTradeableDate,
        ROW_NUMBER() OVER (ORDER BY LastTradeableDate) as RN
    FROM M_ActualContract
    WHERE ExchangeCode = 'LME'
    AND MetalID = 770
    AND LastTradeableDate >= '2025-04-15'  -- まだ取引可能な契約
)
SELECT TOP 5 * FROM AvailableContracts
ORDER BY LastTradeableDate;
GO

-- 4. LPK25（5月契約）の詳細
PRINT '';
PRINT '=== LPK25（5月契約）の詳細 ===';
SELECT 
    ContractTicker,
    ContractMonth,
    ContractMonthCode,
    ContractYear,
    LastTradeableDate,
    DeliveryDate
FROM M_ActualContract
WHERE ContractTicker = 'LPK25';
GO

-- 5. ロールオーバー設定の確認
PRINT '';
PRINT '=== ジェネリック先物のロールオーバー設定 ===';
SELECT 
    GenericTicker,
    RolloverDays,
    LastTradeableDate,
    FutureDeliveryDateLast,
    LastRefreshDate
FROM M_GenericFutures
WHERE GenericTicker = 'LP1 Comdty';
GO

-- 6. 2025年4月14日→15日のロールオーバー確認
PRINT '';
PRINT '=== 2025年4月14日→15日のマッピング変化 ===';
SELECT 
    gcm.TradeDate,
    ac.ContractTicker,
    ac.LastTradeableDate,
    gcm.DaysToExpiry
FROM T_GenericContractMapping gcm
JOIN M_GenericFutures gf ON gcm.GenericID = gf.GenericID
JOIN M_ActualContract ac ON gcm.ActualContractID = ac.ActualContractID
WHERE gcm.TradeDate IN ('2025-04-14', '2025-04-15', '2025-04-16')
AND gf.GenericTicker = 'LP1 Comdty'
ORDER BY gcm.TradeDate;
GO