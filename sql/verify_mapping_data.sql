-- T_GenericContractMappingの現在のデータを確認
-- 6月16日前後のマッピングデータを確認

-- 1. LP1の6月中旬のマッピング状況を確認
SELECT 
    m.TradeDate,
    gf.GenericTicker,
    m.ActualContractID,
    ac.ContractTicker,
    ac.ContractMonth,
    ac.LastTradeableDate,
    m.DaysToExpiry,
    m.CreatedAt,
    CASE 
        WHEN m.TradeDate <= ac.LastTradeableDate THEN 'Before Expiry'
        ELSE 'After Expiry'
    END as Status
FROM T_GenericContractMapping m
INNER JOIN M_GenericFutures gf ON m.GenericID = gf.GenericID
INNER JOIN M_ActualContract ac ON m.ActualContractID = ac.ActualContractID
WHERE gf.GenericTicker = 'LP1 Comdty'
    AND m.TradeDate BETWEEN '2025-06-13' AND '2025-06-20'
ORDER BY m.TradeDate;

-- 2. 実際の契約情報を確認
SELECT 
    ActualContractID,
    ContractTicker,
    ContractMonth,
    ContractMonthCode,
    ContractYear,
    LastTradeableDate,
    DeliveryDate
FROM M_ActualContract
WHERE ContractTicker IN ('LPM25', 'LPN25')
ORDER BY ContractMonth;

-- 3. AutoRolloverManagerの最終実行時刻を確認するため、
-- CreatedAtの分布を見る
SELECT 
    CAST(CreatedAt as DATE) as CreatedDate,
    MIN(CreatedAt) as FirstUpdate,
    MAX(CreatedAt) as LastUpdate,
    COUNT(*) as UpdateCount
FROM T_GenericContractMapping
WHERE CreatedAt >= '2025-06-01'
GROUP BY CAST(CreatedAt as DATE)
ORDER BY CreatedDate DESC;

-- 4. 問題のある6月16日のデータを詳細確認
SELECT 
    m.*,
    gf.GenericTicker,
    ac.ContractTicker
FROM T_GenericContractMapping m
INNER JOIN M_GenericFutures gf ON m.GenericID = gf.GenericID
INNER JOIN M_ActualContract ac ON m.ActualContractID = ac.ActualContractID
WHERE m.TradeDate = '2025-06-16'
    AND gf.GenericTicker = 'LP1 Comdty';