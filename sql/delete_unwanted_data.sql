-- ============================================================
-- 不要なデータの削除用SQL
-- ============================================================

USE [JCL];
GO

-- 1. 重複している契約データを確認
PRINT '=== 重複している契約データ ==='
SELECT 
    ContractTicker,
    COUNT(*) as DuplicateCount
FROM M_ActualContract
GROUP BY ContractTicker
HAVING COUNT(*) > 1
ORDER BY ContractTicker;

-- 2. 古い価格データの確認（必要に応じて削除）
PRINT ''
PRINT '=== 古い価格データの確認（3年以上前） ==='
SELECT 
    YEAR(TradeDate) as Year,
    COUNT(*) as RecordCount
FROM T_CommodityPrice
WHERE TradeDate < DATEADD(YEAR, -3, GETDATE())
GROUP BY YEAR(TradeDate)
ORDER BY Year;

-- 3. 無効な契約データの確認
PRINT ''
PRINT '=== 無効な契約データ（LastTradeableDateが過去1年以上前） ==='
SELECT 
    ActualContractID,
    ContractTicker,
    LastTradeableDate,
    DATEDIFF(DAY, LastTradeableDate, GETDATE()) as DaysAgo
FROM M_ActualContract
WHERE LastTradeableDate < DATEADD(YEAR, -1, GETDATE())
ORDER BY LastTradeableDate;

-- 4. 削除対象の選択（コメントアウトを外して実行）
/*
-- 重複データの削除（最新のものを残す）
WITH DuplicateContracts AS (
    SELECT 
        ActualContractID,
        ContractTicker,
        ROW_NUMBER() OVER (PARTITION BY ContractTicker ORDER BY ActualContractID DESC) as rn
    FROM M_ActualContract
)
DELETE FROM DuplicateContracts
WHERE rn > 1;

-- 古い価格データの削除（3年以上前）
DELETE FROM T_CommodityPrice
WHERE TradeDate < DATEADD(YEAR, -3, GETDATE());

-- 期限切れ契約の削除（必要に応じて）
DELETE FROM M_ActualContract
WHERE LastTradeableDate < DATEADD(YEAR, -2, GETDATE());
*/

-- 5. テスト用の削除（特定の契約のみ）
/*
-- 特定の契約を削除
DELETE FROM M_ActualContract
WHERE ContractTicker IN ('契約名1', '契約名2');

-- 特定期間の価格データを削除
DELETE FROM T_CommodityPrice
WHERE TradeDate BETWEEN '2020-01-01' AND '2020-12-31'
  AND GenericID = 109;  -- LP1 Comdty
*/

PRINT ''
PRINT '削除を実行する場合は、該当部分のコメントアウトを解除してください'