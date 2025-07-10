-- ============================================================
-- M_ActualContractテーブルのContractMonth列を修正
-- date型からint型に変更し、正しい月番号を設定
-- ============================================================

USE [JCL];
GO

-- 1. 新しい列を追加（一時的）
ALTER TABLE M_ActualContract
ADD ContractMonthInt INT NULL;
GO

-- 2. 月コードから正しい月番号を設定
UPDATE M_ActualContract
SET ContractMonthInt = 
    CASE ContractMonthCode
        WHEN 'F' THEN 1   -- January
        WHEN 'G' THEN 2   -- February
        WHEN 'H' THEN 3   -- March
        WHEN 'J' THEN 4   -- April
        WHEN 'K' THEN 5   -- May
        WHEN 'M' THEN 6   -- June
        WHEN 'N' THEN 7   -- July
        WHEN 'Q' THEN 8   -- August
        WHEN 'U' THEN 9   -- September
        WHEN 'V' THEN 10  -- October
        WHEN 'X' THEN 11  -- November
        WHEN 'Z' THEN 12  -- December
        ELSE NULL
    END;
GO

-- 3. 古い列を削除
ALTER TABLE M_ActualContract
DROP COLUMN ContractMonth;
GO

-- 4. 新しい列の名前を変更
EXEC sp_rename 'M_ActualContract.ContractMonthInt', 'ContractMonth', 'COLUMN';
GO

-- 5. 2025年6月先物が存在するか確認
SELECT 
    ActualContractID,
    ContractTicker,
    ContractMonth,
    ContractMonthCode,
    ContractYear,
    LastTradeableDate
FROM M_ActualContract
WHERE ContractTicker = 'LPM25';
GO

-- 6. 結果確認：月別の契約数
SELECT 
    ContractMonth,
    ContractMonthCode,
    COUNT(*) as ContractCount
FROM M_ActualContract
WHERE ExchangeCode = 'LME'
GROUP BY ContractMonth, ContractMonthCode
ORDER BY ContractMonth;
GO

PRINT 'ContractMonth列の修正が完了しました';
GO