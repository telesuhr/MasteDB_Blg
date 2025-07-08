-- ============================================================
-- M_ActualContractのContractMonth列をdate型からint型に変更
-- ============================================================

USE [JCL];
GO

-- 1. 一時列を追加
ALTER TABLE M_ActualContract
ADD ContractMonthTemp INT NULL;
GO

-- 2. 既存のdate値から月番号を抽出
UPDATE M_ActualContract
SET ContractMonthTemp = MONTH(ContractMonth);
GO

-- 3. ContractMonthCodeから正しい月番号を設定（date値が正しくない場合の保険）
UPDATE M_ActualContract
SET ContractMonthTemp = 
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
        ELSE ContractMonthTemp
    END
WHERE ContractMonthCode IS NOT NULL;
GO

-- 4. 既存の列を削除
ALTER TABLE M_ActualContract
DROP COLUMN ContractMonth;
GO

-- 5. 一時列をリネーム
EXEC sp_rename 'M_ActualContract.ContractMonthTemp', 'ContractMonth', 'COLUMN';
GO

-- 6. 2025年の先物を確認
SELECT 
    ActualContractID,
    ContractTicker,
    ContractMonth,
    ContractMonthCode,
    ContractYear,
    LastTradeableDate
FROM M_ActualContract
WHERE MetalID = 770
AND ExchangeCode = 'LME'
AND ContractYear = 2025
ORDER BY ContractMonth;
GO

PRINT 'ContractMonth列の型変更が完了しました';
GO