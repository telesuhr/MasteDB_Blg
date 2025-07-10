-- ============================================================
-- M_ActualContractテーブルのContractMonth列を修正（改良版）
-- インデックスの依存関係を考慮した修正
-- ============================================================

USE [JCL];
GO

-- 1. 現在の状態を確認
SELECT 
    i.name AS IndexName,
    i.type_desc AS IndexType,
    c.name AS ColumnName
FROM sys.indexes i
INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
WHERE i.object_id = OBJECT_ID('M_ActualContract')
AND c.name = 'ContractMonth';
GO

-- 2. 依存するインデックスを削除
IF EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_ActualContract_ExchangeMonth' AND object_id = OBJECT_ID('M_ActualContract'))
    DROP INDEX IX_ActualContract_ExchangeMonth ON M_ActualContract;
GO

-- 3. 新しい列を追加（一時的）
IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('M_ActualContract') AND name = 'ContractMonthInt')
    ALTER TABLE M_ActualContract ADD ContractMonthInt INT NULL;
GO

-- 4. 月コードから正しい月番号を設定
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
        ELSE MONTH(ContractMonth)  -- DATE型からの変換
    END
WHERE ContractMonthInt IS NULL;
GO

-- 5. 古い列を削除
ALTER TABLE M_ActualContract DROP COLUMN ContractMonth;
GO

-- 6. 新しい列の名前を変更
EXEC sp_rename 'M_ActualContract.ContractMonthInt', 'ContractMonth', 'COLUMN';
GO

-- 7. インデックスを再作成
CREATE NONCLUSTERED INDEX IX_ActualContract_ExchangeMonth 
ON M_ActualContract (ExchangeCode, ContractMonth);
GO

-- 8. NULL値を持つレコードを確認
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
   OR LastTradeableDate IS NULL
   OR DeliveryDate IS NULL
ORDER BY ContractTicker;
GO

-- 9. 2025年契約を確認
SELECT 
    ActualContractID,
    ContractTicker,
    ContractMonth,
    ContractMonthCode,
    ContractYear,
    LastTradeableDate,
    DeliveryDate
FROM M_ActualContract
WHERE ContractYear = 2025
AND ExchangeCode = 'LME'
ORDER BY ContractMonth;
GO

-- 10. 結果確認：月別の契約数
SELECT 
    ContractMonth,
    ContractMonthCode,
    COUNT(*) as ContractCount
FROM M_ActualContract
WHERE ExchangeCode = 'LME'
GROUP BY ContractMonth, ContractMonthCode
ORDER BY ContractMonth;
GO

PRINT 'ContractMonth列の修正が完了しました（インデックス再作成含む）';
GO