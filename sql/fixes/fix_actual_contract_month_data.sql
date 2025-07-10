-- ============================================================
-- M_ActualContract テーブルの ContractMonth と ContractMonthCode を修正
-- ティッカー名から月コードと年を抽出して設定
-- ============================================================

USE [JCL];
GO

-- 修正前の状態確認
PRINT '修正前の状態:'
SELECT COUNT(*) as 'ContractMonth が NULL の件数'
FROM M_ActualContract
WHERE ContractMonth IS NULL;

-- LME契約の月コードマッピング
DECLARE @MonthMapping TABLE (
    MonthCode CHAR(1),
    MonthNumber INT,
    MonthName VARCHAR(20)
);

INSERT INTO @MonthMapping VALUES
('F', 1, 'January'),
('G', 2, 'February'),
('H', 3, 'March'),
('J', 4, 'April'),
('K', 5, 'May'),
('M', 6, 'June'),
('N', 7, 'July'),
('Q', 8, 'August'),
('U', 9, 'September'),
('V', 10, 'October'),
('X', 11, 'November'),
('Z', 12, 'December');

-- ContractMonth と ContractMonthCode を更新
UPDATE ac
SET 
    ContractMonthCode = SUBSTRING(ac.ContractTicker, 3, 1),
    ContractMonth = mm.MonthNumber,
    ContractYear = CASE 
        WHEN LEN(SUBSTRING(ac.ContractTicker, 4, 2)) = 2 
        THEN 2000 + CAST(SUBSTRING(ac.ContractTicker, 4, 2) AS INT)
        ELSE NULL
    END
FROM M_ActualContract ac
INNER JOIN @MonthMapping mm ON SUBSTRING(ac.ContractTicker, 3, 1) = mm.MonthCode
WHERE 
    -- LME契約パターン: LP[A-Z][0-9]{2}
    ac.ContractTicker LIKE 'LP[A-Z][0-9][0-9]'
    AND ac.ContractMonth IS NULL;

-- SHFE契約の更新 (CU####)
UPDATE M_ActualContract
SET 
    ContractMonth = CAST(SUBSTRING(ContractTicker, 5, 2) AS INT),
    ContractYear = 2000 + CAST(SUBSTRING(ContractTicker, 3, 2) AS INT)
WHERE 
    ContractTicker LIKE 'CU[0-9][0-9][0-9][0-9]'
    AND ContractMonth IS NULL;

-- COMEX契約の更新 (HG[A-Z]##)
UPDATE ac
SET 
    ContractMonthCode = SUBSTRING(ac.ContractTicker, 3, 1),
    ContractMonth = mm.MonthNumber,
    ContractYear = CASE 
        WHEN LEN(SUBSTRING(ac.ContractTicker, 4, 2)) = 2 
        THEN 2000 + CAST(SUBSTRING(ac.ContractTicker, 4, 2) AS INT)
        ELSE NULL
    END
FROM M_ActualContract ac
INNER JOIN @MonthMapping mm ON SUBSTRING(ac.ContractTicker, 3, 1) = mm.MonthCode
WHERE 
    -- COMEX契約パターン: HG[A-Z][0-9]{2}
    ac.ContractTicker LIKE 'HG[A-Z][0-9][0-9]'
    AND ac.ContractMonth IS NULL;

-- 修正結果の確認
PRINT '';
PRINT '修正後の状態:'
SELECT COUNT(*) as 'ContractMonth が NULL の件数'
FROM M_ActualContract
WHERE ContractMonth IS NULL;

-- 修正された契約の例を表示
PRINT '';
PRINT 'LME契約の修正例:'
SELECT TOP 10
    ActualContractID,
    ContractTicker,
    ContractMonth,
    ContractMonthCode,
    ContractYear,
    LastTradeableDate
FROM M_ActualContract
WHERE ExchangeCode = 'LME'
    AND ContractMonth IS NOT NULL
ORDER BY ContractTicker;

PRINT '';
PRINT 'SHFE契約の修正例:'
SELECT TOP 10
    ActualContractID,
    ContractTicker,
    ContractMonth,
    ContractYear,
    LastTradeableDate
FROM M_ActualContract
WHERE ExchangeCode = 'SHFE'
    AND ContractMonth IS NOT NULL
ORDER BY ContractTicker;

PRINT '';
PRINT 'COMEX契約の修正例:'
SELECT TOP 10
    ActualContractID,
    ContractTicker,
    ContractMonth,
    ContractMonthCode,
    ContractYear,
    LastTradeableDate
FROM M_ActualContract
WHERE ExchangeCode = 'CMX'
    AND ContractMonth IS NOT NULL
ORDER BY ContractTicker;

GO