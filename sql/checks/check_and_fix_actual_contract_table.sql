-- ============================================================
-- M_ActualContract テーブルの構造確認と修正
-- ============================================================

USE [JCL];
GO

-- 1. テーブル構造を確認
PRINT '=== M_ActualContract テーブルの構造 ==='
SELECT 
    COLUMN_NAME,
    DATA_TYPE,
    CHARACTER_MAXIMUM_LENGTH,
    IS_NULLABLE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'M_ActualContract'
ORDER BY ORDINAL_POSITION;

-- 2. IsActiveカラムが存在しない場合は追加
IF NOT EXISTS (SELECT * FROM sys.columns 
               WHERE object_id = OBJECT_ID('M_ActualContract') 
               AND name = 'IsActive')
BEGIN
    PRINT ''
    PRINT 'IsActiveカラムを追加します...'
    ALTER TABLE M_ActualContract
    ADD IsActive BIT NOT NULL DEFAULT 1;
    
    PRINT 'IsActiveカラムを追加しました'
END
ELSE
BEGIN
    PRINT ''
    PRINT 'IsActiveカラムは既に存在します'
END

-- 3. CreatedDateカラムが存在しない場合は追加
IF NOT EXISTS (SELECT * FROM sys.columns 
               WHERE object_id = OBJECT_ID('M_ActualContract') 
               AND name = 'CreatedDate')
BEGIN
    PRINT ''
    PRINT 'CreatedDateカラムを追加します...'
    ALTER TABLE M_ActualContract
    ADD CreatedDate DATETIME NOT NULL DEFAULT GETDATE();
    
    PRINT 'CreatedDateカラムを追加しました'
END

-- 4. LastUpdatedカラムが存在しない場合は追加
IF NOT EXISTS (SELECT * FROM sys.columns 
               WHERE object_id = OBJECT_ID('M_ActualContract') 
               AND name = 'LastUpdated')
BEGIN
    PRINT ''
    PRINT 'LastUpdatedカラムを追加します...'
    ALTER TABLE M_ActualContract
    ADD LastUpdated DATETIME NULL;
    
    PRINT 'LastUpdatedカラムを追加しました'
END

-- 5. 不完全なデータを確認
PRINT ''
PRINT '=== 不完全な契約データ ==='
SELECT 
    ActualContractID,
    ContractTicker,
    MetalID,
    ExchangeCode,
    ContractMonth,
    ContractMonthCode,
    LastTradeableDate,
    DeliveryDate
FROM M_ActualContract
WHERE LastTradeableDate IS NULL 
   OR ContractMonth IS NULL
ORDER BY ContractTicker;

-- 6. 不完全なデータの件数
PRINT ''
SELECT 
    COUNT(*) as '不完全なデータ件数',
    COUNT(CASE WHEN LastTradeableDate IS NULL THEN 1 END) as 'LastTradeableDateがNULL',
    COUNT(CASE WHEN ContractMonth IS NULL THEN 1 END) as 'ContractMonthがNULL',
    COUNT(CASE WHEN ContractMonthCode IS NULL THEN 1 END) as 'ContractMonthCodeがNULL'
FROM M_ActualContract;

GO