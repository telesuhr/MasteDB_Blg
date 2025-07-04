-- ###########################################################
-- LME テナーマッピング修正スクリプト
-- TenorID 5-16をLME LP1-LP12銅先物として正しく設定
-- ###########################################################

USE [JCL];
GO

PRINT '=== LME Tenor Type Mapping Update ===';

-- 現在のTenorType設定を確認
PRINT '=== 修正前のTenorType設定 ===';
SELECT TenorTypeID, TenorTypeName, Description 
FROM M_TenorType 
WHERE TenorTypeID BETWEEN 5 AND 16
ORDER BY TenorTypeID;

-- TenorID 5-16をLME LP1-LP12として明確に設定
PRINT '=== TenorType名称とDescription修正 ===';

UPDATE M_TenorType SET 
    TenorTypeName = 'Generic 1st Future',
    Description = 'LME Generic 1st month future (LP1 Comdty)'
WHERE TenorTypeID = 5;

UPDATE M_TenorType SET 
    TenorTypeName = 'Generic 2nd Future',
    Description = 'LME Generic 2nd month future (LP2 Comdty)'
WHERE TenorTypeID = 6;

UPDATE M_TenorType SET 
    TenorTypeName = 'Generic 3rd Future',
    Description = 'LME Generic 3rd month future (LP3 Comdty)'
WHERE TenorTypeID = 7;

UPDATE M_TenorType SET 
    TenorTypeName = 'Generic 4th Future',
    Description = 'LME Generic 4th month future (LP4 Comdty)'
WHERE TenorTypeID = 8;

UPDATE M_TenorType SET 
    TenorTypeName = 'Generic 5th Future',
    Description = 'LME Generic 5th month future (LP5 Comdty)'
WHERE TenorTypeID = 9;

UPDATE M_TenorType SET 
    TenorTypeName = 'Generic 6th Future',
    Description = 'LME Generic 6th month future (LP6 Comdty)'
WHERE TenorTypeID = 10;

UPDATE M_TenorType SET 
    TenorTypeName = 'Generic 7th Future',
    Description = 'LME Generic 7th month future (LP7 Comdty)'
WHERE TenorTypeID = 11;

UPDATE M_TenorType SET 
    TenorTypeName = 'Generic 8th Future',
    Description = 'LME Generic 8th month future (LP8 Comdty)'
WHERE TenorTypeID = 12;

UPDATE M_TenorType SET 
    TenorTypeName = 'Generic 9th Future',
    Description = 'LME Generic 9th month future (LP9 Comdty)'
WHERE TenorTypeID = 13;

UPDATE M_TenorType SET 
    TenorTypeName = 'Generic 10th Future',
    Description = 'LME Generic 10th month future (LP10 Comdty)'
WHERE TenorTypeID = 14;

UPDATE M_TenorType SET 
    TenorTypeName = 'Generic 11th Future',
    Description = 'LME Generic 11th month future (LP11 Comdty)'
WHERE TenorTypeID = 15;

UPDATE M_TenorType SET 
    TenorTypeName = 'Generic 12th Future',
    Description = 'LME Generic 12th month future (LP12 Comdty)'
WHERE TenorTypeID = 16;

PRINT CONCAT('更新されたレコード数: ', @@ROWCOUNT);

-- COMEXのテナータイプを追加（HG1-HG12用）
PRINT '=== COMEX Tenor Types 追加 ===';

-- 既存のCOMEXテナータイプをチェック
IF NOT EXISTS (SELECT 1 FROM M_TenorType WHERE TenorTypeName LIKE 'COMEX Generic%')
BEGIN
    INSERT INTO M_TenorType (TenorTypeName, Description) VALUES
    ('COMEX Generic 1st Future', 'COMEX Generic 1st month future (HG1 Comdty)'),
    ('COMEX Generic 2nd Future', 'COMEX Generic 2nd month future (HG2 Comdty)'),
    ('COMEX Generic 3rd Future', 'COMEX Generic 3rd month future (HG3 Comdty)'),
    ('COMEX Generic 4th Future', 'COMEX Generic 4th month future (HG4 Comdty)'),
    ('COMEX Generic 5th Future', 'COMEX Generic 5th month future (HG5 Comdty)'),
    ('COMEX Generic 6th Future', 'COMEX Generic 6th month future (HG6 Comdty)'),
    ('COMEX Generic 7th Future', 'COMEX Generic 7th month future (HG7 Comdty)'),
    ('COMEX Generic 8th Future', 'COMEX Generic 8th month future (HG8 Comdty)'),
    ('COMEX Generic 9th Future', 'COMEX Generic 9th month future (HG9 Comdty)'),
    ('COMEX Generic 10th Future', 'COMEX Generic 10th month future (HG10 Comdty)'),
    ('COMEX Generic 11th Future', 'COMEX Generic 11th month future (HG11 Comdty)'),
    ('COMEX Generic 12th Future', 'COMEX Generic 12th month future (HG12 Comdty)');
    
    PRINT CONCAT('COMEX Tenor Types追加: ', @@ROWCOUNT, ' 件');
END
ELSE
BEGIN
    PRINT 'COMEX Tenor Types は既に存在します';
END

-- SHFEのテナータイプを追加（CU1-CU12用）
PRINT '=== SHFE Tenor Types 追加 ===';

IF NOT EXISTS (SELECT 1 FROM M_TenorType WHERE TenorTypeName LIKE 'SHFE Generic%')
BEGIN
    INSERT INTO M_TenorType (TenorTypeName, Description) VALUES
    ('SHFE Generic 1st Future', 'SHFE Generic 1st month future (CU1 Comdty)'),
    ('SHFE Generic 2nd Future', 'SHFE Generic 2nd month future (CU2 Comdty)'),
    ('SHFE Generic 3rd Future', 'SHFE Generic 3rd month future (CU3 Comdty)'),
    ('SHFE Generic 4th Future', 'SHFE Generic 4th month future (CU4 Comdty)'),
    ('SHFE Generic 5th Future', 'SHFE Generic 5th month future (CU5 Comdty)'),
    ('SHFE Generic 6th Future', 'SHFE Generic 6th month future (CU6 Comdty)'),
    ('SHFE Generic 7th Future', 'SHFE Generic 7th month future (CU7 Comdty)'),
    ('SHFE Generic 8th Future', 'SHFE Generic 8th month future (CU8 Comdty)'),
    ('SHFE Generic 9th Future', 'SHFE Generic 9th month future (CU9 Comdty)'),
    ('SHFE Generic 10th Future', 'SHFE Generic 10th month future (CU10 Comdty)'),
    ('SHFE Generic 11th Future', 'SHFE Generic 11th month future (CU11 Comdty)'),
    ('SHFE Generic 12th Future', 'SHFE Generic 12th month future (CU12 Comdty)');
    
    PRINT CONCAT('SHFE Tenor Types追加: ', @@ROWCOUNT, ' 件');
END
ELSE
BEGIN
    PRINT 'SHFE Tenor Types は既に存在します';
END

-- 修正後の全TenorType確認
PRINT '=== 修正後の全TenorType設定 ===';
SELECT TenorTypeID, TenorTypeName, Description 
FROM M_TenorType 
ORDER BY TenorTypeID;

-- LME LP1-LP12マッピング確認
PRINT '=== LME LP1-LP12 マッピング確認 ===';
SELECT TenorTypeID, TenorTypeName, Description 
FROM M_TenorType 
WHERE TenorTypeID BETWEEN 5 AND 16
ORDER BY TenorTypeID;

PRINT '=== LME Tenor Mapping Update Complete ===';
GO