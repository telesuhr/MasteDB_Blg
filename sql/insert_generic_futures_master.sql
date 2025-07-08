-- ###########################################################
-- ジェネリック先物マスターデータ投入スクリプト
-- Phase 1: LP1-LP36, CU1-CU12, HG1-HG26のマスターデータ
-- ###########################################################

-- データベースの選択
USE [JCL];
GO

-- ###########################################################
-- M_Metal テーブルから必要なMetalIDを取得
-- ###########################################################

DECLARE @CopperMetalID INT;
DECLARE @CuShfeMetalID INT;
DECLARE @CuCmxMetalID INT;

-- 既存のメタルIDを取得
SELECT @CopperMetalID = MetalID FROM M_Metal WHERE MetalCode = 'COPPER';
SELECT @CuShfeMetalID = MetalID FROM M_Metal WHERE MetalCode = 'CU_SHFE';
SELECT @CuCmxMetalID = MetalID FROM M_Metal WHERE MetalCode = 'CU_CMX';

-- メタルIDが見つからない場合の処理
IF @CopperMetalID IS NULL
BEGIN
    PRINT 'エラー: COPPER メタルが見つかりません';
    RETURN;
END

IF @CuShfeMetalID IS NULL
BEGIN
    PRINT 'エラー: CU_SHFE メタルが見つかりません';
    RETURN;
END

IF @CuCmxMetalID IS NULL
BEGIN
    PRINT 'エラー: CU_CMX メタルが見つかりません';
    RETURN;
END

PRINT 'メタルID確認完了:';
PRINT '- COPPER: ' + CAST(@CopperMetalID AS NVARCHAR(10));
PRINT '- CU_SHFE: ' + CAST(@CuShfeMetalID AS NVARCHAR(10));
PRINT '- CU_CMX: ' + CAST(@CuCmxMetalID AS NVARCHAR(10));

-- ###########################################################
-- LME 銅先物 (LP1-LP36) の投入
-- ###########################################################

PRINT '=== LME 銅先物 (LP1-LP36) を投入中... ===';

-- 既存データの削除（再実行対応）
DELETE FROM M_GenericFutures WHERE GenericTicker LIKE 'LP[0-9]% Comdty';

-- LP1-LP36の投入
DECLARE @i INT = 1;
WHILE @i <= 36
BEGIN
    DECLARE @ticker NVARCHAR(20) = 'LP' + CAST(@i AS NVARCHAR(2)) + ' Comdty';
    DECLARE @description NVARCHAR(100) = 
        CASE 
            WHEN @i = 1 THEN 'LME Generic 1st Future'
            WHEN @i = 2 THEN 'LME Generic 2nd Future' 
            WHEN @i = 3 THEN 'LME Generic 3rd Future'
            ELSE 'LME Generic ' + CAST(@i AS NVARCHAR(2)) + 'th Future'
        END;
    
    INSERT INTO M_GenericFutures (
        GenericTicker, MetalID, ExchangeCode, GenericNumber, Description
    ) VALUES (
        @ticker, @CopperMetalID, 'LME', @i, @description
    );
    
    SET @i = @i + 1;
END

PRINT 'LP1-LP36 投入完了: ' + CAST(@@ROWCOUNT AS NVARCHAR(10)) + ' 件';

-- ###########################################################
-- SHFE 銅先物 (CU1-CU12) の投入
-- ###########################################################

PRINT '=== SHFE 銅先物 (CU1-CU12) を投入中... ===';

-- 既存データの削除（再実行対応）
DELETE FROM M_GenericFutures WHERE GenericTicker LIKE 'CU[0-9]% Comdty';

-- CU1-CU12の投入
SET @i = 1;
WHILE @i <= 12
BEGIN
    DECLARE @cu_ticker NVARCHAR(20) = 'CU' + CAST(@i AS NVARCHAR(2)) + ' Comdty';
    DECLARE @cu_description NVARCHAR(100) = 
        CASE 
            WHEN @i = 1 THEN 'SHFE Generic 1st Future'
            WHEN @i = 2 THEN 'SHFE Generic 2nd Future'
            WHEN @i = 3 THEN 'SHFE Generic 3rd Future'
            ELSE 'SHFE Generic ' + CAST(@i AS NVARCHAR(2)) + 'th Future'
        END;
    
    INSERT INTO M_GenericFutures (
        GenericTicker, MetalID, ExchangeCode, GenericNumber, Description
    ) VALUES (
        @cu_ticker, @CuShfeMetalID, 'SHFE', @i, @cu_description
    );
    
    SET @i = @i + 1;
END

PRINT 'CU1-CU12 投入完了: ' + CAST(@@ROWCOUNT AS NVARCHAR(10)) + ' 件';

-- ###########################################################
-- CMX 銅先物 (HG1-HG26) の投入
-- ###########################################################

PRINT '=== CMX 銅先物 (HG1-HG26) を投入中... ===';

-- 既存データの削除（再実行対応）
DELETE FROM M_GenericFutures WHERE GenericTicker LIKE 'HG[0-9]% Comdty';

-- HG1-HG26の投入
SET @i = 1;
WHILE @i <= 26
BEGIN
    DECLARE @hg_ticker NVARCHAR(20) = 'HG' + CAST(@i AS NVARCHAR(2)) + ' Comdty';
    DECLARE @hg_description NVARCHAR(100) = 
        CASE 
            WHEN @i = 1 THEN 'COMEX Generic 1st Future'
            WHEN @i = 2 THEN 'COMEX Generic 2nd Future'
            WHEN @i = 3 THEN 'COMEX Generic 3rd Future'
            ELSE 'COMEX Generic ' + CAST(@i AS NVARCHAR(2)) + 'th Future'
        END;
    
    INSERT INTO M_GenericFutures (
        GenericTicker, MetalID, ExchangeCode, GenericNumber, Description
    ) VALUES (
        @hg_ticker, @CuCmxMetalID, 'CMX', @i, @hg_description
    );
    
    SET @i = @i + 1;
END

PRINT 'HG1-HG26 投入完了: ' + CAST(@@ROWCOUNT AS NVARCHAR(10)) + ' 件';

-- ###########################################################
-- 投入結果の確認
-- ###########################################################

PRINT '';
PRINT '=== 投入結果サマリー ===';

SELECT 
    ExchangeCode,
    COUNT(*) as 投入件数,
    MIN(GenericNumber) as 最小番号,
    MAX(GenericNumber) as 最大番号
FROM M_GenericFutures 
GROUP BY ExchangeCode
ORDER BY ExchangeCode;

PRINT '';
PRINT '=== 投入データサンプル ===';

SELECT TOP 10
    GenericTicker,
    ExchangeCode, 
    GenericNumber,
    Description
FROM M_GenericFutures 
ORDER BY ExchangeCode, GenericNumber;

PRINT '';
PRINT 'ジェネリック先物マスターデータ投入完了!';
GO