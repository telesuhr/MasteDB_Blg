-- ###########################################################
-- マルチ取引所対応: SHFE・COMEXのジェネリック先物を追加
-- M_GenericFuturesテーブルに各取引所の銅先物を挿入
-- ###########################################################

-- データベースの選択
USE [JCL];
GO

-- ###########################################################
-- 1. 現在のM_GenericFuturesテーブル状況確認
-- ###########################################################

PRINT '=== 現在のM_GenericFuturesテーブル状況 ===';

SELECT ExchangeCode, COUNT(*) as 件数
FROM M_GenericFutures
GROUP BY ExchangeCode
ORDER BY ExchangeCode;

-- 詳細確認
SELECT TOP 10 GenericID, GenericTicker, ExchangeCode, GenericNumber, MetalID
FROM M_GenericFutures
ORDER BY ExchangeCode, GenericNumber;

-- ###########################################################
-- 2. 金属IDの確認
-- ###########################################################

PRINT '=== 金属マスターの確認 ===';

SELECT MetalID, MetalCode, MetalName, MetalNameJP
FROM M_Metal
WHERE MetalCode IN ('CU', 'COPPER');

-- ###########################################################
-- 3. SHFE 銅先物の挿入 (CU1-CU12)
-- ###########################################################

PRINT '=== SHFE 銅先物の挿入開始 ===';

-- MetalIDを取得（銅）
DECLARE @CopperMetalID INT;
SELECT @CopperMetalID = MetalID FROM M_Metal WHERE MetalCode = 'CU' OR MetalCode = 'COPPER';

IF @CopperMetalID IS NULL
BEGIN
    PRINT 'ERROR: 銅のMetalIDが見つかりません';
    RETURN;
END

PRINT 'CopperMetalID: ' + CAST(@CopperMetalID AS VARCHAR(10));

-- SHFE CU1-CU12の挿入
DECLARE @i INT = 1;
WHILE @i <= 12
BEGIN
    DECLARE @GenericTicker NVARCHAR(20) = 'CU' + CAST(@i AS VARCHAR(2)) + ' Comdty';
    DECLARE @Description NVARCHAR(200) = 'SHFE Copper Generic ' + CAST(@i AS VARCHAR(2)) + 
        CASE 
            WHEN @i = 1 THEN 'st Future'
            WHEN @i = 2 THEN 'nd Future' 
            WHEN @i = 3 THEN 'rd Future'
            ELSE 'th Future'
        END;
    
    -- 既存チェック
    IF NOT EXISTS (SELECT 1 FROM M_GenericFutures WHERE GenericTicker = @GenericTicker)
    BEGIN
        INSERT INTO M_GenericFutures (
            GenericTicker, MetalID, ExchangeCode, GenericNumber, 
            Description, IsActive, CreatedAt
        ) VALUES (
            @GenericTicker, @CopperMetalID, 'SHFE', @i, 
            @Description, 1, GETDATE()
        );
        
        PRINT 'SHFE 挿入: ' + @GenericTicker;
    END
    ELSE
    BEGIN
        PRINT 'SHFE 既存: ' + @GenericTicker;
    END
    
    SET @i = @i + 1;
END

-- ###########################################################
-- 4. COMEX 銅先物の挿入 (HG1-HG26)
-- ###########################################################

PRINT '=== COMEX 銅先物の挿入開始 ===';

-- COMEX HG1-HG26の挿入
SET @i = 1;
WHILE @i <= 26
BEGIN
    DECLARE @GenericTickerCOMEX NVARCHAR(20) = 'HG' + CAST(@i AS VARCHAR(2)) + ' Comdty';
    DECLARE @DescriptionCOMEX NVARCHAR(200) = 'COMEX Copper Generic ' + CAST(@i AS VARCHAR(2)) + 
        CASE 
            WHEN @i = 1 THEN 'st Future'
            WHEN @i = 2 THEN 'nd Future' 
            WHEN @i = 3 THEN 'rd Future'
            ELSE 'th Future'
        END;
    
    -- 既存チェック
    IF NOT EXISTS (SELECT 1 FROM M_GenericFutures WHERE GenericTicker = @GenericTickerCOMEX)
    BEGIN
        INSERT INTO M_GenericFutures (
            GenericTicker, MetalID, ExchangeCode, GenericNumber, 
            Description, IsActive, CreatedAt
        ) VALUES (
            @GenericTickerCOMEX, @CopperMetalID, 'COMEX', @i, 
            @DescriptionCOMEX, 1, GETDATE()
        );
        
        PRINT 'COMEX 挿入: ' + @GenericTickerCOMEX;
    END
    ELSE
    BEGIN
        PRINT 'COMEX 既存: ' + @GenericTickerCOMEX;
    END
    
    SET @i = @i + 1;
END

-- ###########################################################
-- 5. 挿入結果の確認
-- ###########################################################

PRINT '=== 挿入後のM_GenericFuturesテーブル状況 ===';

SELECT 
    ExchangeCode,
    COUNT(*) as 件数,
    MIN(GenericNumber) as 最小番号,
    MAX(GenericNumber) as 最大番号
FROM M_GenericFutures
GROUP BY ExchangeCode
ORDER BY ExchangeCode;

-- 各取引所の先頭3銘柄確認
PRINT '=== 各取引所の先頭3銘柄確認 ===';

SELECT GenericTicker, ExchangeCode, GenericNumber, Description
FROM M_GenericFutures
WHERE GenericNumber <= 3
ORDER BY ExchangeCode, GenericNumber;

-- 総件数確認
PRINT '=== 総件数確認 ===';

SELECT COUNT(*) as 総件数 FROM M_GenericFutures;

-- ###########################################################
-- 6. データ整合性確認
-- ###########################################################

PRINT '=== データ整合性確認 ===';

-- 重複チェック
SELECT GenericTicker, COUNT(*) as 重複数
FROM M_GenericFutures
GROUP BY GenericTicker
HAVING COUNT(*) > 1;

-- 無効なMetalIDチェック
SELECT gf.GenericID, gf.GenericTicker, gf.MetalID
FROM M_GenericFutures gf
LEFT JOIN M_Metal m ON gf.MetalID = m.MetalID
WHERE m.MetalID IS NULL;

PRINT '=== マルチ取引所対応ジェネリック先物挿入完了 ===';
GO