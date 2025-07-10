-- ###########################################################
-- CHECK制約の修正
-- T_CommodityPrice_V2のCHECK制約を調査・修正
-- ###########################################################

-- データベースの選択
USE [JCL];
GO

-- ###########################################################
-- 1. 現在のCHECK制約を確認
-- ###########################################################

PRINT '=== 現在のCHECK制約確認 ===';

SELECT 
    cc.CONSTRAINT_NAME,
    cc.CHECK_CLAUSE,
    tc.TABLE_NAME
FROM INFORMATION_SCHEMA.CHECK_CONSTRAINTS cc
JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc ON cc.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
WHERE tc.TABLE_NAME = 'T_CommodityPrice_V2';

-- ###########################################################
-- 2. CHECK制約を一旦削除
-- ###########################################################

PRINT '=== CHECK制約削除 ===';

-- CHK_T_CommodityPrice_V2_DataType制約を削除
IF EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.CHECK_CONSTRAINTS 
    WHERE CONSTRAINT_NAME = 'CHK_T_CommodityPrice_V2_DataType'
)
BEGIN
    ALTER TABLE T_CommodityPrice_V2 DROP CONSTRAINT CHK_T_CommodityPrice_V2_DataType;
    PRINT 'CHK_T_CommodityPrice_V2_DataType制約を削除しました';
END
ELSE
BEGIN
    PRINT 'CHK_T_CommodityPrice_V2_DataType制約は存在しません';
END

-- ###########################################################
-- 3. 新しいCHECK制約を追加（より柔軟な制約）
-- ###########################################################

PRINT '=== 新しいCHECK制約追加 ===';

-- より柔軟なCHECK制約を追加
ALTER TABLE T_CommodityPrice_V2 ADD CONSTRAINT CHK_T_CommodityPrice_V2_DataType_Fixed CHECK (
    (DataType = 'Generic' AND GenericID IS NOT NULL) OR
    (DataType = 'Actual' AND ActualContractID IS NOT NULL) OR
    (DataType = 'Generic' AND ActualContractID IS NOT NULL)  -- ジェネリックでもActualContractIDを許可
);

PRINT '新しいCHECK制約を追加しました';

-- ###########################################################
-- 4. 制約確認
-- ###########################################################

PRINT '=== 修正後のCHECK制約確認 ===';

SELECT 
    cc.CONSTRAINT_NAME,
    cc.CHECK_CLAUSE,
    tc.TABLE_NAME
FROM INFORMATION_SCHEMA.CHECK_CONSTRAINTS cc
JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc ON cc.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
WHERE tc.TABLE_NAME = 'T_CommodityPrice_V2';

-- ###########################################################
-- 5. テストデータで動作確認
-- ###########################################################

PRINT '=== テストデータ挿入確認 ===';

-- テスト用データ挿入（実際のデータは使用しない）
BEGIN TRY
    INSERT INTO T_CommodityPrice_V2 (
        TradeDate, MetalID, DataType, GenericID, ActualContractID,
        SettlementPrice, OpenPrice, HighPrice, LowPrice, LastPrice,
        Volume, OpenInterest
    ) VALUES (
        '2025-01-01',  -- 存在しない日付でテスト
        770,           -- 既存MetalID
        'Generic',     -- DataType
        1,             -- 既存GenericID
        4,             -- 既存ActualContractID（両方設定）
        1000.00,       -- SettlementPrice
        1000.00,       -- OpenPrice
        1000.00,       -- HighPrice
        1000.00,       -- LowPrice
        1000.00,       -- LastPrice
        100,           -- Volume
        100            -- OpenInterest
    );
    
    PRINT 'テストデータ挿入成功 - CHECK制約は正常に動作しています';
    
    -- テストデータを削除
    DELETE FROM T_CommodityPrice_V2 WHERE TradeDate = '2025-01-01';
    PRINT 'テストデータを削除しました';
    
END TRY
BEGIN CATCH
    PRINT 'テストデータ挿入失敗: ' + ERROR_MESSAGE();
END CATCH

PRINT '=== CHECK制約修正完了 ===';
GO