-- CHECK制約の確認と修正

-- 現在のCHECK制約を確認
SELECT 
    cc.name AS constraint_name,
    cc.definition
FROM sys.check_constraints cc
JOIN sys.tables t ON cc.parent_object_id = t.object_id
WHERE t.name = 'T_CommodityPrice';

-- データタイプ別のレコード数を確認
SELECT 
    DataType,
    COUNT(*) as RecordCount,
    COUNT(DISTINCT GenericID) as UniqueGenericIDs,
    COUNT(DISTINCT ActualContractID) as UniqueActualContractIDs
FROM T_CommodityPrice
GROUP BY DataType
ORDER BY DataType;

-- 問題のあるレコードを確認（DataType='Actual'でActualContractIDがNULLのもの）
SELECT TOP 10
    TradeDate,
    DataType,
    GenericID,
    ActualContractID,
    MetalID
FROM T_CommodityPrice
WHERE DataType = 'Actual' AND ActualContractID IS NULL;

-- CHECK制約を修正する場合の例
-- まず古い制約を削除
-- ALTER TABLE T_CommodityPrice DROP CONSTRAINT CHK_T_CommodityPrice_V2_DataType_Fixed;

-- 新しい制約を追加（Cash、TomNextタイプを許可）
-- ALTER TABLE T_CommodityPrice ADD CONSTRAINT CHK_T_CommodityPrice_DataType
-- CHECK (
--     (DataType = 'Generic' AND GenericID IS NOT NULL AND ActualContractID IS NULL) OR
--     (DataType = 'Actual' AND ActualContractID IS NOT NULL AND GenericID IS NULL) OR
--     (DataType = 'Cash' AND GenericID IS NULL AND ActualContractID IS NULL) OR
--     (DataType = 'TomNext' AND GenericID IS NULL AND ActualContractID IS NULL)
-- );