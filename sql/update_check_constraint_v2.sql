-- CHECK制約を更新して新しいデータタイプを許可

-- 現在の制約を確認
SELECT 
    cc.name AS constraint_name,
    cc.definition
FROM sys.check_constraints cc
JOIN sys.tables t ON cc.parent_object_id = t.object_id
WHERE t.name = 'T_CommodityPrice';

-- 古い制約を削除
ALTER TABLE T_CommodityPrice DROP CONSTRAINT CHK_T_CommodityPrice_DataType;

-- 新しい制約を追加（3MFuturesとSpreadタイプも許可）
ALTER TABLE T_CommodityPrice ADD CONSTRAINT CHK_T_CommodityPrice_DataType
CHECK (
    (DataType = 'Generic' AND GenericID IS NOT NULL AND ActualContractID IS NULL) OR
    (DataType = 'Actual' AND ActualContractID IS NOT NULL AND GenericID IS NULL) OR
    (DataType = 'Cash' AND GenericID IS NULL AND ActualContractID IS NULL) OR
    (DataType = 'TomNext' AND GenericID IS NULL AND ActualContractID IS NULL) OR
    (DataType = '3MFutures' AND GenericID IS NULL AND ActualContractID IS NULL) OR
    (DataType = 'Spread' AND GenericID IS NULL AND ActualContractID IS NULL)
);

-- 更新後の確認
SELECT 
    cc.name AS constraint_name,
    cc.definition
FROM sys.check_constraints cc
JOIN sys.tables t ON cc.parent_object_id = t.object_id
WHERE t.name = 'T_CommodityPrice';