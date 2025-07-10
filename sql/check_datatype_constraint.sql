-- CHECK制約の確認
USE [JCL];
GO

-- CHECK制約の定義を確認
SELECT 
    cc.name AS ConstraintName,
    cc.definition
FROM sys.check_constraints cc
WHERE cc.parent_object_id = OBJECT_ID('T_CommodityPrice')
AND cc.name LIKE '%DataType%';
GO

-- 現在のデータを確認
SELECT TOP 10
    DataType,
    GenericID,
    ActualContractID,
    CASE 
        WHEN DataType = 'Generic' AND GenericID IS NOT NULL AND ActualContractID IS NOT NULL THEN '両方設定'
        WHEN DataType = 'Generic' AND GenericID IS NOT NULL AND ActualContractID IS NULL THEN 'GenericIDのみ'
        WHEN DataType = 'Actual' AND GenericID IS NULL AND ActualContractID IS NOT NULL THEN 'ActualIDのみ'
        ELSE 'その他'
    END as Status
FROM T_CommodityPrice
WHERE DataType = 'Generic'
ORDER BY TradeDate DESC;
GO