-- ###########################################################
-- TenorTypeID > 4 データ削除（シンプル版）
-- 手動実行用
-- ###########################################################

USE [JCL];
GO

-- 削除前の確認
PRINT '=== 削除前のデータ確認 ===';
SELECT 
    tt.TenorTypeID,
    tt.TenorTypeName,
    COUNT(*) as データ件数
FROM T_CommodityPrice cp
JOIN M_TenorType tt ON cp.TenorTypeID = tt.TenorTypeID
WHERE tt.TenorTypeID > 4
GROUP BY tt.TenorTypeID, tt.TenorTypeName
ORDER BY tt.TenorTypeID;

-- 削除対象の総件数
SELECT COUNT(*) as 削除対象総件数
FROM T_CommodityPrice
WHERE TenorTypeID > 4;

PRINT '=== データ削除実行 ===';

-- 実際の削除
DELETE FROM T_CommodityPrice 
WHERE TenorTypeID > 4;

PRINT CONCAT('削除されたレコード数: ', @@ROWCOUNT);

-- 削除後の確認
PRINT '=== 削除後の確認 ===';
SELECT COUNT(*) as 残存TenorID_GT4_データ件数
FROM T_CommodityPrice
WHERE TenorTypeID > 4;

-- 現在のTenorType別データ件数（銅のみ）
PRINT '=== 現在の銅価格データ件数（TenorType別） ===';
SELECT 
    tt.TenorTypeID,
    tt.TenorTypeName,
    COUNT(*) as データ件数
FROM T_CommodityPrice cp
JOIN M_TenorType tt ON cp.TenorTypeID = tt.TenorTypeID
JOIN M_Metal m ON cp.MetalID = m.MetalID
WHERE m.MetalCode = 'COPPER'
GROUP BY tt.TenorTypeID, tt.TenorTypeName
ORDER BY tt.TenorTypeID;

PRINT '=== 削除完了 ===';
GO