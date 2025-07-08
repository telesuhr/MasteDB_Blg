-- 旧テーブルの削除とクリーンアップ

-- 1. T_CommodityPrice_OLDの削除
DROP TABLE IF EXISTS T_CommodityPrice_OLD;

-- 2. 現在のテーブル確認
SELECT 
    'T_CommodityPrice' as TableName,
    COUNT(*) as RecordCount
FROM T_CommodityPrice

UNION ALL

SELECT 
    'Tables with T_CommodityPrice in name' as TableName,
    COUNT(*) as TableCount
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_NAME LIKE '%T_CommodityPrice%';

-- 3. T_CommodityPriceの構造確認（GenericIDがあることを確認）
SELECT TOP 5
    p.PriceID,
    p.TradeDate,
    p.GenericID,
    g.GenericTicker,
    p.LastPrice,
    p.Volume
FROM T_CommodityPrice p
INNER JOIN M_GenericFutures g ON p.GenericID = g.GenericID
ORDER BY p.TradeDate DESC;