-- 他取引所在庫データの確認クエリ

-- SHFE在庫データの確認
SELECT 
    ExchangeCode,
    COUNT(*) as RecordCount,
    COUNT(TotalStock) as TotalStock_Count,
    COUNT(OnWarrant) as OnWarrant_Count,
    AVG(CASE WHEN TotalStock IS NOT NULL THEN TotalStock ELSE NULL END) as Avg_TotalStock,
    AVG(CASE WHEN OnWarrant IS NOT NULL THEN OnWarrant ELSE NULL END) as Avg_OnWarrant,
    MIN(ReportDate) as OldestDate,
    MAX(ReportDate) as LatestDate
FROM T_OtherExchangeInventory
WHERE ExchangeCode = 'SHFE'
GROUP BY ExchangeCode;

-- CMX在庫データの確認
SELECT 
    ExchangeCode,
    COUNT(*) as RecordCount,
    COUNT(TotalStock) as TotalStock_Count,
    COUNT(OnWarrant) as OnWarrant_Count,
    AVG(CASE WHEN TotalStock IS NOT NULL THEN TotalStock ELSE NULL END) as Avg_TotalStock,
    AVG(CASE WHEN OnWarrant IS NOT NULL THEN OnWarrant ELSE NULL END) as Avg_OnWarrant,
    MIN(ReportDate) as OldestDate,
    MAX(ReportDate) as LatestDate
FROM T_OtherExchangeInventory
WHERE ExchangeCode = 'CMX'
GROUP BY ExchangeCode;

-- 最新10件のデータサンプル（SHFE）
SELECT TOP 10
    ReportDate,
    ExchangeCode,
    TotalStock,
    OnWarrant,
    LastUpdated
FROM T_OtherExchangeInventory
WHERE ExchangeCode = 'SHFE'
ORDER BY ReportDate DESC;

-- 最新10件のデータサンプル（CMX）
SELECT TOP 10
    ReportDate,
    ExchangeCode,
    TotalStock,
    OnWarrant,
    LastUpdated
FROM T_OtherExchangeInventory
WHERE ExchangeCode = 'CMX'
ORDER BY ReportDate DESC;