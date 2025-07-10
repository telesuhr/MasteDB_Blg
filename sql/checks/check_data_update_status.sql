-- ============================================================
-- データ更新状況の確認
-- ============================================================

USE [JCL];
GO

-- 1. 価格データの更新状況
PRINT '=== T_CommodityPrice (価格データ) ==='
SELECT 
    CASE 
        WHEN gf.ExchangeCode = 'LME' THEN 'LME銅先物'
        WHEN gf.ExchangeCode = 'SHFE' THEN 'SHFE銅先物'
        WHEN gf.ExchangeCode = 'CMX' THEN 'COMEX銅先物'
        ELSE gf.ExchangeCode
    END as DataCategory,
    MIN(cp.TradeDate) as OldestDate,
    MAX(cp.TradeDate) as LatestDate,
    COUNT(DISTINCT cp.TradeDate) as TradingDays,
    COUNT(*) as RecordCount
FROM T_CommodityPrice cp
LEFT JOIN M_GenericFutures gf ON cp.GenericID = gf.GenericID
WHERE cp.TradeDate >= '2025-01-01'
GROUP BY gf.ExchangeCode
ORDER BY DataCategory;

-- 2. 在庫データの更新状況
PRINT ''
PRINT '=== T_LMEInventory (LME在庫データ) ==='
SELECT 
    MIN(ReportDate) as OldestDate,
    MAX(ReportDate) as LatestDate,
    COUNT(DISTINCT ReportDate) as ReportDays,
    COUNT(*) as RecordCount
FROM T_LMEInventory
WHERE ReportDate >= '2025-01-01';

-- 3. SHFE在庫データ
PRINT ''
PRINT '=== T_OtherExchangeInventory (SHFE/CMX在庫) ==='
SELECT 
    ExchangeCode,
    MIN(ReportDate) as OldestDate,
    MAX(ReportDate) as LatestDate,
    COUNT(DISTINCT ReportDate) as ReportDays,
    COUNT(*) as RecordCount
FROM T_OtherExchangeInventory
WHERE ReportDate >= '2025-01-01'
GROUP BY ExchangeCode;

-- 4. 金利データ
PRINT ''
PRINT '=== T_MarketIndicator (金利・その他指標) ==='
SELECT 
    mi.IndicatorCode,
    MIN(mi.ReportDate) as OldestDate,
    MAX(mi.ReportDate) as LatestDate,
    COUNT(DISTINCT mi.ReportDate) as ReportDays,
    COUNT(*) as RecordCount
FROM T_MarketIndicator mi
INNER JOIN M_Indicator ind ON mi.IndicatorID = ind.IndicatorID
WHERE mi.ReportDate >= '2025-01-01'
GROUP BY mi.IndicatorCode, ind.IndicatorName
ORDER BY mi.IndicatorCode;

-- 5. 2025年1-3月の詳細な取得状況
PRINT ''
PRINT '=== 2025年1-3月の月別取得状況 ==='
SELECT 
    YEAR(TradeDate) as Year,
    MONTH(TradeDate) as Month,
    COUNT(*) as PriceRecords
FROM T_CommodityPrice
WHERE TradeDate BETWEEN '2025-01-01' AND '2025-03-31'
GROUP BY YEAR(TradeDate), MONTH(TradeDate)
ORDER BY Year, Month;

-- 6. データタイプ別の価格データ
PRINT ''
PRINT '=== データタイプ別価格データ (2025年) ==='
SELECT 
    DataType,
    COUNT(*) as RecordCount,
    COUNT(DISTINCT TradeDate) as TradingDays
FROM T_CommodityPrice
WHERE TradeDate >= '2025-01-01'
GROUP BY DataType
ORDER BY DataType;