-- ============================================================
-- 欠損している契約日付情報を更新
-- 2025年のLME契約（LPF25〜LPK25）の日付を設定
-- ============================================================

USE [JCL];
GO

-- 1. 欠損データの確認
PRINT '=== 日付情報が欠損している契約 ===';
SELECT 
    ContractTicker,
    ContractMonth,
    ContractMonthCode,
    ContractYear,
    LastTradeableDate,
    DeliveryDate
FROM M_ActualContract
WHERE (LastTradeableDate IS NULL OR DeliveryDate IS NULL)
AND ExchangeCode = 'LME'
ORDER BY ContractYear, ContractMonth;
GO

-- 2. LME契約の標準的な日付ルール
-- 最終取引日: 月の第3水曜日の2営業日前（通常は月曜日）
-- 受渡日: 最終取引日の2営業日後（通常は水曜日）

-- 2025年契約の日付を更新
UPDATE M_ActualContract
SET 
    LastTradeableDate = CASE ContractTicker
        WHEN 'LPF25' THEN '2025-01-13'  -- 2025年1月第3月曜日
        WHEN 'LPG25' THEN '2025-02-17'  -- 2025年2月第3月曜日
        WHEN 'LPH25' THEN '2025-03-17'  -- 2025年3月第3月曜日
        WHEN 'LPJ25' THEN '2025-04-14'  -- 2025年4月第3月曜日
        WHEN 'LPK25' THEN '2025-05-19'  -- 2025年5月第3月曜日
        ELSE LastTradeableDate
    END,
    DeliveryDate = CASE ContractTicker
        WHEN 'LPF25' THEN '2025-01-15'  -- 2営業日後
        WHEN 'LPG25' THEN '2025-02-19'  -- 2営業日後
        WHEN 'LPH25' THEN '2025-03-19'  -- 2営業日後
        WHEN 'LPJ25' THEN '2025-04-16'  -- 2営業日後
        WHEN 'LPK25' THEN '2025-05-21'  -- 2営業日後
        ELSE DeliveryDate
    END
WHERE ContractTicker IN ('LPF25', 'LPG25', 'LPH25', 'LPJ25', 'LPK25')
AND (LastTradeableDate IS NULL OR DeliveryDate IS NULL);
GO

-- 3. 更新結果の確認
PRINT '';
PRINT '=== 更新後の2025年LME契約 ===';
SELECT 
    ContractTicker,
    ContractMonth,
    ContractMonthCode,
    ContractYear,
    LastTradeableDate,
    DeliveryDate,
    DATEDIFF(day, GETDATE(), LastTradeableDate) as DaysToExpiry
FROM M_ActualContract
WHERE ContractYear = 2025
AND ExchangeCode = 'LME'
ORDER BY ContractMonth;
GO

-- 4. マッピングテーブルのDaysToExpiry更新
PRINT '';
PRINT '=== マッピングテーブルのDaysToExpiry更新 ===';

UPDATE gcm
SET gcm.DaysToExpiry = DATEDIFF(day, gcm.TradeDate, ac.LastTradeableDate)
FROM T_GenericContractMapping gcm
INNER JOIN M_ActualContract ac ON gcm.ActualContractID = ac.ActualContractID
WHERE gcm.DaysToExpiry IS NULL
AND ac.LastTradeableDate IS NOT NULL;

-- 影響を受けた行数を表示
PRINT CONCAT('更新された行数: ', @@ROWCOUNT);
GO

-- 5. 最近のマッピング状況を確認
PRINT '';
PRINT '=== 最近のマッピング状況（LP1、LP2） ===';
SELECT TOP 20
    gf.GenericTicker,
    ac.ContractTicker,
    gcm.TradeDate,
    gcm.DaysToExpiry,
    ac.LastTradeableDate
FROM T_GenericContractMapping gcm
JOIN M_GenericFutures gf ON gcm.GenericID = gf.GenericID
JOIN M_ActualContract ac ON gcm.ActualContractID = ac.ActualContractID
WHERE gf.GenericTicker IN ('LP1 Comdty', 'LP2 Comdty')
AND gcm.TradeDate >= DATEADD(day, -30, GETDATE())
ORDER BY gcm.TradeDate DESC, gf.GenericTicker;
GO

PRINT '';
PRINT '日付情報の更新が完了しました';
GO