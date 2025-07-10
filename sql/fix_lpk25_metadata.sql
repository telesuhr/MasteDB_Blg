-- ============================================================
-- LPK25の誤ったメタデータを修正
-- K = 5月、LastTradeableDateも5月に修正
-- ============================================================

USE [JCL];
GO

-- 1. 修正前の状態を記録
PRINT '=== 修正前のLPK25の状態 ===';
SELECT 
    ActualContractID,
    ContractTicker,
    ContractMonth,
    ContractMonthCode,
    LastTradeableDate,
    DeliveryDate
FROM M_ActualContract
WHERE ContractTicker = 'LPK25';
GO

-- 2. LPK25のメタデータを修正
UPDATE M_ActualContract
SET 
    ContractMonth = 5,  -- K = 5月
    LastTradeableDate = '2025-05-19',  -- 2025年5月第3月曜日
    DeliveryDate = '2025-05-21'  -- 2営業日後
WHERE ContractTicker = 'LPK25';
GO

-- 3. 修正後の確認
PRINT '';
PRINT '=== 修正後のLPK25の状態 ===';
SELECT 
    ActualContractID,
    ContractTicker,
    ContractMonth,
    ContractMonthCode,
    LastTradeableDate,
    DeliveryDate
FROM M_ActualContract
WHERE ContractTicker = 'LPK25';
GO

-- 4. 2025年4月のマッピングを再計算
-- 4月14日（LPJ25の満期）→ 4月15日（LPK25へロールオーバー）
PRINT '';
PRINT '=== 2025年4月のマッピング再計算 ===';

-- LPJ25（4月契約）の情報確認
SELECT 
    'LPJ25（4月契約）' as Contract,
    LastTradeableDate,
    DATEDIFF(day, '2025-04-14', LastTradeableDate) as DaysToExpiryOn0414,
    DATEDIFF(day, '2025-04-15', LastTradeableDate) as DaysToExpiryOn0415
FROM M_ActualContract
WHERE ContractTicker = 'LPJ25';

-- LPK25（5月契約）の情報確認
SELECT 
    'LPK25（5月契約）' as Contract,
    LastTradeableDate,
    DATEDIFF(day, '2025-04-14', LastTradeableDate) as DaysToExpiryOn0414,
    DATEDIFF(day, '2025-04-15', LastTradeableDate) as DaysToExpiryOn0415
FROM M_ActualContract
WHERE ContractTicker = 'LPK25';
GO

-- 5. 2025年4月のLP1マッピングを更新
DECLARE @GenericID INT;
SELECT @GenericID = GenericID FROM M_GenericFutures WHERE GenericTicker = 'LP1 Comdty';

-- 4月1日～14日：LP1 → LPJ25（4月契約）
UPDATE gcm
SET gcm.ActualContractID = ac.ActualContractID,
    gcm.DaysToExpiry = DATEDIFF(day, gcm.TradeDate, ac.LastTradeableDate)
FROM T_GenericContractMapping gcm
CROSS JOIN M_ActualContract ac
WHERE gcm.GenericID = @GenericID
AND gcm.TradeDate BETWEEN '2025-04-01' AND '2025-04-14'
AND ac.ContractTicker = 'LPJ25';

-- 4月15日～30日：LP1 → LPK25（5月契約）
UPDATE gcm
SET gcm.ActualContractID = ac.ActualContractID,
    gcm.DaysToExpiry = DATEDIFF(day, gcm.TradeDate, ac.LastTradeableDate)
FROM T_GenericContractMapping gcm
CROSS JOIN M_ActualContract ac
WHERE gcm.GenericID = @GenericID
AND gcm.TradeDate BETWEEN '2025-04-15' AND '2025-04-30'
AND ac.ContractTicker = 'LPK25';

PRINT '';
PRINT CONCAT('更新された行数: ', @@ROWCOUNT);
GO

-- 6. 更新後のマッピング確認
PRINT '';
PRINT '=== 更新後の2025年4月LP1マッピング ===';
SELECT 
    gcm.TradeDate,
    ac.ContractTicker,
    ac.ContractMonth,
    ac.LastTradeableDate,
    gcm.DaysToExpiry
FROM T_GenericContractMapping gcm
JOIN M_GenericFutures gf ON gcm.GenericID = gf.GenericID
JOIN M_ActualContract ac ON gcm.ActualContractID = ac.ActualContractID
WHERE gcm.TradeDate BETWEEN '2025-04-10' AND '2025-04-20'
AND gf.GenericTicker = 'LP1 Comdty'
ORDER BY gcm.TradeDate;
GO

PRINT '';
PRINT 'LPK25のメタデータ修正とマッピング更新が完了しました';
GO