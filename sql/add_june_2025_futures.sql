-- ============================================================
-- 2025年6月先物（LPM25）をM_ActualContractに追加
-- ============================================================

USE [JCL];
GO

-- 既存のデータから推測して6月先物を追加
-- LMEの銅先物は通常、月の第3月曜日が最終取引日
INSERT INTO M_ActualContract (
    ContractTicker,
    MetalID,
    ExchangeCode,
    ContractMonth,
    ContractYear,
    ContractMonthCode,
    LastTradeableDate,
    DeliveryDate,
    ContractSize,
    TickSize,
    CreatedDate
)
SELECT 
    'LPM25' as ContractTicker,
    770 as MetalID,  -- Copper
    'LME' as ExchangeCode,
    6 as ContractMonth,  -- June
    2025 as ContractYear,
    'M' as ContractMonthCode,
    '2025-06-16' as LastTradeableDate,  -- 2025年6月の第3月曜日
    '2025-06-18' as DeliveryDate,  -- 最終取引日の2営業日後
    ContractSize,
    TickSize,
    GETDATE() as CreatedDate
FROM M_ActualContract
WHERE ContractTicker = 'LPN25';  -- 7月先物から設定をコピー
GO

-- 追加した6月先物を確認
SELECT 
    ActualContractID,
    ContractTicker,
    ContractMonth,
    ContractMonthCode,
    ContractYear,
    LastTradeableDate
FROM M_ActualContract
WHERE ContractTicker = 'LPM25';
GO

-- 他の不足している2025年前半の先物も追加
-- 1月先物（LPF25）
IF NOT EXISTS (SELECT 1 FROM M_ActualContract WHERE ContractTicker = 'LPF25')
BEGIN
    INSERT INTO M_ActualContract (
        ContractTicker, MetalID, ExchangeCode, ContractMonth, ContractYear,
        ContractMonthCode, LastTradeableDate, DeliveryDate, ContractSize, TickSize, CreatedDate
    )
    SELECT 
        'LPF25', 770, 'LME', 1, 2025, 'F', '2025-01-13', '2025-01-15',
        ContractSize, TickSize, GETDATE()
    FROM M_ActualContract WHERE ContractTicker = 'LPN25';
END
GO

-- 2月先物（LPG25）
IF NOT EXISTS (SELECT 1 FROM M_ActualContract WHERE ContractTicker = 'LPG25')
BEGIN
    INSERT INTO M_ActualContract (
        ContractTicker, MetalID, ExchangeCode, ContractMonth, ContractYear,
        ContractMonthCode, LastTradeableDate, DeliveryDate, ContractSize, TickSize, CreatedDate
    )
    SELECT 
        'LPG25', 770, 'LME', 2, 2025, 'G', '2025-02-17', '2025-02-19',
        ContractSize, TickSize, GETDATE()
    FROM M_ActualContract WHERE ContractTicker = 'LPN25';
END
GO

-- 3月先物（LPH25）
IF NOT EXISTS (SELECT 1 FROM M_ActualContract WHERE ContractTicker = 'LPH25')
BEGIN
    INSERT INTO M_ActualContract (
        ContractTicker, MetalID, ExchangeCode, ContractMonth, ContractYear,
        ContractMonthCode, LastTradeableDate, DeliveryDate, ContractSize, TickSize, CreatedDate
    )
    SELECT 
        'LPH25', 770, 'LME', 3, 2025, 'H', '2025-03-17', '2025-03-19',
        ContractSize, TickSize, GETDATE()
    FROM M_ActualContract WHERE ContractTicker = 'LPN25';
END
GO

-- 4月先物（LPJ25）
IF NOT EXISTS (SELECT 1 FROM M_ActualContract WHERE ContractTicker = 'LPJ25')
BEGIN
    INSERT INTO M_ActualContract (
        ContractTicker, MetalID, ExchangeCode, ContractMonth, ContractYear,
        ContractMonthCode, LastTradeableDate, DeliveryDate, ContractSize, TickSize, CreatedDate
    )
    SELECT 
        'LPJ25', 770, 'LME', 4, 2025, 'J', '2025-04-14', '2025-04-16',
        ContractSize, TickSize, GETDATE()
    FROM M_ActualContract WHERE ContractTicker = 'LPN25';
END
GO

-- 5月先物（LPK25）
IF NOT EXISTS (SELECT 1 FROM M_ActualContract WHERE ContractTicker = 'LPK25')
BEGIN
    INSERT INTO M_ActualContract (
        ContractTicker, MetalID, ExchangeCode, ContractMonth, ContractYear,
        ContractMonthCode, LastTradeableDate, DeliveryDate, ContractSize, TickSize, CreatedDate
    )
    SELECT 
        'LPK25', 770, 'LME', 5, 2025, 'K', '2025-05-12', '2025-05-14',
        ContractSize, TickSize, GETDATE()
    FROM M_ActualContract WHERE ContractTicker = 'LPN25';
END
GO

-- 2025年の全先物を確認
SELECT 
    ContractTicker,
    ContractMonth,
    ContractMonthCode,
    ContractYear,
    LastTradeableDate
FROM M_ActualContract
WHERE MetalID = 770
AND ExchangeCode = 'LME'
AND ContractYear = 2025
ORDER BY ContractMonth;
GO

PRINT '2025年先物の追加が完了しました';
GO