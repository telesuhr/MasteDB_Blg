-- 2025年4月15日の既存データ確認
USE [JCL];
GO

-- 既存データの確認
SELECT 
    PriceID,
    TradeDate,
    MetalID,
    DataType,
    GenericID,
    ActualContractID,
    SettlementPrice
FROM T_CommodityPrice
WHERE TradeDate = '2025-04-15'
AND MetalID = 770  -- Copper
AND DataType = 'Generic'
AND GenericID = 109  -- LP1
ORDER BY PriceID;
GO

-- マッピングテーブルの確認
SELECT 
    gcm.TradeDate,
    gcm.GenericID,
    gf.GenericTicker,
    gcm.ActualContractID,
    ac.ContractTicker,
    gcm.DaysToExpiry
FROM T_GenericContractMapping gcm
JOIN M_GenericFutures gf ON gcm.GenericID = gf.GenericID
JOIN M_ActualContract ac ON gcm.ActualContractID = ac.ActualContractID
WHERE gcm.TradeDate = '2025-04-15'
AND gcm.GenericID = 109;
GO