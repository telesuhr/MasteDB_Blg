-- ============================================================
-- 価格データとマッピング情報を結合したビュー
-- Generic先物の実際の契約情報を確認できる
-- ============================================================

USE [JCL];
GO

-- 既存のビューがあれば削除
IF OBJECT_ID('dbo.V_CommodityPriceWithMapping', 'V') IS NOT NULL
    DROP VIEW dbo.V_CommodityPriceWithMapping;
GO

CREATE VIEW dbo.V_CommodityPriceWithMapping AS
SELECT 
    cp.PriceID,
    cp.TradeDate,
    cp.MetalID,
    m.MetalCode,
    m.MetalName,
    cp.DataType,
    
    -- Generic先物の情報
    cp.GenericID,
    gf.GenericTicker,
    gf.GenericNumber,
    
    -- マッピングされた実契約情報
    gcm.ActualContractID AS MappedContractID,
    ac.ContractTicker AS MappedContractTicker,
    ac.ContractMonth AS MappedContractMonth,
    ac.ContractMonthCode AS MappedContractMonthCode,
    ac.LastTradeableDate AS MappedLastTradeable,
    gcm.DaysToExpiry,
    
    -- 価格情報
    cp.SettlementPrice,
    cp.OpenPrice,
    cp.HighPrice,
    cp.LowPrice,
    cp.LastPrice,
    cp.Volume,
    cp.OpenInterest,
    
    -- マッピング状態
    CASE 
        WHEN cp.DataType = 'Generic' AND gcm.ActualContractID IS NOT NULL THEN 'マッピング済み'
        WHEN cp.DataType = 'Generic' AND gcm.ActualContractID IS NULL THEN 'マッピングなし'
        ELSE 'N/A'
    END AS MappingStatus
    
FROM dbo.T_CommodityPrice cp
INNER JOIN dbo.M_Metal m ON cp.MetalID = m.MetalID
LEFT JOIN dbo.M_GenericFutures gf ON cp.GenericID = gf.GenericID
LEFT JOIN dbo.T_GenericContractMapping gcm 
    ON cp.GenericID = gcm.GenericID AND cp.TradeDate = gcm.TradeDate
LEFT JOIN dbo.M_ActualContract ac ON gcm.ActualContractID = ac.ActualContractID;
GO

-- ビューの権限設定
GRANT SELECT ON dbo.V_CommodityPriceWithMapping TO PUBLIC;
GO

PRINT 'V_CommodityPriceWithMapping ビューを作成しました';
GO

-- サンプルクエリ
-- 2025年4月のLP1マッピング状況を確認
SELECT 
    TradeDate,
    GenericTicker,
    MappedContractTicker,
    MappedContractMonth,
    DaysToExpiry,
    SettlementPrice
FROM V_CommodityPriceWithMapping
WHERE TradeDate BETWEEN '2025-04-01' AND '2025-04-30'
AND GenericTicker = 'LP1 Comdty'
ORDER BY TradeDate;
GO