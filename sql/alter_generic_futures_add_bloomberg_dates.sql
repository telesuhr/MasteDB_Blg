-- M_GenericFuturesテーブルにBloombergの満期日フィールドを追加
-- LAST_TRADEABLE_DT: 最終取引可能日
-- FUT_DLV_DT_LAST: 満期日（最終受渡日）

-- 1. 新しいカラムを追加
ALTER TABLE M_GenericFutures
ADD 
    -- Bloombergから取得する最終取引可能日
    LastTradeableDate DATE NULL,
    
    -- Bloombergから取得する満期日（最終受渡日）
    FutureDeliveryDateLast DATE NULL,
    
    -- データ取得日時
    LastRefreshDate DATETIME2 NULL;

GO

-- 2. インデックスを作成（検索性能向上のため）
CREATE NONCLUSTERED INDEX IX_GenericFutures_LastTradeableDate 
ON M_GenericFutures (LastTradeableDate) 
INCLUDE (GenericID, ExchangeCode, GenericNumber);

GO

-- 3. 満期情報を含む拡張ビューを再作成
CREATE OR ALTER VIEW V_CommodityPriceWithMaturityEx AS
SELECT 
    s.*,
    
    -- Bloombergから取得した実際の日付
    g.LastTradeableDate,
    g.FutureDeliveryDateLast,
    g.LastRefreshDate,
    
    -- 残取引可能日数（実際の最終取引日ベース）
    CASE 
        WHEN g.LastTradeableDate IS NOT NULL THEN
            DATEDIFF(day, s.TradeDate, g.LastTradeableDate)
        ELSE NULL
    END as ActualTradingDaysRemaining,
    
    -- ロールオーバー推奨日（実際の最終取引日ベース）
    CASE 
        WHEN g.LastTradeableDate IS NOT NULL AND g.RolloverDays IS NOT NULL THEN
            DATEADD(day, -g.RolloverDays, g.LastTradeableDate)
        ELSE NULL
    END as ActualRolloverDate,
    
    -- ロールオーバーフラグ（実際の最終取引日ベース）
    CASE 
        WHEN g.LastTradeableDate IS NOT NULL 
            AND g.RolloverDays IS NOT NULL
            AND s.TradeDate >= DATEADD(day, -g.RolloverDays, g.LastTradeableDate)
        THEN 'Yes'
        ELSE 'No'
    END as ActualShouldRollover,
    
    -- 満期までの日数
    CASE 
        WHEN g.FutureDeliveryDateLast IS NOT NULL THEN
            DATEDIFF(day, s.TradeDate, g.FutureDeliveryDateLast)
        ELSE NULL
    END as DaysToDelivery,
    
    -- 取引終了から満期までの日数
    CASE 
        WHEN g.LastTradeableDate IS NOT NULL AND g.FutureDeliveryDateLast IS NOT NULL THEN
            DATEDIFF(day, g.LastTradeableDate, g.FutureDeliveryDateLast)
        ELSE NULL
    END as SettlementPeriodDays,
    
    g.RolloverDays,
    g.MaturityRule,
    g.LastTradingDayRule
    
FROM V_CommodityPriceSimple s
INNER JOIN M_GenericFutures g ON s.GenericID = g.GenericID

GO

-- 4. 満期日更新用のストアドプロシージャを作成
CREATE OR ALTER PROCEDURE sp_UpdateGenericFuturesMaturityDates
AS
BEGIN
    SET NOCOUNT ON;
    
    -- 更新対象のジェネリック先物を記録
    DECLARE @UpdateLog TABLE (
        GenericID INT,
        GenericTicker NVARCHAR(20),
        OldLastTradeableDate DATE,
        NewLastTradeableDate DATE,
        OldDeliveryDate DATE,
        NewDeliveryDate DATE
    );
    
    -- この部分は実際のBloomberg APIからのデータ取得後に実行される
    -- 現在は構造のみ定義
    
    SELECT 'ストアドプロシージャが作成されました。Bloomberg APIからデータを取得して更新してください。' as Message;
END

GO

-- 使用例

-- 1. 実際の満期日情報を含む価格データ
SELECT 
    ExchangeDisplayName,
    GenericTicker,
    GenericDescription,
    TradeDate,
    LastTradeableDate,
    FutureDeliveryDateLast,
    ActualTradingDaysRemaining,
    ActualRolloverDate,
    ActualShouldRollover,
    PriceForAnalysis,
    Volume
FROM V_CommodityPriceWithMaturityEx
WHERE TradeDate = (SELECT MAX(TradeDate) FROM V_CommodityPriceWithMaturityEx)
    AND MetalCode LIKE 'CU%'
ORDER BY ExchangeDisplayName, GenericNumber

-- 2. ロールオーバーが必要な契約（実際の日付ベース）
SELECT 
    ExchangeDisplayName,
    GenericTicker,
    LastTradeableDate,
    ActualTradingDaysRemaining,
    ActualRolloverDate,
    Volume,
    TradingActivity
FROM V_CommodityPriceWithMaturityEx
WHERE TradeDate = (SELECT MAX(TradeDate) FROM V_CommodityPriceWithMaturityEx)
    AND ActualShouldRollover = 'Yes'
ORDER BY ActualTradingDaysRemaining

-- 3. 満期日情報の更新状況確認
SELECT 
    ExchangeCode,
    COUNT(*) as TotalContracts,
    COUNT(LastTradeableDate) as WithLastTradeable,
    COUNT(FutureDeliveryDateLast) as WithDeliveryDate,
    MIN(LastRefreshDate) as OldestRefresh,
    MAX(LastRefreshDate) as NewestRefresh
FROM M_GenericFutures
GROUP BY ExchangeCode
ORDER BY ExchangeCode