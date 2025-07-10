-- ============================================================
-- V_CommodityPriceWithMaturityEx 動的マッピングビューのテスト
-- ============================================================

USE [JCL];
GO

-- 1. ビューが存在するか確認
IF NOT EXISTS (SELECT * FROM sys.objects WHERE name = 'V_CommodityPriceWithMaturityEx' AND type = 'V')
BEGIN
    PRINT 'ERROR: V_CommodityPriceWithMaturityEx ビューが存在しません'
    PRINT 'sql/create_custom_mapping_view.sql を実行してください'
END
ELSE
BEGIN
    PRINT 'V_CommodityPriceWithMaturityEx ビューが存在します'
    
    -- 2. 最新のデータを確認（LP1の直近30日）
    PRINT ''
    PRINT '=== LP1 Comdty の直近30日のマッピング状況 ==='
    SELECT TOP 30
        TradeDate,
        GenericTicker,
        GenericNumber,
        ActualContract,
        ContractMonth,
        ContractMonthCode,
        LastTradeableDate,
        CalendarDaysToExpiry,
        TradingDaysToExpiry,
        RolloverRecommendation,
        SettlementPrice
    FROM V_CommodityPriceWithMaturityEx
    WHERE GenericTicker = 'LP1 Comdty'
    ORDER BY TradeDate DESC;

    -- 3. ロールオーバーが発生した日付を確認
    PRINT ''
    PRINT '=== ロールオーバーが発生した日付（契約が切り替わった箇所） ==='
    WITH ContractChanges AS (
        SELECT 
            TradeDate,
            GenericTicker,
            ActualContract,
            LAG(ActualContract) OVER (PARTITION BY GenericTicker ORDER BY TradeDate) as PrevContract,
            LastTradeableDate,
            CalendarDaysToExpiry
        FROM V_CommodityPriceWithMaturityEx
        WHERE GenericTicker = 'LP1 Comdty'
            AND TradeDate >= DATEADD(month, -6, GETDATE())
    )
    SELECT 
        TradeDate,
        GenericTicker,
        PrevContract as '前日の契約',
        ActualContract as '当日の契約',
        LastTradeableDate,
        CalendarDaysToExpiry,
        'ロールオーバー発生' as Status
    FROM ContractChanges
    WHERE ActualContract != PrevContract
        OR PrevContract IS NULL
    ORDER BY TradeDate DESC;

    -- 4. 各ジェネリック番号の現在の契約を確認
    PRINT ''
    PRINT '=== 各ジェネリック先物の最新契約マッピング ==='
    WITH LatestData AS (
        SELECT 
            GenericTicker,
            GenericNumber,
            ActualContract,
            ContractMonth,
            LastTradeableDate,
            CalendarDaysToExpiry,
            ROW_NUMBER() OVER (PARTITION BY GenericTicker ORDER BY TradeDate DESC) as rn
        FROM V_CommodityPriceWithMaturityEx
        WHERE TradeDate = (SELECT MAX(TradeDate) FROM T_CommodityPrice WHERE DataType = 'Generic')
    )
    SELECT 
        GenericTicker,
        GenericNumber,
        ActualContract,
        ContractMonth,
        LastTradeableDate,
        CalendarDaysToExpiry
    FROM LatestData
    WHERE rn = 1
        AND GenericTicker LIKE 'LP%'
    ORDER BY GenericNumber;

    -- 5. データ品質チェック
    PRINT ''
    PRINT '=== データ品質チェック ==='
    SELECT 
        'ActualContractがNULLのレコード数' as CheckItem,
        COUNT(*) as Count
    FROM V_CommodityPriceWithMaturityEx
    WHERE ActualContract IS NULL
        AND TradeDate >= DATEADD(month, -1, GETDATE())
    UNION ALL
    SELECT 
        'ContractMonthがNULLのレコード数' as CheckItem,
        COUNT(*) as Count
    FROM V_CommodityPriceWithMaturityEx
    WHERE ContractMonth IS NULL
        AND TradeDate >= DATEADD(month, -1, GETDATE())
    UNION ALL
    SELECT 
        'LastTradeableDateがNULLのレコード数' as CheckItem,
        COUNT(*) as Count
    FROM V_CommodityPriceWithMaturityEx
    WHERE LastTradeableDate IS NULL
        AND TradeDate >= DATEADD(month, -1, GETDATE());
END
GO