-- 動的マッピングの問題をデバッグ
USE [JCL];
GO

-- 1. M_ActualContract テーブルの LPJ25 と LPK25 のデータを確認
PRINT '=== M_ActualContract の LPJ25 と LPK25 のデータ ==='
SELECT 
    ActualContractID,
    ContractTicker,
    MetalID,
    ExchangeCode,
    ContractMonth,
    ContractMonthCode,
    ContractYear,
    LastTradeableDate,
    DeliveryDate,
    IsActive
FROM M_ActualContract
WHERE ContractTicker IN ('LPJ25', 'LPK25')
ORDER BY ContractTicker;

-- 2. 2025年4月時点で利用可能な全てのLME銅契約を確認
PRINT ''
PRINT '=== 2025年4月時点で利用可能なLME銅契約 ==='
SELECT 
    ActualContractID,
    ContractTicker,
    ContractMonth,
    ContractYear,
    LastTradeableDate,
    DeliveryDate,
    DATEDIFF(day, '2025-04-15', LastTradeableDate) as DaysFromApril15
FROM M_ActualContract
WHERE MetalID = 770
    AND ExchangeCode = 'LME'
    AND IsActive = 1
    AND LastTradeableDate >= '2025-04-01'
ORDER BY LastTradeableDate;

-- 3. 動的マッピングのロジックをテスト（2025年4月15日の例）
PRINT ''
PRINT '=== 2025年4月15日時点の動的マッピングロジック ==='
WITH TestMapping AS (
    SELECT 
        gf.GenericID,
        gf.GenericTicker,
        gf.GenericNumber,
        '2025-04-15' as TestDate,
        ac.ActualContractID,
        ac.ContractTicker,
        ac.ContractMonth,
        ac.LastTradeableDate,
        -- 契約選択の優先順位
        ROW_NUMBER() OVER (
            PARTITION BY gf.GenericID
            ORDER BY 
                -- 1. 取引最終日前の契約を優先
                CASE 
                    WHEN '2025-04-15' <= ac.LastTradeableDate THEN 0
                    ELSE 1
                END,
                -- 2. 最も近い限月を選択
                ac.ContractMonth,
                ac.ContractYear
        ) as rn
    FROM M_GenericFutures gf
    INNER JOIN M_ActualContract ac 
        ON gf.MetalID = ac.MetalID 
        AND gf.ExchangeCode = ac.ExchangeCode
        AND ac.IsActive = 1
    WHERE gf.GenericTicker = 'LP1 Comdty'
        AND ac.LastTradeableDate IS NOT NULL
)
SELECT 
    GenericTicker,
    GenericNumber,
    TestDate,
    ContractTicker,
    ContractMonth,
    LastTradeableDate,
    rn as 順位,
    CASE WHEN rn = GenericNumber THEN '選択される契約' ELSE '' END as Status
FROM TestMapping
WHERE rn <= 5
ORDER BY rn;

-- 4. ビューの定義を確認（CustomMappingのロジック部分）
PRINT ''
PRINT '=== ビューの現在のマッピングロジックを確認 ==='
EXEC sp_helptext 'V_CommodityPriceWithMaturityEx';

-- 5. 正しいマッピング例（期待される結果）
PRINT ''
PRINT '=== 期待されるマッピング ==='
PRINT '4月14日まで: LP1 → LPJ25 (2025年4月契約)'
PRINT '4月15日から: LP1 → LPK25 (2025年5月契約)'
PRINT ''
PRINT 'LPJ25のLastTradeableDate: 2025-04-14'
PRINT 'LPK25のLastTradeableDate: 2025-05-19'