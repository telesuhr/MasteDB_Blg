-- 更新された契約情報を確認
USE [JCL];
GO

-- 2025年のLME契約の情報を確認
SELECT 
    ActualContractID,
    ContractTicker,
    ContractMonth,
    ContractYear,
    LastTradeableDate,
    DeliveryDate,
    ContractSize,
    TickSize,
    DATENAME(WEEKDAY, LastTradeableDate) as LastTradeWeekday
FROM M_ActualContract
WHERE ContractTicker IN ('LPF25', 'LPG25', 'LPH25', 'LPJ25')
ORDER BY ContractMonth;

-- まだ不完全なデータがあるか確認
SELECT 
    ActualContractID,
    ContractTicker,
    ContractMonth,
    LastTradeableDate
FROM M_ActualContract
WHERE LastTradeableDate IS NULL 
   OR ContractMonth IS NULL
ORDER BY ContractTicker;