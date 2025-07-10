-- M_ActualContract テーブルの内容確認
SELECT TOP 20
    ActualContractID,
    ContractTicker,
    MetalID,
    ExchangeCode,
    ContractMonth,
    ContractMonthCode,
    ContractYear,
    LastTradeableDate,
    DeliveryDate,
    IsActive,
    CreatedDate
FROM M_ActualContract
WHERE ContractTicker IN ('LPJ25', 'LPK25')
ORDER BY ActualContractID;

-- ContractMonthがNULLの契約を確認
SELECT COUNT(*) as NullContractMonthCount
FROM M_ActualContract
WHERE ContractMonth IS NULL;

-- 最近作成された契約を確認
SELECT TOP 20
    ActualContractID,
    ContractTicker,
    ContractMonth,
    ContractMonthCode,
    CreatedDate
FROM M_ActualContract
WHERE ExchangeCode = 'LME'
ORDER BY CreatedDate DESC;