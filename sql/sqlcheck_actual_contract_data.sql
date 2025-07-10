-- M_ActualContract テーブルの内容確認
SELECT TOP 10
    ActualContractID,
    ContractTicker,
    MetalID,
    ExchangeCode,
    ContractMonth,
    ContractMonthCode,
    ContractYear,
    LastTradeableDate,
    DeliveryDate
FROM M_ActualContract
WHERE ContractTicker IN ('LPJ25', 'LPK25')
ORDER BY ActualContractID;
