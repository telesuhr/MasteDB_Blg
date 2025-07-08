-- ============================================================
-- ロールオーバー管理用テーブル作成
-- ============================================================

USE [JCL];
GO

-- 1. M_ActualContract（実契約マスタ）
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'M_ActualContract')
BEGIN
    CREATE TABLE M_ActualContract (
        ActualContractID INT IDENTITY(1,1) PRIMARY KEY,
        ContractTicker NVARCHAR(50) NOT NULL UNIQUE,
        MetalID INT NOT NULL,
        ExchangeCode NVARCHAR(10) NOT NULL,
        ContractMonth DATE NULL,
        ContractYear INT NULL,
        ContractMonthCode CHAR(1) NULL,
        LastTradeableDate DATE NULL,
        DeliveryDate DATE NULL,
        ContractSize DECIMAL(18,4) NULL,
        TickSize DECIMAL(18,6) NULL,
        CreatedDate DATETIME2(7) DEFAULT GETDATE(),
        CONSTRAINT FK_ActualContract_Metal FOREIGN KEY (MetalID) REFERENCES M_Metal(MetalID)
    );
    
    CREATE INDEX IX_ActualContract_ExchangeMonth ON M_ActualContract(ExchangeCode, ContractMonth);
    CREATE INDEX IX_ActualContract_LastTradeable ON M_ActualContract(LastTradeableDate);
    
    PRINT 'M_ActualContract table created successfully';
END
ELSE
BEGIN
    PRINT 'M_ActualContract table already exists';
END
GO

-- 2. T_GenericContractMapping（ジェネリック・実契約マッピング）
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'T_GenericContractMapping')
BEGIN
    CREATE TABLE T_GenericContractMapping (
        MappingID INT IDENTITY(1,1) PRIMARY KEY,
        TradeDate DATE NOT NULL,
        GenericID INT NOT NULL,
        ActualContractID INT NOT NULL,
        DaysToExpiry INT NULL,
        CreatedAt DATETIME2(7) DEFAULT GETDATE(),
        CONSTRAINT FK_Mapping_Generic FOREIGN KEY (GenericID) REFERENCES M_GenericFutures(GenericID),
        CONSTRAINT FK_Mapping_Actual FOREIGN KEY (ActualContractID) REFERENCES M_ActualContract(ActualContractID),
        CONSTRAINT UQ_Mapping_Date_Generic UNIQUE (TradeDate, GenericID)
    );
    
    CREATE INDEX IX_Mapping_TradeDate ON T_GenericContractMapping(TradeDate);
    CREATE INDEX IX_Mapping_GenericID ON T_GenericContractMapping(GenericID);
    CREATE INDEX IX_Mapping_ActualContractID ON T_GenericContractMapping(ActualContractID);
    
    PRINT 'T_GenericContractMapping table created successfully';
END
ELSE
BEGIN
    PRINT 'T_GenericContractMapping table already exists';
END
GO

-- 3. RolloverDaysカラムがM_GenericFuturesに存在しない場合は追加
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS 
               WHERE TABLE_NAME = 'M_GenericFutures' AND COLUMN_NAME = 'RolloverDays')
BEGIN
    ALTER TABLE M_GenericFutures 
    ADD RolloverDays INT DEFAULT 0;
    
    -- 取引所別のデフォルトロールオーバー日数を設定
    UPDATE M_GenericFutures SET RolloverDays = 0 WHERE ExchangeCode = 'LME';   -- LMEは満期まで取引
    UPDATE M_GenericFutures SET RolloverDays = 5 WHERE ExchangeCode = 'CMX';   -- COMEXは5営業日前
    UPDATE M_GenericFutures SET RolloverDays = 3 WHERE ExchangeCode = 'SHFE';  -- SHFEは3営業日前
    
    PRINT 'RolloverDays column added to M_GenericFutures';
END
GO

-- 4. ロールオーバー状況確認ビュー
CREATE OR ALTER VIEW V_RolloverStatus AS
WITH CurrentMapping AS (
    SELECT 
        gf.GenericID,
        gf.GenericTicker,
        gf.ExchangeCode,
        gf.GenericNumber,
        gf.LastTradeableDate,
        gf.RolloverDays,
        gcm.ActualContractID,
        ac.ContractTicker as CurrentContract,
        gcm.DaysToExpiry,
        gcm.TradeDate
    FROM M_GenericFutures gf
    LEFT JOIN T_GenericContractMapping gcm ON gf.GenericID = gcm.GenericID
        AND gcm.TradeDate = CAST(GETDATE() AS DATE)
    LEFT JOIN M_ActualContract ac ON gcm.ActualContractID = ac.ActualContractID
    WHERE gf.IsActive = 1
)
SELECT 
    GenericTicker,
    CASE ExchangeCode 
        WHEN 'CMX' THEN 'COMEX'
        ELSE ExchangeCode 
    END as Exchange,
    CurrentContract,
    LastTradeableDate,
    DaysToExpiry,
    CASE 
        WHEN DaysToExpiry IS NULL THEN 'NO_MAPPING'
        WHEN DaysToExpiry <= RolloverDays THEN 'ROLLOVER_NEEDED'
        WHEN DaysToExpiry <= RolloverDays + 5 THEN 'ROLLOVER_SOON'
        ELSE 'OK'
    END as RolloverStatus,
    RolloverDays,
    DATEDIFF(day, GETDATE(), LastTradeableDate) as ActualDaysToExpiry
FROM CurrentMapping
ORDER BY ExchangeCode, GenericNumber;
GO

PRINT '=== ロールオーバー管理テーブル作成完了 ===';
GO