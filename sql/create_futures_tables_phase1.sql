-- ###########################################################
-- 先物契約管理テーブル - Phase 1: 基本テーブル作成
-- 対象データベース: SQL Server (JCL)
-- ###########################################################

-- データベースの選択
USE [JCL];
GO

-- ###########################################################
-- 1. ジェネリック先物マスタ
-- ###########################################################

-- M_GenericFutures (ジェネリック先物マスタ)
IF OBJECT_ID('dbo.M_GenericFutures', 'U') IS NOT NULL
    DROP TABLE dbo.M_GenericFutures;
CREATE TABLE dbo.M_GenericFutures (
    GenericID INT IDENTITY(1,1) NOT NULL,
    GenericTicker NVARCHAR(20) NOT NULL,        -- 'LP1 Comdty'
    MetalID INT NOT NULL,                       -- M_Metalへの外部キー
    ExchangeCode NVARCHAR(10) NOT NULL,         -- 'LME', 'SHFE', 'CMX'
    GenericNumber INT NOT NULL,                 -- 1, 2, 3...36
    Description NVARCHAR(100) NULL,             -- '1st Generic Future'
    IsActive BIT NOT NULL DEFAULT 1,
    CreatedDate DATETIME2(0) NOT NULL DEFAULT GETDATE(),
    CONSTRAINT PK_M_GenericFutures PRIMARY KEY CLUSTERED (GenericID),
    CONSTRAINT UQ_M_GenericFutures_Ticker UNIQUE (GenericTicker),
    CONSTRAINT FK_M_GenericFutures_MetalID FOREIGN KEY (MetalID) 
        REFERENCES dbo.M_Metal (MetalID)
);
GO

-- インデックス作成
CREATE NONCLUSTERED INDEX IX_M_GenericFutures_MetalExchange 
    ON dbo.M_GenericFutures (MetalID, ExchangeCode, GenericNumber);
GO

-- ###########################################################
-- 2. 実契約マスタ（Phase 1では最小限の構造）
-- ###########################################################

-- M_ActualContract (実契約マスタ)
IF OBJECT_ID('dbo.M_ActualContract', 'U') IS NOT NULL
    DROP TABLE dbo.M_ActualContract;
CREATE TABLE dbo.M_ActualContract (
    ActualContractID INT IDENTITY(1,1) NOT NULL,
    ContractTicker NVARCHAR(20) NOT NULL,       -- 'LPZ25 Comdty'
    MetalID INT NOT NULL,                       -- M_Metalへの外部キー
    ExchangeCode NVARCHAR(10) NOT NULL,         -- 'LME', 'SHFE', 'CMX'
    ContractMonth DATE NOT NULL,                -- '2025-12-01' (契約月の1日)
    ContractYear INT NOT NULL,                  -- 2025
    ContractMonthCode CHAR(1) NOT NULL,         -- 'Z' (12月)
    LastTradeableDate DATE NULL,                -- 最終取引可能日
    DeliveryDate DATE NULL,                     -- 最終引渡日
    ContractSize DECIMAL(18,4) NULL,            -- 契約サイズ
    TickSize DECIMAL(18,8) NULL,                -- ティックサイズ
    IsActive BIT NOT NULL DEFAULT 1,
    CreatedDate DATETIME2(0) NOT NULL DEFAULT GETDATE(),
    CONSTRAINT PK_M_ActualContract PRIMARY KEY CLUSTERED (ActualContractID),
    CONSTRAINT UQ_M_ActualContract_Ticker UNIQUE (ContractTicker),
    CONSTRAINT FK_M_ActualContract_MetalID FOREIGN KEY (MetalID) 
        REFERENCES dbo.M_Metal (MetalID)
);
GO

-- インデックス作成
CREATE NONCLUSTERED INDEX IX_M_ActualContract_MetalMonth 
    ON dbo.M_ActualContract (MetalID, ContractMonth);
GO

-- ###########################################################
-- 3. ジェネリック・実契約マッピング（Phase 1では最小限）
-- ###########################################################

-- T_GenericContractMapping (ジェネリック・実契約マッピング)
IF OBJECT_ID('dbo.T_GenericContractMapping', 'U') IS NOT NULL
    DROP TABLE dbo.T_GenericContractMapping;
CREATE TABLE dbo.T_GenericContractMapping (
    MappingID BIGINT IDENTITY(1,1) NOT NULL,
    TradeDate DATE NOT NULL,                    -- 取引日
    GenericID INT NOT NULL,                     -- ジェネリック先物ID
    ActualContractID INT NOT NULL,              -- 実契約ID
    DaysToExpiry INT NULL,                      -- 残存日数
    IsActive BIT NOT NULL DEFAULT 1,
    CreatedAt DATETIME2(0) NOT NULL DEFAULT GETDATE(),
    CONSTRAINT PK_T_GenericContractMapping PRIMARY KEY CLUSTERED (MappingID),
    CONSTRAINT UQ_T_GenericContractMapping UNIQUE (TradeDate, GenericID),
    CONSTRAINT FK_T_GenericContractMapping_Generic 
        FOREIGN KEY (GenericID) REFERENCES dbo.M_GenericFutures (GenericID),
    CONSTRAINT FK_T_GenericContractMapping_Actual 
        FOREIGN KEY (ActualContractID) REFERENCES dbo.M_ActualContract (ActualContractID)
);
GO

-- インデックス作成
CREATE NONCLUSTERED INDEX IX_T_GenericContractMapping_DateGeneric 
    ON dbo.T_GenericContractMapping (TradeDate, GenericID);
GO

-- ###########################################################
-- テーブル作成完了確認
-- ###########################################################

PRINT 'Phase 1 テーブル作成完了:';
PRINT '- M_GenericFutures';
PRINT '- M_ActualContract'; 
PRINT '- T_GenericContractMapping';
GO