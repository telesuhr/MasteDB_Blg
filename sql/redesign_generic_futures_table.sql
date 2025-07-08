-- ###########################################################
-- M_GenericFuturesテーブル再設計
-- GenericIDを主キーから外し、N番限月を表す属性とする
-- ###########################################################

USE [JCL];
GO

-- 既存の外部キー制約を削除
IF EXISTS (SELECT * FROM sys.foreign_keys WHERE name = 'FK_T_GenericContractMapping_Generic')
    ALTER TABLE dbo.T_GenericContractMapping DROP CONSTRAINT FK_T_GenericContractMapping_Generic;
GO

IF EXISTS (SELECT * FROM sys.foreign_keys WHERE name = 'FK_T_CommodityPrice_V2_GenericID')
    ALTER TABLE dbo.T_CommodityPrice_V2 DROP CONSTRAINT FK_T_CommodityPrice_V2_GenericID;
GO

-- 既存のテーブルを削除
IF OBJECT_ID('dbo.M_GenericFutures', 'U') IS NOT NULL
    DROP TABLE dbo.M_GenericFutures;
GO

-- 新しいM_GenericFuturesテーブル作成
CREATE TABLE dbo.M_GenericFutures (
    ID INT IDENTITY(1,1) NOT NULL,                 -- 新しい主キー
    GenericID INT NOT NULL,                        -- N番限月（1-60, 1-12, 1-36）
    GenericTicker NVARCHAR(20) NOT NULL,           -- 'LP1 Comdty'
    MetalID INT NOT NULL,                          -- M_Metalへの外部キー
    ExchangeCode NVARCHAR(10) NOT NULL,            -- 'LME', 'SHFE', 'CMX'
    GenericNumber INT NOT NULL,                    -- 1, 2, 3...（GenericIDと同じ値）
    Description NVARCHAR(100) NULL,                -- '1st Generic Future'
    IsActive BIT NOT NULL DEFAULT 1,
    CreatedDate DATETIME2(0) NOT NULL DEFAULT GETDATE(),
    CONSTRAINT PK_M_GenericFutures PRIMARY KEY CLUSTERED (ID),
    CONSTRAINT UQ_M_GenericFutures_Ticker UNIQUE (GenericTicker),
    CONSTRAINT UQ_M_GenericFutures_Exchange_GenericID UNIQUE (ExchangeCode, GenericID),
    CONSTRAINT FK_M_GenericFutures_MetalID FOREIGN KEY (MetalID) 
        REFERENCES dbo.M_Metal (MetalID)
);
GO

-- インデックス作成
CREATE NONCLUSTERED INDEX IX_M_GenericFutures_MetalExchange 
    ON dbo.M_GenericFutures (MetalID, ExchangeCode, GenericID);
GO

-- T_GenericContractMappingも修正
IF OBJECT_ID('dbo.T_GenericContractMapping', 'U') IS NOT NULL
    DROP TABLE dbo.T_GenericContractMapping;
GO

CREATE TABLE dbo.T_GenericContractMapping (
    MappingID BIGINT IDENTITY(1,1) NOT NULL,
    TradeDate DATE NOT NULL,                    -- 取引日
    GenericFutureID INT NOT NULL,               -- M_GenericFuturesのIDへの外部キー
    ActualContractID INT NOT NULL,              -- 実契約ID
    DaysToExpiry INT NULL,                      -- 残存日数
    IsActive BIT NOT NULL DEFAULT 1,
    CreatedAt DATETIME2(0) NOT NULL DEFAULT GETDATE(),
    CONSTRAINT PK_T_GenericContractMapping PRIMARY KEY CLUSTERED (MappingID),
    CONSTRAINT UQ_T_GenericContractMapping UNIQUE (TradeDate, GenericFutureID),
    CONSTRAINT FK_T_GenericContractMapping_Generic 
        FOREIGN KEY (GenericFutureID) REFERENCES dbo.M_GenericFutures (ID),
    CONSTRAINT FK_T_GenericContractMapping_Actual 
        FOREIGN KEY (ActualContractID) REFERENCES dbo.M_ActualContract (ActualContractID)
);
GO

-- T_CommodityPrice_V2も修正が必要
-- ただし、既存データがあるため注意が必要