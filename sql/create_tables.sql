-- ###########################################################
-- LME テナースプレッド分析データベース テーブル作成スクリプト
-- 対象データベース: SQL Server
-- ###########################################################

-- データベースの選択
USE [JCL];
GO

-- ###########################################################
-- 1. マスタデータ (M_*) テーブル
-- ###########################################################

-- M_Metal (金属銘柄マスタ)
IF OBJECT_ID('dbo.M_Metal', 'U') IS NOT NULL
    DROP TABLE dbo.M_Metal;
CREATE TABLE dbo.M_Metal (
    MetalID INT IDENTITY(1,1) NOT NULL,
    MetalCode NVARCHAR(10) NOT NULL,
    MetalName NVARCHAR(50) NOT NULL,
    CurrencyCode NVARCHAR(3) NOT NULL,
    ExchangeCode NVARCHAR(10) NULL, -- LME, SHFE, CMXなど
    Description NVARCHAR(255) NULL,
    CONSTRAINT PK_M_Metal PRIMARY KEY CLUSTERED (MetalID),
    CONSTRAINT UQ_M_Metal_MetalCode UNIQUE (MetalCode)
);
GO

-- M_TenorType (限月タイプマスタ)
IF OBJECT_ID('dbo.M_TenorType', 'U') IS NOT NULL
    DROP TABLE dbo.M_TenorType;
CREATE TABLE dbo.M_TenorType (
    TenorTypeID INT IDENTITY(1,1) NOT NULL,
    TenorTypeName NVARCHAR(50) NOT NULL,
    Description NVARCHAR(255) NULL,
    CONSTRAINT PK_M_TenorType PRIMARY KEY CLUSTERED (TenorTypeID),
    CONSTRAINT UQ_M_TenorType_TenorTypeName UNIQUE (TenorTypeName)
);
GO

-- M_Indicator (指標マスタ)
IF OBJECT_ID('dbo.M_Indicator', 'U') IS NOT NULL
    DROP TABLE dbo.M_Indicator;
CREATE TABLE dbo.M_Indicator (
    IndicatorID INT IDENTITY(1,1) NOT NULL,
    IndicatorCode NVARCHAR(50) NOT NULL,
    IndicatorName NVARCHAR(100) NOT NULL,
    Category NVARCHAR(50) NULL, -- 例: 'Interest Rate', 'FX', 'Macro Economic', 'Commodity Index', 'Equity Index', 'Energy'
    Unit NVARCHAR(20) NULL, -- 例: '%', 'Index Points', 'USD/JPY'
    Freq NVARCHAR(10) NULL, -- 例: 'Daily', 'Weekly', 'Monthly', 'Yearly'
    Description NVARCHAR(255) NULL,
    CONSTRAINT PK_M_Indicator PRIMARY KEY CLUSTERED (IndicatorID),
    CONSTRAINT UQ_M_Indicator_IndicatorCode UNIQUE (IndicatorCode)
);
GO

-- M_Region (地域マスタ)
IF OBJECT_ID('dbo.M_Region', 'U') IS NOT NULL
    DROP TABLE dbo.M_Region;
CREATE TABLE dbo.M_Region (
    RegionID INT IDENTITY(1,1) NOT NULL,
    RegionCode NVARCHAR(10) NOT NULL, -- 例: 'GLOBAL', 'ASIA', 'EU', 'US', 'MEST'
    RegionName NVARCHAR(50) NOT NULL,
    Description NVARCHAR(255) NULL,
    CONSTRAINT PK_M_Region PRIMARY KEY CLUSTERED (RegionID),
    CONSTRAINT UQ_M_Region_RegionCode UNIQUE (RegionCode)
);
GO

-- M_COTRCategory (COTRカテゴリーマスタ)
IF OBJECT_ID('dbo.M_COTRCategory', 'U') IS NOT NULL
    DROP TABLE dbo.M_COTRCategory;
CREATE TABLE dbo.M_COTRCategory (
    COTRCategoryID INT IDENTITY(1,1) NOT NULL,
    CategoryName NVARCHAR(50) NOT NULL, -- 例: 'Producer/Merchant/Processor/User', 'Investment Funds'
    Description NVARCHAR(255) NULL,
    CONSTRAINT PK_M_COTRCategory PRIMARY KEY CLUSTERED (COTRCategoryID),
    CONSTRAINT UQ_M_COTRCategory_CategoryName UNIQUE (CategoryName)
);
GO

-- M_HoldingBand (保有比率バンドマスタ)
IF OBJECT_ID('dbo.M_HoldingBand', 'U') IS NOT NULL
    DROP TABLE dbo.M_HoldingBand;
CREATE TABLE dbo.M_HoldingBand (
    BandID INT IDENTITY(1,1) NOT NULL,
    BandRange NVARCHAR(20) NOT NULL, -- 例: '5-9%', '40%+', '90%+'
    MinValue DECIMAL(5,2) NULL, -- バンドの下限 (%)
    MaxValue DECIMAL(5,2) NULL, -- バンドの上限 (%)
    Description NVARCHAR(255) NULL,
    CONSTRAINT PK_M_HoldingBand PRIMARY KEY CLUSTERED (BandID),
    CONSTRAINT UQ_M_HoldingBand_BandRange UNIQUE (BandRange)
);
GO

-- ###########################################################
-- 2. 取引データ (T_*) テーブル
-- ###########################################################

-- T_CommodityPrice (商品価格データ)
IF OBJECT_ID('dbo.T_CommodityPrice', 'U') IS NOT NULL
    DROP TABLE dbo.T_CommodityPrice;
CREATE TABLE dbo.T_CommodityPrice (
    PriceID BIGINT IDENTITY(1,1) NOT NULL,
    TradeDate DATE NOT NULL,
    MetalID INT NOT NULL,
    TenorTypeID INT NOT NULL,
    SpecificTenorDate DATE NULL, -- 具体的な満期日 (LPxxなどジェネリックな場合はNULL)
    SettlementPrice DECIMAL(18,4) NULL,
    OpenPrice DECIMAL(18,4) NULL,
    HighPrice DECIMAL(18,4) NULL,
    LowPrice DECIMAL(18,4) NULL,
    LastPrice DECIMAL(18,4) NULL,
    Volume BIGINT NULL,
    OpenInterest BIGINT NULL,
    MaturityDate DATE NULL, -- 満期日 (Fieldsに指定された場合)
    LastUpdated DATETIME2(0) NOT NULL DEFAULT GETDATE(),
    CONSTRAINT PK_T_CommodityPrice PRIMARY KEY CLUSTERED (PriceID),
    CONSTRAINT UQ_T_CommodityPrice UNIQUE (TradeDate, MetalID, TenorTypeID, SpecificTenorDate),
    CONSTRAINT FK_T_CommodityPrice_MetalID FOREIGN KEY (MetalID) REFERENCES dbo.M_Metal (MetalID),
    CONSTRAINT FK_T_CommodityPrice_TenorTypeID FOREIGN KEY (TenorTypeID) REFERENCES dbo.M_TenorType (TenorTypeID)
);
GO
CREATE NONCLUSTERED INDEX IX_T_CommodityPrice_MetalDateTenor ON dbo.T_CommodityPrice (MetalID, TradeDate, TenorTypeID);
GO

-- T_LMEInventory (LME在庫データ)
IF OBJECT_ID('dbo.T_LMEInventory', 'U') IS NOT NULL
    DROP TABLE dbo.T_LMEInventory;
CREATE TABLE dbo.T_LMEInventory (
    InventoryID BIGINT IDENTITY(1,1) NOT NULL,
    ReportDate DATE NOT NULL,
    MetalID INT NOT NULL,
    RegionID INT NOT NULL, -- 地域別在庫 (M_RegionへのFK)
    TotalStock DECIMAL(18,0) NULL,
    OnWarrant DECIMAL(18,0) NULL,
    CancelledWarrant DECIMAL(18,0) NULL,
    Inflow DECIMAL(18,0) NULL, -- 在庫流入量
    Outflow DECIMAL(18,0) NULL, -- 在庫流出量
    LastUpdated DATETIME2(0) NOT NULL DEFAULT GETDATE(),
    CONSTRAINT PK_T_LMEInventory PRIMARY KEY CLUSTERED (InventoryID),
    CONSTRAINT UQ_T_LMEInventory UNIQUE (ReportDate, MetalID, RegionID),
    CONSTRAINT FK_T_LMEInventory_MetalID FOREIGN KEY (MetalID) REFERENCES dbo.M_Metal (MetalID),
    CONSTRAINT FK_T_LMEInventory_RegionID FOREIGN KEY (RegionID) REFERENCES dbo.M_Region (RegionID)
);
GO
CREATE NONCLUSTERED INDEX IX_T_LMEInventory_MetalDateRegion ON dbo.T_LMEInventory (MetalID, ReportDate, RegionID);
GO

-- T_OtherExchangeInventory (他取引所在庫データ - SHFE, CMX)
IF OBJECT_ID('dbo.T_OtherExchangeInventory', 'U') IS NOT NULL
    DROP TABLE dbo.T_OtherExchangeInventory;
CREATE TABLE dbo.T_OtherExchangeInventory (
    OtherInvID BIGINT IDENTITY(1,1) NOT NULL,
    ReportDate DATE NOT NULL,
    MetalID INT NOT NULL,
    ExchangeCode NVARCHAR(10) NOT NULL, -- SHFE, CMXなど
    TotalStock DECIMAL(18,0) NULL,
    OnWarrant DECIMAL(18,0) NULL, -- SHFEのOn Warrant Stocksなど
    LastUpdated DATETIME2(0) NOT NULL DEFAULT GETDATE(),
    CONSTRAINT PK_T_OtherExchangeInventory PRIMARY KEY CLUSTERED (OtherInvID),
    CONSTRAINT UQ_T_OtherExchangeInventory UNIQUE (ReportDate, MetalID, ExchangeCode),
    CONSTRAINT FK_T_OtherExchangeInventory_MetalID FOREIGN KEY (MetalID) REFERENCES dbo.M_Metal (MetalID)
);
GO
CREATE NONCLUSTERED INDEX IX_T_OtherExchangeInventory_MetalDateExchange ON dbo.T_OtherExchangeInventory (MetalID, ReportDate, ExchangeCode);
GO

-- T_MarketIndicator (市場指標データ - 金利, 為替, コモディティ指数, 株価指数, エネルギー価格, 現物プレミアム)
IF OBJECT_ID('dbo.T_MarketIndicator', 'U') IS NOT NULL
    DROP TABLE dbo.T_MarketIndicator;
CREATE TABLE dbo.T_MarketIndicator (
    MarketIndID BIGINT IDENTITY(1,1) NOT NULL,
    ReportDate DATE NOT NULL,
    IndicatorID INT NOT NULL,
    MetalID INT NULL, -- 金属に特化した指標の場合 (例: 洋山プレミアム)
    Value DECIMAL(18,4) NULL,
    LastUpdated DATETIME2(0) NOT NULL DEFAULT GETDATE(),
    CONSTRAINT PK_T_MarketIndicator PRIMARY KEY CLUSTERED (MarketIndID),
    CONSTRAINT UQ_T_MarketIndicator UNIQUE (ReportDate, IndicatorID, MetalID), -- MetalIDがNULLの場合も考慮
    CONSTRAINT FK_T_MarketIndicator_IndicatorID FOREIGN KEY (IndicatorID) REFERENCES dbo.M_Indicator (IndicatorID),
    CONSTRAINT FK_T_MarketIndicator_MetalID FOREIGN KEY (MetalID) REFERENCES dbo.M_Metal (MetalID)
);
GO
CREATE NONCLUSTERED INDEX IX_T_MarketIndicator_DateIndicator ON dbo.T_MarketIndicator (ReportDate, IndicatorID);
GO

-- T_MacroEconomicIndicator (マクロ経済指標データ - PMI, GDP, CPIなど)
IF OBJECT_ID('dbo.T_MacroEconomicIndicator', 'U') IS NOT NULL
    DROP TABLE dbo.T_MacroEconomicIndicator;
CREATE TABLE dbo.T_MacroEconomicIndicator (
    MacroIndID BIGINT IDENTITY(1,1) NOT NULL,
    ReportDate DATE NOT NULL, -- 月次/四半期/年次など
    IndicatorID INT NOT NULL,
    CountryCode NVARCHAR(3) NULL, -- 対象国コード (e.g., 'US', 'CN', 'EU')
    Value DECIMAL(18,4) NULL,
    LastUpdated DATETIME2(0) NOT NULL DEFAULT GETDATE(),
    CONSTRAINT PK_T_MacroEconomicIndicator PRIMARY KEY CLUSTERED (MacroIndID),
    CONSTRAINT UQ_T_MacroEconomicIndicator UNIQUE (ReportDate, IndicatorID, CountryCode),
    CONSTRAINT FK_T_MacroEconomicIndicator_IndicatorID FOREIGN KEY (IndicatorID) REFERENCES dbo.M_Indicator (IndicatorID)
);
GO
CREATE NONCLUSTERED INDEX IX_T_MacroEconomicIndicator_DateIndicator ON dbo.T_MacroEconomicIndicator (ReportDate, IndicatorID);
GO

-- T_COTR (LME COTRデータ)
IF OBJECT_ID('dbo.T_COTR', 'U') IS NOT NULL
    DROP TABLE dbo.T_COTR;
CREATE TABLE dbo.T_COTR (
    COTRID BIGINT IDENTITY(1,1) NOT NULL,
    ReportDate DATE NOT NULL, -- 週次レポート対象日 (通常 火曜日)
    MetalID INT NOT NULL,
    COTRCategoryID INT NOT NULL,
    LongPosition BIGINT NULL,
    ShortPosition BIGINT NULL,
    SpreadPosition BIGINT NULL, -- レポートに明確な項目があれば
    NetPosition BIGINT NULL, -- Long - Short (計算値)
    LongChange BIGINT NULL, -- 前週からの変化量
    ShortChange BIGINT NULL, -- 前週からの変化量
    NetChange BIGINT NULL, -- NetPositionの変化量 (計算値)
    LongPctOpenInterest DECIMAL(5,2) NULL, -- 総建玉に占めるロング割合 (%)
    ShortPctOpenInterest DECIMAL(5,2) NULL, -- 総建玉に占めるショート割合 (%)
    TotalOpenInterest BIGINT NULL, -- レポート全体の総建玉
    LastUpdated DATETIME2(0) NOT NULL DEFAULT GETDATE(),
    CONSTRAINT PK_T_COTR PRIMARY KEY CLUSTERED (COTRID),
    CONSTRAINT UQ_T_COTR UNIQUE (ReportDate, MetalID, COTRCategoryID),
    CONSTRAINT FK_T_COTR_MetalID FOREIGN KEY (MetalID) REFERENCES dbo.M_Metal (MetalID),
    CONSTRAINT FK_T_COTR_COTRCategoryID FOREIGN KEY (COTRCategoryID) REFERENCES dbo.M_COTRCategory (COTRCategoryID)
);
GO
CREATE NONCLUSTERED INDEX IX_T_COTR_MetalDateCategory ON dbo.T_COTR (MetalID, ReportDate, COTRCategoryID);
GO

-- T_BandingReport (保有比率/ワラントバンディングレポートデータ)
IF OBJECT_ID('dbo.T_BandingReport', 'U') IS NOT NULL
    DROP TABLE dbo.T_BandingReport;
CREATE TABLE dbo.T_BandingReport (
    BandingID BIGINT IDENTITY(1,1) NOT NULL,
    ReportDate DATE NOT NULL,
    MetalID INT NOT NULL,
    ReportType NVARCHAR(50) NOT NULL, -- 例: 'Futures Long', 'Futures Short', 'Warrant', 'Cash', 'Tom'
    TenorTypeID INT NULL, -- 'Futures Long/Short' の場合は LP1, LP2, LP3 の TenorTypeID
    BandID INT NOT NULL, -- M_HoldingBandへの参照
    Value DECIMAL(18,0) NULL, -- そのバンドに属するポジションの量または割合（レポートによる）
    LastUpdated DATETIME2(0) NOT NULL DEFAULT GETDATE(),
    CONSTRAINT PK_T_BandingReport PRIMARY KEY CLUSTERED (BandingID),
    CONSTRAINT UQ_T_BandingReport UNIQUE (ReportDate, MetalID, ReportType, TenorTypeID, BandID),
    CONSTRAINT FK_T_BandingReport_MetalID FOREIGN KEY (MetalID) REFERENCES dbo.M_Metal (MetalID),
    CONSTRAINT FK_T_BandingReport_TenorTypeID FOREIGN KEY (TenorTypeID) REFERENCES dbo.M_TenorType (TenorTypeID),
    CONSTRAINT FK_T_BandingReport_BandID FOREIGN KEY (BandID) REFERENCES dbo.M_HoldingBand (BandID)
);
GO
CREATE NONCLUSTERED INDEX IX_T_BandingReport_MetalDateType ON dbo.T_BandingReport (MetalID, ReportDate, ReportType);
GO

-- T_CompanyStockPrice (企業株価データ)
IF OBJECT_ID('dbo.T_CompanyStockPrice', 'U') IS NOT NULL
    DROP TABLE dbo.T_CompanyStockPrice;
CREATE TABLE dbo.T_CompanyStockPrice (
    CompanyPriceID BIGINT IDENTITY(1,1) NOT NULL,
    TradeDate DATE NOT NULL,
    CompanyTicker NVARCHAR(20) NOT NULL, -- Bloombergティッカー (e.g., 'GLEN LN Equity')
    OpenPrice DECIMAL(18,4) NULL,
    HighPrice DECIMAL(18,4) NULL,
    LowPrice DECIMAL(18,4) NULL,
    LastPrice DECIMAL(18,4) NULL,
    Volume BIGINT NULL,
    LastUpdated DATETIME2(0) NOT NULL DEFAULT GETDATE(),
    CONSTRAINT PK_T_CompanyStockPrice PRIMARY KEY CLUSTERED (CompanyPriceID),
    CONSTRAINT UQ_T_CompanyStockPrice UNIQUE (TradeDate, CompanyTicker)
);
GO
CREATE NONCLUSTERED INDEX IX_T_CompanyStockPrice_TickerDate ON dbo.T_CompanyStockPrice (CompanyTicker, TradeDate);
GO