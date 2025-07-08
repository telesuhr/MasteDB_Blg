-- ###########################################################
-- Phase 3: 新しい価格データテーブル T_CommodityPrice_V2 作成
-- ジェネリック先物と実契約の両方に対応
-- ###########################################################

-- データベースの選択
USE [JCL];
GO

-- ###########################################################
-- T_CommodityPrice_V2 (新しい価格データテーブル)
-- ###########################################################

-- 既存テーブルがあれば削除
IF OBJECT_ID('dbo.T_CommodityPrice_V2', 'U') IS NOT NULL
    DROP TABLE dbo.T_CommodityPrice_V2;

CREATE TABLE dbo.T_CommodityPrice_V2 (
    PriceID BIGINT IDENTITY(1,1) NOT NULL,
    TradeDate DATE NOT NULL,
    MetalID INT NOT NULL,                       -- M_Metalへの外部キー
    DataType NVARCHAR(10) NOT NULL,             -- 'Generic' or 'Actual'
    GenericID INT NULL,                         -- ジェネリック先物の場合 (M_GenericFuturesへのFK)
    ActualContractID INT NULL,                  -- 実契約の場合 (M_ActualContractへのFK)
    SettlementPrice DECIMAL(18,4) NULL,        -- 決済価格
    OpenPrice DECIMAL(18,4) NULL,              -- 始値
    HighPrice DECIMAL(18,4) NULL,              -- 高値
    LowPrice DECIMAL(18,4) NULL,               -- 安値
    LastPrice DECIMAL(18,4) NULL,              -- 終値
    Volume BIGINT NULL,                         -- 出来高
    OpenInterest BIGINT NULL,                   -- 建玉残高
    LastUpdated DATETIME2(0) NOT NULL DEFAULT GETDATE(),
    
    -- 制約定義
    CONSTRAINT PK_T_CommodityPrice_V2 PRIMARY KEY CLUSTERED (PriceID),
    
    -- データタイプに応じた制約
    CONSTRAINT CHK_T_CommodityPrice_V2_DataType CHECK (
        (DataType = 'Generic' AND GenericID IS NOT NULL AND ActualContractID IS NULL) OR
        (DataType = 'Actual' AND GenericID IS NULL AND ActualContractID IS NOT NULL)
    ),
    
    -- 外部キー制約
    CONSTRAINT FK_T_CommodityPrice_V2_MetalID 
        FOREIGN KEY (MetalID) REFERENCES dbo.M_Metal (MetalID),
    CONSTRAINT FK_T_CommodityPrice_V2_GenericID 
        FOREIGN KEY (GenericID) REFERENCES dbo.M_GenericFutures (GenericID),
    CONSTRAINT FK_T_CommodityPrice_V2_ActualContractID 
        FOREIGN KEY (ActualContractID) REFERENCES dbo.M_ActualContract (ActualContractID)
);
GO

-- ユニーク制約（重複防止）
-- ジェネリック先物の場合
CREATE UNIQUE NONCLUSTERED INDEX UQ_T_CommodityPrice_V2_Generic 
    ON dbo.T_CommodityPrice_V2 (TradeDate, GenericID)
    WHERE DataType = 'Generic';

-- 実契約の場合
CREATE UNIQUE NONCLUSTERED INDEX UQ_T_CommodityPrice_V2_Actual 
    ON dbo.T_CommodityPrice_V2 (TradeDate, ActualContractID)
    WHERE DataType = 'Actual';

-- パフォーマンス用インデックス
CREATE NONCLUSTERED INDEX IX_T_CommodityPrice_V2_TradeDate 
    ON dbo.T_CommodityPrice_V2 (TradeDate, MetalID, DataType);

CREATE NONCLUSTERED INDEX IX_T_CommodityPrice_V2_Generic 
    ON dbo.T_CommodityPrice_V2 (GenericID, TradeDate)
    WHERE DataType = 'Generic';

CREATE NONCLUSTERED INDEX IX_T_CommodityPrice_V2_Actual 
    ON dbo.T_CommodityPrice_V2 (ActualContractID, TradeDate)
    WHERE DataType = 'Actual';

GO

-- ###########################################################
-- テーブル作成確認
-- ###########################################################

PRINT 'T_CommodityPrice_V2 テーブル作成完了';
PRINT '制約とインデックス:';
PRINT '- CHK_DataType: Generic/Actual の排他制御';
PRINT '- UQ_Generic: ジェネリック先物の重複防止';  
PRINT '- UQ_Actual: 実契約の重複防止';
PRINT '- IX_TradeDate: 日付・メタル・タイプ検索用';
PRINT '- IX_Generic: ジェネリック検索用';
PRINT '- IX_Actual: 実契約検索用';

-- テーブル構造確認
SELECT 
    COLUMN_NAME,
    DATA_TYPE,
    IS_NULLABLE,
    COLUMN_DEFAULT
FROM INFORMATION_SCHEMA.COLUMNS 
WHERE TABLE_NAME = 'T_CommodityPrice_V2'
ORDER BY ORDINAL_POSITION;

GO