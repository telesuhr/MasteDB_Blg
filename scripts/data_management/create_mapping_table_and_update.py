"""
T_GenericContractMappingテーブルを作成し、マッピングを更新するスクリプト
"""
import sys
import os
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.bloomberg_api import BloombergDataFetcher
from src.database import DatabaseManager
from config.logging_config import logger
import pandas as pd

def create_mapping_table():
    """T_GenericContractMappingテーブルを作成"""
    db_manager = DatabaseManager()
    db_manager.connect()
    
    try:
        with db_manager.get_connection() as conn:
            cursor = conn.cursor()
            
            # テーブルが存在しない場合は作成
            cursor.execute("""
            IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[T_GenericContractMapping]') AND type in (N'U'))
            BEGIN
                CREATE TABLE dbo.T_GenericContractMapping (
                    MappingID BIGINT IDENTITY(1,1) NOT NULL,
                    MappingDate DATE NOT NULL,
                    GenericID INT NOT NULL,
                    ActualContractID INT NULL,
                    DaysToExpiry INT NULL,
                    LastUpdated DATETIME2(0) NOT NULL DEFAULT GETDATE(),
                    CONSTRAINT PK_T_GenericContractMapping PRIMARY KEY CLUSTERED (MappingID),
                    CONSTRAINT UQ_T_GenericContractMapping UNIQUE (MappingDate, GenericID),
                    CONSTRAINT FK_T_GenericContractMapping_GenericID FOREIGN KEY (GenericID) REFERENCES dbo.M_GenericFutures (GenericID),
                    CONSTRAINT FK_T_GenericContractMapping_ActualContractID FOREIGN KEY (ActualContractID) REFERENCES dbo.M_ActualContract (ActualContractID)
                );
                
                CREATE NONCLUSTERED INDEX IX_T_GenericContractMapping_Date ON dbo.T_GenericContractMapping (MappingDate);
                CREATE NONCLUSTERED INDEX IX_T_GenericContractMapping_Generic ON dbo.T_GenericContractMapping (GenericID);
            END
            """)
            conn.commit()
            logger.info("T_GenericContractMapping table created/verified")
            
    finally:
        db_manager.disconnect()

def update_actual_contracts():
    """Bloomberg APIから取得した実契約情報をM_ActualContractに登録"""
    bloomberg = BloombergDataFetcher()
    db_manager = DatabaseManager()
    
    if not bloomberg.connect():
        logger.error("Failed to connect to Bloomberg API")
        return
    
    db_manager.connect()
    
    try:
        with db_manager.get_connection() as conn:
            # 全Generic先物を取得
            query = """
            SELECT GenericID, GenericTicker, ExchangeCode, MetalID
            FROM M_GenericFutures
            WHERE IsActive = 1
            ORDER BY ExchangeCode, GenericNumber
            """
            generics = pd.read_sql(query, conn)
            
            # 今日のマッピングを作成
            today = datetime.now().date()
            
            for _, row in generics.iterrows():
                generic_ticker = row['GenericTicker']
                exchange = row['ExchangeCode']
                metal_id = row['MetalID']
                generic_id = row['GenericID']
                
                try:
                    # Bloomberg APIから実契約情報を取得
                    ref_data = bloomberg.get_reference_data(
                        [generic_ticker], 
                        ['FUT_CUR_GEN_TICKER', 'LAST_TRADEABLE_DT', 'FUT_MONTH_YR']
                    )
                    
                    if not ref_data.empty:
                        actual_ticker = ref_data.iloc[0].get('FUT_CUR_GEN_TICKER')
                        last_tradeable = ref_data.iloc[0].get('LAST_TRADEABLE_DT')
                        
                        if actual_ticker and pd.notna(actual_ticker):
                            # Bloombergフォーマットを修正（必要に応じて）
                            if exchange == 'COMEX' and actual_ticker.startswith('HG') and len(actual_ticker) == 4:
                                actual_ticker = actual_ticker + ' Comdty'
                            elif exchange == 'SHFE' and actual_ticker.startswith('CU') and len(actual_ticker) == 4:
                                actual_ticker = actual_ticker + ' Comdty'
                            
                            logger.info(f"{generic_ticker} -> {actual_ticker}")
                            
                            cursor = conn.cursor()
                            
                            # M_ActualContractに登録
                            cursor.execute("""
                                IF NOT EXISTS (SELECT 1 FROM M_ActualContract WHERE ContractTicker = ?)
                                BEGIN
                                    INSERT INTO M_ActualContract 
                                    (ContractTicker, MetalID, ExchangeCode, LastTradeableDate, IsActive, CreatedDate)
                                    VALUES (?, ?, ?, ?, 1, GETDATE())
                                END
                                ELSE
                                BEGIN
                                    UPDATE M_ActualContract
                                    SET LastTradeableDate = ?
                                    WHERE ContractTicker = ?
                                END
                            """, (actual_ticker, actual_ticker, metal_id, exchange, last_tradeable, last_tradeable, actual_ticker))
                            
                            # ActualContractIDを取得
                            cursor.execute("SELECT ActualContractID FROM M_ActualContract WHERE ContractTicker = ?", (actual_ticker,))
                            actual_contract_id = cursor.fetchone()[0]
                            
                            # 満期までの日数を計算
                            days_to_expiry = None
                            if last_tradeable and pd.notna(last_tradeable):
                                try:
                                    # datetimeオブジェクトの場合はdateに変換
                                    if hasattr(last_tradeable, 'date'):
                                        last_tradeable_date = last_tradeable.date()
                                    else:
                                        last_tradeable_date = last_tradeable
                                    days_to_expiry = (last_tradeable_date - today).days
                                except:
                                    pass
                            
                            # T_GenericContractMappingに登録
                            cursor.execute("""
                                MERGE T_GenericContractMapping AS target
                                USING (SELECT ? AS MappingDate, ? AS GenericID, ? AS ActualContractID, ? AS DaysToExpiry) AS source
                                ON target.MappingDate = source.MappingDate AND target.GenericID = source.GenericID
                                WHEN MATCHED THEN
                                    UPDATE SET ActualContractID = source.ActualContractID, 
                                               DaysToExpiry = source.DaysToExpiry,
                                               LastUpdated = GETDATE()
                                WHEN NOT MATCHED THEN
                                    INSERT (MappingDate, GenericID, ActualContractID, DaysToExpiry)
                                    VALUES (source.MappingDate, source.GenericID, source.ActualContractID, source.DaysToExpiry);
                            """, (today, generic_id, actual_contract_id, days_to_expiry))
                            
                            conn.commit()
                            
                except Exception as e:
                    logger.warning(f"Failed to process {generic_ticker}: {e}")
                    
    finally:
        bloomberg.disconnect()
        db_manager.disconnect()

def verify_mapping():
    """マッピング結果を確認"""
    db_manager = DatabaseManager()
    db_manager.connect()
    
    try:
        with db_manager.get_connection() as conn:
            query = """
            SELECT 
                gf.ExchangeCode,
                gf.GenericTicker,
                ac.ContractTicker as ActualContract,
                gm.DaysToExpiry,
                gm.MappingDate
            FROM T_GenericContractMapping gm
            JOIN M_GenericFutures gf ON gm.GenericID = gf.GenericID
            LEFT JOIN M_ActualContract ac ON gm.ActualContractID = ac.ActualContractID
            WHERE gm.MappingDate = CAST(GETDATE() as DATE)
            ORDER BY gf.ExchangeCode, gf.GenericNumber
            """
            
            result = pd.read_sql(query, conn)
            print("\nToday's Generic-Actual Contract Mappings:")
            print(result)
            
    finally:
        db_manager.disconnect()

def main():
    """メイン処理"""
    logger.info("=== Starting mapping table creation and update ===")
    
    # 1. マッピングテーブルを作成
    create_mapping_table()
    
    # 2. 実契約情報を更新してマッピング
    update_actual_contracts()
    
    # 3. 結果を確認
    verify_mapping()
    
    logger.info("=== Mapping update completed ===")

if __name__ == "__main__":
    main()