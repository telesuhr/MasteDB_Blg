"""
SQL Serverデータベース接続・操作モジュール
"""
import pyodbc
import pandas as pd
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime
import time
import sys
import os
from contextlib import contextmanager

# プロジェクトルートとsrcディレクトリをPythonパスに追加
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
src_dir = os.path.join(project_root, 'src')
sys.path.insert(0, project_root)
sys.path.insert(0, src_dir)

from config.database_config import (
    get_connection_string, TABLES, BATCH_SIZE, MAX_RETRIES, RETRY_DELAY
)
from config.logging_config import logger


class DatabaseManager:
    """データベース接続・操作を管理するクラス"""
    
    def __init__(self):
        self.connection_string = get_connection_string()
        self.connection = None
        self.master_data = {}
        
    @contextmanager
    def get_connection(self):
        """コンテキストマネージャーを使用したデータベース接続"""
        conn = None
        try:
            conn = pyodbc.connect(self.connection_string)
            yield conn
        except Exception as e:
            logger.error(f"Database connection error: {e}")
            raise
        finally:
            if conn:
                conn.close()
                
    def connect(self):
        """データベースに接続"""
        try:
            self.connection = pyodbc.connect(self.connection_string)
            logger.info("Successfully connected to SQL Server database")
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise
            
    def disconnect(self):
        """データベース接続を切断"""
        if self.connection:
            self.connection.close()
            logger.info("Disconnected from database")
            
    def load_master_data(self):
        """マスタデータをメモリにロード"""
        try:
            with self.get_connection() as conn:
                # M_Metal
                query = "SELECT MetalID, MetalCode, MetalName FROM M_Metal"
                metals_df = pd.read_sql(query, conn)
                self.master_data['metals'] = dict(zip(metals_df['MetalCode'], metals_df['MetalID']))
                
                # M_TenorType
                query = "SELECT TenorTypeID, TenorTypeName FROM M_TenorType"
                tenor_df = pd.read_sql(query, conn)
                self.master_data['tenor_types'] = dict(zip(tenor_df['TenorTypeName'], tenor_df['TenorTypeID']))
                
                # M_Indicator
                query = "SELECT IndicatorID, IndicatorCode FROM M_Indicator"
                indicator_df = pd.read_sql(query, conn)
                self.master_data['indicators'] = dict(zip(indicator_df['IndicatorCode'], indicator_df['IndicatorID']))
                
                # M_Region
                query = "SELECT RegionID, RegionCode FROM M_Region"
                region_df = pd.read_sql(query, conn)
                self.master_data['regions'] = dict(zip(region_df['RegionCode'], region_df['RegionID']))
                
                # M_COTRCategory
                query = "SELECT COTRCategoryID, CategoryName FROM M_COTRCategory"
                cotr_df = pd.read_sql(query, conn)
                self.master_data['cotr_categories'] = dict(zip(cotr_df['CategoryName'], cotr_df['COTRCategoryID']))
                
                # M_HoldingBand
                query = "SELECT BandID, BandRange FROM M_HoldingBand"
                band_df = pd.read_sql(query, conn)
                self.master_data['holding_bands'] = dict(zip(band_df['BandRange'], band_df['BandID']))
                
                logger.info("Master data loaded successfully")
                logger.debug(f"Loaded {len(self.master_data['metals'])} metals, "
                           f"{len(self.master_data['tenor_types'])} tenor types, "
                           f"{len(self.master_data['indicators'])} indicators, "
                           f"{len(self.master_data['regions'])} regions")
                
        except Exception as e:
            logger.error(f"Failed to load master data: {e}")
            raise
            
    def get_or_create_master_id(self, category: str, code: str, name: Optional[str] = None, 
                               additional_fields: Optional[Dict] = None) -> int:
        """
        マスタデータのIDを取得、存在しない場合は作成
        
        Args:
            category: マスタデータのカテゴリ（'metals', 'indicators'など）
            code: コード値
            name: 名前（新規作成時に必要）
            additional_fields: 追加フィールド（新規作成時）
            
        Returns:
            int: マスタデータのID
        """
        # 既存のIDを返す
        if code in self.master_data.get(category, {}):
            return self.master_data[category][code]
            
        # 新規作成
        table_mapping = {
            'metals': 'M_Metal',
            'tenor_types': 'M_TenorType',
            'indicators': 'M_Indicator',
            'regions': 'M_Region',
            'cotr_categories': 'M_COTRCategory',
            'holding_bands': 'M_HoldingBand'
        }
        
        field_mapping = {
            'metals': ('MetalCode', 'MetalName'),
            'tenor_types': ('TenorTypeName', 'Description'),
            'indicators': ('IndicatorCode', 'IndicatorName'),
            'regions': ('RegionCode', 'RegionName'),
            'cotr_categories': ('CategoryName', 'Description'),
            'holding_bands': ('BandRange', 'MinValue')
        }
        
        if category not in table_mapping:
            raise ValueError(f"Unknown master data category: {category}")
            
        table_name = table_mapping[category]
        code_field, name_field = field_mapping[category][:2]
        
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            # 既に存在するかチェック（別セッションで作成された可能性）
            check_query = f"SELECT {code_field.replace('Name', 'ID').replace('Code', 'ID').replace('Range', 'ID')} " \
                         f"FROM {table_name} WHERE {code_field} = ?"
            cursor.execute(check_query, code)
            result = cursor.fetchone()
            
            if result:
                new_id = result[0]
                self.master_data[category][code] = new_id
                return new_id
                
            # 新規挿入
            if name is None:
                name = code  # 名前が提供されない場合はコードを使用
                
            insert_fields = [code_field, name_field]
            insert_values = [code, name]
            
            if additional_fields:
                insert_fields.extend(additional_fields.keys())
                insert_values.extend(additional_fields.values())
                
            insert_query = f"INSERT INTO {table_name} ({', '.join(insert_fields)}) " \
                          f"OUTPUT INSERTED.{code_field.replace('Name', 'ID').replace('Code', 'ID').replace('Range', 'ID')} " \
                          f"VALUES ({', '.join(['?'] * len(insert_values))})"
            
            cursor.execute(insert_query, insert_values)
            new_id = cursor.fetchone()[0]
            conn.commit()
            
            self.master_data[category][code] = new_id
            logger.info(f"Created new {category} entry: {code} with ID {new_id}")
            
            return new_id
            
    def upsert_dataframe(self, df: pd.DataFrame, table_name: str, 
                        unique_columns: List[str], retry_count: int = 0) -> int:
        """
        DataFrameをテーブルにUPSERT（存在する場合は更新、なければ挿入）
        
        Args:
            df: 挿入/更新するデータフレーム
            table_name: テーブル名
            unique_columns: ユニークキーとなるカラムのリスト
            retry_count: リトライ回数
            
        Returns:
            int: 処理された行数
        """
        if df.empty:
            logger.warning(f"Empty dataframe provided for table {table_name}")
            return 0
            
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                processed_count = 0
                
                # バッチ処理
                for i in range(0, len(df), BATCH_SIZE):
                    batch_df = df.iloc[i:i + BATCH_SIZE]
                    
                    # MERGEステートメントを構築
                    merge_query = self._build_merge_query(table_name, batch_df.columns.tolist(), 
                                                         unique_columns)
                    
                    # バッチデータを処理
                    for _, row in batch_df.iterrows():
                        values = [row[col] if pd.notna(row[col]) else None 
                                 for col in batch_df.columns]
                        cursor.execute(merge_query, values * 2)  # MERGEには値が2回必要
                        processed_count += cursor.rowcount
                        
                    conn.commit()
                    logger.debug(f"Processed batch {i//BATCH_SIZE + 1} for table {table_name}")
                    
                logger.info(f"Successfully upserted {processed_count} rows to {table_name}")
                return processed_count
                
        except Exception as e:
            logger.error(f"Error upserting data to {table_name}: {e}")
            
            if retry_count < MAX_RETRIES:
                logger.info(f"Retrying... (attempt {retry_count + 1}/{MAX_RETRIES})")
                time.sleep(RETRY_DELAY)
                return self.upsert_dataframe(df, table_name, unique_columns, retry_count + 1)
            else:
                raise
                
    def _build_merge_query(self, table_name: str, columns: List[str], 
                          unique_columns: List[str]) -> str:
        """
        MERGEクエリを構築
        
        Args:
            table_name: テーブル名
            columns: カラムリスト
            unique_columns: ユニークキーカラムリスト
            
        Returns:
            str: MERGEクエリ
        """
        # ソーステーブルの値プレースホルダー
        source_values = ', '.join(['?' for _ in columns])
        source_columns = ', '.join(columns)
        
        # JOIN条件
        join_conditions = ' AND '.join([f"target.{col} = source.{col}" 
                                       for col in unique_columns])
        
        # UPDATE句（LastUpdated以外）
        update_columns = [col for col in columns 
                         if col not in unique_columns and col != 'LastUpdated']
        update_clause = ', '.join([f"target.{col} = source.{col}" 
                                   for col in update_columns])
        
        # INSERT句
        insert_columns = ', '.join(columns)
        insert_values = ', '.join([f"source.{col}" for col in columns])
        
        merge_query = f"""
        MERGE {table_name} AS target
        USING (SELECT {', '.join([f'? AS {col}' for col in columns])}) AS source
        ON {join_conditions}
        WHEN MATCHED THEN
            UPDATE SET {update_clause}, LastUpdated = GETDATE()
        WHEN NOT MATCHED THEN
            INSERT ({insert_columns}, LastUpdated)
            VALUES ({insert_values}, GETDATE());
        """
        
        return merge_query
        
    def execute_query(self, query: str, params: Optional[List] = None) -> pd.DataFrame:
        """
        クエリを実行してDataFrameを返す
        
        Args:
            query: SQLクエリ
            params: パラメータリスト
            
        Returns:
            pd.DataFrame: クエリ結果
        """
        with self.get_connection() as conn:
            if params:
                return pd.read_sql(query, conn, params=params)
            else:
                return pd.read_sql(query, conn)
                
    def get_latest_date(self, table_name: str, date_column: str, 
                       where_clause: Optional[str] = None) -> Optional[datetime]:
        """
        テーブルの最新日付を取得
        
        Args:
            table_name: テーブル名
            date_column: 日付カラム名
            where_clause: WHERE句（オプション）
            
        Returns:
            datetime: 最新日付（データがない場合はNone）
        """
        query = f"SELECT MAX({date_column}) as latest_date FROM {table_name}"
        if where_clause:
            query += f" WHERE {where_clause}"
            
        try:
            result = self.execute_query(query)
            if not result.empty and result['latest_date'][0] is not None:
                return pd.to_datetime(result['latest_date'][0])
            return None
        except Exception as e:
            logger.error(f"Error getting latest date from {table_name}: {e}")
            return None