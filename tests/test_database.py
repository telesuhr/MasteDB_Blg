"""
データベース接続のテスト
"""
import pytest
import pandas as pd
from datetime import datetime, date
import sys
import os

# テスト用のパスを追加
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from database import DatabaseManager


class TestDatabaseManager:
    """DatabaseManagerのテストクラス"""
    
    @pytest.fixture
    def db_manager(self):
        """テスト用のデータベースマネージャー"""
        return DatabaseManager()
        
    def test_connection_string_generation(self, db_manager):
        """接続文字列の生成テスト"""
        conn_str = db_manager.connection_string
        assert 'jcz.database.windows.net' in conn_str
        assert 'JCL' in conn_str
        assert 'TKJCZ01' in conn_str
        
    def test_master_data_structure(self, db_manager):
        """マスタデータ構造のテスト"""
        # 初期化されていることを確認
        assert hasattr(db_manager, 'master_data')
        assert isinstance(db_manager.master_data, dict)
        
    def test_unique_columns_mapping(self, db_manager):
        """ユニークカラムマッピングのテスト"""
        from main import BloombergSQLIngestor
        
        ingestor = BloombergSQLIngestor()
        
        # 各テーブルのユニークカラムが定義されていることを確認
        unique_cols = ingestor._get_unique_columns('T_CommodityPrice')
        assert 'TradeDate' in unique_cols
        assert 'MetalID' in unique_cols
        
        unique_cols = ingestor._get_unique_columns('T_LMEInventory')
        assert 'ReportDate' in unique_cols
        assert 'MetalID' in unique_cols
        assert 'RegionID' in unique_cols