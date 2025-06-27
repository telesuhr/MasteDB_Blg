"""
データベース接続設定
"""
import os
from typing import Dict

# Azure SQL Database接続情報
DATABASE_CONFIG: Dict[str, str] = {
    'server': 'jcz.database.windows.net',
    'database': 'JCL',
    'username': 'TKJCZ01',
    'password': 'P@ssw0rdmbkazuresql',
    'driver': '{ODBC Driver 18 for SQL Server}',
    'timeout': '30'
}

def get_connection_string() -> str:
    """
    SQL Server接続文字列を生成
    
    Returns:
        str: pyodbc用の接続文字列
    """
    return (
        f"DRIVER={DATABASE_CONFIG['driver']};"
        f"SERVER={DATABASE_CONFIG['server']};"
        f"DATABASE={DATABASE_CONFIG['database']};"
        f"UID={DATABASE_CONFIG['username']};"
        f"PWD={DATABASE_CONFIG['password']};"
        f"Encrypt=yes;"
        f"TrustServerCertificate=no;"
        f"Connection Timeout={DATABASE_CONFIG['timeout']};"
    )

# テーブル名定義
TABLES = {
    'commodity_price': 'T_CommodityPrice',
    'lme_inventory': 'T_LMEInventory',
    'other_inventory': 'T_OtherExchangeInventory',
    'market_indicator': 'T_MarketIndicator',
    'macro_indicator': 'T_MacroEconomicIndicator',
    'cotr': 'T_COTR',
    'banding_report': 'T_BandingReport',
    'company_stock': 'T_CompanyStockPrice',
    # マスタテーブル
    'metal': 'M_Metal',
    'tenor_type': 'M_TenorType',
    'indicator': 'M_Indicator',
    'region': 'M_Region',
    'cotr_category': 'M_COTRCategory',
    'holding_band': 'M_HoldingBand'
}

# バッチサイズ設定
BATCH_SIZE = 1000

# リトライ設定
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds