"""
データベース接続設定
"""
import os
from typing import Dict
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Azure SQL Database接続情報
DATABASE_CONFIG: Dict[str, str] = {
    'server': os.getenv('DB_SERVER', 'jcz.database.windows.net'),
    'database': os.getenv('DB_DATABASE', 'JCL'),
    'username': os.getenv('DB_USERNAME', 'TKJCZ01'),
    'password': os.getenv('DB_PASSWORD', ''),
    'driver': '{ODBC Driver 17 for SQL Server}',
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

# テーブルマッピング（テーブル名 → プロセッサメソッド名）
TABLE_MAPPINGS = {
    'T_CommodityPrice': 'process_commodity_prices',
    'T_CommodityPrice_V2': 'process_commodity_prices',
    'T_LMEInventory': 'process_lme_inventory',
    'T_OtherExchangeInventory': 'process_other_inventory',
    'T_MarketIndicator': 'process_market_indicators',
    'T_MacroEconomicIndicator': 'process_macro_indicators',
    'T_COTR': 'process_cotr_data',
    'T_BandingReport': 'process_banding_data',
    'T_CompanyStockPrice': 'process_company_prices'
}

# テーブルごとのユニークキー定義
TABLE_UNIQUE_KEYS = {
    'T_CommodityPrice': ['TradeDate', 'MetalID', 'DataType', 'GenericID', 'ActualContractID'],
    'T_CommodityPrice_V2': ['TradeDate', 'DataType', 'GenericID', 'ActualContractID'],
    'T_LMEInventory': ['TradeDate', 'MetalID', 'RegionID'],
    'T_OtherExchangeInventory': ['TradeDate', 'MetalID', 'Exchange'],
    'T_MarketIndicator': ['TradeDate', 'IndicatorID'],
    'T_MacroEconomicIndicator': ['TradeDate', 'IndicatorID'],
    'T_COTR': ['TradeDate', 'MetalID', 'COTRCategoryID'],
    'T_BandingReport': ['TradeDate', 'MetalID', 'HoldingBandID'],
    'T_CompanyStockPrice': ['TradeDate', 'CompanyTicker']
}

# バッチサイズ設定
BATCH_SIZE = 1000

# リトライ設定
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds