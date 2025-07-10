"""
可視化システム用データベース設定（新規）
"""
import os
from typing import Dict, Any

# SQL Server接続設定 - メインプロジェクトと同じ設定
DATABASE_CONFIG = {
    'driver': '{ODBC Driver 17 for SQL Server}',
    'server': 'jcz.database.windows.net',  # 正しいサーバー名
    'database': 'JCL',
    'username': 'TKJCZ01',
    'password': 'P@ssw0rdmbkazuresql',
    'timeout': '30'
}

def get_connection_string() -> str:
    """SQL Server接続文字列を取得"""
    connection_string = (
        f"DRIVER={DATABASE_CONFIG['driver']};"
        f"SERVER={DATABASE_CONFIG['server']};"
        f"DATABASE={DATABASE_CONFIG['database']};"
        f"UID={DATABASE_CONFIG['username']};"
        f"PWD={DATABASE_CONFIG['password']};"
        f"Encrypt=yes;"
        f"TrustServerCertificate=no;"
        f"Connection Timeout={DATABASE_CONFIG['timeout']};"
    )
    
    # デバッグ用：接続情報を確認
    print(f"DEBUG: Server: {DATABASE_CONFIG['server']}")
    print(f"DEBUG: Database: {DATABASE_CONFIG['database']}")
    print(f"DEBUG: User: {DATABASE_CONFIG['username']}")
    
    return connection_string

# テーブル名定義（メインプロジェクトと同じ）
TABLES = {
    'commodity_prices': 'T_CommodityPrice',
    'lme_inventory': 'T_LMEInventory',
    'other_inventory': 'T_OtherExchangeInventory',
    'market_indicators': 'T_MarketIndicator',
    'macro_indicators': 'T_MacroEconomicIndicator',
    'cotr': 'T_COTR',
    'banding': 'T_BandingReport',
    'company_stocks': 'T_CompanyStockPrice'
}

# マスターテーブル定義
MASTER_TABLES = {
    'metals': 'M_Metal',
    'tenor_types': 'M_TenorType',
    'indicators': 'M_Indicator',
    'regions': 'M_Region',
    'cotr_categories': 'M_COTRCategory',
    'holding_bands': 'M_HoldingBand'
}

# 可視化設定
VISUALIZATION_CONFIG = {
    'figure_size': (12, 6),
    'date_format': '%Y-%m-%d',
    'color_scheme': {
        'lme': '#1f77b4',
        'shfe': '#ff7f0e',
        'cmx': '#2ca02c',
        'inventory': '#d62728',
        'spread': '#9467bd'
    },
    'output_dir': '../outputs'
}