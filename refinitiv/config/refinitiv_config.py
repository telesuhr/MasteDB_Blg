"""
Refinitiv EIKON Data API設定
"""
import os
from datetime import datetime, timedelta

# Refinitiv API設定
REFINITIV_API_KEY = os.getenv('REFINITIV_API_KEY', 'YOUR_API_KEY_HERE')

# データ取得期間設定
INITIAL_LOAD_PERIODS = {
    'prices': 5,      # 年
    'inventory': 5,   # 年  
    'indicators': 5,  # 年
    'macro': 10,      # 年
    'cotr': 5,        # 年
    'banding': 3,     # 年
    'stocks': 5       # 年
}

# Refinitiv フィールド定義
REFINITIV_FIELDS = {
    'price_fields': ['TR.PriceClose', 'TR.PriceOpen', 'TR.PriceHigh', 'TR.PriceLow', 'TR.Volume', 'TR.OpenInterest'],
    'inventory_fields': ['TR.InventoryTotal', 'TR.InventoryOnWarrant', 'TR.InventoryFlow'],
    'indicator_fields': ['TR.PriceClose'],
    'stock_fields': ['TR.PriceClose', 'TR.PriceOpen', 'TR.PriceHigh', 'TR.PriceLow', 'TR.Volume'],
    'macro_fields': ['TR.EconomicIndicatorValue']
}

# RIC (Reuters Instrument Code) 定義
REFINITIV_RICS = {
    # LME 銅価格
    'LME_COPPER_PRICES': {
        'rics': [
            'CMCU3',           # LME Copper Cash
            'CMCU03',          # LME Copper 3M
            'CMCU1',           # LME Copper Generic 1st
            'CMCU2',           # LME Copper Generic 2nd
            'CMCU3',           # LME Copper Generic 3rd
            'CMCU4',           # LME Copper Generic 4th
            'CMCU5',           # LME Copper Generic 5th
            'CMCU6',           # LME Copper Generic 6th
        ],
        'fields': REFINITIV_FIELDS['price_fields'],
        'table': 'T_CommodityPrice',
        'metal': 'COPPER',
        'exchange': 'LME',
        'tenor_mapping': {
            'CMCU3': 'Cash',
            'CMCU03': '3M Futures',
            'CMCU1': 'Generic 1st Future',
            'CMCU2': 'Generic 2nd Future',
            'CMCU3': 'Generic 3rd Future',
            'CMCU4': 'Generic 4th Future',
            'CMCU5': 'Generic 5th Future',
            'CMCU6': 'Generic 6th Future',
        }
    },
    
    # SHFE 銅価格
    'SHFE_COPPER_PRICES': {
        'rics': [f'CUc{i}' for i in range(1, 13)],  # CUc1-CUc12
        'fields': REFINITIV_FIELDS['price_fields'],
        'table': 'T_CommodityPrice',
        'metal': 'COPPER',
        'exchange': 'SHFE',
        'tenor_mapping': {
            f'CUc{i}': f'Generic {i}st Future' if i == 1
            else f'Generic {i}nd Future' if i == 2
            else f'Generic {i}rd Future' if i == 3
            else f'Generic {i}th Future' for i in range(1, 13)
        }
    },
    
    # CMX 銅価格
    'CMX_COPPER_PRICES': {
        'rics': [f'HGc{i}' for i in range(1, 13)],  # HGc1-HGc12
        'fields': REFINITIV_FIELDS['price_fields'],
        'table': 'T_CommodityPrice',
        'metal': 'COPPER',
        'exchange': 'CMX',
        'tenor_mapping': {
            f'HGc{i}': f'Generic {i}st Future' if i == 1
            else f'Generic {i}nd Future' if i == 2
            else f'Generic {i}rd Future' if i == 3
            else f'Generic {i}th Future' for i in range(1, 13)
        }
    },
    
    # LME 在庫データ
    'LME_INVENTORY': {
        'rics': [
            'LMCASTK',         # LME Copper Total Stock
            'LMCAWRT',         # LME Copper On Warrant
            # 注意: 地域別在庫は別途調査が必要
        ],
        'fields': REFINITIV_FIELDS['inventory_fields'],
        'table': 'T_LMEInventory',
        'metal': 'COPPER'
    },
    
    # 金利
    'INTEREST_RATES': {
        'rics': [
            'USSOFR=',         # US SOFR Rate
            'USD1ML=',         # USD 1M LIBOR
            'USD3ML=',         # USD 3M LIBOR
        ],
        'fields': REFINITIV_FIELDS['indicator_fields'],
        'table': 'T_MarketIndicator',
        'category': 'Interest Rate'
    },
    
    # 為替レート
    'FX_RATES': {
        'rics': [
            'JPY=',            # USD/JPY
            'EUR=',            # EUR/USD
            'CNY=',            # USD/CNY
        ],
        'fields': REFINITIV_FIELDS['indicator_fields'],
        'table': 'T_MarketIndicator',
        'category': 'FX'
    },
    
    # コモディティ指数
    'COMMODITY_INDICES': {
        'rics': [
            '.SPBCOM',         # Bloomberg Commodity Index (代替)
            '.SPGSCI',         # S&P GSCI
        ],
        'fields': REFINITIV_FIELDS['indicator_fields'],
        'table': 'T_MarketIndicator',
        'category': 'Commodity Index'
    },
    
    # 株価指数
    'EQUITY_INDICES': {
        'rics': [
            '.SPX',            # S&P 500
            '.N225',           # Nikkei 225
            '.SSEC',           # Shanghai Composite
        ],
        'fields': REFINITIV_FIELDS['indicator_fields'],
        'table': 'T_MarketIndicator',
        'category': 'Equity Index'
    },
    
    # エネルギー価格
    'ENERGY_PRICES': {
        'rics': [
            'CLc1',            # WTI Crude Oil
            'LCOc1',           # Brent Crude Oil
            'NGc1',            # Natural Gas
        ],
        'fields': REFINITIV_FIELDS['indicator_fields'],
        'table': 'T_MarketIndicator',
        'category': 'Energy'
    },
    
    # 企業株価
    'COMPANY_STOCKS': {
        'rics': [
            'GLEN.L',          # Glencore
            'FCX.N',           # Freeport-McMoRan
            'SCCO.N',          # Southern Copper
        ],
        'fields': REFINITIV_FIELDS['stock_fields'],
        'table': 'T_CompanyStockPrice'
    }
}

# データ取得設定
EIKON_CONFIG = {
    'timeout': 30,
    'max_retries': 3,
    'retry_delay': 1,
    'chunk_size': 300,  # API制限に応じて調整
    'request_interval': 0.1  # リクエスト間隔(秒)
}

# 日付範囲計算関数
def get_date_range(mode: str, category_type: str) -> tuple:
    """
    データ取得の日付範囲を計算
    
    Args:
        mode: 'initial' または 'daily'
        category_type: 'prices', 'inventory', 'indicators', 'macro', 'cotr', 'banding', 'stocks'
    
    Returns:
        tuple: (start_date, end_date) の文字列形式
    """
    end_date = datetime.now()
    
    if mode == 'initial':
        years = INITIAL_LOAD_PERIODS.get(category_type, 5)
        start_date = end_date - timedelta(days=years * 365)
    elif mode == 'daily':
        start_date = end_date - timedelta(days=3)  # 過去3日分
    else:
        raise ValueError(f"Unknown mode: {mode}")
    
    return start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')