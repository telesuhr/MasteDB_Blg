"""
Bloomberg ティッカーとフィールドの定義
"""
from datetime import datetime, timedelta

# Bloomberg API設定
BLOOMBERG_HOST = "localhost"
BLOOMBERG_PORT = 8194

# データ取得期間設定
INITIAL_LOAD_PERIODS = {
    'prices': 5,  # 年
    'inventory': 5,  # 年
    'indicators': 5,  # 年
    'macro': 10,  # 年
    'cotr': 5,  # 年
    'banding': 3,  # 年
    'stocks': 5  # 年
}

# Bloomberg フィールド定義
PRICE_FIELDS = ['PX_LAST', 'PX_OPEN', 'PX_HIGH', 'PX_LOW', 'PX_VOLUME', 'OPEN_INT', 'FUT_DLV_DT']
INVENTORY_FIELDS = ['PX_LAST', 'LAST_PRICE', 'CUR_MKT_VALUE']
INDICATOR_FIELDS = ['PX_LAST']
STOCK_FIELDS = ['PX_LAST', 'PX_OPEN', 'PX_HIGH', 'PX_LOW', 'PX_VOLUME']

# ティッカー定義
BLOOMBERG_TICKERS = {
    # LME 銅価格
    'LME_COPPER_PRICES': {
        'securities': [
            'LMCADY Index',      # LME銅 現物価格
            'CAD TT00 Comdty',   # LME銅 トムネクスト
            'LMCADS03 Comdty',   # LME銅 3ヶ月先物価格
            'LMCADS 0003 Comdty', # LME銅 Cash/3mスプレッド
        ] + [f'LP{i} Comdty' for i in range(1, 13)],  # LP1-LP12 Generic futures
        'fields': PRICE_FIELDS,
        'table': 'T_CommodityPrice',
        'metal': 'COPPER',
        'exchange': 'LME',
        'tenor_mapping': {
            'LMCADY Index': 'Cash',
            'CAD TT00 Comdty': 'Tom-Next',
            'LMCADS03 Comdty': '3M Futures',
            'LMCADS 0003 Comdty': 'Cash/3M Spread',
            **{f'LP{i} Comdty': f'Generic {i}st Future' if i == 1 
               else f'Generic {i}nd Future' if i == 2
               else f'Generic {i}rd Future' if i == 3
               else f'Generic {i}th Future' for i in range(1, 13)}
        }
    },
    
    # SHFE 銅価格
    'SHFE_COPPER_PRICES': {
        'securities': [f'CU{i} Comdty' for i in range(1, 13)],  # CU1-CU12
        'fields': PRICE_FIELDS,
        'table': 'T_CommodityPrice',
        'metal': 'COPPER',
        'exchange': 'SHFE',
        'tenor_mapping': {
            f'CU{i} Comdty': f'Generic {i}st Future' if i == 1
            else f'Generic {i}nd Future' if i == 2
            else f'Generic {i}rd Future' if i == 3
            else f'Generic {i}th Future' for i in range(1, 13)
        }
    },
    
    # CMX 銅価格
    'CMX_COPPER_PRICES': {
        'securities': [f'HG{i} Comdty' for i in range(1, 13)],  # HG1-HG12
        'fields': PRICE_FIELDS,
        'table': 'T_CommodityPrice',
        'metal': 'COPPER',
        'exchange': 'CMX',
        'tenor_mapping': {
            f'HG{i} Comdty': f'Generic {i}st Future' if i == 1
            else f'Generic {i}nd Future' if i == 2
            else f'Generic {i}rd Future' if i == 3
            else f'Generic {i}th Future' for i in range(1, 13)
        }
    },
    
    # LME 在庫データ
    'LME_INVENTORY': {
        'securities': {
            'total_stock': ['NLSCA Index', 'NLSCA @AMER Index', 'NLSCA @ASIA Index', 
                           'NLSCA @EURO Index', 'NLSCA @MEST Index'],
            'on_warrant': ['NLECA Index', 'NLECA @AMER Index', 'NLECA @ASIA Index',
                          'NLECA @EURO Index', 'NLECA @MEST Index'],
            'cancelled_warrant': ['NLFCA Index', 'NLFCA @AMER Index', 'NLFCA @ASIA Index',
                                 'NLFCA @EURO Index', 'NLFCA @MEST Index'],
            'inflow': ['NLJCA Index', 'NLJCA @AMER Index', 'NLJCA @ASIA Index',
                      'NLJCA @EURO Index', 'NLJCA @MEST Index'],
            'outflow': ['NLKCA Index', 'NLKCA @AMER Index', 'NLKCA @ASIA Index',
                       'NLKCA @EURO Index', 'NLKCA @MEST Index']
        },
        'fields': INVENTORY_FIELDS,
        'table': 'T_LMEInventory',
        'metal': 'COPPER',
        'region_mapping': {
            '@AMER Index': 'AMER',
            '@ASIA Index': 'ASIA',
            '@EURO Index': 'EURO',
            '@MEST Index': 'MEST'
        }
    },
    
    # SHFE 在庫データ
    'SHFE_INVENTORY': {
        'securities': ['SHFCCOPD Index', 'SHFCCOPO Index', 'SFCDTOTL Index'],
        'fields': INVENTORY_FIELDS,
        'table': 'T_OtherExchangeInventory',
        'metal': 'COPPER',
        'exchange': 'SHFE',
        'type_mapping': {
            'SHFCCOPD Index': 'on_warrant',  # SHFE delivered (on warrant)
            'SHFCCOPO Index': 'total_stock',  # SHFE open interest (total)
            'SFCDTOTL Index': 'total_stock'   # SHFE total
        }
    },
    
    # CMX 在庫データ
    'CMX_INVENTORY': {
        'securities': ['COMXCOPR Index'],
        'fields': INVENTORY_FIELDS,
        'table': 'T_OtherExchangeInventory',
        'metal': 'COPPER',
        'exchange': 'CMX',
        'type_mapping': {
            'COMXCOPR Index': 'total_stock'
        }
    },
    
    # 金利指標
    'INTEREST_RATES': {
        'securities': ['SOFRRATE Index', 'TSFR1M Index', 'TSFR3M Index', 
                      'US0001M Index', 'US0003M Index'],
        'fields': INDICATOR_FIELDS,
        'table': 'T_MarketIndicator',
        'category': 'Interest Rate'
    },
    
    # 為替レート
    'FX_RATES': {
        'securities': ['USDJPY Curncy', 'EURUSD Curncy', 'USDCNY Curncy',
                      'USDCLP Curncy', 'USDPEN Curncy'],
        'fields': INDICATOR_FIELDS,
        'table': 'T_MarketIndicator',
        'category': 'FX'
    },
    
    # コモディティ指数
    'COMMODITY_INDICES': {
        'securities': ['BCOM Index', 'SPGSCI Index'],
        'fields': INDICATOR_FIELDS,
        'table': 'T_MarketIndicator',
        'category': 'Commodity Index'
    },
    
    # 株式市場指数
    'EQUITY_INDICES': {
        'securities': ['SPX Index', 'NKY Index', 'SHCOMP Index'],
        'fields': INDICATOR_FIELDS,
        'table': 'T_MarketIndicator',
        'category': 'Equity Index'
    },
    
    # マクロ経済指標
    'MACRO_INDICATORS': {
        'securities': {
            'US': ['NAPMPMI Index', 'EHGDUSY Index', 'EHIUUSY Index', 'EHPIUSY Index'],
            'CN': ['CPMINDX Index', 'EHGDCNY Index', 'EHIUCNY Index', 'EHPICNY Index'],
            'EU': ['MPMIEUMA Index']
        },
        'fields': INDICATOR_FIELDS,
        'table': 'T_MacroEconomicIndicator',
        'category': 'Macro Economic',
        'frequency': {
            'PMI': 'Monthly',
            'GDP': 'Yearly',
            'Industrial': 'Yearly',
            'CPI': 'Yearly'
        }
    },
    
    # 先物保有比率レポート
    'FUTURES_BANDING': {
        'securities': {
            'long': {
                f'M{i}': [f'LMFBJ{band}M{i} Index' for band in ['A', 'B', 'C', 'D', 'E']]
                for i in range(1, 4)
            },
            'short': {
                f'M{i}': [f'LMFBJ{band}M{i} Index' for band in ['F', 'G', 'H', 'I', 'J']]
                for i in range(1, 4)
            }
        },
        'fields': INVENTORY_FIELDS,
        'table': 'T_BandingReport',
        'band_mapping': {
            'A': '5-9%', 'B': '10-19%', 'C': '20-29%', 'D': '30-39%', 'E': '40+%',
            'F': '5-9%', 'G': '10-19%', 'H': '20-29%', 'I': '30-39%', 'J': '40+%'
        }
    },
    
    # ワラントバンディングレポート
    'WARRANT_BANDING': {
        'securities': {
            'warrant': [f'LMWHCAD{band} Index' for band in ['A', 'B', 'C', 'D', 'E']],
            'cash': [f'LMWHCAC{band} Index' for band in ['A', 'B', 'C', 'D', 'E']],
            'tom': [f'LMWHCAT{band} Index' for band in ['A', 'B', 'C', 'D', 'E']]
        },
        'fields': INVENTORY_FIELDS,
        'table': 'T_BandingReport',
        'band_mapping': {
            'A': '30-39%', 'B': '40-49%', 'C': '50-79%', 'D': '80-89%', 'E': '90+%'
        }
    },
    
    # LME COTRデータ
    'COTR_DATA': {
        'securities': {
            'investment_funds': {
                'long': ['CTCTMHZA Index', 'CTCTLUZX Index'],
                'short': ['CTCTGKLQ Index', 'CTCTWVTK Index']
            },
            'commercial': {
                'long': ['CTCTVDQG Index', 'CTCTFSWP Index'],
                'short': ['CTCTFHAX Index', 'CTCTZTIH Index']
            }
        },
        'fields': INVENTORY_FIELDS,
        'table': 'T_COTR',
        'frequency': 'Weekly'
    },
    
    # エネルギー価格
    'ENERGY_PRICES': {
        'securities': ['CP1 Comdty', 'CO1 Comdty', 'NG1 Comdty'],
        'fields': INDICATOR_FIELDS,
        'table': 'T_MarketIndicator',
        'category': 'Energy'
    },
    
    # 現物プレミアム
    'PHYSICAL_PREMIUMS': {
        'securities': ['CECN0001 Index', 'CECN0002 Index'],
        'fields': INDICATOR_FIELDS,
        'table': 'T_MarketIndicator',
        'category': 'Physical Premium',
        'metal': 'COPPER'
    },
    
    # その他指標
    'OTHER_INDICATORS': {
        'securities': ['BDIY Index'],
        'fields': INDICATOR_FIELDS,
        'table': 'T_MarketIndicator',
        'category': 'Shipping'
    },
    
    # 企業株価
    'COMPANY_STOCKS': {
        'securities': ['GLEN LN Equity', 'RIO LN Equity', 'BHP AU Equity',
                      'FCX US Equity', '2600 HK Equity'],
        'fields': STOCK_FIELDS,
        'table': 'T_CompanyStockPrice'
    }
}

def get_date_range(mode: str, category: str) -> tuple:
    """
    データ取得期間を決定
    
    Args:
        mode: 'initial' または 'daily'
        category: データカテゴリ
        
    Returns:
        tuple: (start_date, end_date) as strings in YYYYMMDD format
    """
    today = datetime.now()
    
    if mode == 'initial':
        # 初回ロード：カテゴリに応じた過去データ
        years_back = INITIAL_LOAD_PERIODS.get(category, 5)
        start_date = today - timedelta(days=365 * years_back)
    else:
        # 日次更新：過去3日分（週末や休日を考慮）
        start_date = today - timedelta(days=3)
    
    return start_date.strftime('%Y%m%d'), today.strftime('%Y%m%d')