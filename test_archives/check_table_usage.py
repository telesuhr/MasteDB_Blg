"""
Check which database tables have recent data and their usage
"""
import pyodbc
from datetime import datetime, timedelta
import sys
import os

# Add project paths
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), 'src'))

from config.database_config import get_connection_string

# Get connection
conn_str = get_connection_string()
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

# Define 7 days ago
seven_days_ago = datetime.now() - timedelta(days=7)

# Check all T_ tables for recent data
t_tables = [
    ('T_CommodityPrice', 'TradeDate'),
    ('T_CommodityPrice_V2', 'TradeDate'),
    ('T_LMEInventory', 'ReportDate'),
    ('T_OtherExchangeInventory', 'ReportDate'),
    ('T_MarketIndicator', 'ReportDate'),
    ('T_MacroEconomicIndicator', 'ReportDate'),
    ('T_COTR', 'ReportDate'),
    ('T_BandingReport', 'ReportDate'),
    ('T_CompanyStockPrice', 'TradeDate'),
    ('T_GenericContractMapping', 'TradeDate')
]

print('=== Tables with Recent Data (last 7 days) ===')
for table, date_col in t_tables:
    try:
        query = f"""
        SELECT COUNT(*) as total_rows,
               MAX({date_col}) as latest_date,
               MIN({date_col}) as earliest_date,
               COUNT(CASE WHEN {date_col} >= ? THEN 1 END) as recent_rows
        FROM {table}
        """
        cursor.execute(query, seven_days_ago)
        result = cursor.fetchone()
        if result and result[0] > 0:
            print(f'{table}:')
            print(f'  Total rows: {result[0]:,}')
            print(f'  Latest date: {result[1]}')
            print(f'  Earliest date: {result[2]}')
            print(f'  Recent rows (7 days): {result[3]:,}')
            print()
    except Exception as e:
        print(f'{table}: ERROR - {str(e)}')
        print()

# Check M_ tables (master data)
m_tables = [
    'M_Metal',
    'M_TenorType',
    'M_Indicator',
    'M_Region',
    'M_COTRCategory',
    'M_HoldingBand',
    'M_GenericFutures',
    'M_ActualContract'
]

print('\n=== Master Tables Row Counts ===')
for table in m_tables:
    try:
        cursor.execute(f'SELECT COUNT(*) FROM {table}')
        count = cursor.fetchone()[0]
        if count > 0:
            print(f'{table}: {count} rows')
    except Exception as e:
        print(f'{table}: ERROR - {str(e)}')

# Check views
views = [
    'V_CommodityPriceWithAttributes',
    'V_CommodityPriceSimple',
    'V_CommodityPriceWithChange',
    'V_CommodityPriceWithMaturity',
    'V_GenericFuturesWithMaturity'
]

print('\n=== Views ===')
for view in views:
    try:
        cursor.execute(f'SELECT COUNT(*) FROM {view}')
        count = cursor.fetchone()[0]
        print(f'{view}: {count} rows')
    except Exception as e:
        print(f'{view}: ERROR - {str(e)}')

conn.close()