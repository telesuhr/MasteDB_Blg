#!/usr/bin/env python3
"""
Debug script to check database contents
"""
import sys
import os
import pandas as pd
import pyodbc
import warnings

# Add project root to Python path
project_root = os.getcwd()
sys.path.insert(0, project_root)

from config.database_config import get_connection_string

warnings.filterwarnings('ignore')

def main():
    try:
        # Connect to database
        conn = pyodbc.connect(get_connection_string())
        print('Connected to database')
        
        # Step 1: Check total records
        query = 'SELECT COUNT(*) as TotalRecords FROM T_CommodityPrice'
        result = pd.read_sql(query, conn)
        total_records = result['TotalRecords'].iloc[0]
        print(f'Total records in T_CommodityPrice: {total_records:,}')
        
        if total_records > 0:
            # Step 2: Check recent data
            query = '''
            SELECT TOP 5
                p.TradeDate,
                m.MetalCode,
                m.ExchangeCode,
                t.TenorTypeName,
                p.SettlementPrice
            FROM T_CommodityPrice p
            INNER JOIN M_Metal m ON p.MetalID = m.MetalID
            INNER JOIN M_TenorType t ON p.TenorTypeID = t.TenorTypeID
            WHERE p.SettlementPrice IS NOT NULL
            ORDER BY p.TradeDate DESC
            '''
            
            recent_data = pd.read_sql(query, conn)
            print('\nRecent price data (top 5):')
            print(recent_data.to_string(index=False))
            
            # Step 3: Check unique tenor types
            query = '''
            SELECT DISTINCT t.TenorTypeName
            FROM T_CommodityPrice p
            INNER JOIN M_TenorType t ON p.TenorTypeID = t.TenorTypeID
            WHERE p.SettlementPrice IS NOT NULL
            ORDER BY t.TenorTypeName
            '''
            
            tenors = pd.read_sql(query, conn)
            print('\nAll tenor types with data:')
            for tenor in tenors['TenorTypeName']:
                print(f'  - {tenor}')
                
            # Step 4: Check exchanges
            query = '''
            SELECT DISTINCT m.ExchangeCode, COUNT(*) as RecordCount
            FROM T_CommodityPrice p
            INNER JOIN M_Metal m ON p.MetalID = m.MetalID
            WHERE p.SettlementPrice IS NOT NULL
            GROUP BY m.ExchangeCode
            '''
            
            exchanges = pd.read_sql(query, conn)
            print('\nData count by exchange:')
            print(exchanges.to_string(index=False))
            
            # Step 5: Check date range
            query = '''
            SELECT 
                MIN(TradeDate) as FirstDate,
                MAX(TradeDate) as LastDate
            FROM T_CommodityPrice
            WHERE SettlementPrice IS NOT NULL
            '''
            
            date_range = pd.read_sql(query, conn)
            print('\nDate range:')
            print(date_range.to_string(index=False))
        else:
            print('No data found in T_CommodityPrice table')
        
        conn.close()
        print('\nDatabase connection closed')
        
    except Exception as e:
        print(f'Error: {e}')
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    main()