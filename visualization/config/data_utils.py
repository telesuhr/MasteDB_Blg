"""
データ取得・処理用ユーティリティ関数
"""
import pandas as pd
import pyodbc
from typing import Dict, List, Optional, Union
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

try:
    from .db_config import get_connection_string, TABLES, MASTER_TABLES
except ImportError:
    # 相対インポートが失敗した場合の絶対インポート
    from db_config import get_connection_string, TABLES, MASTER_TABLES


class DataFetcher:
    """データベースからデータを取得するクラス"""
    
    def __init__(self):
        self.connection_string = get_connection_string()
        # 接続文字列の一部を表示（パスワードは隠す）
        masked_conn_str = self.connection_string
        if 'pwd=' in masked_conn_str.lower():
            # パスワード部分をマスク
            import re
            masked_conn_str = re.sub(r'pwd=[^;]*', 'pwd=***', masked_conn_str, flags=re.IGNORECASE)
        print(f"Database connection config: {masked_conn_str[:100]}...")
        
    def test_connection(self):
        """データベース接続をテスト"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                result = cursor.fetchone()
                if result and result[0] == 1:
                    print("SUCCESS: Database connection test passed")
                    return True
        except Exception as e:
            print(f"FAILED: Database connection test failed: {e}")
            return False
        
    def get_connection(self):
        """データベース接続を取得"""
        try:
            return pyodbc.connect(self.connection_string)
        except Exception as e:
            print(f"ERROR: Database connection error: {e}")
            print("Please check the following:")
            print("  1. Azure SQL Server is running")
            print("  2. Connection string is correct")
            print("  3. Network connection is normal")
            print("  4. Firewall settings are appropriate")
            print("  5. ODBC Driver is installed")
            raise ConnectionError(f"Cannot connect to database: {e}")
    
    def get_copper_prices(self, days: int = 365, exchanges: List[str] = None) -> pd.DataFrame:
        """
        銅価格データを取得
        
        Args:
            days: 取得する日数（デフォルト：365日）
            exchanges: 取引所フィルタ（例：['LME', 'SHFE', 'CMX']）
            
        Returns:
            pd.DataFrame: 銅価格データ
        """
        query = """
        SELECT 
            cp.TradeDate,
            m.MetalCode,
            m.ExchangeCode,
            tt.TenorTypeName,
            cp.SettlementPrice,
            cp.LastPrice,
            cp.Volume,
            cp.OpenInterest
        FROM T_CommodityPrice cp
        JOIN M_Metal m ON cp.MetalID = m.MetalID
        JOIN M_TenorType tt ON cp.TenorTypeID = tt.TenorTypeID
        WHERE m.MetalCode LIKE '%COPPER%'
        AND cp.TradeDate >= DATEADD(day, -?, GETDATE())
        """
        
        params = [days]
        
        if exchanges:
            placeholders = ','.join(['?' for _ in exchanges])
            query += f" AND m.ExchangeCode IN ({placeholders})"
            params.extend(exchanges)
            
        query += " ORDER BY cp.TradeDate DESC, m.ExchangeCode, tt.TenorTypeName"
        
        try:
            with self.get_connection() as conn:
                df = pd.read_sql(query, conn, params=params)
                df['TradeDate'] = pd.to_datetime(df['TradeDate'])
                
                if df.empty:
                    print("WARNING: No copper price data found")
                    print("Please check the following:")
                    print("  1. T_CommodityPrice table has data")
                    print("  2. M_Metal, M_TenorType tables are properly configured")
                    print("  3. Copper data exists within the past 365 days")
                
                return df
                
        except Exception as e:
            print(f"ERROR: Failed to fetch copper price data: {e}")
            raise RuntimeError(f"Could not fetch copper price data: {e}")
    
    def get_lme_inventory(self, days: int = 365) -> pd.DataFrame:
        """
        LME在庫データを取得
        
        Args:
            days: 取得する日数
            
        Returns:
            pd.DataFrame: LME在庫データ
        """
        query = """
        SELECT 
            li.ReportDate,
            m.MetalCode,
            r.RegionCode,
            r.RegionName,
            li.TotalStock,
            li.OnWarrant,
            li.CancelledWarrant,
            li.Inflow,
            li.Outflow
        FROM T_LMEInventory li
        JOIN M_Metal m ON li.MetalID = m.MetalID
        JOIN M_Region r ON li.RegionID = r.RegionID
        WHERE m.MetalCode LIKE '%COPPER%'
        AND li.ReportDate >= DATEADD(day, -?, GETDATE())
        ORDER BY li.ReportDate DESC, r.RegionCode
        """
        
        with self.get_connection() as conn:
            df = pd.read_sql(query, conn, params=[days])
            df['ReportDate'] = pd.to_datetime(df['ReportDate'])
            return df
    
    def get_other_inventory(self, days: int = 365) -> pd.DataFrame:
        """
        他取引所在庫データを取得
        
        Args:
            days: 取得する日数
            
        Returns:
            pd.DataFrame: 他取引所在庫データ
        """
        query = """
        SELECT 
            oi.ReportDate,
            m.MetalCode,
            oi.ExchangeCode,
            oi.TotalStock,
            oi.OnWarrant
        FROM T_OtherExchangeInventory oi
        JOIN M_Metal m ON oi.MetalID = m.MetalID
        WHERE m.MetalCode LIKE '%COPPER%'
        AND oi.ReportDate >= DATEADD(day, -?, GETDATE())
        ORDER BY oi.ReportDate DESC, oi.ExchangeCode
        """
        
        with self.get_connection() as conn:
            df = pd.read_sql(query, conn, params=[days])
            df['ReportDate'] = pd.to_datetime(df['ReportDate'])
            return df
    
    def get_tenor_spread_data(self, days: int = 365) -> pd.DataFrame:
        """
        テナースプレッドデータを取得
        
        Args:
            days: 取得する日数
            
        Returns:
            pd.DataFrame: テナースプレッドデータ
        """
        query = """
        SELECT 
            cp.TradeDate,
            m.MetalCode,
            m.ExchangeCode,
            tt.TenorTypeName,
            cp.SettlementPrice,
            cp.LastPrice
        FROM T_CommodityPrice cp
        JOIN M_Metal m ON cp.MetalID = m.MetalID
        JOIN M_TenorType tt ON cp.TenorTypeID = tt.TenorTypeID
        WHERE m.MetalCode LIKE '%COPPER%'
        AND tt.TenorTypeName IN ('Cash', '3M Futures', 'Generic 1st Future', 'Generic 2nd Future', 'Generic 3rd Future')
        AND cp.TradeDate >= DATEADD(day, -?, GETDATE())
        ORDER BY cp.TradeDate DESC, m.ExchangeCode, tt.TenorTypeName
        """
        
        with self.get_connection() as conn:
            df = pd.read_sql(query, conn, params=[days])
            df['TradeDate'] = pd.to_datetime(df['TradeDate'])
            return df
    
    def get_market_indicators(self, days: int = 365, categories: List[str] = None) -> pd.DataFrame:
        """
        市場指標データを取得
        
        Args:
            days: 取得する日数
            categories: 指標カテゴリフィルタ（例：['Interest Rate', 'FX']）
            
        Returns:
            pd.DataFrame: 市場指標データ
        """
        query = """
        SELECT 
            mi.ReportDate,
            ind.IndicatorCode,
            ind.IndicatorName,
            ind.Category,
            ind.Unit,
            mi.Value
        FROM T_MarketIndicator mi
        JOIN M_Indicator ind ON mi.IndicatorID = ind.IndicatorID
        WHERE mi.ReportDate >= DATEADD(day, -?, GETDATE())
        """
        
        params = [days]
        
        if categories:
            placeholders = ','.join(['?' for _ in categories])
            query += f" AND ind.Category IN ({placeholders})"
            params.extend(categories)
            
        query += " ORDER BY mi.ReportDate DESC, ind.Category, ind.IndicatorCode"
        
        with self.get_connection() as conn:
            df = pd.read_sql(query, conn, params=params)
            df['ReportDate'] = pd.to_datetime(df['ReportDate'])
            return df
    
    def calculate_spreads(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        スプレッドを計算
        
        Args:
            df: 価格データ
            
        Returns:
            pd.DataFrame: スプレッド計算結果
        """
        spreads = []
        
        for date in df['TradeDate'].unique():
            date_data = df[df['TradeDate'] == date]
            
            for exchange in date_data['ExchangeCode'].unique():
                exchange_data = date_data[date_data['ExchangeCode'] == exchange]
                
                # 現物価格
                cash_price = exchange_data[exchange_data['TenorTypeName'] == 'Cash']['LastPrice'].iloc[0] if len(exchange_data[exchange_data['TenorTypeName'] == 'Cash']) > 0 else None
                
                # 3ヶ月先物価格
                three_m_price = exchange_data[exchange_data['TenorTypeName'] == '3M Futures']['LastPrice'].iloc[0] if len(exchange_data[exchange_data['TenorTypeName'] == '3M Futures']) > 0 else None
                
                # 1番限月vs2番限月
                generic_1st = exchange_data[exchange_data['TenorTypeName'] == 'Generic 1st Future']['LastPrice'].iloc[0] if len(exchange_data[exchange_data['TenorTypeName'] == 'Generic 1st Future']) > 0 else None
                generic_2nd = exchange_data[exchange_data['TenorTypeName'] == 'Generic 2nd Future']['LastPrice'].iloc[0] if len(exchange_data[exchange_data['TenorTypeName'] == 'Generic 2nd Future']) > 0 else None
                
                if cash_price and three_m_price:
                    spreads.append({
                        'TradeDate': date,
                        'ExchangeCode': exchange,
                        'SpreadType': 'Cash-3M',
                        'SpreadValue': three_m_price - cash_price
                    })
                
                if generic_1st and generic_2nd:
                    spreads.append({
                        'TradeDate': date,
                        'ExchangeCode': exchange,
                        'SpreadType': '1st-2nd',
                        'SpreadValue': generic_2nd - generic_1st
                    })
        
        return pd.DataFrame(spreads)