"""
Bloomberg データの処理・変換モジュール
"""
import pandas as pd
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, date
import re

from config.bloomberg_config import BLOOMBERG_TICKERS
from config.logging_config import logger


class DataProcessor:
    """Bloombergデータを処理・変換するクラス"""
    
    def __init__(self, db_manager):
        self.db_manager = db_manager
        
    def process_commodity_prices(self, df: pd.DataFrame, ticker_info: Dict) -> pd.DataFrame:
        """
        商品価格データを処理
        
        Args:
            df: Bloombergから取得した生データ
            ticker_info: ティッカー設定情報
            
        Returns:
            pd.DataFrame: 処理済みデータ
        """
        if df.empty:
            return pd.DataFrame()
            
        processed_data = []
        
        for _, row in df.iterrows():
            try:
                # 基本情報の取得
                security = row['security']
                trade_date = pd.to_datetime(row['date']).date()
                
                # メタルIDの取得
                metal_code = ticker_info.get('metal', 'COPPER')
                metal_id = self.db_manager.get_or_create_master_id('metals', metal_code)
                
                # テナータイプIDの取得
                tenor_mapping = ticker_info.get('tenor_mapping', {})
                tenor_name = tenor_mapping.get(security, 'Unknown')
                tenor_type_id = self.db_manager.get_or_create_master_id('tenor_types', tenor_name)
                
                # 価格データの構築
                processed_row = {
                    'TradeDate': trade_date,
                    'MetalID': metal_id,
                    'TenorTypeID': tenor_type_id,
                    'SpecificTenorDate': None,  # ジェネリック先物なのでNULL
                    'SettlementPrice': row.get('PX_LAST'),
                    'OpenPrice': row.get('PX_OPEN'),
                    'HighPrice': row.get('PX_HIGH'),
                    'LowPrice': row.get('PX_LOW'),
                    'LastPrice': row.get('PX_LAST'),
                    'Volume': row.get('PX_VOLUME'),
                    'OpenInterest': row.get('OPEN_INT'),
                    'MaturityDate': row.get('FUT_DLV_DT')
                }
                
                # データ型の変換とクリーニング
                processed_row = self._clean_numeric_fields(processed_row)
                processed_data.append(processed_row)
                
            except Exception as e:
                logger.error(f"Error processing price data for {row.get('security')}: {e}")
                continue
                
        result_df = pd.DataFrame(processed_data)
        logger.info(f"Processed {len(result_df)} price records")
        return result_df
        
    def process_lme_inventory(self, df: pd.DataFrame, ticker_info: Dict) -> pd.DataFrame:
        """
        LME在庫データを処理
        
        Args:
            df: Bloombergから取得した生データ
            ticker_info: ティッカー設定情報
            
        Returns:
            pd.DataFrame: 処理済みデータ
        """
        if df.empty:
            return pd.DataFrame()
            
        # 在庫データを地域・タイプ別に整理
        inventory_by_region = {}
        
        for _, row in df.iterrows():
            try:
                security = row['security']
                report_date = pd.to_datetime(row['date']).date()
                value = row.get('PX_LAST', 0)
                
                # 地域の識別
                region_code = 'GLOBAL'
                for suffix, region in ticker_info['region_mapping'].items():
                    if security.endswith(suffix):
                        region_code = region
                        break
                        
                # データタイプの識別
                data_type = None
                if 'NLSCA' in security:
                    data_type = 'TotalStock'
                elif 'NLECA' in security:
                    data_type = 'OnWarrant'
                elif 'NLFCA' in security:
                    data_type = 'CancelledWarrant'
                elif 'NLJCA' in security:
                    data_type = 'Inflow'
                elif 'NLKCA' in security:
                    data_type = 'Outflow'
                    
                if data_type:
                    key = (report_date, region_code)
                    if key not in inventory_by_region:
                        inventory_by_region[key] = {
                            'ReportDate': report_date,
                            'RegionID': self.db_manager.get_or_create_master_id('regions', region_code),
                            'MetalID': self.db_manager.get_or_create_master_id('metals', ticker_info['metal'])
                        }
                    inventory_by_region[key][data_type] = value
                    
            except Exception as e:
                logger.error(f"Error processing inventory data for {row.get('security')}: {e}")
                continue
                
        # DataFrameに変換
        result_df = pd.DataFrame(list(inventory_by_region.values()))
        logger.info(f"Processed {len(result_df)} inventory records")
        return result_df
        
    def process_market_indicators(self, df: pd.DataFrame, ticker_info: Dict) -> pd.DataFrame:
        """
        市場指標データを処理
        
        Args:
            df: Bloombergから取得した生データ
            ticker_info: ティッカー設定情報
            
        Returns:
            pd.DataFrame: 処理済みデータ
        """
        if df.empty:
            return pd.DataFrame()
            
        processed_data = []
        
        for _, row in df.iterrows():
            try:
                security = row['security']
                report_date = pd.to_datetime(row['date']).date()
                value = row.get('PX_LAST')
                
                # インジケーターコードの生成
                indicator_code = security.split()[0]  # "SOFRRATE Index" -> "SOFRRATE"
                
                # インジケーターIDの取得
                indicator_id = self.db_manager.get_or_create_master_id(
                    'indicators', 
                    indicator_code,
                    name=security,
                    additional_fields={
                        'Category': ticker_info.get('category', 'Unknown'),
                        'Unit': self._determine_unit(security),
                        'Freq': 'Daily'
                    }
                )
                
                # メタルIDの取得（金属特有の指標の場合）
                metal_id = None
                if 'metal' in ticker_info:
                    metal_id = self.db_manager.get_or_create_master_id('metals', ticker_info['metal'])
                    
                processed_row = {
                    'ReportDate': report_date,
                    'IndicatorID': indicator_id,
                    'MetalID': metal_id,
                    'Value': value
                }
                
                processed_data.append(processed_row)
                
            except Exception as e:
                logger.error(f"Error processing indicator data for {row.get('security')}: {e}")
                continue
                
        result_df = pd.DataFrame(processed_data)
        logger.info(f"Processed {len(result_df)} indicator records")
        return result_df
        
    def process_cotr_data(self, df: pd.DataFrame, ticker_info: Dict) -> pd.DataFrame:
        """
        COTRデータを処理
        
        Args:
            df: Bloombergから取得した生データ
            ticker_info: ティッカー設定情報
            
        Returns:
            pd.DataFrame: 処理済みデータ
        """
        if df.empty:
            return pd.DataFrame()
            
        # COTRデータをカテゴリ別に整理
        cotr_by_date = {}
        
        for _, row in df.iterrows():
            try:
                security = row['security']
                report_date = pd.to_datetime(row['date']).date()
                value = row.get('PX_LAST', 0)
                
                # カテゴリの識別
                category = None
                position_type = None
                
                if 'CTCTMHZA' in security or 'CTCTLUZX' in security:
                    category = 'Investment Funds'
                    position_type = 'long'
                elif 'CTCTGKLQ' in security or 'CTCTWVTK' in security:
                    category = 'Investment Funds'
                    position_type = 'short'
                elif 'CTCTVDQG' in security or 'CTCTFSWP' in security:
                    category = 'Commercial Undertakings'
                    position_type = 'long'
                elif 'CTCTFHAX' in security or 'CTCTZTIH' in security:
                    category = 'Commercial Undertakings'
                    position_type = 'short'
                    
                if category:
                    key = (report_date, category)
                    if key not in cotr_by_date:
                        cotr_by_date[key] = {
                            'ReportDate': report_date,
                            'MetalID': self.db_manager.get_or_create_master_id('metals', 'COPPER'),
                            'COTRCategoryID': self.db_manager.get_or_create_master_id('cotr_categories', category)
                        }
                        
                    # ポジションまたはパーセンテージの設定
                    if 'LUZX' in security or 'WVTK' in security or 'FSWP' in security or 'ZTIH' in security:
                        # パーセンテージデータ
                        if position_type == 'long':
                            cotr_by_date[key]['LongPctOpenInterest'] = value
                        else:
                            cotr_by_date[key]['ShortPctOpenInterest'] = value
                    else:
                        # ポジションデータ
                        if position_type == 'long':
                            cotr_by_date[key]['LongPosition'] = value
                        else:
                            cotr_by_date[key]['ShortPosition'] = value
                            
            except Exception as e:
                logger.error(f"Error processing COTR data for {row.get('security')}: {e}")
                continue
                
        # NetPositionを計算
        for data in cotr_by_date.values():
            if 'LongPosition' in data and 'ShortPosition' in data:
                data['NetPosition'] = data.get('LongPosition', 0) - data.get('ShortPosition', 0)
                
        result_df = pd.DataFrame(list(cotr_by_date.values()))
        logger.info(f"Processed {len(result_df)} COTR records")
        return result_df
        
    def process_banding_report(self, df: pd.DataFrame, ticker_info: Dict) -> pd.DataFrame:
        """
        バンディングレポートデータを処理
        
        Args:
            df: Bloombergから取得した生データ
            ticker_info: ティッカー設定情報
            
        Returns:
            pd.DataFrame: 処理済みデータ
        """
        if df.empty:
            return pd.DataFrame()
            
        processed_data = []
        
        for _, row in df.iterrows():
            try:
                security = row['security']
                report_date = pd.to_datetime(row['date']).date()
                value = row.get('PX_LAST', 0)
                
                # レポートタイプとバンドの識別
                report_type = None
                band_code = None
                tenor_type_id = None
                
                # 先物バンディング
                if 'LMFBJ' in security:
                    match = re.search(r'LMFBJ([A-J])M(\d)', security)
                    if match:
                        band_letter = match.group(1)
                        month_num = match.group(2)
                        
                        if band_letter in ['A', 'B', 'C', 'D', 'E']:
                            report_type = 'Futures Long'
                        else:
                            report_type = 'Futures Short'
                            
                        band_code = ticker_info['band_mapping'].get(band_letter, 'Unknown')
                        tenor_name = f'Generic {month_num}st Future' if month_num == '1' else \
                                    f'Generic {month_num}nd Future' if month_num == '2' else \
                                    f'Generic {month_num}rd Future'
                        tenor_type_id = self.db_manager.get_or_create_master_id('tenor_types', tenor_name)
                        
                # ワラントバンディング
                elif 'LMWHCA' in security:
                    match = re.search(r'LMWHCA([A-Z])([A-E])', security)
                    if match:
                        type_letter = match.group(1)
                        band_letter = match.group(2)
                        
                        if type_letter == 'D':
                            report_type = 'Warrant'
                        elif type_letter == 'C':
                            report_type = 'Cash'
                        elif type_letter == 'T':
                            report_type = 'Tom'
                            
                        band_code = ticker_info['band_mapping'].get(band_letter, 'Unknown')
                        
                if report_type and band_code:
                    band_id = self.db_manager.get_or_create_master_id('holding_bands', band_code)
                    
                    processed_row = {
                        'ReportDate': report_date,
                        'MetalID': self.db_manager.get_or_create_master_id('metals', 'COPPER'),
                        'ReportType': report_type,
                        'TenorTypeID': tenor_type_id,
                        'BandID': band_id,
                        'Value': value
                    }
                    
                    processed_data.append(processed_row)
                    
            except Exception as e:
                logger.error(f"Error processing banding data for {row.get('security')}: {e}")
                continue
                
        result_df = pd.DataFrame(processed_data)
        logger.info(f"Processed {len(result_df)} banding records")
        return result_df
        
    def process_company_stocks(self, df: pd.DataFrame, ticker_info: Dict) -> pd.DataFrame:
        """
        企業株価データを処理
        
        Args:
            df: Bloombergから取得した生データ
            ticker_info: ティッカー設定情報
            
        Returns:
            pd.DataFrame: 処理済みデータ
        """
        if df.empty:
            return pd.DataFrame()
            
        processed_data = []
        
        for _, row in df.iterrows():
            try:
                processed_row = {
                    'TradeDate': pd.to_datetime(row['date']).date(),
                    'CompanyTicker': row['security'],
                    'OpenPrice': row.get('PX_OPEN'),
                    'HighPrice': row.get('PX_HIGH'),
                    'LowPrice': row.get('PX_LOW'),
                    'LastPrice': row.get('PX_LAST'),
                    'Volume': row.get('PX_VOLUME')
                }
                
                # データ型の変換とクリーニング
                processed_row = self._clean_numeric_fields(processed_row)
                processed_data.append(processed_row)
                
            except Exception as e:
                logger.error(f"Error processing stock data for {row.get('security')}: {e}")
                continue
                
        result_df = pd.DataFrame(processed_data)
        logger.info(f"Processed {len(result_df)} stock records")
        return result_df
        
    def _clean_numeric_fields(self, data: Dict) -> Dict:
        """
        数値フィールドをクリーニング
        
        Args:
            data: データ辞書
            
        Returns:
            Dict: クリーニング済みデータ
        """
        numeric_fields = ['SettlementPrice', 'OpenPrice', 'HighPrice', 'LowPrice', 
                         'LastPrice', 'Volume', 'OpenInterest', 'Value',
                         'TotalStock', 'OnWarrant', 'CancelledWarrant',
                         'Inflow', 'Outflow', 'LongPosition', 'ShortPosition']
                         
        for field in numeric_fields:
            if field in data:
                value = data[field]
                if pd.isna(value) or value == '' or value == 'N.A.':
                    data[field] = None
                else:
                    try:
                        if field in ['Volume', 'OpenInterest', 'TotalStock', 'OnWarrant',
                                    'CancelledWarrant', 'Inflow', 'Outflow', 'LongPosition',
                                    'ShortPosition']:
                            data[field] = int(float(value))
                        else:
                            data[field] = float(value)
                    except (ValueError, TypeError):
                        data[field] = None
                        
        return data
        
    def _determine_unit(self, security: str) -> str:
        """
        証券名から単位を推定
        
        Args:
            security: 証券名
            
        Returns:
            str: 単位
        """
        if 'Index' in security:
            return 'Index Points'
        elif 'Curncy' in security:
            return 'Currency'
        elif '%' in security or 'RATE' in security:
            return '%'
        elif 'Comdty' in security:
            return 'USD'
        else:
            return 'Units'