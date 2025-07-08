#!/usr/bin/env python3
"""
LME LP1-LP12 データ取得スクリプト（シンプル版）
2020/6/29以降のデータを取得
"""
import sys
import os
from datetime import datetime

# プロジェクトルートとsrcディレクトリをPythonパスに追加
project_root = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.join(project_root, 'src')
sys.path.insert(0, project_root)
sys.path.insert(0, src_dir)

from src.bloomberg_api import BloombergDataFetcher
from src.database import DatabaseManager
from src.data_processor import DataProcessor
from config.logging_config import logger

def main():
    """メイン処理"""
    print("=== LME LP1-LP12 Data Fetch Start ===")
    
    # 1. データベース接続
    db_manager = DatabaseManager()
    try:
        db_manager.connect()
        print("Database connection successful")
        
        # マスターデータロード
        db_manager.load_master_data()
        print("Master data loaded")
        
    except Exception as e:
        print(f"Database connection failed: {e}")
        return False
    
    # 2. Bloomberg API接続
    bloomberg_fetcher = BloombergDataFetcher()
    try:
        bloomberg_fetcher.connect()
        print("Bloomberg API connected")
    except Exception as e:
        print(f"Bloomberg API connection failed: {e} (continuing with mock)")
    
    try:
        # 3. LME LP1-LP12ティッカー
        lme_securities = [f'LP{i} Comdty' for i in range(1, 13)]
        print(f"Target securities: {lme_securities}")
        
        # 4. 期間設定 (2020/6/29以降)
        start_date = "20200629"
        end_date = datetime.now().strftime("%Y%m%d")
        print(f"Date range: {start_date} - {end_date}")
        
        # 5. フィールド
        fields = ['PX_LAST', 'PX_OPEN', 'PX_HIGH', 'PX_LOW', 'PX_VOLUME', 'OPEN_INT']
        
        # 6. データ取得
        print("Fetching data from Bloomberg API...")
        df = bloomberg_fetcher.batch_request(
            securities=lme_securities,
            fields=fields,
            start_date=start_date,
            end_date=end_date,
            request_type="historical"
        )
        
        if df.empty:
            print("No data retrieved")
            return False
            
        print(f"Retrieved {len(df)} records")
        print(f"Securities found: {list(df['security'].unique())}")
        
        # 7. データ処理
        print("Processing data...")
        data_processor = DataProcessor(db_manager)
        
        # LME設定
        ticker_info = {
            'metal': 'COPPER',
            'exchange': 'LME',
            'tenor_mapping': {
                'LP1 Comdty': 'Generic 1st Future',
                'LP2 Comdty': 'Generic 2nd Future',
                'LP3 Comdty': 'Generic 3rd Future',
                'LP4 Comdty': 'Generic 4th Future',
                'LP5 Comdty': 'Generic 5th Future',
                'LP6 Comdty': 'Generic 6th Future',
                'LP7 Comdty': 'Generic 7th Future',
                'LP8 Comdty': 'Generic 8th Future',
                'LP9 Comdty': 'Generic 9th Future',
                'LP10 Comdty': 'Generic 10th Future',
                'LP11 Comdty': 'Generic 11th Future',
                'LP12 Comdty': 'Generic 12th Future'
            }
        }
        
        processed_df = data_processor.process_commodity_prices(df, ticker_info)
        
        if processed_df.empty:
            print("No processed data")
            return False
            
        print(f"Processed {len(processed_df)} records")
        
        # TenorTypeID分布確認
        tenor_counts = processed_df['TenorTypeID'].value_counts().sort_index()
        print("TenorTypeID distribution:")
        for tenor_id, count in tenor_counts.items():
            print(f"  TenorTypeID {tenor_id}: {count} records")
        
        # 8. データベースに格納
        print("Storing data to database...")
        unique_columns = ['TradeDate', 'MetalID', 'TenorTypeID', 'SpecificTenorDate']
        success_count = db_manager.upsert_dataframe(processed_df, 'T_CommodityPrice', unique_columns)
        
        if success_count > 0:
            print(f"Successfully stored {success_count} records")
            return True
        else:
            print("Failed to store data")
            return False
            
    except Exception as e:
        print(f"Error during processing: {e}")
        import traceback
        traceback.print_exc()
        return False
        
    finally:
        # 9. 接続終了
        try:
            bloomberg_fetcher.disconnect()
            db_manager.disconnect()
        except:
            pass

def verify_data():
    """データ検証"""
    print("\n=== Data Verification ===")
    
    db_manager = DatabaseManager()
    try:
        db_manager.connect()
        
        # 格納されたデータの確認
        query = """
        SELECT 
            tt.TenorTypeID,
            tt.TenorTypeName,
            COUNT(*) as DataCount,
            MIN(cp.TradeDate) as EarliestDate,
            MAX(cp.TradeDate) as LatestDate,
            AVG(cp.LastPrice) as AvgPrice
        FROM T_CommodityPrice cp
        JOIN M_TenorType tt ON cp.TenorTypeID = tt.TenorTypeID
        JOIN M_Metal m ON cp.MetalID = m.MetalID
        WHERE m.MetalCode = 'COPPER' AND tt.TenorTypeID > 4
        GROUP BY tt.TenorTypeID, tt.TenorTypeName
        ORDER BY tt.TenorTypeID
        """
        
        result_df = db_manager.execute_query(query)
        
        if not result_df.empty:
            print(f"Verification: Found {len(result_df)} TenorTypes with data")
            for _, row in result_df.iterrows():
                print(f"TenorID {row['TenorTypeID']}: {row['TenorTypeName']}")
                print(f"  Records: {row['DataCount']}")
                print(f"  Period: {row['EarliestDate']} - {row['LatestDate']}")
                print(f"  Avg Price: {row['AvgPrice']:.2f}")
            return True
        else:
            print("No data found for TenorTypeID > 4")
            return False
            
    except Exception as e:
        print(f"Error during verification: {e}")
        return False
        
    finally:
        try:
            db_manager.disconnect()
        except:
            pass

if __name__ == "__main__":
    print("LME LP1-LP12 Data Fetch Script")
    
    success = main()
    
    if success:
        print("\n" + "="*50)
        verify_data()
        print("\nScript completed successfully")
    else:
        print("\nScript failed")
    
    input("\nPress Enter to exit...")