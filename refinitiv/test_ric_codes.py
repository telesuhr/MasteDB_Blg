#!/usr/bin/env python3
"""
Refinitiv RIC コード検証スクリプト
Bloomberg ティッカーに対応するRICコードを一つずつテストします
"""
import sys
import os
from datetime import datetime, timedelta
import pandas as pd
import time

# プロジェクトルートをPythonパスに追加
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)

def test_eikon_connection():
    """EIKON API接続テスト"""
    try:
        import eikon as ek
        print("✅ EIKON library imported successfully")
        
        # API Key設定（環境変数または直接設定）
        api_key = os.getenv('REFINITIV_API_KEY')
        if not api_key or api_key == 'YOUR_API_KEY_HERE':
            print("⚠️  API Key not found. Please set REFINITIV_API_KEY environment variable")
            print("   Or get API Key from: Refinitiv Workspace > EIKON > App Studio > API Key")
            api_key = input("Enter your API Key (or press Enter to skip): ").strip()
            
        if api_key:
            ek.set_app_key(api_key)
            print("✅ API Key set successfully")
            return ek, True
        else:
            print("❌ No API Key provided. Connection test skipped.")
            return ek, False
            
    except ImportError as e:
        print(f"❌ EIKON library not found: {e}")
        print("   Install with: pip install eikon")
        return None, False
    except Exception as e:
        print(f"❌ Error setting up EIKON: {e}")
        return None, False

def test_single_ric(ek, ric_code, fields=['TR.PriceClose'], description=""):
    """単一RICコードのテスト"""
    try:
        print(f"\n--- Testing: {ric_code} ({description}) ---")
        
        # 簡単なデータ取得テスト
        start_date = (datetime.now() - timedelta(days=5)).strftime('%Y-%m-%d')
        end_date = datetime.now().strftime('%Y-%m-%d')
        
        print(f"  Requesting: ek.get_data(['{ric_code}'], {fields})")
        
        # リファレンスデータテスト（最新値）
        ref_data, ref_err = ek.get_data(ric_code, fields)
        
        if ref_err is not None and not ref_err.empty:
            print(f"  ❌ Reference data error: {ref_err}")
            return False, None
            
        if ref_data is not None and not ref_data.empty:
            print(f"  ✅ Reference data retrieved: {len(ref_data)} rows")
            print(f"      Data: {ref_data.iloc[0].to_dict()}")
            
            # 時系列データテスト
            try:
                print(f"  Testing time series data...")
                ts_data, ts_err = ek.get_timeseries(
                    ric_code, 
                    start_date=start_date, 
                    end_date=end_date,
                    fields=['CLOSE']
                )
                
                if ts_err is not None:
                    print(f"  ⚠️  Time series error: {ts_err}")
                elif ts_data is not None and not ts_data.empty:
                    print(f"  ✅ Time series data: {len(ts_data)} rows")
                    print(f"      Latest: {ts_data.tail(1).iloc[0].to_dict()}")
                else:
                    print(f"  ⚠️  No time series data available")
                    
            except Exception as e:
                print(f"  ⚠️  Time series test failed: {e}")
                
            return True, ref_data
        else:
            print(f"  ❌ No reference data retrieved")
            return False, None
            
    except Exception as e:
        print(f"  ❌ Error testing {ric_code}: {e}")
        return False, None

def get_bloomberg_ric_mapping():
    """Bloomberg→RIC マッピングのテスト用リスト"""
    return {
        # === LME 銅価格 ===
        'LME_COPPER_PRICES': {
            'LMCADY Index': ['CMCU3', 'LME Copper Cash'],
            'LMCADS03 Comdty': ['CMCU03', 'LME Copper 3M'],
            'LP1 Comdty': ['CMCU1', 'LME Copper Generic 1st'],
            'LP2 Comdty': ['CMCU2', 'LME Copper Generic 2nd'],
            'LP3 Comdty': ['CMCU3', 'LME Copper Generic 3rd'],
        },
        
        # === 金利 ===
        'INTEREST_RATES': {
            'SOFRRATE Index': ['USSOFR=', 'US SOFR Rate'],
            'US0001M Index': ['USD1ML=', 'USD 1M LIBOR'],
            'US0003M Index': ['USD3ML=', 'USD 3M LIBOR'],
        },
        
        # === 為替 ===
        'FX_RATES': {
            'USDJPY Curncy': ['JPY=', 'USD/JPY'],
            'EURUSD Curncy': ['EUR=', 'EUR/USD'],
            'USDCNY Curncy': ['CNY=', 'USD/CNY'],
        },
        
        # === 株価指数 ===
        'EQUITY_INDICES': {
            'SPX Index': ['.SPX', 'S&P 500'],
            'NKY Index': ['.N225', 'Nikkei 225'],
            'SHCOMP Index': ['.SSEC', 'Shanghai Composite'],
        },
        
        # === エネルギー ===
        'ENERGY_PRICES': {
            'CP1 Index': ['CLc1', 'WTI Crude Oil'],
            'CO1 Index': ['LCOc1', 'Brent Crude Oil'],
            'NG1 Index': ['NGc1', 'Natural Gas'],
        },
        
        # === 企業株価 ===
        'COMPANY_STOCKS': {
            'GLEN LN Equity': ['GLEN.L', 'Glencore'],
            'FCX US Equity': ['FCX.N', 'Freeport-McMoRan'],
        },
        
        # === LME 在庫 ===
        'LME_INVENTORY': {
            'NLSCA Index': ['LMCASTK', 'LME Copper Total Stock'],
            'NLECA Index': ['LMCAWRT', 'LME Copper On Warrant'],
        },
        
        # === SHFE 銅価格 ===
        'SHFE_COPPER_PRICES': {
            'CU1 Comdty': ['CUc1', 'SHFE Copper Generic 1st'],
            'CU2 Comdty': ['CUc2', 'SHFE Copper Generic 2nd'],
        },
        
        # === CMX 銅価格 ===
        'CMX_COPPER_PRICES': {
            'HG1 Comdty': ['HGc1', 'COMEX Copper Generic 1st'],
            'HG2 Comdty': ['HGc2', 'COMEX Copper Generic 2nd'],
        }
    }

def run_ric_validation(ek, category_filter=None, max_tests=None):
    """RICコード検証の実行"""
    mapping = get_bloomberg_ric_mapping()
    
    results = {
        'success': [],
        'failed': [],
        'total_tested': 0
    }
    
    for category, items in mapping.items():
        if category_filter and category != category_filter:
            continue
            
        print(f"\n{'='*60}")
        print(f"Category: {category}")
        print(f"{'='*60}")
        
        for bloomberg_ticker, (ric_code, description) in items.items():
            if max_tests and results['total_tested'] >= max_tests:
                break
                
            print(f"\nBloomberg: {bloomberg_ticker}")
            print(f"RIC: {ric_code}")
            
            success, data = test_single_ric(ek, ric_code, description=description)
            
            results['total_tested'] += 1
            if success:
                results['success'].append({
                    'category': category,
                    'bloomberg': bloomberg_ticker,
                    'ric': ric_code,
                    'description': description
                })
            else:
                results['failed'].append({
                    'category': category,
                    'bloomberg': bloomberg_ticker,
                    'ric': ric_code,
                    'description': description
                })
                
            # API制限を考慮して少し待機
            time.sleep(0.5)
            
    return results

def print_summary(results):
    """結果サマリーの表示"""
    print(f"\n{'='*60}")
    print(f"RIC Code Validation Summary")
    print(f"{'='*60}")
    
    total = results['total_tested']
    success_count = len(results['success'])
    failed_count = len(results['failed'])
    
    print(f"Total Tested: {total}")
    print(f"✅ Success: {success_count} ({success_count/total*100:.1f}%)")
    print(f"❌ Failed: {failed_count} ({failed_count/total*100:.1f}%)")
    
    if results['success']:
        print(f"\n✅ Working RIC Codes:")
        for item in results['success']:
            print(f"  {item['bloomberg']} → {item['ric']} ({item['description']})")
    
    if results['failed']:
        print(f"\n❌ Failed RIC Codes (Manual investigation needed):")
        for item in results['failed']:
            print(f"  {item['bloomberg']} → {item['ric']} ({item['description']})")
            
    return results

def main():
    """メイン処理"""
    print("Refinitiv RIC Code Validation")
    print("=" * 60)
    
    # EIKON接続テスト
    ek, connected = test_eikon_connection()
    
    if not connected:
        print("\n❌ Cannot proceed without EIKON connection")
        return
        
    # テスト設定
    print("\nTest Options:")
    print("1. Test all categories")
    print("2. Test specific category")
    print("3. Test first 5 items only")
    
    choice = input("\nEnter choice (1-3) or press Enter for option 3: ").strip()
    
    category_filter = None
    max_tests = None
    
    if choice == "2":
        categories = list(get_bloomberg_ric_mapping().keys())
        print("\nAvailable categories:")
        for i, cat in enumerate(categories, 1):
            print(f"  {i}. {cat}")
        
        cat_choice = input("Enter category number: ").strip()
        try:
            category_filter = categories[int(cat_choice) - 1]
        except (ValueError, IndexError):
            print("Invalid choice, testing all categories")
    elif choice == "3" or not choice:
        max_tests = 5
        
    # RIC検証実行
    print(f"\nStarting RIC validation...")
    if category_filter:
        print(f"Category filter: {category_filter}")
    if max_tests:
        print(f"Max tests: {max_tests}")
        
    results = run_ric_validation(ek, category_filter, max_tests)
    
    # 結果表示
    summary = print_summary(results)
    
    # 結果をファイルに保存
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = f"ric_validation_results_{timestamp}.txt"
    
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write("RIC Code Validation Results\n")
        f.write("=" * 40 + "\n\n")
        f.write(f"Test Date: {datetime.now()}\n")
        f.write(f"Total Tested: {results['total_tested']}\n")
        f.write(f"Success: {len(results['success'])}\n")
        f.write(f"Failed: {len(results['failed'])}\n\n")
        
        f.write("Successful Mappings:\n")
        f.write("-" * 20 + "\n")
        for item in results['success']:
            f.write(f"{item['bloomberg']} → {item['ric']} ({item['description']})\n")
            
        f.write("\nFailed Mappings (Need Manual Investigation):\n")
        f.write("-" * 45 + "\n")
        for item in results['failed']:
            f.write(f"{item['bloomberg']} → {item['ric']} ({item['description']})\n")
    
    print(f"\n📝 Results saved to: {output_file}")
    print("\nNext steps for failed RIC codes:")
    print("1. Open Refinitiv Workspace")
    print("2. Search for the Bloomberg ticker or description")
    print("3. Find the correct RIC code")
    print("4. Update the mapping in refinitiv_config.py")

if __name__ == "__main__":
    main()