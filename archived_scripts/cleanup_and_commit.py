"""
フォルダ整理とGitコミット準備
"""
import os
import shutil
from datetime import datetime

def cleanup_folder():
    """作業フォルダの整理"""
    print("=== フォルダ整理開始 ===")
    
    # 1. 一時ファイル・テストファイルを移動
    temp_files = [
        # データベース操作関連（完了済み）
        'rename_commodity_price_tables.py',
        'cleanup_database_objects.py',
        'delete_only_unused_tables.py',
        'verify_rename_result.py',
        'check_rename_status.py',
        'final_cleanup.py',
        'verify_final_state.py',
        'list_unused_objects.py',
        'list_unused_objects_fixed.py',
        
        # V2移行関連（完了済み）
        'update_main_to_v2.py',
        'database_v2_methods.txt',
        
        # 営業日計算テスト（完了済み）
        'test_trading_days_calendar.py',
        'test_long_term_analysis.py',
        'test_final_trading_days.py',
        'test_final_trading_days_simple.py',
        'fix_trading_days_calculation.py',
        
        # LME特殊データ関連（完了済み）
        'add_lme_cash_spread_data.py',
        'fix_lme_special_tickers.py',
        'load_lme_special_manually.py',
        'reload_all_lme_data.py',
        'verify_lme_cash_ticker.py',
        'check_lme_special_data.py',
        'lme_special_data_summary.py',
        'lme_data_final_summary.py',
        
        # その他のテストファイル
        'check_table_columns.py',
        'check_current_structure.py',
        'summary_trading_days_implementation.md'
    ]
    
    # archivedフォルダを作成
    archive_dir = 'archived_scripts'
    if not os.path.exists(archive_dir):
        os.makedirs(archive_dir)
    
    moved_count = 0
    for file in temp_files:
        if os.path.exists(file):
            try:
                shutil.move(file, os.path.join(archive_dir, file))
                moved_count += 1
                print(f"移動: {file}")
            except Exception as e:
                print(f"エラー: {file} - {e}")
    
    print(f"\n{moved_count}個のファイルをarchivedに移動しました")
    
    # 2. 重要なファイルの確認
    print("\n【重要ファイル（保持）】")
    important_files = [
        'run.py',
        'final_optimized_loader.py',
        'daily_update_multi_exchange_simple.py',
        'expand_trading_calendar.py',
        'fetch_bloomberg_maturity_dates.py',
        'apply_trading_days_views_final.py',
        'CLAUDE.md',
        'requirements.txt',
        'requirements_no_blpapi.txt'
    ]
    
    for file in important_files:
        if os.path.exists(file):
            print(f"✓ {file}")
        else:
            print(f"✗ {file} (見つかりません)")
    
    # 3. SQLファイルの整理
    print("\n【SQLファイル】")
    sql_dir = 'sql'
    if os.path.exists(sql_dir):
        sql_files = [f for f in os.listdir(sql_dir) if f.endswith('.sql')]
        print(f"{len(sql_files)}個のSQLファイル")
        
        # 不要なSQLファイルを移動
        old_sql = ['cleanup_old_table.sql', 'delete_unused_objects.sql']
        for file in old_sql:
            if file in sql_files:
                src = os.path.join(sql_dir, file)
                dst = os.path.join(archive_dir, file)
                shutil.move(src, dst)
                print(f"移動: sql/{file}")

def prepare_git_commit():
    """Gitコミット準備"""
    print("\n=== Gitコミット準備 ===")
    
    # .gitignoreの確認
    gitignore_content = """# Python
__pycache__/
*.py[cod]
*$py.class
*.so
.Python
env/
venv/
.env

# IDE
.vscode/
.idea/
*.swp
*.swo

# Logs
logs/
*.log

# Database
*.db
*.sqlite

# Temporary files
archived_scripts/
temp/
*.tmp
*.bak

# Config with sensitive data
config/database_config_local.py

# OS
.DS_Store
Thumbs.db

# Project specific
PreviousDataUpdateFile/
analysis/
*.code-workspace
"""
    
    with open('.gitignore', 'w', encoding='utf-8') as f:
        f.write(gitignore_content)
    print("✓ .gitignore を更新")
    
    # README.mdの作成
    readme_content = """# MasteDB_Blg - Bloomberg データ取得システム

## 概要
Bloomberg APIから金融市場データを取得し、Azure SQL Serverに格納するシステム。

## 主な機能
- LME/SHFE/COMEX の銅先物価格取得
- 在庫データ、市場指標の取得
- 営業日ベースの満期計算
- 2000-2039年の取引カレンダー

## 使用方法

### 初回実行（ヒストリカルデータ取得）
```bash
python run.py --mode initial
```

### 日次更新
```bash
python run.py --mode daily
```

## 重要な変更（2025-07-08）
1. T_CommodityPrice_V2 → T_CommodityPrice にリネーム
   - 新しいGenericID構造を採用
   - 旧データは削除済み

2. 営業日計算機能の追加
   - M_TradingCalendar テーブル（2000-2039年）
   - GetTradingDaysBetween 関数
   - V_CommodityPriceWithMaturityEx ビュー

3. LME特殊データの追加
   - LMCADY Comdty (Cash価格)
   - LMCADS03 Comdty (3M価格)
   - LMCADS 0003 Comdty (Cash/3Mスプレッド)

4. 削除されたテーブル
   - M_ActualContract
   - T_GenericContractMapping

## 依存関係
- Python 3.8+
- Bloomberg Terminal (BBComm実行中)
- Azure SQL Server
- 詳細は requirements.txt 参照
"""
    
    with open('README.md', 'w', encoding='utf-8') as f:
        f.write(readme_content)
    print("✓ README.md を作成")

def main():
    """メイン処理"""
    print(f"実行時刻: {datetime.now()}\n")
    
    # 1. フォルダ整理
    cleanup_folder()
    
    # 2. Git準備
    prepare_git_commit()
    
    print("\n=== 次のステップ ===")
    print("1. git add .")
    print("2. git commit -m \"Major update: T_CommodityPrice V2 migration, trading calendar implementation\"")
    print("3. git push origin main")
    print("\n重要な変更:")
    print("- T_CommodityPriceが新構造（GenericID使用）に移行")
    print("- 営業日カレンダー機能追加")
    print("- LME特殊データ（Cash/3M/Spread）追加")
    print("- 不要テーブル削除（M_ActualContract, T_GenericContractMapping）")

if __name__ == "__main__":
    main()