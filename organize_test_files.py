"""
testやcheckで始まるファイルを整理
"""
import os
import shutil
from datetime import datetime

def organize_test_files():
    """テストファイルの整理"""
    print("=== テスト・チェックファイルの整理 ===")
    print(f"実行時刻: {datetime.now()}\n")
    
    # 整理対象のパターン
    patterns = ['test_', 'check_', 'debug_', 'verify_', 'analyze_']
    
    # 現在のファイルリスト取得
    all_files = [f for f in os.listdir('.') if os.path.isfile(f)]
    
    # パターンに一致するファイルを分類
    test_files = []
    for file in all_files:
        for pattern in patterns:
            if file.startswith(pattern) and file.endswith('.py'):
                test_files.append(file)
                break
    
    print(f"対象ファイル数: {len(test_files)}")
    
    # ファイルを種類別に分類
    categorized = {
        'test': [],
        'check': [],
        'debug': [],
        'verify': [],
        'analyze': [],
        'other': []
    }
    
    for file in test_files:
        categorized_flag = False
        for category in ['test', 'check', 'debug', 'verify', 'analyze']:
            if file.startswith(category + '_'):
                categorized[category].append(file)
                categorized_flag = True
                break
        if not categorized_flag:
            categorized['other'].append(file)
    
    # カテゴリ別に表示
    print("\n【ファイル分類】")
    for category, files in categorized.items():
        if files:
            print(f"\n{category.upper()} ({len(files)}ファイル):")
            for file in sorted(files)[:5]:  # 最初の5つを表示
                print(f"  - {file}")
            if len(files) > 5:
                print(f"  ... 他 {len(files) - 5} ファイル")
    
    # 移動先ディレクトリ
    test_archive_dir = 'test_archives'
    if not os.path.exists(test_archive_dir):
        os.makedirs(test_archive_dir)
    
    # 重要なファイル（保持するもの）
    important_files = [
        'test_simple.py',  # 基本的なテストは残す
        'check_table_contents.py'  # テーブル確認用は残す
    ]
    
    # ファイル移動の確認
    print(f"\n【移動計画】")
    print(f"移動先: {test_archive_dir}/")
    print(f"移動予定: {len(test_files) - len(important_files)}ファイル")
    print(f"保持: {len(important_files)}ファイル")
    
    return test_files, important_files, test_archive_dir

def move_files(test_files, important_files, test_archive_dir):
    """ファイルを移動"""
    moved_count = 0
    error_count = 0
    
    for file in test_files:
        if file not in important_files:
            try:
                src = file
                dst = os.path.join(test_archive_dir, file)
                shutil.move(src, dst)
                moved_count += 1
            except Exception as e:
                print(f"エラー: {file} - {e}")
                error_count += 1
    
    print(f"\n【移動結果】")
    print(f"成功: {moved_count}ファイル")
    print(f"エラー: {error_count}ファイル")
    
    # 残ったファイルの確認
    remaining = []
    for file in test_files:
        if os.path.exists(file):
            remaining.append(file)
    
    if remaining:
        print(f"\n【保持されたファイル】")
        for file in remaining:
            print(f"  - {file}")

def cleanup_other_files():
    """その他の不要ファイルも整理"""
    print("\n\n=== その他のファイル整理 ===")
    
    # 追加で移動するファイル
    other_files = [
        'cleanup_and_commit.py',
        'historical_data_*.py',
        'load_*.py',
        'fix_*.py',
        'rebuild_*.py',
        'restore_*.py',
        'store_*.py'
    ]
    
    # ワイルドカード処理
    import glob
    files_to_move = []
    for pattern in other_files:
        files_to_move.extend(glob.glob(pattern))
    
    if files_to_move:
        print(f"\n追加移動対象: {len(files_to_move)}ファイル")
        for file in files_to_move[:10]:
            print(f"  - {file}")
        if len(files_to_move) > 10:
            print(f"  ... 他 {len(files_to_move) - 10} ファイル")
        
        # archiveに移動
        archive_dir = 'archived_scripts'
        if not os.path.exists(archive_dir):
            os.makedirs(archive_dir)
        
        moved = 0
        for file in files_to_move:
            try:
                shutil.move(file, os.path.join(archive_dir, file))
                moved += 1
            except:
                pass
        
        print(f"追加移動完了: {moved}ファイル")

def list_remaining_files():
    """整理後の状態を表示"""
    print("\n\n=== 整理後のファイル構成 ===")
    
    # ルートディレクトリのPythonファイル
    py_files = [f for f in os.listdir('.') if f.endswith('.py') and os.path.isfile(f)]
    
    print(f"\nルートディレクトリのPythonファイル ({len(py_files)}個):")
    for file in sorted(py_files):
        print(f"  - {file}")
    
    # ディレクトリ構成
    print("\n【ディレクトリ構成】")
    dirs = [d for d in os.listdir('.') if os.path.isdir(d) and not d.startswith('.')]
    for dir in sorted(dirs):
        file_count = len([f for f in os.listdir(dir) if os.path.isfile(os.path.join(dir, f))])
        print(f"  {dir}/ ({file_count}ファイル)")

def main():
    """メイン処理"""
    # 1. テストファイルの整理
    test_files, important_files, test_archive_dir = organize_test_files()
    
    # 自動で移動実行
    print("\n自動でファイルを移動します...")
    move_files(test_files, important_files, test_archive_dir)
    
    # 2. その他のファイルも整理
    cleanup_other_files()
    
    # 3. 整理後の状態表示
    list_remaining_files()
    
    print("\n=== 整理完了 ===")
    print("不要なtest/check/debugファイルをtest_archives/に移動しました")
    print("その他の作業ファイルをarchived_scripts/に移動しました")

if __name__ == "__main__":
    main()