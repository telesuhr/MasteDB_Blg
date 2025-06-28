#!/usr/bin/env python3
"""
Bloomberg データ取得システム メインランナー
プロジェクトルートから実行するためのエントリーポイント
"""
import sys
import os

# プロジェクトルートとsrcディレクトリをPythonパスに追加
project_root = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.join(project_root, 'src')
sys.path.insert(0, project_root)
sys.path.insert(0, src_dir)

# メインモジュールをインポートして実行
if __name__ == "__main__":
    try:
        from src.main import main
        main()
    except ImportError as e:
        print(f"Import error: {e}")
        print("Trying alternative import method...")
        # 代替方法: srcディレクトリに移動してから実行
        original_dir = os.getcwd()
        os.chdir(src_dir)
        sys.path.insert(0, os.getcwd())
        try:
            from main import main
            main()
        finally:
            # 元のディレクトリに戻る
            os.chdir(original_dir)