#!/usr/bin/env python3
"""
Bloomberg データ取得システム メインランナー
プロジェクトルートから実行するためのエントリーポイント
"""
import sys
import os

# プロジェクトルートをPythonパスに追加
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)

# メインモジュールをインポートして実行
if __name__ == "__main__":
    from src.main import main
    main()