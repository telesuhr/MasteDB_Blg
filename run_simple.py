#!/usr/bin/env python3
"""
シンプルな実行スクリプト
パス問題を最小限に抑えた実行方法
"""
import sys
import os

# 現在のディレクトリをsrcに変更
script_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.join(script_dir, 'src')

# srcディレクトリに移動
os.chdir(src_dir)

# プロジェクトルートとsrcディレクトリをパスに追加
sys.path.insert(0, script_dir)  # プロジェクトルート
sys.path.insert(0, src_dir)     # srcディレクトリ

if __name__ == "__main__":
    # main.pyを直接実行
    exec(open('main.py').read())