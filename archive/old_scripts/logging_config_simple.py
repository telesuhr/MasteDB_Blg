"""
シンプルなロギング設定（標準ライブラリ使用）
loguruがインストールされていない場合の代替
"""
import logging
import os
from datetime import datetime

# ログディレクトリの作成
LOG_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'logs')
os.makedirs(LOG_DIR, exist_ok=True)

# ログファイル名（日付付き）
LOG_FILENAME = os.path.join(LOG_DIR, f"bloomberg_ingestion_{datetime.now().strftime('%Y%m%d')}.log")

def setup_logger():
    """
    標準ライブラリでロガーをセットアップ
    """
    # ロガーの作成
    logger = logging.getLogger('bloomberg_ingestor')
    logger.setLevel(logging.DEBUG)
    
    # 既存のハンドラーをクリア
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # フォーマッターの作成
    formatter = logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(name)s:%(funcName)s:%(lineno)d - %(message)s'
    )
    
    # コンソールハンドラー
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # ファイルハンドラー
    file_handler = logging.FileHandler(LOG_FILENAME, encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    # エラーログ専用ファイル
    error_handler = logging.FileHandler(
        os.path.join(LOG_DIR, "errors.log"), 
        encoding='utf-8'
    )
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(formatter)
    logger.addHandler(error_handler)
    
    return logger

# ロガーの初期化
logger = setup_logger()