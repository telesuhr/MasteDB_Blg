"""
Refinitiv システム用ログ設定
"""
import logging
import os
from datetime import datetime
import sys

# ログディレクトリの作成
log_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'logs')
os.makedirs(log_dir, exist_ok=True)

# ログファイル名（日付付き）
log_filename = f"refinitiv_ingestion_{datetime.now().strftime('%Y%m%d')}.log"
log_filepath = os.path.join(log_dir, log_filename)

# ログフォーマット
LOG_FORMAT = '%(asctime)s | %(levelname)s | %(name)s | %(message)s'
DATE_FORMAT = '%Y-%m-%d %H:%M:%S'

# ログレベル設定
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO').upper()

# ログ設定の初期化
def setup_logger(name='refinitiv', level=LOG_LEVEL):
    """ログ設定の初期化"""
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level))
    
    # 重複ハンドラーの防止
    if logger.handlers:
        return logger
    
    # フォーマッターの作成
    formatter = logging.Formatter(LOG_FORMAT, DATE_FORMAT)
    
    # ファイルハンドラー
    file_handler = logging.FileHandler(log_filepath, encoding='utf-8')
    file_handler.setLevel(getattr(logging, level))
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    # コンソールハンドラー
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(getattr(logging, level))
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    return logger

# デフォルトロガー
logger = setup_logger()

# 使用例:
# from config.logging_config import logger
# logger.info("This is an info message")