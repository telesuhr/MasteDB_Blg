"""
ロギング設定
"""
import os
import sys
from loguru import logger
from datetime import datetime

# ログディレクトリの作成
LOG_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'logs')
os.makedirs(LOG_DIR, exist_ok=True)

# ログファイル名（日付付き）
LOG_FILENAME = os.path.join(LOG_DIR, f"bloomberg_ingestion_{datetime.now().strftime('%Y%m%d')}.log")

# ロガーの設定
def setup_logger():
    """
    ロガーをセットアップ
    """
    # 既存のハンドラーをクリア
    logger.remove()
    
    # コンソール出力（INFO以上）
    logger.add(
        sys.stdout,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        level="INFO",
        colorize=True
    )
    
    # ファイル出力（DEBUG以上）
    logger.add(
        LOG_FILENAME,
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
        level="DEBUG",
        rotation="100 MB",
        retention="30 days",
        compression="zip",
        encoding="utf-8"
    )
    
    # エラーログ専用ファイル
    logger.add(
        os.path.join(LOG_DIR, "errors.log"),
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
        level="ERROR",
        rotation="50 MB",
        retention="60 days",
        encoding="utf-8"
    )
    
    return logger

# ロガーの初期化
logger = setup_logger()