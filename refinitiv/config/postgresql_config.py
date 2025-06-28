"""
PostgreSQL データベース接続設定
"""
import os
from typing import Dict, Any

# PostgreSQL接続設定
POSTGRESQL_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'localhost'),
    'port': os.getenv('POSTGRES_PORT', 5432),
    'database': os.getenv('POSTGRES_DB', 'refinitiv_data'),
    'username': os.getenv('POSTGRES_USER', 'postgres'),
    'password': os.getenv('POSTGRES_PASSWORD', 'your_password_here'),
    'schema': os.getenv('POSTGRES_SCHEMA', 'public')
}

# SQLAlchemy接続URL
def get_connection_url() -> str:
    """PostgreSQL接続URLを生成"""
    config = POSTGRESQL_CONFIG
    return (f"postgresql://{config['username']}:{config['password']}"
            f"@{config['host']}:{config['port']}/{config['database']}")

# pyodbc接続文字列（必要に応じて）
def get_connection_string() -> str:
    """PostgreSQL接続文字列を生成"""
    config = POSTGRESQL_CONFIG
    return (f"host={config['host']} "
            f"port={config['port']} "
            f"dbname={config['database']} "
            f"user={config['username']} "
            f"password={config['password']}")

# 接続プール設定
CONNECTION_POOL_CONFIG = {
    'pool_size': 5,
    'max_overflow': 10,
    'pool_timeout': 30,
    'pool_recycle': 3600
}

# デバッグ設定
DEBUG_CONFIG = {
    'echo_sql': os.getenv('DEBUG_SQL', 'false').lower() == 'true',
    'log_queries': True,
    'log_slow_queries': True,
    'slow_query_threshold': 1.0  # seconds
}