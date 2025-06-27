"""
ユーティリティ関数
"""
import time
from functools import wraps
from typing import Callable, Any, List
import pandas as pd
from datetime import datetime, date, timedelta
import sys
import os

# プロジェクトルートをPythonパスに追加
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from config.logging_config import logger
from config.database_config import MAX_RETRIES, RETRY_DELAY


def retry_on_error(max_retries: int = MAX_RETRIES, delay: int = RETRY_DELAY) -> Callable:
    """
    エラー時にリトライするデコレータ
    
    Args:
        max_retries: 最大リトライ回数
        delay: リトライ間隔（秒）
        
    Returns:
        Callable: デコレータ関数
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            last_exception = None
            
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    if attempt < max_retries - 1:
                        logger.warning(
                            f"Attempt {attempt + 1}/{max_retries} failed for {func.__name__}: {e}. "
                            f"Retrying in {delay} seconds..."
                        )
                        time.sleep(delay)
                    else:
                        logger.error(f"All {max_retries} attempts failed for {func.__name__}")
                        
            raise last_exception
            
        return wrapper
    return decorator
    

def validate_date_format(date_str: str) -> bool:
    """
    日付フォーマットを検証（YYYYMMDD形式）
    
    Args:
        date_str: 日付文字列
        
    Returns:
        bool: 有効な形式の場合True
    """
    try:
        datetime.strptime(date_str, '%Y%m%d')
        return True
    except ValueError:
        return False
        

def convert_date_format(date_obj: Any, target_format: str = '%Y%m%d') -> str:
    """
    様々な日付形式を指定フォーマットに変換
    
    Args:
        date_obj: 日付オブジェクト（datetime, date, pd.Timestamp, str）
        target_format: 目標フォーマット
        
    Returns:
        str: フォーマット済み日付文字列
    """
    if isinstance(date_obj, str):
        # 既に文字列の場合
        if validate_date_format(date_obj):
            return date_obj
        else:
            # 他の形式から変換を試みる
            for fmt in ['%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y', '%Y%m%d']:
                try:
                    dt = datetime.strptime(date_obj, fmt)
                    return dt.strftime(target_format)
                except ValueError:
                    continue
            raise ValueError(f"Unable to parse date string: {date_obj}")
            
    elif isinstance(date_obj, (datetime, date)):
        return date_obj.strftime(target_format)
        
    elif isinstance(date_obj, pd.Timestamp):
        return date_obj.strftime(target_format)
        
    else:
        raise TypeError(f"Unsupported date type: {type(date_obj)}")
        

def get_business_days(start_date: date, end_date: date) -> List[date]:
    """
    指定期間の営業日リストを取得
    
    Args:
        start_date: 開始日
        end_date: 終了日
        
    Returns:
        List[date]: 営業日のリスト
    """
    business_days = []
    current_date = start_date
    
    while current_date <= end_date:
        # 土日を除外（0=月曜日, 6=日曜日）
        if current_date.weekday() < 5:
            business_days.append(current_date)
        current_date += timedelta(days=1)
        
    return business_days
    

def chunk_list(lst: List[Any], chunk_size: int) -> List[List[Any]]:
    """
    リストを指定サイズのチャンクに分割
    
    Args:
        lst: 分割するリスト
        chunk_size: チャンクサイズ
        
    Returns:
        List[List[Any]]: チャンクのリスト
    """
    return [lst[i:i + chunk_size] for i in range(0, len(lst), chunk_size)]
    

def measure_execution_time(func: Callable) -> Callable:
    """
    関数の実行時間を測定するデコレータ
    
    Args:
        func: 測定対象の関数
        
    Returns:
        Callable: ラップされた関数
    """
    @wraps(func)
    def wrapper(*args, **kwargs) -> Any:
        start_time = time.time()
        
        try:
            result = func(*args, **kwargs)
            elapsed_time = time.time() - start_time
            logger.info(f"{func.__name__} completed in {elapsed_time:.2f} seconds")
            return result
            
        except Exception as e:
            elapsed_time = time.time() - start_time
            logger.error(f"{func.__name__} failed after {elapsed_time:.2f} seconds")
            raise
            
    return wrapper
    

def safe_float_conversion(value: Any, default: float = None) -> float:
    """
    安全にfloatに変換
    
    Args:
        value: 変換する値
        default: 変換失敗時のデフォルト値
        
    Returns:
        float: 変換された値またはデフォルト値
    """
    if pd.isna(value) or value is None or value == '':
        return default
        
    try:
        return float(value)
    except (ValueError, TypeError):
        logger.warning(f"Failed to convert {value} to float, using default {default}")
        return default
        

def safe_int_conversion(value: Any, default: int = None) -> int:
    """
    安全にintに変換
    
    Args:
        value: 変換する値
        default: 変換失敗時のデフォルト値
        
    Returns:
        int: 変換された値またはデフォルト値
    """
    if pd.isna(value) or value is None or value == '':
        return default
        
    try:
        # floatを経由してintに変換（小数点を含む文字列対応）
        return int(float(value))
    except (ValueError, TypeError):
        logger.warning(f"Failed to convert {value} to int, using default {default}")
        return default
        

def create_summary_report(data_counts: Dict[str, int]) -> str:
    """
    データ取得の要約レポートを作成
    
    Args:
        data_counts: テーブル名と件数の辞書
        
    Returns:
        str: 要約レポート文字列
    """
    total_records = sum(data_counts.values())
    
    report = f"\n{'='*50}\n"
    report += f"Data Ingestion Summary Report\n"
    report += f"Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
    report += f"{'='*50}\n\n"
    
    for table_name, count in sorted(data_counts.items()):
        report += f"{table_name:<30}: {count:>10,} records\n"
        
    report += f"\n{'Total':<30}: {total_records:>10,} records\n"
    report += f"{'='*50}\n"
    
    return report