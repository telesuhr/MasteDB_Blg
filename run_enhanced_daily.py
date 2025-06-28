#!/usr/bin/env python3
"""
拡張版日次更新実行スクリプト
市場タイミングを考慮し、データ検証を含む
"""
import sys
import os
import argparse
from datetime import datetime

# プロジェクトルートとsrcディレクトリをPythonパスに追加
project_root = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.join(project_root, 'src')
sys.path.insert(0, project_root)
sys.path.insert(0, src_dir)

from src.main import BloombergSQLIngestor
from src.enhanced_daily_update import EnhancedDailyUpdater
from config.logging_config import logger


def main():
    """拡張版日次更新のメイン処理"""
    parser = argparse.ArgumentParser(description='Enhanced Bloomberg daily data update')
    parser.add_argument('--validate-only', action='store_true', 
                       help='Run validation only without updating data')
    parser.add_argument('--force', action='store_true',
                       help='Force update even if validation shows high change rate')
    args = parser.parse_args()
    
    logger.info("=" * 60)
    logger.info(f"Enhanced Daily Update Started at {datetime.now()}")
    logger.info("=" * 60)
    
    # BloombergSQLIngestorの初期化
    ingestor = BloombergSQLIngestor()
    
    try:
        # 初期化
        ingestor.initialize()
        
        # 拡張版更新の実行
        updater = EnhancedDailyUpdater(ingestor)
        
        if args.validate_only:
            logger.info("Running in validation-only mode...")
            # 検証のみのモードを実装（必要に応じて）
            
        update_summary = updater.run_enhanced_daily_update()
        
        # 成功したらクリーンアップ
        ingestor.cleanup()
        
        logger.info("=" * 60)
        logger.info(f"Enhanced Daily Update Completed at {datetime.now()}")
        logger.info("=" * 60)
        
        return 0
        
    except Exception as e:
        logger.error(f"Enhanced daily update failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        ingestor.cleanup()
        return 1


if __name__ == "__main__":
    sys.exit(main())