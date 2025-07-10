"""
自動マッピング機能付きデータ取得スクリプト
どんなTradeDateのデータでも自動的に正しいマッピングが適用される
"""
import sys
import os
from datetime import datetime

# プロジェクトルートをPythonパスに追加
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)
sys.path.insert(0, os.path.join(project_root, 'src'))

from main_with_auto_mapping import BloombergSQLIngestorWithMapping
from loguru import logger


def fetch_data_with_auto_mapping(start_date: str, end_date: str, categories: list = None):
    """
    指定期間のデータを自動マッピング付きで取得
    
    Args:
        start_date: 開始日 (YYYY-MM-DD)
        end_date: 終了日 (YYYY-MM-DD) 
        categories: 取得するカテゴリのリスト（Noneの場合は全て）
    """
    logger.info(f"=== 自動マッピング付きデータ取得: {start_date} から {end_date} ===")
    
    # Ingestorの初期化
    ingestor = BloombergSQLIngestorWithMapping()
    
    try:
        # 接続
        if not ingestor._connect():
            logger.error("接続に失敗しました")
            return False
            
        # マスターデータのロード
        ingestor.db_manager.load_master_data()
        
        # Enhanced Data Processorの初期化
        from enhanced_data_processor_v2 import EnhancedDataProcessorV2
        ingestor.processor = EnhancedDataProcessorV2(ingestor.db_manager)
        
        # 日付フォーマット変換
        start_date_bloomberg = start_date.replace('-', '')
        end_date_bloomberg = end_date.replace('-', '')
        
        # カテゴリ指定
        from config.bloomberg_config import BLOOMBERG_TICKERS
        
        if categories is None:
            # デフォルトカテゴリ
            categories_to_process = [
                'LME_COPPER_PRICES',
                'SHFE_COPPER_PRICES',
                'CMX_COPPER_PRICES',
                'LME_INVENTORY',
                'SHFE_INVENTORY',
                'CMX_INVENTORY',
                'INTEREST_RATES',
                'CURRENCY_RATES',
                'COMMODITY_INDICES',
                'SHANGHAI_PREMIUM'
            ]
        else:
            categories_to_process = categories
            
        total_records = 0
        mapping_stats = {
            'total_generic': 0,
            'mapped': 0,
            'unmapped': 0
        }
        
        # 各カテゴリを処理
        for category_name in categories_to_process:
            if category_name not in BLOOMBERG_TICKERS:
                logger.warning(f"{category_name}は定義されていません")
                continue
                
            logger.info(f"\n{category_name}を処理中...")
            ticker_info = BLOOMBERG_TICKERS[category_name]
            
            # データ取得と処理
            record_count = ingestor.process_category(
                category_name,
                ticker_info,
                start_date_bloomberg,
                end_date_bloomberg
            )
            
            total_records += record_count
            logger.info(f"{category_name}: {record_count}件のレコードを保存")
            
            # 価格データの場合、マッピング統計を収集
            if ticker_info['table'] == 'T_CommodityPrice':
                # 最新の処理結果から統計を取得
                with ingestor.db_manager.get_connection() as conn:
                    cursor = conn.cursor()
                    # 統計情報は後で実装（スキーマ確認が必要）
                    # 一時的にスキップ
                    if False:
                        mapping_stats['total_generic'] += result[0]
                        mapping_stats['mapped'] += result[1]
                        
        mapping_stats['unmapped'] = mapping_stats['total_generic'] - mapping_stats['mapped']
        
        # サマリー表示
        logger.info("\n=== 処理完了サマリー ===")
        logger.info(f"合計レコード数: {total_records}")
        logger.info(f"ジェネリック先物: {mapping_stats['total_generic']}件")
        logger.info(f"  - マッピング済み: {mapping_stats['mapped']}件")
        logger.info(f"  - マッピングなし: {mapping_stats['unmapped']}件")
        
        if mapping_stats['unmapped'] > 0:
            logger.warning(f"\n{mapping_stats['unmapped']}件のマッピングが不足しています")
            logger.warning("マッピングデータの更新が必要かもしれません")
            
        return True
        
    except Exception as e:
        logger.error(f"エラーが発生しました: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False
        
    finally:
        ingestor._disconnect()


def main():
    """メインエントリーポイント"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='自動マッピング機能付きデータ取得スクリプト'
    )
    parser.add_argument(
        '--start-date',
        required=True,
        help='開始日 (YYYY-MM-DD形式)'
    )
    parser.add_argument(
        '--end-date',
        required=True,
        help='終了日 (YYYY-MM-DD形式)'
    )
    parser.add_argument(
        '--categories',
        nargs='+',
        help='取得するカテゴリ（スペース区切り）'
    )
    
    args = parser.parse_args()
    
    # 日付検証
    try:
        datetime.strptime(args.start_date, '%Y-%m-%d')
        datetime.strptime(args.end_date, '%Y-%m-%d')
    except ValueError:
        logger.error("日付はYYYY-MM-DD形式で指定してください")
        sys.exit(1)
        
    # 実行
    success = fetch_data_with_auto_mapping(
        args.start_date,
        args.end_date,
        args.categories
    )
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()