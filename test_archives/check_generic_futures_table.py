"""
M_GenericFuturesテーブルの現在の状況確認とSHFE/COMEX追加
"""
import sys
import os
import pandas as pd

# プロジェクトルートとsrcディレクトリをPythonパスに追加
project_root = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.join(project_root, 'src')
sys.path.insert(0, project_root)
sys.path.insert(0, src_dir)

from database import DatabaseManager
from config.logging_config import logger

class GenericFuturesManager:
    """ジェネリック先物管理クラス"""
    
    def __init__(self):
        self.db_manager = DatabaseManager()
        
    def check_and_add_multi_exchange(self):
        """マルチ取引所対応の確認と追加"""
        logger.info("=== M_GenericFuturesテーブル確認・追加開始 ===")
        
        try:
            # データベース接続
            self.db_manager.connect()
            
            # 1. 現在の状況確認
            self._check_current_status()
            
            # 2. 金属IDの確認
            copper_metal_id = self._get_copper_metal_id()
            
            if copper_metal_id is None:
                logger.error("銅のMetalIDが見つかりません")
                return False
                
            # 3. SHFE銅先物の追加
            self._add_shfe_futures(copper_metal_id)
            
            # 4. COMEX銅先物の追加
            self._add_comex_futures(copper_metal_id)
            
            # 5. 追加後の状況確認
            self._check_final_status()
            
            logger.info("=== M_GenericFuturesテーブル確認・追加完了 ===")
            return True
            
        except Exception as e:
            logger.error(f"ジェネリック先物管理中にエラーが発生: {e}")
            return False
            
        finally:
            self.db_manager.disconnect()
            
    def _check_current_status(self):
        """現在の状況確認"""
        print("\n=== 現在のM_GenericFuturesテーブル状況 ===")
        
        with self.db_manager.get_connection() as conn:
            # 取引所別件数
            summary_query = """
                SELECT ExchangeCode, COUNT(*) as 件数
                FROM M_GenericFutures
                GROUP BY ExchangeCode
                ORDER BY ExchangeCode
            """
            summary_df = pd.read_sql(summary_query, conn)
            print("【取引所別件数】")
            print(summary_df.to_string(index=False))
            
            # 詳細確認（先頭10件）
            detail_query = """
                SELECT TOP 10 GenericID, GenericTicker, ExchangeCode, GenericNumber, MetalID
                FROM M_GenericFutures
                ORDER BY ExchangeCode, GenericNumber
            """
            detail_df = pd.read_sql(detail_query, conn)
            print("\n【詳細（先頭10件）】")
            print(detail_df.to_string(index=False))
            
    def _get_copper_metal_id(self):
        """銅のMetalIDを取得"""
        print("\n=== 金属マスターの確認 ===")
        
        with self.db_manager.get_connection() as conn:
            query = """
                SELECT MetalID, MetalCode, MetalName, MetalNameJP
                FROM M_Metal
                WHERE MetalCode IN ('CU', 'COPPER')
            """
            metal_df = pd.read_sql(query, conn)
            print("【銅のMetalID】")
            print(metal_df.to_string(index=False))
            
            if not metal_df.empty:
                copper_metal_id = metal_df.iloc[0]['MetalID']
                logger.info(f"銅のMetalID: {copper_metal_id}")
                return copper_metal_id
            else:
                return None
                
    def _add_shfe_futures(self, copper_metal_id):
        """SHFE銅先物の追加 (CU1-CU12)"""
        logger.info("SHFE銅先物の追加開始...")
        
        with self.db_manager.get_connection() as conn:
            cursor = conn.cursor()
            
            added_count = 0
            for i in range(1, 13):  # CU1-CU12
                generic_ticker = f'CU{i} Comdty'
                description = f'SHFE Copper Generic {i}{"st" if i == 1 else "nd" if i == 2 else "rd" if i == 3 else "th"} Future'
                
                # 既存チェック
                cursor.execute(
                    "SELECT COUNT(*) FROM M_GenericFutures WHERE GenericTicker = ?",
                    (generic_ticker,)
                )
                exists = cursor.fetchone()[0] > 0
                
                if not exists:
                    # 新規挿入
                    cursor.execute("""
                        INSERT INTO M_GenericFutures (
                            GenericTicker, MetalID, ExchangeCode, GenericNumber, 
                            Description, IsActive, CreatedAt
                        ) VALUES (?, ?, ?, ?, ?, ?, GETDATE())
                    """, (generic_ticker, copper_metal_id, 'SHFE', i, description, 1))
                    
                    logger.info(f"SHFE挿入: {generic_ticker}")
                    added_count += 1
                else:
                    logger.info(f"SHFE既存: {generic_ticker}")
            
            conn.commit()
            logger.info(f"SHFE銅先物追加完了: {added_count}件")
            
    def _add_comex_futures(self, copper_metal_id):
        """COMEX銅先物の追加 (HG1-HG26)"""
        logger.info("COMEX銅先物の追加開始...")
        
        with self.db_manager.get_connection() as conn:
            cursor = conn.cursor()
            
            added_count = 0
            for i in range(1, 27):  # HG1-HG26
                generic_ticker = f'HG{i} Comdty'
                description = f'COMEX Copper Generic {i}{"st" if i == 1 else "nd" if i == 2 else "rd" if i == 3 else "th"} Future'
                
                # 既存チェック
                cursor.execute(
                    "SELECT COUNT(*) FROM M_GenericFutures WHERE GenericTicker = ?",
                    (generic_ticker,)
                )
                exists = cursor.fetchone()[0] > 0
                
                if not exists:
                    # 新規挿入
                    cursor.execute("""
                        INSERT INTO M_GenericFutures (
                            GenericTicker, MetalID, ExchangeCode, GenericNumber, 
                            Description, IsActive, CreatedAt
                        ) VALUES (?, ?, ?, ?, ?, ?, GETDATE())
                    """, (generic_ticker, copper_metal_id, 'COMEX', i, description, 1))
                    
                    logger.info(f"COMEX挿入: {generic_ticker}")
                    added_count += 1
                else:
                    logger.info(f"COMEX既存: {generic_ticker}")
            
            conn.commit()
            logger.info(f"COMEX銅先物追加完了: {added_count}件")
            
    def _check_final_status(self):
        """追加後の状況確認"""
        print("\n=== 追加後のM_GenericFuturesテーブル状況 ===")
        
        with self.db_manager.get_connection() as conn:
            # 取引所別詳細サマリー
            summary_query = """
                SELECT 
                    ExchangeCode,
                    COUNT(*) as 件数,
                    MIN(GenericNumber) as 最小番号,
                    MAX(GenericNumber) as 最大番号
                FROM M_GenericFutures
                GROUP BY ExchangeCode
                ORDER BY ExchangeCode
            """
            summary_df = pd.read_sql(summary_query, conn)
            print("【取引所別詳細サマリー】")
            print(summary_df.to_string(index=False))
            
            # 各取引所の先頭3銘柄確認
            head_query = """
                SELECT GenericTicker, ExchangeCode, GenericNumber, Description
                FROM M_GenericFutures
                WHERE GenericNumber <= 3
                ORDER BY ExchangeCode, GenericNumber
            """
            head_df = pd.read_sql(head_query, conn)
            print("\n【各取引所の先頭3銘柄】")
            print(head_df.to_string(index=False))
            
            # 総件数
            total_query = "SELECT COUNT(*) as 総件数 FROM M_GenericFutures"
            total_df = pd.read_sql(total_query, conn)
            print(f"\n【総件数】: {total_df.iloc[0, 0]}件")
            
            # データ整合性確認
            print("\n=== データ整合性確認 ===")
            
            # 重複チェック
            dup_query = """
                SELECT GenericTicker, COUNT(*) as 重複数
                FROM M_GenericFutures
                GROUP BY GenericTicker
                HAVING COUNT(*) > 1
            """
            dup_df = pd.read_sql(dup_query, conn)
            if not dup_df.empty:
                print("【重複データあり】")
                print(dup_df.to_string(index=False))
            else:
                print("【重複データなし】: OK")
                
            # 無効なMetalIDチェック
            invalid_query = """
                SELECT gf.GenericID, gf.GenericTicker, gf.MetalID
                FROM M_GenericFutures gf
                LEFT JOIN M_Metal m ON gf.MetalID = m.MetalID
                WHERE m.MetalID IS NULL
            """
            invalid_df = pd.read_sql(invalid_query, conn)
            if not invalid_df.empty:
                print("【無効なMetalIDあり】")
                print(invalid_df.to_string(index=False))
            else:
                print("【MetalID整合性】: OK")

def main():
    """メイン実行関数"""
    logger.info("M_GenericFuturesテーブル確認・追加開始")
    
    manager = GenericFuturesManager()
    success = manager.check_and_add_multi_exchange()
    
    if success:
        print("\n" + "🎉 " * 20)
        print("🎉 マルチ取引所対応ジェネリック先物設定完了！")
        print("🎉 LME、SHFE、COMEX すべて対応済み")
        print("🎉 " * 20)
    else:
        logger.error("M_GenericFuturesテーブル設定失敗")
        
    return success

if __name__ == "__main__":
    main()