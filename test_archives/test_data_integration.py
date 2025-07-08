"""
Phase 3最終: ジェネリック先物と実契約の価格データ連携確認
価格データ + マッピング + 実契約の統合クエリテスト
"""
import sys
import os
import pandas as pd
from datetime import datetime, date

# プロジェクトルートとsrcディレクトリをPythonパスに追加
project_root = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.join(project_root, 'src')
sys.path.insert(0, project_root)
sys.path.insert(0, src_dir)

from database import DatabaseManager
from config.logging_config import logger

class DataIntegrationTester:
    """データ連携テストクラス"""
    
    def __init__(self):
        self.db_manager = DatabaseManager()
        
    def test_data_integration(self):
        """データ連携の動作確認"""
        logger.info("=== データ連携テスト開始 ===")
        
        try:
            # データベース接続
            self.db_manager.connect()
            
            # 1. 基本的な連携クエリ
            self._test_basic_integration()
            
            # 2. スプレッド計算デモ
            self._test_spread_calculation()
            
            # 3. 残存日数分析
            self._test_days_to_expiry_analysis()
            
            # 4. 時系列分析
            self._test_time_series_analysis()
            
            logger.info("=== データ連携テスト完了 ===")
            return True
            
        except Exception as e:
            logger.error(f"データ連携テスト中にエラーが発生: {e}")
            return False
            
        finally:
            self.db_manager.disconnect()
            
    def _test_basic_integration(self):
        """基本的な連携クエリテスト"""
        print("\n" + "="*60)
        print("1. 基本的なデータ連携テスト")
        print("="*60)
        
        with self.db_manager.get_connection() as conn:
            query = """
                SELECT 
                    p.TradeDate,
                    g.GenericTicker as ジェネリック,
                    a.ContractTicker as 実契約,
                    a.ContractMonth as 契約月,
                    m.DaysToExpiry as 残存日数,
                    p.SettlementPrice as 決済価格,
                    p.Volume as 出来高,
                    p.OpenInterest as 建玉
                FROM T_CommodityPrice_V2 p
                JOIN M_GenericFutures g ON p.GenericID = g.GenericID
                JOIN T_GenericContractMapping m ON m.GenericID = g.GenericID 
                    AND m.TradeDate = p.TradeDate
                JOIN M_ActualContract a ON m.ActualContractID = a.ActualContractID
                WHERE p.DataType = 'Generic'
                    AND p.TradeDate = '2025-07-07'  -- 最新日
                ORDER BY g.GenericNumber
            """
            
            df = pd.read_sql(query, conn)
            print(df.to_string(index=False))
            
    def _test_spread_calculation(self):
        """スプレッド計算デモ"""
        print("\n" + "="*60)
        print("2. LP1-LP2スプレッド計算 (Jul25-Aug25)")
        print("="*60)
        
        with self.db_manager.get_connection() as conn:
            query = """
                SELECT 
                    p1.TradeDate,
                    g1.GenericTicker + ' - ' + g2.GenericTicker as スプレッド名,
                    a1.ContractTicker + ' - ' + a2.ContractTicker as 実契約スプレッド,
                    FORMAT(a1.ContractMonth, 'MMM-yy') + ' / ' + FORMAT(a2.ContractMonth, 'MMM-yy') as 限月,
                    p1.SettlementPrice as LP1価格,
                    p2.SettlementPrice as LP2価格,
                    (p1.SettlementPrice - p2.SettlementPrice) as スプレッド値,
                    (m1.DaysToExpiry - m2.DaysToExpiry) as 期間差,
                    m1.DaysToExpiry as LP1残存日数,
                    m2.DaysToExpiry as LP2残存日数
                FROM T_CommodityPrice_V2 p1
                JOIN T_CommodityPrice_V2 p2 ON p1.TradeDate = p2.TradeDate 
                    AND p1.MetalID = p2.MetalID
                JOIN M_GenericFutures g1 ON p1.GenericID = g1.GenericID
                JOIN M_GenericFutures g2 ON p2.GenericID = g2.GenericID
                JOIN T_GenericContractMapping m1 ON m1.GenericID = g1.GenericID 
                    AND m1.TradeDate = p1.TradeDate
                JOIN T_GenericContractMapping m2 ON m2.GenericID = g2.GenericID 
                    AND m2.TradeDate = p2.TradeDate
                JOIN M_ActualContract a1 ON m1.ActualContractID = a1.ActualContractID
                JOIN M_ActualContract a2 ON m2.ActualContractID = a2.ActualContractID
                WHERE p1.DataType = 'Generic' 
                    AND p2.DataType = 'Generic'
                    AND g1.GenericNumber = 1  -- LP1
                    AND g2.GenericNumber = 2  -- LP2
                    AND p1.SettlementPrice IS NOT NULL 
                    AND p2.SettlementPrice IS NOT NULL
                ORDER BY p1.TradeDate DESC
            """
            
            df = pd.read_sql(query, conn)
            print(df.to_string(index=False))
            
    def _test_days_to_expiry_analysis(self):
        """残存日数分析"""
        print("\n" + "="*60)
        print("3. 残存日数と価格関係分析")
        print("="*60)
        
        with self.db_manager.get_connection() as conn:
            query = """
                SELECT 
                    g.GenericTicker,
                    a.ContractTicker,
                    FORMAT(a.ContractMonth, 'yyyy-MM') as 契約月,
                    m.DaysToExpiry as 残存日数,
                    AVG(p.SettlementPrice) as 平均価格,
                    COUNT(*) as データ件数,
                    MIN(p.TradeDate) as 開始日,
                    MAX(p.TradeDate) as 終了日
                FROM T_CommodityPrice_V2 p
                JOIN M_GenericFutures g ON p.GenericID = g.GenericID
                JOIN T_GenericContractMapping m ON m.GenericID = g.GenericID 
                    AND m.TradeDate = p.TradeDate
                JOIN M_ActualContract a ON m.ActualContractID = a.ActualContractID
                WHERE p.DataType = 'Generic'
                    AND p.SettlementPrice IS NOT NULL
                GROUP BY g.GenericTicker, g.GenericNumber, a.ContractTicker, 
                         a.ContractMonth, m.DaysToExpiry
                ORDER BY g.GenericNumber, m.DaysToExpiry
            """
            
            df = pd.read_sql(query, conn)
            print(df.to_string(index=False))
            
    def _test_time_series_analysis(self):
        """時系列分析"""
        print("\n" + "="*60)
        print("4. LP1の時系列価格推移 (実契約情報付き)")
        print("="*60)
        
        with self.db_manager.get_connection() as conn:
            query = """
                SELECT 
                    p.TradeDate,
                    g.GenericTicker,
                    a.ContractTicker as 対応実契約,
                    FORMAT(a.LastTradeableDate, 'MM/dd') as 最終取引日,
                    m.DaysToExpiry as 残存日数,
                    p.SettlementPrice as 決済価格,
                    LAG(p.SettlementPrice) OVER (ORDER BY p.TradeDate) as 前日価格,
                    p.SettlementPrice - LAG(p.SettlementPrice) OVER (ORDER BY p.TradeDate) as 日次変動,
                    p.Volume as 出来高
                FROM T_CommodityPrice_V2 p
                JOIN M_GenericFutures g ON p.GenericID = g.GenericID
                JOIN T_GenericContractMapping m ON m.GenericID = g.GenericID 
                    AND m.TradeDate = p.TradeDate
                JOIN M_ActualContract a ON m.ActualContractID = a.ActualContractID
                WHERE p.DataType = 'Generic'
                    AND g.GenericNumber = 1  -- LP1のみ
                    AND p.SettlementPrice IS NOT NULL
                ORDER BY p.TradeDate
            """
            
            df = pd.read_sql(query, conn)
            print(df.to_string(index=False))
            
    def _show_system_summary(self):
        """システム全体サマリー表示"""
        print("\n" + "="*60)
        print("📊 システム全体サマリー")
        print("="*60)
        
        with self.db_manager.get_connection() as conn:
            # データ件数確認
            summary_queries = {
                "ジェネリック先物マスター": "SELECT COUNT(*) as 件数 FROM M_GenericFutures",
                "実契約マスター": "SELECT COUNT(*) as 件数 FROM M_ActualContract", 
                "マッピングデータ": "SELECT COUNT(*) as 件数 FROM T_GenericContractMapping",
                "価格データ": "SELECT COUNT(*) as 件数 FROM T_CommodityPrice_V2"
            }
            
            for name, query in summary_queries.items():
                count = pd.read_sql(query, conn).iloc[0, 0]
                print(f"{name}: {count:,}件")

def main():
    """メイン実行関数"""
    logger.info("データ連携テスト開始")
    
    tester = DataIntegrationTester()
    success = tester.test_data_integration()
    
    if success:
        tester._show_system_summary()
        print("\n" + "🎉 " * 20)
        print("🎉 Phase 1-3 完全成功！新しい先物データ管理システムが完成しました！")
        print("🎉 " * 20)
        
        print("\n📈 次に可能な分析:")
        print("- テナースプレッド分析 (LP1-LP2, LP2-LP3)")
        print("- ロールオーバー影響分析")
        print("- 残存日数と価格変動の関係")
        print("- フォワードカーブ構築")
        print("- 取引量・建玉分析")
        
    else:
        logger.error("データ連携テスト失敗")
        
    return success

if __name__ == "__main__":
    main()