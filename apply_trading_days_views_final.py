"""
満期ビューを営業日計算対応に更新（最終版）
"""
import pyodbc
import pandas as pd
from datetime import datetime
from config.logging_config import logger

# データベース接続文字列
CONNECTION_STRING = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=jcz.database.windows.net;"
    "DATABASE=JCL;"
    "UID=TKJCZ01;"
    "PWD=P@ssw0rdmbkazuresql;"
    "Encrypt=yes;"
    "TrustServerCertificate=no;"
    "Connection Timeout=30;"
)

def apply_views():
    """営業日計算対応のビューを適用"""
    logger.info("=== 満期ビューを営業日計算対応に更新（最終版） ===")
    
    conn = pyodbc.connect(CONNECTION_STRING)
    cursor = conn.cursor()
    
    try:
        # SQLファイルを読み込み
        with open('sql/update_maturity_views_fixed.sql', 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # GOステートメントで分割して実行
        sql_statements = sql_content.split('\nGO\n')
        
        success_count = 0
        for i, stmt in enumerate(sql_statements):
            if stmt.strip():
                logger.info(f"\nステートメント {i+1} を実行中...")
                try:
                    cursor.execute(stmt)
                    conn.commit()
                    logger.info("   ✓ 完了")
                    success_count += 1
                except Exception as e:
                    logger.error(f"   ✗ エラー: {e}")
                    conn.rollback()
        
        logger.info(f"\n=== {success_count}/{len([s for s in sql_statements if s.strip()])} ステートメント成功 ===")
        
    except Exception as e:
        logger.error(f"ビュー更新エラー: {e}")
        conn.rollback()
        
    finally:
        conn.close()

def verify_views_created():
    """ビューが正しく作成されたか確認"""
    logger.info("\n=== ビュー作成確認 ===")
    
    conn = pyodbc.connect(CONNECTION_STRING)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT name 
        FROM sys.views 
        WHERE name IN (
            'V_CommodityPriceWithMaturityEx',
            'V_MaturitySummaryWithTradingDays',
            'V_RolloverAlertsWithTradingDays',
            'V_TradingDaysCalculationDetail'
        )
        ORDER BY name
    """)
    
    created_views = [row[0] for row in cursor.fetchall()]
    logger.info(f"作成されたビュー: {', '.join(created_views)}")
    
    conn.close()
    return len(created_views) == 4

def test_trading_days_views():
    """営業日計算ビューのテスト"""
    logger.info("\n=== 営業日計算ビューのテスト ===")
    
    conn = pyodbc.connect(CONNECTION_STRING)
    
    try:
        # 1. 満期情報サマリー
        query1 = """
        SELECT *
        FROM V_MaturitySummaryWithTradingDays
        ORDER BY ExchangeCode
        """
        
        df1 = pd.read_sql(query1, conn)
        logger.info("\n【取引所別満期情報サマリー（営業日ベース）】")
        pd.set_option('display.max_columns', None)
        print(df1.to_string(index=False))
        
        # 2. 具体的な営業日計算例
        query2 = """
        SELECT TOP 10
            GenericTicker,
            FORMAT(TradeDate, 'MM/dd') as 取引日,
            FORMAT(LastTradeableDate, 'MM/dd') as 最終取引日,
            CalendarDaysRemaining as 暦日,
            TradingDaysRemaining as 営業日,
            HolidaysInPeriod as 休日,
            TradingDayRate as '営業日率(%)',
            Volume
        FROM V_CommodityPriceWithMaturityEx
        WHERE TradeDate = (SELECT MAX(TradeDate) FROM V_CommodityPriceWithMaturityEx)
            AND TradingDaysRemaining IS NOT NULL
            AND Volume > 0
        ORDER BY TradingDaysRemaining
        """
        
        df2 = pd.read_sql(query2, conn)
        logger.info("\n\n【営業日計算の具体例（最も近い10件）】")
        print(df2.to_string(index=False))
        
        # 3. 計算詳細
        query3 = """
        SELECT TOP 5 *
        FROM V_TradingDaysCalculationDetail
        ORDER BY TradingDaysRemaining
        """
        
        df3 = pd.read_sql(query3, conn)
        logger.info("\n\n【営業日計算の詳細説明】")
        print(df3.to_string(index=False))
        
        # 4. ロールオーバー警告
        query4 = """
        SELECT TOP 10
            ExchangeCode,
            GenericTicker,
            TradingDaysRemaining as '残営業日',
            TradingDaysToRollover as 'ロール推奨まで',
            RolloverStatus as 'ステータス',
            StatusMessage as 'メッセージ'
        FROM V_RolloverAlertsWithTradingDays
        ORDER BY TradingDaysToRollover
        """
        
        df4 = pd.read_sql(query4, conn)
        logger.info("\n\n【ロールオーバー警告（営業日ベース）】")
        print(df4.to_string(index=False))
        
        # 5. 営業日率の分析
        query5 = """
        SELECT 
            ExchangeCode,
            COUNT(*) as 銘柄数,
            AVG(CalendarDaysRemaining) as 平均暦日,
            AVG(TradingDaysRemaining) as 平均営業日,
            AVG(TradingDayRate) as '平均営業日率(%)',
            MIN(TradingDayRate) as '最小営業日率(%)',
            MAX(TradingDayRate) as '最大営業日率(%)'
        FROM V_CommodityPriceWithMaturityEx
        WHERE TradeDate = (SELECT MAX(TradeDate) FROM V_CommodityPriceWithMaturityEx)
            AND TradingDaysRemaining IS NOT NULL
            AND CalendarDaysRemaining > 0
            AND Volume > 0
        GROUP BY ExchangeCode
        ORDER BY ExchangeCode
        """
        
        df5 = pd.read_sql(query5, conn)
        logger.info("\n\n【取引所別営業日率統計】")
        print(df5.to_string(index=False))
        
    except Exception as e:
        logger.error(f"テストエラー: {e}")
        
    finally:
        conn.close()

def main():
    """メイン処理"""
    logger.info(f"実行開始時刻: {datetime.now()}")
    
    try:
        # 1. ビュー適用
        apply_views()
        
        # 2. 作成確認
        if verify_views_created():
            # 3. テスト実行
            test_trading_days_views()
            logger.info("\n=== 営業日計算対応完了 ===")
        else:
            logger.error("一部のビューが作成されませんでした")
        
    except Exception as e:
        logger.error(f"処理失敗: {e}")
        
    logger.info(f"\n実行終了時刻: {datetime.now()}")

if __name__ == "__main__":
    main()