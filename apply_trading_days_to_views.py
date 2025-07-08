"""
満期ビューを営業日計算対応に更新
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

def apply_trading_days_views():
    """営業日計算対応のビューを適用"""
    logger.info("=== 満期ビューを営業日計算対応に更新 ===")
    
    conn = pyodbc.connect(CONNECTION_STRING)
    cursor = conn.cursor()
    
    try:
        # SQLファイルを読み込み
        with open('sql/update_maturity_views_with_trading_days.sql', 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # GOステートメントで分割して実行
        sql_statements = sql_content.split('\nGO\n')
        
        for i, stmt in enumerate(sql_statements):
            if stmt.strip():
                logger.info(f"\nステートメント {i+1} を実行中...")
                try:
                    cursor.execute(stmt)
                    conn.commit()
                    logger.info("   完了")
                except Exception as e:
                    logger.error(f"   エラー: {e}")
                    conn.rollback()
        
        logger.info("\n=== ビュー更新完了 ===")
        
    except Exception as e:
        logger.error(f"ビュー更新エラー: {e}")
        conn.rollback()
        
    finally:
        conn.close()

def test_updated_views():
    """更新されたビューのテスト"""
    logger.info("\n=== 営業日計算対応ビューのテスト ===")
    
    conn = pyodbc.connect(CONNECTION_STRING)
    
    # 1. 満期情報サマリー（営業日ベース）
    query1 = """
    SELECT *
    FROM V_MaturitySummaryWithTradingDays
    ORDER BY ExchangeCode
    """
    
    df1 = pd.read_sql(query1, conn)
    logger.info("\n【取引所別満期情報サマリー（営業日ベース）】")
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    print(df1.to_string(index=False))
    
    # 2. ロールオーバー警告（営業日ベース）
    query2 = """
    SELECT TOP 20 *
    FROM V_RolloverAlertsWithTradingDays
    ORDER BY TradingDaysToRollover
    """
    
    df2 = pd.read_sql(query2, conn)
    logger.info("\n\n【ロールオーバー警告（営業日ベース）】")
    print(df2.to_string(index=False))
    
    # 3. 営業日計算詳細
    query3 = """
    SELECT TOP 10 *
    FROM V_TradingDaysCalculationDetail
    ORDER BY TradingDaysRemaining
    """
    
    df3 = pd.read_sql(query3, conn)
    logger.info("\n\n【営業日計算の詳細】")
    print(df3.to_string(index=False))
    
    # 4. 暦日と営業日の比較
    query4 = """
    SELECT TOP 15
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
    
    df4 = pd.read_sql(query4, conn)
    logger.info("\n\n【暦日vs営業日の比較（最新取引日）】")
    print(df4.to_string(index=False))
    
    # 5. 長期契約の営業日分析
    query5 = """
    SELECT 
        ExchangeCode,
        CASE 
            WHEN CalendarDaysRemaining <= 30 THEN '1ヶ月以内'
            WHEN CalendarDaysRemaining <= 90 THEN '3ヶ月以内'
            WHEN CalendarDaysRemaining <= 180 THEN '6ヶ月以内'
            WHEN CalendarDaysRemaining <= 365 THEN '1年以内'
            ELSE '1年超'
        END as 期間区分,
        COUNT(*) as 契約数,
        AVG(CalendarDaysRemaining) as 平均暦日,
        AVG(TradingDaysRemaining) as 平均営業日,
        AVG(TradingDayRate) as '平均営業日率(%)'
    FROM V_CommodityPriceWithMaturityEx
    WHERE TradeDate = (SELECT MAX(TradeDate) FROM V_CommodityPriceWithMaturityEx)
        AND TradingDaysRemaining IS NOT NULL
        AND Volume > 0
    GROUP BY 
        ExchangeCode,
        CASE 
            WHEN CalendarDaysRemaining <= 30 THEN '1ヶ月以内'
            WHEN CalendarDaysRemaining <= 90 THEN '3ヶ月以内'
            WHEN CalendarDaysRemaining <= 180 THEN '6ヶ月以内'
            WHEN CalendarDaysRemaining <= 365 THEN '1年以内'
            ELSE '1年超'
        END
    ORDER BY ExchangeCode, 平均暦日
    """
    
    df5 = pd.read_sql(query5, conn)
    logger.info("\n\n【期間別営業日率分析】")
    print(df5.to_string(index=False))
    
    conn.close()

def verify_calculation_accuracy():
    """計算精度の検証"""
    logger.info("\n=== 営業日計算精度検証 ===")
    
    conn = pyodbc.connect(CONNECTION_STRING)
    cursor = conn.cursor()
    
    # 特定の銘柄で手動計算と比較
    cursor.execute("""
        -- 最も近い満期の銘柄を選択
        SELECT TOP 1
            GenericTicker,
            TradeDate,
            LastTradeableDate,
            CalendarDaysRemaining,
            TradingDaysRemaining,
            HolidaysInPeriod
        FROM V_CommodityPriceWithMaturityEx
        WHERE TradeDate = (SELECT MAX(TradeDate) FROM V_CommodityPriceWithMaturityEx)
            AND TradingDaysRemaining IS NOT NULL
            AND Volume > 0
        ORDER BY TradingDaysRemaining
    """)
    
    result = cursor.fetchone()
    if result:
        ticker, trade_date, last_tradeable, cal_days, trading_days, holidays = result
        
        logger.info(f"\n【{ticker} の計算検証】")
        logger.info(f"取引日: {trade_date}")
        logger.info(f"最終取引可能日: {last_tradeable}")
        logger.info(f"暦日: {cal_days}日")
        logger.info(f"営業日: {trading_days}日")
        logger.info(f"休日: {holidays}日")
        logger.info(f"計算式検証: {cal_days} = {trading_days} + {holidays} → {trading_days + holidays}")
        
        if cal_days == trading_days + holidays:
            logger.info("✓ 計算が正しく行われています")
        else:
            logger.error("✗ 計算に誤りがあります")
    
    # 年間営業日率の確認
    cursor.execute("""
        SELECT 
            ExchangeCode,
            YEAR(CalendarDate) as Year,
            COUNT(*) as TotalDays,
            SUM(CAST(IsTradingDay as INT)) as TradingDays,
            CAST(SUM(CAST(IsTradingDay as INT)) * 100.0 / COUNT(*) as DECIMAL(5,2)) as TradingDayRate
        FROM M_TradingCalendar
        WHERE YEAR(CalendarDate) = 2025
        GROUP BY ExchangeCode, YEAR(CalendarDate)
        ORDER BY ExchangeCode
    """)
    
    logger.info("\n\n【2025年の年間営業日率】")
    for row in cursor.fetchall():
        logger.info(f"{row[0]}: {row[3]}営業日 / {row[2]}日 = {row[4]}%")
    
    conn.close()

def main():
    """メイン処理"""
    logger.info(f"実行開始時刻: {datetime.now()}")
    
    try:
        # 1. ビュー更新
        apply_trading_days_views()
        
        # 2. テスト実行
        test_updated_views()
        
        # 3. 精度検証
        verify_calculation_accuracy()
        
        logger.info("\n=== 営業日計算対応完了 ===")
        
    except Exception as e:
        logger.error(f"処理失敗: {e}")
        
    logger.info(f"\n実行終了時刻: {datetime.now()}")

if __name__ == "__main__":
    main()