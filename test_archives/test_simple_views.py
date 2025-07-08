"""
シンプルなビューのテスト
"""
import pyodbc
import pandas as pd
from datetime import datetime

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

def create_views():
    """ビューを作成"""
    print("=== ビュー作成開始 ===")
    
    conn = pyodbc.connect(CONNECTION_STRING)
    cursor = conn.cursor()
    
    try:
        with open('sql/create_simple_price_view.sql', 'r', encoding='utf-8') as f:
            sql_content = f.read()
            # GOステートメントで分割
            sql_statements = sql_content.split('\nGO\n')
            
            # 各ステートメントを実行
            for i, stmt in enumerate(sql_statements):
                if stmt.strip():
                    # コメント行と使用例を除外
                    if stmt.strip().startswith('-- 使用例'):
                        break
                    if not stmt.strip().startswith('--'):
                        print(f"\nステートメント {i+1} を実行中...")
                        try:
                            cursor.execute(stmt)
                            conn.commit()
                            print("   完了")
                        except Exception as e:
                            print(f"   エラー: {e}")
                    
    except Exception as e:
        print(f"エラー: {e}")
        conn.rollback()
        
    conn.close()

def test_simple_view():
    """シンプルビューのテスト"""
    print("\n=== シンプルビューテスト (V_CommodityPriceSimple) ===")
    
    conn = pyodbc.connect(CONNECTION_STRING)
    
    # 最新データの確認
    query = """
    SELECT TOP 15
        ExchangeDisplayName,
        MetalCode,
        GenericNumber,
        GenericDescription,
        TradeDate,
        PriceForAnalysis,
        Volume,
        DataQuality,
        TradingActivity
    FROM V_CommodityPriceSimple
    WHERE TradeDate = (SELECT MAX(TradeDate) FROM V_CommodityPriceSimple)
    ORDER BY ExchangeDisplayName, GenericNumber
    """
    
    df = pd.read_sql(query, conn)
    print("\n最新データ（上位15件）:")
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    print(df.to_string(index=False))
    
    # データ品質サマリー
    query2 = """
    SELECT 
        ExchangeDisplayName,
        DataQuality,
        COUNT(*) as RecordCount,
        AVG(Volume) as AvgVolume
    FROM V_CommodityPriceSimple
    WHERE TradeDate >= DATEADD(day, -3, (SELECT MAX(TradeDate) FROM V_CommodityPriceSimple))
    GROUP BY ExchangeDisplayName, DataQuality
    ORDER BY ExchangeDisplayName, DataQuality
    """
    
    df2 = pd.read_sql(query2, conn)
    print("\n\nデータ品質サマリー（過去3日間）:")
    print(df2.to_string(index=False))
    
    conn.close()

def test_change_view():
    """前日比ビューのテスト"""
    print("\n=== 前日比ビューテスト (V_CommodityPriceWithChange) ===")
    
    conn = pyodbc.connect(CONNECTION_STRING)
    
    # 変化率の大きい銘柄
    query = """
    SELECT TOP 10
        ExchangeDisplayName,
        GenericTicker,
        TradeDate,
        PriceForAnalysis,
        PrevSettlementPrice,
        PriceChange,
        PriceChangePercent,
        Volume,
        TradingActivity
    FROM V_CommodityPriceWithChange
    WHERE TradeDate = (SELECT MAX(TradeDate) FROM V_CommodityPriceWithChange)
        AND PriceChangePercent IS NOT NULL
    ORDER BY ABS(PriceChangePercent) DESC
    """
    
    df = pd.read_sql(query, conn)
    print("\n価格変化率の大きい銘柄（上位10件）:")
    print(df.to_string(index=False))
    
    # 取引所別平均変化率
    query2 = """
    SELECT 
        ExchangeDisplayName,
        COUNT(*) as 銘柄数,
        AVG(PriceChangePercent) as 平均変化率,
        MIN(PriceChangePercent) as 最小変化率,
        MAX(PriceChangePercent) as 最大変化率
    FROM V_CommodityPriceWithChange
    WHERE TradeDate = (SELECT MAX(TradeDate) FROM V_CommodityPriceWithChange)
        AND PriceChangePercent IS NOT NULL
    GROUP BY ExchangeDisplayName
    ORDER BY ExchangeDisplayName
    """
    
    df2 = pd.read_sql(query2, conn)
    print("\n\n取引所別価格変化サマリー:")
    print(df2.to_string(index=False))
    
    conn.close()

def main():
    """メイン処理"""
    print(f"実行時刻: {datetime.now()}")
    
    # ビュー作成
    create_views()
    
    # ビューテスト
    test_simple_view()
    test_change_view()
    
    print("\n=== 完了 ===")

if __name__ == "__main__":
    main()