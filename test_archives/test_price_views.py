"""
作成したビューのテストと実行
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
        # 基本ビューの作成
        print("\n1. 基本ビュー (V_CommodityPriceWithAttributes) を作成中...")
        with open('sql/create_price_view_with_attributes.sql', 'r', encoding='utf-8') as f:
            sql_content = f.read()
            # GOステートメントで分割
            sql_statements = sql_content.split('\nGO\n')
            cursor.execute(sql_statements[0])  # CREATE VIEW部分のみ実行
            conn.commit()
            print("   作成完了")
        
        # 拡張ビューの作成
        print("\n2. 拡張ビュー (V_CommodityPriceEnhanced) を作成中...")
        with open('sql/create_enhanced_price_view.sql', 'r', encoding='utf-8') as f:
            sql_content = f.read()
            sql_statements = sql_content.split('\nGO\n')
            cursor.execute(sql_statements[0])  # CREATE VIEW部分のみ実行
            conn.commit()
            print("   作成完了")
            
    except Exception as e:
        print(f"エラー: {e}")
        conn.rollback()
        
    conn.close()

def test_basic_view():
    """基本ビューのテスト"""
    print("\n=== 基本ビューテスト (V_CommodityPriceWithAttributes) ===")
    
    conn = pyodbc.connect(CONNECTION_STRING)
    
    # 最新データの確認
    query = """
    SELECT TOP 10
        ExchangeDisplayName,
        MetalCode,
        GenericNumber,
        GenericTicker,
        ContractMonth,
        DaysToExpiry,
        TradeDate,
        SettlementPrice,
        Volume,
        DataQuality
    FROM V_CommodityPriceWithAttributes
    WHERE TradeDate = (SELECT MAX(TradeDate) FROM V_CommodityPriceWithAttributes)
    ORDER BY ExchangeDisplayName, GenericNumber
    """
    
    df = pd.read_sql(query, conn)
    print("\n最新データ（上位10件）:")
    print(df.to_string(index=False))
    
    conn.close()

def test_enhanced_view():
    """拡張ビューのテスト"""
    print("\n=== 拡張ビューテスト (V_CommodityPriceEnhanced) ===")
    
    conn = pyodbc.connect(CONNECTION_STRING)
    
    # 取引所別サマリー
    query1 = """
    SELECT 
        ExchangeFullName,
        COUNT(DISTINCT GenericTicker) as 銘柄数,
        AVG(CAST(DaysToExpiry AS FLOAT)) as 平均残存日数,
        SUM(Volume) as 総出来高,
        COUNT(CASE WHEN DataCompleteness = 'Complete' THEN 1 END) as 完全データ数
    FROM V_CommodityPriceEnhanced
    WHERE TradeDate = (SELECT MAX(TradeDate) FROM V_CommodityPriceEnhanced)
    GROUP BY ExchangeFullName
    ORDER BY ExchangeFullName
    """
    
    df1 = pd.read_sql(query1, conn)
    print("\n取引所別サマリー:")
    print(df1.to_string(index=False))
    
    # 残存期間が短い契約
    query2 = """
    SELECT TOP 5
        ExchangeFullName,
        GenericTicker,
        ContractMonth,
        DaysToExpiry,
        SettlementPrice,
        Volume,
        TradingActivity
    FROM V_CommodityPriceEnhanced
    WHERE TradeDate = (SELECT MAX(TradeDate) FROM V_CommodityPriceEnhanced)
        AND DaysToExpiry IS NOT NULL
        AND DaysToExpiry > 0
    ORDER BY DaysToExpiry
    """
    
    df2 = pd.read_sql(query2, conn)
    print("\n\n残存期間が短い契約（上位5件）:")
    print(df2.to_string(index=False))
    
    # データ品質分析
    query3 = """
    SELECT 
        ExchangeFullName,
        DataCompleteness,
        COUNT(*) as レコード数
    FROM V_CommodityPriceEnhanced
    WHERE TradeDate >= DATEADD(day, -3, (SELECT MAX(TradeDate) FROM V_CommodityPriceEnhanced))
    GROUP BY ExchangeFullName, DataCompleteness
    ORDER BY ExchangeFullName, DataCompleteness
    """
    
    df3 = pd.read_sql(query3, conn)
    print("\n\nデータ品質分析（過去3日間）:")
    print(df3.to_string(index=False))
    
    conn.close()

def main():
    """メイン処理"""
    print(f"実行時刻: {datetime.now()}")
    
    # ビュー作成
    create_views()
    
    # ビューテスト
    test_basic_view()
    test_enhanced_view()
    
    print("\n=== 完了 ===")

if __name__ == "__main__":
    main()