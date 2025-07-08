"""
T_CommodityPriceテーブルのデータ確認
"""
import pyodbc
import pandas as pd

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

def verify_data():
    """データ検証"""
    print("=== T_CommodityPriceテーブル確認 ===")
    
    conn = pyodbc.connect(CONNECTION_STRING)
    
    # 1. 最近のデータ数確認
    print("\n1. 最近のデータ数:")
    query = """
        SELECT 
            TradeDate,
            COUNT(*) as レコード数
        FROM T_CommodityPrice
        WHERE TradeDate >= DATEADD(day, -10, GETDATE())
        GROUP BY TradeDate
        ORDER BY TradeDate DESC
    """
    df = pd.read_sql(query, conn)
    print(df.to_string(index=False))
    
    # 2. テナータイプ別サマリー
    print("\n2. テナータイプ別サマリー（最新日）:")
    query = """
        SELECT TOP 20
            t.Name as テナータイプ,
            COUNT(*) as レコード数,
            MIN(p.SettlementPrice) as 最小価格,
            MAX(p.SettlementPrice) as 最大価格
        FROM T_CommodityPrice p
        JOIN M_TenorType t ON p.TenorTypeID = t.TenorTypeID
        WHERE p.TradeDate = (SELECT MAX(TradeDate) FROM T_CommodityPrice)
        GROUP BY t.TenorTypeID, t.Name
        ORDER BY t.TenorTypeID
    """
    df = pd.read_sql(query, conn)
    print(df.to_string(index=False))
    
    # 3. サンプルデータ
    print("\n3. サンプルデータ（Generic先物）:")
    query = """
        SELECT TOP 10
            p.TradeDate,
            m.Name as 金属,
            t.Name as テナー,
            p.SettlementPrice,
            p.Volume,
            p.OpenInterest
        FROM T_CommodityPrice p
        JOIN M_Metal m ON p.MetalID = m.MetalID
        JOIN M_TenorType t ON p.TenorTypeID = t.TenorTypeID
        WHERE p.TradeDate = (SELECT MAX(TradeDate) FROM T_CommodityPrice)
            AND t.Name LIKE 'Generic%'
        ORDER BY t.TenorTypeID
    """
    df = pd.read_sql(query, conn)
    print(df.to_string(index=False))
    
    conn.close()
    print("\n=== 確認完了 ===")

if __name__ == "__main__":
    verify_data()