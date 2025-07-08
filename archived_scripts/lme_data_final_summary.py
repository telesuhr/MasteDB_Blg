"""
LME特殊データの最終確認（シンプル版）
"""
import pyodbc
import pandas as pd
from datetime import datetime

def create_summary():
    conn_str = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=jcz.database.windows.net;"
        "DATABASE=JCL;"
        "UID=TKJCZ01;"
        "PWD=P@ssw0rdmbkazuresql;"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )
    
    conn = pyodbc.connect(conn_str)
    
    # スプレッド計算確認
    query = """
    SELECT TOP 5
        c.TradeDate,
        cash.LastPrice as CashPrice,
        m3.LastPrice as M3Price,
        spread.LastPrice as SpreadPrice,
        m3.LastPrice - cash.LastPrice as CalcSpread
    FROM (
        SELECT DISTINCT TradeDate 
        FROM T_CommodityPrice_V2 p
        INNER JOIN M_GenericFutures g ON p.GenericID = g.GenericID
        WHERE g.GenericTicker IN ('LMCADY Comdty', 'LMCADS03 Comdty', 'LMCADS 0003 Comdty')
    ) c
    LEFT JOIN (
        SELECT p.TradeDate, p.LastPrice
        FROM T_CommodityPrice_V2 p
        INNER JOIN M_GenericFutures g ON p.GenericID = g.GenericID
        WHERE g.GenericTicker = 'LMCADY Comdty'
    ) cash ON c.TradeDate = cash.TradeDate
    LEFT JOIN (
        SELECT p.TradeDate, p.LastPrice
        FROM T_CommodityPrice_V2 p
        INNER JOIN M_GenericFutures g ON p.GenericID = g.GenericID
        WHERE g.GenericTicker = 'LMCADS03 Comdty'
    ) m3 ON c.TradeDate = m3.TradeDate
    LEFT JOIN (
        SELECT p.TradeDate, p.LastPrice
        FROM T_CommodityPrice_V2 p
        INNER JOIN M_GenericFutures g ON p.GenericID = g.GenericID
        WHERE g.GenericTicker = 'LMCADS 0003 Comdty'
    ) spread ON c.TradeDate = spread.TradeDate
    WHERE cash.LastPrice IS NOT NULL 
        AND m3.LastPrice IS NOT NULL 
        AND spread.LastPrice IS NOT NULL
    ORDER BY c.TradeDate DESC
    """
    
    df = pd.read_sql(query, conn)
    
    print("\n【LME特殊データ スプレッド計算検証】")
    print("=" * 80)
    print("取引日     Cash価格   3M価格    Spread   計算値(3M-Cash)")
    print("-" * 80)
    
    for _, row in df.iterrows():
        print(f"{row['TradeDate']}  {row['CashPrice']:8.1f}  {row['M3Price']:8.1f}  {row['SpreadPrice']:7.1f}  {row['CalcSpread']:7.1f}")
    
    if not df.empty:
        latest = df.iloc[0]
        print("\n【最新市場状況】")
        if latest['SpreadPrice'] > 0:
            print(f"コンタンゴ: 3M先物が現物より${latest['SpreadPrice']:.2f}高い")
            print("→ 通常の市場状態（保管コスト・金利を反映）")
        else:
            print(f"バックワーデーション: 現物が3M先物より${-latest['SpreadPrice']:.2f}高い")
            print("→ 供給逼迫の可能性")
    
    print("\n【データ取得完了】")
    print("✓ LMCADY Comdty (LME Copper Cash) - 現物価格")
    print("✓ LMCADS03 Comdty (LME Copper 3M Outright) - 3ヶ月先物")
    print("✓ LMCADS 0003 Comdty (LME Copper Cash/3M Spread) - スプレッド")
    
    conn.close()

if __name__ == "__main__":
    create_summary()