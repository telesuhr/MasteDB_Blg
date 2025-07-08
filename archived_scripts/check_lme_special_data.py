"""
LME特殊データの確認
"""
import pyodbc
import pandas as pd

def check_data():
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
    
    # 1. 現在のLMEマスターデータ確認
    query1 = """
    SELECT GenericID, GenericTicker, GenericNumber, Description
    FROM M_GenericFutures
    WHERE ExchangeCode = 'LME'
        AND (GenericTicker LIKE '%CAD%' OR GenericTicker LIKE '%Index%' OR GenericNumber IN (0, 3, -1))
    ORDER BY GenericNumber, GenericTicker
    """
    
    df1 = pd.read_sql(query1, conn)
    print("【LME特殊ティッカーのマスターデータ】")
    print(df1.to_string(index=False))
    
    # 2. 価格データの存在確認
    query2 = """
    SELECT 
        g.GenericTicker,
        COUNT(DISTINCT p.TradeDate) as DataDays,
        MIN(p.TradeDate) as FirstDate,
        MAX(p.TradeDate) as LatestDate,
        MAX(p.LastPrice) as LatestPrice,
        MAX(p.SettlementPrice) as LatestSettle
    FROM T_CommodityPrice_V2 p
    INNER JOIN M_GenericFutures g ON p.GenericID = g.GenericID
    WHERE g.ExchangeCode = 'LME'
        AND g.GenericTicker IN ('LMCADY Index', 'CAD TT00 Comdty', 'LMCADS03 Comdty', 'LMCADS 0003 Comdty')
    GROUP BY g.GenericTicker
    ORDER BY g.GenericTicker
    """
    
    df2 = pd.read_sql(query2, conn)
    print("\n\n【LME特殊ティッカーの価格データ】")
    if not df2.empty:
        print(df2.to_string(index=False))
    else:
        print("データなし")
    
    # 3. 設定ファイルの確認
    print("\n\n【bloomberg_config.pyの設定】")
    print("LME_COPPER_PRICES securities に以下が含まれています：")
    print("- 'LMCADY Index'      # LME銅 現物価格")
    print("- 'CAD TT00 Comdty'   # LME銅 トムネクスト")
    print("- 'LMCADS03 Comdty'   # LME銅 3ヶ月先物価格")
    print("- 'LMCADS 0003 Comdty' # LME銅 Cash/3mスプレッド")
    
    # 4. 最近のLMEデータロード状況
    query3 = """
    SELECT 
        g.GenericTicker,
        p.TradeDate,
        p.LastPrice,
        p.SettlementPrice,
        p.Volume
    FROM T_CommodityPrice_V2 p
    INNER JOIN M_GenericFutures g ON p.GenericID = g.GenericID
    WHERE g.GenericTicker IN ('LMCADY Index', 'CAD TT00 Comdty', 'LMCADS03 Comdty', 'LMCADS 0003 Comdty', 'LP1 Comdty')
        AND p.TradeDate >= DATEADD(day, -7, GETDATE())
    ORDER BY g.GenericTicker, p.TradeDate DESC
    """
    
    df3 = pd.read_sql(query3, conn)
    print("\n\n【過去7日間のデータ】")
    if not df3.empty:
        print(df3.to_string(index=False))
    else:
        print("過去7日間のデータなし")
    
    conn.close()

if __name__ == "__main__":
    check_data()