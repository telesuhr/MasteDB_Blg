"""
LME Cash価格のティッカー問題を確認
"""
import pyodbc
import pandas as pd

def check_cash_ticker():
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
    
    # 1. LMCADY関連のティッカーを全て確認
    query1 = """
    SELECT GenericID, GenericTicker, Description, GenericNumber
    FROM M_GenericFutures
    WHERE GenericTicker LIKE '%LMCADY%' OR GenericTicker LIKE '%Index%'
    ORDER BY GenericTicker
    """
    
    df1 = pd.read_sql(query1, conn)
    print("【LMCADY関連のマスターデータ】")
    print(df1.to_string(index=False))
    
    # 2. bloomberg_config.pyでの定義確認
    print("\n【bloomberg_config.pyでの定義】")
    print("'LMCADY Index' がLME_COPPER_PRICESのsecuritiesリストに含まれています")
    
    # 3. 実際のデータ確認
    query2 = """
    SELECT TOP 10
        g.GenericTicker,
        p.TradeDate,
        p.LastPrice,
        p.SettlementPrice
    FROM T_CommodityPrice_V2 p
    INNER JOIN M_GenericFutures g ON p.GenericID = g.GenericID
    WHERE g.GenericTicker = 'LMCADY Index'
    ORDER BY p.TradeDate DESC
    """
    
    df2 = pd.read_sql(query2, conn)
    print("\n【LMCADY Indexの価格データ】")
    if not df2.empty:
        print(df2.to_string(index=False))
    else:
        print("データなし")
        
        # 別名で確認
        query3 = """
        SELECT TOP 10
            g.GenericTicker,
            p.TradeDate,
            p.LastPrice,
            p.SettlementPrice
        FROM T_CommodityPrice_V2 p
        INNER JOIN M_GenericFutures g ON p.GenericID = g.GenericID
        WHERE g.Description LIKE '%Cash%' AND g.ExchangeCode = 'LME'
        ORDER BY p.TradeDate DESC
        """
        
        df3 = pd.read_sql(query3, conn)
        print("\n【LME Cash関連の価格データ】")
        if not df3.empty:
            print(df3.to_string(index=False))
        else:
            print("Cashデータなし")
    
    # 4. データロード時のマッピング確認
    print("\n【推測される問題】")
    print("1. bloomberg_config.pyでは 'LMCADY Index' として定義")
    print("2. M_GenericFuturesでは 'LMCADY Comdty' として登録")
    print("3. このミスマッチによりデータがロードされていない可能性")
    
    # 5. 修正案
    print("\n【修正案】")
    print("bloomberg_config.pyのティッカーを 'LMCADY Comdty' に変更")
    print("または、M_GenericFuturesのティッカーを 'LMCADY Index' に変更")
    
    conn.close()

if __name__ == "__main__":
    check_cash_ticker()