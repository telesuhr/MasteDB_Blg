"""
残取引可能日数の計算が正しいか確認
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

def check_calculation():
    """計算ロジックの確認"""
    print("=== 残取引可能日数計算確認 ===")
    
    conn = pyodbc.connect(CONNECTION_STRING)
    
    # ビューの計算内容を確認
    query1 = """
    SELECT 
        ExchangeDisplayName,
        GenericTicker,
        GenericNumber,
        TradeDate,
        LastTradeableDate,
        FutureDeliveryDateLast,
        
        -- ビューで計算されている値
        ActualTradingDaysRemaining,
        
        -- 手動計算（最終取引日まで）
        DATEDIFF(day, TradeDate, LastTradeableDate) as ManualDaysToLastTradeable,
        
        -- 手動計算（満期日まで）
        DATEDIFF(day, TradeDate, FutureDeliveryDateLast) as ManualDaysToDelivery,
        
        -- 最終取引日から満期日までの日数
        DATEDIFF(day, LastTradeableDate, FutureDeliveryDateLast) as SettlementDays,
        
        Volume
        
    FROM V_CommodityPriceWithMaturityEx
    WHERE TradeDate = (SELECT MAX(TradeDate) FROM V_CommodityPriceWithMaturityEx)
        AND LastTradeableDate IS NOT NULL
        AND MetalCode LIKE 'CU%'
        AND Volume > 0
    ORDER BY ActualTradingDaysRemaining
    LIMIT 10
    """
    
    # SQL Server用にLIMITをTOPに変更
    query1 = query1.replace("LIMIT 10", "")
    query1 = query1.replace("SELECT", "SELECT TOP 10")
    
    df1 = pd.read_sql(query1, conn)
    print("\n【計算確認（満期が近い順）】")
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    print(df1.to_string(index=False))
    
    # ビューの定義を確認
    query2 = """
    SELECT 
        OBJECT_DEFINITION(OBJECT_ID('V_CommodityPriceWithMaturityEx')) as ViewDefinition
    """
    
    cursor = conn.cursor()
    cursor.execute(query2)
    view_def = cursor.fetchone()[0]
    
    # ActualTradingDaysRemainingの計算部分を抽出
    print("\n\n【ビュー定義内のActualTradingDaysRemaining計算部分】")
    import re
    pattern = r'ActualTradingDaysRemaining.*?END as ActualTradingDaysRemaining'
    match = re.search(pattern, view_def, re.DOTALL)
    if match:
        calc_part = match.group(0)
        # 読みやすく整形
        calc_part = calc_part.replace("    ", "  ")
        print(calc_part)
    
    # 具体例で確認
    print("\n\n【具体例での確認】")
    query3 = """
    SELECT TOP 5
        GenericTicker,
        CONVERT(varchar, TradeDate, 111) as TradeDate,
        CONVERT(varchar, LastTradeableDate, 111) as LastTradeable,
        CONVERT(varchar, FutureDeliveryDateLast, 111) as DeliveryDate,
        ActualTradingDaysRemaining as '計算された残日数',
        CASE 
            WHEN ActualTradingDaysRemaining = DATEDIFF(day, TradeDate, LastTradeableDate) 
            THEN '○ 正しい（最終取引日まで）'
            WHEN ActualTradingDaysRemaining = DATEDIFF(day, TradeDate, FutureDeliveryDateLast)
            THEN '× 間違い（満期日まで）'
            ELSE '? 不明'
        END as '計算確認'
    FROM V_CommodityPriceWithMaturityEx
    WHERE TradeDate = (SELECT MAX(TradeDate) FROM V_CommodityPriceWithMaturityEx)
        AND LastTradeableDate IS NOT NULL
        AND Volume > 0
    ORDER BY ActualTradingDaysRemaining
    """
    
    df3 = pd.read_sql(query3, conn)
    print(df3.to_string(index=False))
    
    conn.close()

def main():
    """メイン処理"""
    print(f"実行時刻: {datetime.now()}")
    check_calculation()
    print("\n=== 完了 ===")

if __name__ == "__main__":
    main()