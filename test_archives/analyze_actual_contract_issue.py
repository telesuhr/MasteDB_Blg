"""
ActualContractID重複問題の分析
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

def analyze_actual_contract_issue():
    """ActualContractID重複問題の分析"""
    print("=== ActualContractID重複問題分析 ===")
    
    try:
        conn = pyodbc.connect(CONNECTION_STRING)
        
        # 1. 重複状況の確認
        print("\n1. 同一日付・取引所で複数GenericIDが同じActualContractIDを持つケース:")
        query1 = """
            SELECT 
                p.TradeDate,
                g.ExchangeCode,
                p.ActualContractID,
                a.ContractTicker,
                COUNT(DISTINCT p.GenericID) as GenericID数,
                STRING_AGG(CAST(p.GenericID as VARCHAR), ', ') as GenericIDリスト
            FROM T_CommodityPrice_V2 p
            JOIN M_GenericFutures g ON p.GenericID = g.GenericID
            JOIN M_ActualContract a ON p.ActualContractID = a.ActualContractID
            GROUP BY p.TradeDate, g.ExchangeCode, p.ActualContractID, a.ContractTicker
            HAVING COUNT(DISTINCT p.GenericID) > 1
            ORDER BY p.TradeDate, g.ExchangeCode, p.ActualContractID
        """
        df1 = pd.read_sql(query1, conn)
        print("重複件数:", len(df1))
        if not df1.empty:
            print(df1.head(20).to_string(index=False))
        
        # 2. 正しいマッピングの例
        print("\n2. 正しいマッピングの例（各GenericIDが異なるActualContractIDを持つべき）:")
        query2 = """
            SELECT TOP 10
                g.GenericTicker,
                g.GenericNumber,
                a.ContractTicker,
                a.ContractMonthCode,
                a.ContractYear
            FROM M_GenericFutures g
            LEFT JOIN T_GenericContractMapping m ON g.GenericID = m.GenericID 
                AND m.TradeDate = '2025-07-07'
            LEFT JOIN M_ActualContract a ON m.ActualContractID = a.ActualContractID
            WHERE g.ExchangeCode = 'CMX'
            ORDER BY g.GenericNumber
        """
        df2 = pd.read_sql(query2, conn)
        print(df2.to_string(index=False))
        
        # 3. 期待される正しいマッピング
        print("\n3. 期待される正しいマッピング（例）:")
        print("HG1 Comdty (1番限) -> HGN5 (2025年7月限)")
        print("HG2 Comdty (2番限) -> HGU5 (2025年9月限)")
        print("HG3 Comdty (3番限) -> HGZ5 (2025年12月限)")
        print("HG4 Comdty (4番限) -> HGH6 (2026年3月限)")
        print("...")
        
        # 4. 実契約の種類確認
        print("\n4. 実際に作成されたM_ActualContract:")
        query4 = """
            SELECT ActualContractID, ContractTicker, ExchangeCode, ContractMonthCode, ContractYear
            FROM M_ActualContract
            WHERE ExchangeCode IN ('CMX', 'LME', 'SHFE')
            ORDER BY ExchangeCode, ActualContractID
        """
        df4 = pd.read_sql(query4, conn)
        print(df4.to_string(index=False))
        
        conn.close()
        
    except Exception as e:
        print(f"分析エラー: {e}")

if __name__ == "__main__":
    analyze_actual_contract_issue()