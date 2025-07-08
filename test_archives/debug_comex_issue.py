"""
COMEX問題のデバッグ
M_GenericFuturesテーブルでCOMEXデータを確認
"""
import pyodbc
import pandas as pd

# データベース接続文字列（Azure SQL Database）
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

def debug_comex_issue():
    """COMEX問題をデバッグ"""
    print("=== COMEX問題デバッグ開始 ===")
    
    try:
        conn = pyodbc.connect(CONNECTION_STRING)
        
        # 1. COMEXデータの存在確認
        print("\n1. M_GenericFuturesでのCOMEXデータ確認")
        comex_query = """
            SELECT GenericID, GenericTicker, ExchangeCode, GenericNumber, MetalID
            FROM M_GenericFutures 
            WHERE ExchangeCode = 'COMEX'
            ORDER BY GenericNumber
        """
        comex_df = pd.read_sql(comex_query, conn)
        print(f"COMEXデータ件数: {len(comex_df)}件")
        print(comex_df.head(10).to_string(index=False))
        
        # 2. 正確なExchangeCode確認
        print("\n2. ExchangeCodeの実際の値確認")
        exchange_query = """
            SELECT DISTINCT ExchangeCode, COUNT(*) as 件数
            FROM M_GenericFutures
            GROUP BY ExchangeCode
            ORDER BY ExchangeCode
        """
        exchange_df = pd.read_sql(exchange_query, conn)
        print("実際のExchangeCode一覧:")
        print(exchange_df.to_string(index=False))
        
        # 3. HGティッカーの検索
        print("\n3. HGで始まるティッカー検索")
        hg_query = """
            SELECT GenericID, GenericTicker, ExchangeCode, GenericNumber
            FROM M_GenericFutures 
            WHERE GenericTicker LIKE 'HG%'
            ORDER BY GenericNumber
        """
        hg_df = pd.read_sql(hg_query, conn)
        print(f"HGティッカー件数: {len(hg_df)}件")
        if not hg_df.empty:
            print(hg_df.head(10).to_string(index=False))
        
        # 4. CMXでの検索
        print("\n4. CMXでの検索（COMEXの代わり）")
        cmx_query = """
            SELECT GenericID, GenericTicker, ExchangeCode, GenericNumber, MetalID
            FROM M_GenericFutures 
            WHERE ExchangeCode = 'CMX'
            ORDER BY GenericNumber
        """
        cmx_df = pd.read_sql(cmx_query, conn)
        print(f"CMXデータ件数: {len(cmx_df)}件")
        if not cmx_df.empty:
            print(cmx_df.head(10).to_string(index=False))
        
        # 5. 検索パターンの確認
        print("\n5. 問題の特定")
        if len(cmx_df) > 0:
            print("✅ 解決策: ExchangeCodeは'CMX'が正しい（'COMEX'ではない）")
            print("日次更新プログラムでExchangeCode修正が必要")
        elif len(hg_df) > 0:
            print("✅ HGティッカーは存在するが、ExchangeCodeが異なる")
            print(f"実際のExchangeCode: {hg_df.iloc[0]['ExchangeCode']}")
        else:
            print("❌ HGティッカーが存在しない - M_GenericFuturesへの追加が必要")
        
        conn.close()
        
    except Exception as e:
        print(f"デバッグエラー: {e}")

if __name__ == "__main__":
    debug_comex_issue()