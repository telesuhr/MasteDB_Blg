"""
COMEX価格データ取得問題の詳細調査
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

def debug_comex_issue():
    """COMEX問題の詳細調査"""
    print("=== COMEX価格データ問題調査 ===")
    
    try:
        conn = pyodbc.connect(CONNECTION_STRING)
        
        # 1. M_GenericFuturesのCOMEX情報確認
        print("\n1. M_GenericFuturesのCOMEX情報:")
        query1 = """
            SELECT GenericID, GenericTicker, MetalID, ExchangeCode, GenericNumber
            FROM M_GenericFutures 
            WHERE ExchangeCode = 'CMX'
            ORDER BY GenericNumber
        """
        df1 = pd.read_sql(query1, conn)
        print(f"CMXレコード数: {len(df1)}")
        if not df1.empty:
            print("CMX先頭5件:")
            print(df1.head().to_string(index=False))
        
        # 2. MetalIDの確認
        print("\n2. MetalID情報:")
        query2 = """
            SELECT DISTINCT MetalID, COUNT(*) as 件数
            FROM M_GenericFutures 
            GROUP BY MetalID
            ORDER BY MetalID
        """
        df2 = pd.read_sql(query2, conn)
        print("MetalID別件数:")
        print(df2.to_string(index=False))
        
        # 3. M_Metal情報確認
        print("\n3. M_Metal情報:")
        query3 = """
            SELECT MetalID, MetalCode, MetalName
            FROM M_Metal
            ORDER BY MetalID
        """
        df3 = pd.read_sql(query3, conn)
        print("金属マスター:")
        print(df3.to_string(index=False))
        
        # 4. T_GenericContractMappingのCMX確認
        print("\n4. T_GenericContractMappingのCMX:")
        query4 = """
            SELECT TOP 10 m.TradeDate, g.GenericTicker, g.ExchangeCode, m.ActualContractID
            FROM T_GenericContractMapping m
            JOIN M_GenericFutures g ON m.GenericID = g.GenericID
            WHERE g.ExchangeCode = 'CMX'
            ORDER BY m.TradeDate DESC, g.GenericNumber
        """
        df4 = pd.read_sql(query4, conn)
        print(f"CMXマッピング件数: {len(df4)}")
        if not df4.empty:
            print("CMXマッピング:")
            print(df4.to_string(index=False))
        
        # 5. M_ActualContractのCMX確認
        print("\n5. M_ActualContractのCMX:")
        query5 = """
            SELECT ActualContractID, ContractTicker, MetalID, ExchangeCode
            FROM M_ActualContract
            WHERE ExchangeCode = 'CMX' OR ContractTicker LIKE 'HG%'
            ORDER BY ActualContractID
        """
        df5 = pd.read_sql(query5, conn)
        print(f"CMX実契約件数: {len(df5)}")
        if not df5.empty:
            print("CMX実契約:")
            print(df5.to_string(index=False))
        
        # 6. T_CommodityPrice_V2のCMX確認
        print("\n6. T_CommodityPrice_V2のCMX:")
        query6 = """
            SELECT TOP 10 p.TradeDate, g.GenericTicker, g.ExchangeCode, p.SettlementPrice
            FROM T_CommodityPrice_V2 p
            JOIN M_GenericFutures g ON p.GenericID = g.GenericID
            WHERE g.ExchangeCode = 'CMX'
            ORDER BY p.TradeDate DESC
        """
        df6 = pd.read_sql(query6, conn)
        print(f"CMX価格データ件数: {len(df6)}")
        if not df6.empty:
            print("CMX価格データ:")
            print(df6.to_string(index=False))
        else:
            print("❌ CMX価格データが存在しません")
        
        # 7. 処理ログの確認（なぜCMXデータが作成されないか）
        print("\n7. CMX処理チェック:")
        
        # CMXのジェネリック先物が存在するか
        if len(df1) > 0:
            print("✅ CMXジェネリック先物は存在")
            
            # マッピングが存在するか
            if len(df4) > 0:
                print("✅ CMXマッピングは存在")
                
                # 価格データが存在しないのはなぜか
                print("❌ 問題: 価格データが作成されていない")
                print("原因候補:")
                print("  1. historical_data_simple.pyで価格データ処理がSHFEのみに限定されている")
                print("  2. CMXのMetalIDが異なる可能性")
                print("  3. CMXの価格データ処理でエラーが発生している")
                
                # CMXのMetalIDチェック
                cmx_metal_id = df1.iloc[0]['MetalID'] if len(df1) > 0 else None
                if cmx_metal_id:
                    print(f"  CMXのMetalID: {cmx_metal_id}")
                    
                    # このMetalIDの金属名確認
                    metal_info = df3[df3['MetalID'] == cmx_metal_id]
                    if not metal_info.empty:
                        print(f"  CMX金属: {metal_info.iloc[0]['MetalName']}")
                    
            else:
                print("❌ CMXマッピングが存在しない")
        else:
            print("❌ CMXジェネリック先物が存在しない")
        
        conn.close()
        
    except Exception as e:
        print(f"調査エラー: {e}")

if __name__ == "__main__":
    debug_comex_issue()