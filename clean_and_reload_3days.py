"""
既存データを削除して3営業日分を再ロード
"""
import pyodbc
from datetime import datetime
import subprocess
import sys

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

def clean_existing_data():
    """既存データのクリーンアップ"""
    print("=== 既存データのクリーンアップ ===")
    
    try:
        conn = pyodbc.connect(CONNECTION_STRING)
        cursor = conn.cursor()
        
        # T_CommodityPrice_V2の全データを削除
        print("T_CommodityPrice_V2のデータを削除中...")
        cursor.execute("DELETE FROM T_CommodityPrice_V2")
        deleted_count = cursor.rowcount
        print(f"  {deleted_count}件削除しました")
        
        # T_GenericContractMappingも削除（関連データ）
        print("T_GenericContractMappingのデータを削除中...")
        cursor.execute("DELETE FROM T_GenericContractMapping")
        mapping_count = cursor.rowcount
        print(f"  {mapping_count}件削除しました")
        
        # M_ActualContractも削除（関連マスタ）
        print("M_ActualContractのデータを削除中...")
        cursor.execute("DELETE FROM M_ActualContract")
        contract_count = cursor.rowcount
        print(f"  {contract_count}件削除しました")
        
        conn.commit()
        conn.close()
        
        print(f"\n合計: {deleted_count + mapping_count + contract_count}件のデータを削除しました")
        return True
        
    except Exception as e:
        print(f"エラー: {e}")
        return False

def reload_3days_data():
    """3営業日分のデータを再ロード"""
    print("\n=== 3営業日分のデータ再ロード ===")
    
    try:
        # final_multi_exchange_loader.pyを実行
        print("最終版データローダーを実行中...")
        result = subprocess.run(
            [sys.executable, "final_multi_exchange_loader.py"],
            capture_output=True,
            text=True
        )
        
        if result.returncode == 0:
            print("データロード成功")
            # 成功時の出力から重要な部分を抽出
            output_lines = result.stdout.split('\n')
            for line in output_lines:
                if '合計:' in line or '件' in line or 'Settlement:' in line:
                    print(f"  {line.strip()}")
            return True
        else:
            print("データロード失敗")
            print(f"エラー: {result.stderr}")
            return False
            
    except Exception as e:
        print(f"実行エラー: {e}")
        return False

def verify_loaded_data():
    """ロードされたデータの確認"""
    print("\n=== ロードデータ確認 ===")
    
    try:
        conn = pyodbc.connect(CONNECTION_STRING)
        cursor = conn.cursor()
        
        # データ件数確認
        cursor.execute("""
            SELECT 
                g.ExchangeCode,
                COUNT(DISTINCT p.TradeDate) as 営業日数,
                COUNT(DISTINCT p.GenericID) as 銘柄数,
                COUNT(*) as レコード数,
                MIN(p.TradeDate) as 開始日,
                MAX(p.TradeDate) as 終了日
            FROM T_CommodityPrice_V2 p
            JOIN M_GenericFutures g ON p.GenericID = g.GenericID
            WHERE p.DataType = 'Generic'
            GROUP BY g.ExchangeCode
            ORDER BY g.ExchangeCode
        """)
        
        print("\n取引所別サマリー:")
        print(f"{'取引所':<10} {'営業日数':<10} {'銘柄数':<10} {'レコード数':<12} {'期間':<20}")
        print("-" * 65)
        
        total_records = 0
        for row in cursor.fetchall():
            exchange = 'COMEX' if row[0] == 'CMX' else row[0]
            period = f"{row[4]} - {row[5]}"
            print(f"{exchange:<10} {row[1]:<10} {row[2]:<10} {row[3]:<12} {period:<20}")
            total_records += row[3]
        
        print("-" * 65)
        print(f"{'合計':<10} {'':<10} {'':<10} {total_records:<12}")
        
        # 最新データのタイムスタンプ確認
        cursor.execute("""
            SELECT TOP 1 LastUpdated 
            FROM T_CommodityPrice_V2 
            ORDER BY LastUpdated DESC
        """)
        latest_update = cursor.fetchone()[0]
        print(f"\n最終更新時刻: {latest_update} (JST)")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"確認エラー: {e}")
        return False

def main():
    """メイン処理"""
    print("=== 既存データ削除と3営業日分再ロード ===")
    print(f"実行開始時刻: {datetime.now()}")
    
    # 1. 既存データのクリーンアップ
    if not clean_existing_data():
        print("クリーンアップ失敗")
        return False
    
    # 2. 3営業日分のデータ再ロード
    if not reload_3days_data():
        print("データロード失敗")
        return False
    
    # 3. ロードデータの確認
    if not verify_loaded_data():
        print("データ確認失敗")
        return False
    
    print(f"\n実行完了時刻: {datetime.now()}")
    print("=== 処理完了 ===")
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)