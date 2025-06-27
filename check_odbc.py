#!/usr/bin/env python3
"""
ODBC ドライバー確認スクリプト
実行環境でこのスクリプトを実行して、利用可能なODBCドライバーを確認してください
"""
import pyodbc
import sys

def check_odbc_drivers():
    """利用可能なODBCドライバーを確認"""
    print("=== 利用可能なODBCドライバー一覧 ===")
    drivers = pyodbc.drivers()
    
    if not drivers:
        print("❌ ODBCドライバーが見つかりません")
        return
    
    print(f"✅ {len(drivers)}個のODBCドライバーが見つかりました:")
    for i, driver in enumerate(drivers, 1):
        print(f"  {i}. {driver}")
        
    # SQL Server関連のドライバーをチェック
    sql_server_drivers = [d for d in drivers if 'SQL Server' in d]
    
    if sql_server_drivers:
        print(f"\n=== SQL Server関連ドライバー ===")
        for driver in sql_server_drivers:
            print(f"✅ {driver}")
    else:
        print(f"\n❌ SQL Server関連のドライバーが見つかりません")
        
    return drivers

def test_connection_strings(drivers):
    """異なる接続文字列でテスト"""
    print(f"\n=== 接続テスト ===")
    
    # Azure SQL Database接続情報
    server = 'jcz.database.windows.net'
    database = 'JCL'
    username = 'TKJCZ01'
    password = 'P@ssw0rdmbkazuresql'
    
    # SQL Server関連のドライバーでテスト
    sql_server_drivers = [d for d in drivers if 'SQL Server' in d]
    
    if not sql_server_drivers:
        print("❌ テスト可能なSQL Serverドライバーがありません")
        return
        
    for driver in sql_server_drivers[:3]:  # 上位3つをテスト
        print(f"\n--- {driver} でテスト ---")
        
        conn_str = (
            f"DRIVER={{{driver}}};"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"UID={username};"
            f"PWD={password};"
            f"Encrypt=yes;"
            f"TrustServerCertificate=no;"
            f"Connection Timeout=30;"
        )
        
        try:
            conn = pyodbc.connect(conn_str)
            print(f"✅ {driver}: 接続成功！")
            conn.close()
            return driver  # 成功したドライバーを返す
        except Exception as e:
            print(f"❌ {driver}: 接続失敗 - {str(e)[:100]}...")
            
    return None

if __name__ == "__main__":
    print("Python バージョン:", sys.version)
    print("pyodbc バージョン:", pyodbc.version)
    print()
    
    try:
        drivers = check_odbc_drivers()
        if drivers:
            successful_driver = test_connection_strings(drivers)
            if successful_driver:
                print(f"\n🎉 推奨ドライバー: {successful_driver}")
                print("\nconfig/database_config.py を以下のように更新してください:")
                print(f"    'driver': '{{{successful_driver}}}',")
        
    except Exception as e:
        print(f"❌ エラーが発生しました: {e}")
        import traceback
        traceback.print_exc()