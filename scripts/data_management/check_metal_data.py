"""
M_Metalテーブルの現在のデータを確認
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.database import DatabaseManager

db_manager = DatabaseManager()
db_manager.connect()

with db_manager.get_connection() as conn:
    cursor = conn.cursor()
    cursor.execute("""
        SELECT MetalID, MetalCode, MetalName, ExchangeCode, CurrencyCode
        FROM M_Metal
        ORDER BY MetalID
    """)
    
    print("Current M_Metal data:")
    print("ID | Code     | Name           | Exchange | Currency")
    print("-" * 55)
    for row in cursor:
        print(f"{row[0]:2} | {row[1]:8} | {row[2]:14} | {row[3] or 'NULL':8} | {row[4]}")

db_manager.disconnect()