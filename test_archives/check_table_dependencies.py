"""
Check dependencies between tables and views, and identify potential duplicates
"""
import pyodbc
import sys
import os

# Add project paths
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), 'src'))

from config.database_config import get_connection_string

# Get connection
conn_str = get_connection_string()
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

print('=== Table/View Dependencies ===\n')

# Check view definitions
query = """
SELECT 
    o.name AS ViewName,
    m.definition
FROM sys.sql_modules m
INNER JOIN sys.objects o ON m.object_id = o.object_id
WHERE o.type IN ('V') -- Views
ORDER BY o.name
"""

cursor.execute(query)
views = cursor.fetchall()

for view_name, definition in views:
    print(f"View: {view_name}")
    # Extract table references from the definition
    tables_referenced = []
    for line in definition.split('\n'):
        if 'FROM' in line or 'JOIN' in line:
            # Simple parsing to find table names
            words = line.split()
            for i, word in enumerate(words):
                if word.upper() in ['FROM', 'JOIN'] and i+1 < len(words):
                    table_name = words[i+1].strip().replace('[', '').replace(']', '').replace(',', '')
                    if table_name.startswith('T_') or table_name.startswith('M_'):
                        tables_referenced.append(table_name)
    
    if tables_referenced:
        print(f"  References: {', '.join(set(tables_referenced))}")
    print()

# Check for potential duplicate tables based on structure
print('\n=== Potential Duplicate/Superseded Tables ===\n')

# Compare T_CommodityPrice and T_CommodityPrice_V2
print("1. T_CommodityPrice vs T_CommodityPrice_V2:")
print("   - Both store commodity price data")
print("   - T_CommodityPrice: Original table with TenorTypeID and SpecificTenorDate")
print("   - T_CommodityPrice_V2: New design with GenericID and ActualContractID")
print("   - Status: T_CommodityPrice_V2 appears to be the new version")
print()

# Check for tables with similar naming patterns
cursor.execute("""
SELECT TABLE_NAME 
FROM INFORMATION_SCHEMA.TABLES 
WHERE TABLE_TYPE = 'BASE TABLE' 
AND TABLE_NAME LIKE 'T_%'
ORDER BY TABLE_NAME
""")

tables = [row[0] for row in cursor.fetchall()]

# Look for version suffixes or temp/test patterns
print("2. Tables with version/test indicators:")
for table in tables:
    if any(pattern in table.lower() for pattern in ['_v2', '_temp', '_test', '_old', '_backup']):
        print(f"   - {table}")

# Check column overlap between similar tables
print("\n3. Column overlap analysis:")

# Compare CommodityPrice tables
cursor.execute("""
SELECT COLUMN_NAME, DATA_TYPE 
FROM INFORMATION_SCHEMA.COLUMNS 
WHERE TABLE_NAME = 'T_CommodityPrice'
ORDER BY ORDINAL_POSITION
""")
cp1_columns = {row[0]: row[1] for row in cursor.fetchall()}

cursor.execute("""
SELECT COLUMN_NAME, DATA_TYPE 
FROM INFORMATION_SCHEMA.COLUMNS 
WHERE TABLE_NAME = 'T_CommodityPrice_V2'
ORDER BY ORDINAL_POSITION
""")
cp2_columns = {row[0]: row[1] for row in cursor.fetchall()}

common_columns = set(cp1_columns.keys()) & set(cp2_columns.keys())
print(f"   T_CommodityPrice and T_CommodityPrice_V2 share {len(common_columns)} columns:")
print(f"   Common: {', '.join(sorted(common_columns))}")
print(f"   Only in T_CommodityPrice: {', '.join(sorted(set(cp1_columns.keys()) - set(cp2_columns.keys())))}")
print(f"   Only in T_CommodityPrice_V2: {', '.join(sorted(set(cp2_columns.keys()) - set(cp1_columns.keys())))}")

# Check for tables that might be test/temporary based on data patterns
print("\n=== Tables with Suspicious Data Patterns ===\n")

for table, date_col in [('T_CommodityPrice', 'TradeDate'), ('T_CommodityPrice_V2', 'TradeDate')]:
    cursor.execute(f"""
    SELECT 
        COUNT(*) as total_rows,
        COUNT(DISTINCT {date_col}) as unique_dates,
        DATEDIFF(day, MIN({date_col}), MAX({date_col})) as date_span_days
    FROM {table}
    """)
    result = cursor.fetchone()
    if result:
        print(f"{table}:")
        print(f"  Total rows: {result[0]}")
        print(f"  Unique dates: {result[1]}")
        print(f"  Date span: {result[2]} days")
        print(f"  Average rows per date: {result[0]/result[1] if result[1] > 0 else 0:.1f}")
        print()

conn.close()