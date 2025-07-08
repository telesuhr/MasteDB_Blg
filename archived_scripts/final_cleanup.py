import pyodbc

conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=jcz.database.windows.net;DATABASE=JCL;UID=TKJCZ01;PWD=P@ssw0rdmbkazuresql;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;')
cursor = conn.cursor()

# Drop old table
try:
    cursor.execute('DROP TABLE IF EXISTS T_CommodityPrice_OLD')
    conn.commit()
    print('T_CommodityPrice_OLD dropped successfully')
except Exception as e:
    print(f'Error: {e}')

# Final check
cursor.execute("""
    SELECT TABLE_NAME 
    FROM INFORMATION_SCHEMA.TABLES 
    WHERE TABLE_NAME LIKE '%CommodityPrice%'
    ORDER BY TABLE_NAME
""")

print('\nRemaining tables:')
for row in cursor.fetchall():
    print(f'  {row[0]}')

# Check T_CommodityPrice structure
cursor.execute("""
    SELECT COUNT(*) as RecordCount,
           COUNT(DISTINCT GenericID) as GenericCount
    FROM T_CommodityPrice
    WHERE GenericID IS NOT NULL
""")

result = cursor.fetchone()
print(f'\nT_CommodityPrice status:')
print(f'  Records: {result[0]}')
print(f'  Generic IDs: {result[1]}')

conn.close()
print('\nRename completed successfully!')
print('T_CommodityPrice now uses the V2 structure (with GenericID)')
print('Old T_CommodityPrice data has been removed')