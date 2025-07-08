import pyodbc

conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=jcz.database.windows.net;DATABASE=JCL;UID=TKJCZ01;PWD=P@ssw0rdmbkazuresql;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;')
cursor = conn.cursor()

# Check T_CommodityPrice_V2 columns
cursor.execute("""
SELECT COLUMN_NAME, DATA_TYPE 
FROM INFORMATION_SCHEMA.COLUMNS 
WHERE TABLE_NAME = 'T_CommodityPrice_V2'
ORDER BY ORDINAL_POSITION
""")
print('T_CommodityPrice_V2 columns:')
for row in cursor.fetchall():
    print(f'  {row[0]}: {row[1]}')

# Check M_GenericFutures columns
cursor.execute("""
SELECT COLUMN_NAME, DATA_TYPE 
FROM INFORMATION_SCHEMA.COLUMNS 
WHERE TABLE_NAME = 'M_GenericFutures'
ORDER BY ORDINAL_POSITION
""")
print('\nM_GenericFutures columns:')
for row in cursor.fetchall():
    print(f'  {row[0]}: {row[1]}')

# Check M_Metal columns
cursor.execute("""
SELECT COLUMN_NAME, DATA_TYPE 
FROM INFORMATION_SCHEMA.COLUMNS 
WHERE TABLE_NAME = 'M_Metal'
ORDER BY ORDINAL_POSITION
""")
print('\nM_Metal columns:')
for row in cursor.fetchall():
    print(f'  {row[0]}: {row[1]}')

conn.close()